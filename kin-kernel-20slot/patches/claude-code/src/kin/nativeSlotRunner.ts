/**
 * NativeSlotRunner — per-slot concurrent host loop.
 *
 * stdin reader never waits on a job. Each slot owns a FileStateCache and the
 * in-flight QueryEngine for its current job. Client tools park the same
 * generator until kin_tool_result arrives.
 */
import { createInterface } from 'node:readline'
import { randomUUID } from 'node:crypto'
import { z } from 'zod/v4'
import type { ContentBlockParam } from '@anthropic-ai/sdk/resources/index.mjs'
import { QueryEngine } from '../QueryEngine.js'
import type { Tool } from '../Tool.js'
import type { Message } from '../types/message.js'
import {
  createAssistantMessage,
  createUserMessage,
} from '../utils/messages.js'
import { leftoverFromSystemPrompt, getSystemLayout, getKinTimezone } from './systemLayout.js'
import {
  KIN_CAPABILITIES,
  KIN_PROTOCOL_VERSION,
  parseStdinLine,
  slotId,
  writeStdout,
  type KinStdin,
} from './stdioProtocol.js'
import { createFileStateCacheWithSizeLimit } from '../utils/fileStateCache.js'
import type { FileStateCache } from '../utils/fileStateCache.js'
import type { ThinkingConfig } from '../utils/thinking.js'

export type NativeHost = {
  getAppState: () => unknown
  setAppState: (f: (prev: unknown) => unknown) => void
  commands: unknown[]
  tools: unknown[]
  agents: unknown[]
  cwd: () => string
  canUseTool: (...args: unknown[]) => Promise<unknown>
  options: {
    verbose?: boolean
    thinkingConfig?: unknown
    maxTurns?: number
    systemPrompt?: string
    appendSystemPrompt?: string
    userSpecifiedModel?: string
    fallbackModel?: string
    replayUserMessages?: boolean
  }
}

type Phase = 'idle' | 'running' | 'parked'

type ToolWaiter = {
  resolve: (content: unknown) => void
  reject: (err: Error) => void
}

type SlotState = {
  id: string
  phase: Phase
  jobId?: string
  abort?: AbortController
  engine?: QueryEngine
  cache: FileStateCache
  toolWaiters: Map<string, ToolWaiter>
  parkedIds?: string[]
}

export function nativeSlotCount(): number {
  const raw = process.env.CLAUDE_CODE_KIN_NATIVE_SLOTS
  const n = raw ? Number(raw) : 0
  if (!Number.isFinite(n) || n <= 0) return 0
  return Math.min(20, Math.max(1, Math.floor(n)))
}

export async function runNativeSlotLoop(host: NativeHost): Promise<void> {
  const n = nativeSlotCount()
  if (n <= 0) return
  process.stderr.write(`[kin] native slot loop n=${n} protocol=${KIN_PROTOCOL_VERSION}\n`)
  const slots = new Map<string, SlotState>()
  for (let i = 0; i < n; i++) {
    const id = slotId(i)
    slots.set(id, {
      id,
      phase: 'idle',
      cache: createFileStateCacheWithSizeLimit(100),
      toolWaiters: new Map(),
    })
  }
  await writeStdout({
    type: 'kin_host_ready',
    protocol_version: KIN_PROTOCOL_VERSION,
    slots: n,
    system_layout: getSystemLayout(),
    timezone: getKinTimezone(),
    capabilities: [...KIN_CAPABILITIES],
  })
  for (const id of slots.keys()) {
    await writeStdout({ type: 'kin_slot_ready', slot_id: id })
  }

  const rl = createInterface({ input: process.stdin, crlfDelay: Infinity })
  for await (const line of rl) {
    const msg = parseStdinLine(line)
    if (!msg) continue
    try {
      dispatch(host, slots, msg)
    } catch (err) {
      const jobId = 'job_id' in msg ? String(msg.job_id) : 'unknown'
      const slotId = 'slot_id' in msg ? String(msg.slot_id) : undefined
      void writeStdout({
        type: 'kin_job_error',
        job_id: jobId,
        slot_id: slotId,
        error: err instanceof Error ? err.message : String(err),
      })
    }
  }
}

function dispatch(
  host: NativeHost,
  slots: Map<string, SlotState>,
  msg: KinStdin,
): void {
  switch (msg.type) {
    case 'kin_hello':
      return
    case 'kin_cancel': {
      const slot =
        (msg.slot_id && slots.get(msg.slot_id)) ||
        [...slots.values()].find(s => s.jobId === msg.job_id)
      if (!slot) return
      slot.abort?.abort()
      for (const waiter of slot.toolWaiters.values()) {
        waiter.reject(new Error('canceled'))
      }
      slot.toolWaiters.clear()
      void writeStdout({
        type: 'kin_cancel_ack',
        job_id: msg.job_id,
        slot_id: slot.id,
      })
      return
    }
    case 'kin_tool_result': {
      const slot = slots.get(msg.slot_id)
      if (!slot) return
      const waiter = slot.toolWaiters.get(msg.tool_use_id)
      if (!waiter) return
      slot.toolWaiters.delete(msg.tool_use_id)
      waiter.resolve(msg.content)
      if (slot.toolWaiters.size === 0 && slot.phase === 'parked') {
        slot.phase = 'running'
        slot.parkedIds = undefined
      }
      return
    }
    case 'kin_job_start': {
      const slot = slots.get(msg.slot_id)
      if (!slot) {
        void writeStdout({
          type: 'kin_job_error',
          job_id: msg.job_id,
          slot_id: msg.slot_id,
          error: `unknown slot ${msg.slot_id}`,
        })
        return
      }
      if (slot.phase !== 'idle') {
        void writeStdout({
          type: 'kin_job_error',
          job_id: msg.job_id,
          slot_id: slot.id,
          error: `slot ${slot.id} busy phase=${slot.phase}`,
        })
        return
      }
      slot.phase = 'running'
      slot.jobId = msg.job_id
      void runJob(host, slot, msg.job_id, msg.request).catch(err =>
        writeStdout({
          type: 'kin_job_error',
          job_id: msg.job_id,
          slot_id: slot.id,
          error: err instanceof Error ? err.message : String(err),
        }),
      )
    }
  }
}

async function runJob(
  host: NativeHost,
  slot: SlotState,
  jobId: string,
  request: Record<string, unknown>,
): Promise<void> {
  slot.abort?.abort()
  const abort = new AbortController()
  slot.abort = abort
  slot.toolWaiters.clear()
  slot.parkedIds = undefined

  const leftover = leftoverFromRequest(request) || host.options.systemPrompt
  const { prompt, prior } = splitMessages(request)
  const model =
    typeof request.model === 'string' && request.model
      ? request.model
      : host.options.userSpecifiedModel
  const thinking = thinkingFromRequest(request, host.options.thinkingConfig as ThinkingConfig | undefined)
  const tools = mergeTools(host.tools as Tool[], request, slot, jobId, abort)

  const engine = new QueryEngine({
    cwd: host.cwd?.() || process.cwd(),
    tools: tools as never,
    commands: host.commands as never,
    mcpClients: [],
    agents: host.agents as never,
    canUseTool: host.canUseTool as never,
    getAppState: host.getAppState as never,
    setAppState: host.setAppState as never,
    initialMessages: prior,
    readFileCache: slot.cache,
    customSystemPrompt: leftover,
    appendSystemPrompt: undefined,
    userSpecifiedModel: model,
    fallbackModel: host.options.fallbackModel,
    thinkingConfig: thinking as never,
    maxTurns: host.options.maxTurns,
    verbose: Boolean(host.options.verbose),
    includePartialMessages: true,
    abortController: abort,
  })
  slot.engine = engine

  let stopReason = 'end_turn'
  let usage: unknown = {}
  try {
    for await (const message of engine.submitMessage(prompt)) {
      if (abort.signal.aborted) break
      const rec = message as {
        type?: string
        event?: unknown
        stop_reason?: string
        usage?: unknown
        message?: { stop_reason?: string; usage?: unknown }
      }
      if (rec.type === 'stream_event' && rec.event) {
        await writeStdout({
          type: 'kin_stream_event',
          job_id: jobId,
          slot_id: slot.id,
          event: rec.event,
        })
        continue
      }
      if (rec.type === 'assistant') {
        stopReason = rec.message?.stop_reason || rec.stop_reason || stopReason
        usage = rec.message?.usage || rec.usage || usage
      }
      if (rec.type === 'result') {
        stopReason = rec.stop_reason || stopReason
        usage = rec.usage || usage
      }
    }
  } finally {
    slot.cache = engine.getReadFileState()
    slot.engine = undefined
    if (slot.jobId === jobId) {
      slot.phase = 'idle'
      slot.jobId = undefined
      slot.abort = undefined
      slot.parkedIds = undefined
      for (const waiter of slot.toolWaiters.values()) {
        waiter.reject(new Error('job ended'))
      }
      slot.toolWaiters.clear()
    }
  }

  if (abort.signal.aborted) return

  await writeStdout({
    type: 'kin_job_done',
    job_id: jobId,
    slot_id: slot.id,
    stop_reason: stopReason,
    usage,
  })
}

function mergeTools(
  hostTools: Tool[],
  request: Record<string, unknown>,
  slot: SlotState,
  jobId: string,
  abort: AbortController,
): Tool[] {
  const listed = Array.isArray(request.tools) ? request.tools : []
  const hostNames = new Set(
    hostTools.map(t => t?.name).filter((n): n is string => Boolean(n)),
  )
  const stubs: Tool[] = []
  for (const raw of listed) {
    if (!raw || typeof raw !== 'object') continue
    const rec = raw as { name?: string; input_schema?: unknown; inputSchema?: unknown }
    const name = rec.name
    if (!name || hostNames.has(name)) continue
    if (name.toLowerCase().includes('web_search') || name.toLowerCase().includes('websearch')) {
      continue
    }
    stubs.push(clientToolStub(name, rec.input_schema || rec.inputSchema, slot, jobId, abort))
  }
  return stubs.length ? [...hostTools, ...stubs] : hostTools
}

function clientToolStub(
  name: string,
  schema: unknown,
  slot: SlotState,
  jobId: string,
  abort: AbortController,
): Tool {
  const inputJSONSchema =
    schema && typeof schema === 'object'
      ? (schema as { type?: string })
      : { type: 'object', properties: {} }
  return {
    name,
    maxResultSizeChars: Infinity,
    inputJSONSchema: inputJSONSchema as never,
    inputSchema: z.object({}).passthrough(),
    isEnabled: () => true,
    isReadOnly: () => true,
    isConcurrencySafe: () => true,
    description: async () => name,
    checkPermissions: async () => ({ behavior: 'allow' as const }),
    call: async (args, _ctx, _can, parentMessage) => {
      const ids = toolUseIds(parentMessage, name)
      const id = matchToolUseId(parentMessage, name, args) || ids[0] || randomUUID()
      if (slot.phase !== 'parked') {
        const allIds = allClientToolUseIds(parentMessage, slot)
        slot.phase = 'parked'
        slot.parkedIds = allIds.length ? allIds : [id]
        await writeStdout({
          type: 'kin_job_parked',
          job_id: jobId,
          slot_id: slot.id,
          tool_use_ids: slot.parkedIds,
        })
      }
      const content = await waitToolResult(slot, id, abort)
      return { data: content }
    },
  } as unknown as Tool
}

function allClientToolUseIds(parent: unknown, slot: SlotState): string[] {
  const content = (parent as { message?: { content?: unknown } })?.message?.content
  if (!Array.isArray(content)) return slot.parkedIds || []
  return content
    .filter(
      (b: { type?: string; id?: string }) =>
        b && b.type === 'tool_use' && typeof b.id === 'string',
    )
    .map((b: { id: string }) => b.id)
}

function toolUseIds(parent: unknown, name: string): string[] {
  const content = (parent as { message?: { content?: unknown } })?.message?.content
  if (!Array.isArray(content)) return []
  return content
    .filter(
      (b: { type?: string; name?: string; id?: string }) =>
        b && b.type === 'tool_use' && b.name === name && typeof b.id === 'string',
    )
    .map((b: { id: string }) => b.id)
}

function matchToolUseId(parent: unknown, name: string, args: unknown): string | undefined {
  const content = (parent as { message?: { content?: unknown } })?.message?.content
  if (!Array.isArray(content)) return undefined
  const encoded = JSON.stringify(args)
  const hits = content.filter(
    (b: { type?: string; name?: string; id?: string; input?: unknown }) =>
      b && b.type === 'tool_use' && b.name === name,
  ) as { id: string; input?: unknown }[]
  if (hits.length === 1) return hits[0].id
  return hits.find(b => JSON.stringify(b.input) === encoded)?.id
}

function waitToolResult(
  slot: SlotState,
  toolUseId: string,
  abort: AbortController,
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    if (abort.signal.aborted) {
      reject(new Error('canceled'))
      return
    }
    const onAbort = () => {
      slot.toolWaiters.delete(toolUseId)
      reject(new Error('canceled'))
    }
    abort.signal.addEventListener('abort', onAbort, { once: true })
    slot.toolWaiters.set(toolUseId, {
      resolve: value => {
        abort.signal.removeEventListener('abort', onAbort)
        resolve(value)
      },
      reject: err => {
        abort.signal.removeEventListener('abort', onAbort)
        reject(err)
      },
    })
  })
}

function leftoverFromRequest(request: Record<string, unknown>): string | undefined {
  const system = request.system
  if (typeof system === 'string') return leftoverFromSystemPrompt([system])
  if (!Array.isArray(system)) return undefined
  const texts = system.map(block => {
    if (typeof block === 'string') return block
    if (block && typeof block === 'object' && 'text' in block) {
      return String((block as { text?: unknown }).text || '')
    }
    return ''
  })
  return leftoverFromSystemPrompt(texts)
}

function splitMessages(request: Record<string, unknown>): {
  prompt: string | ContentBlockParam[]
  prior: Message[]
} {
  const messages = Array.isArray(request.messages) ? request.messages : []
  let lastUser = -1
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i] as { role?: string }
    if (msg?.role === 'user') {
      lastUser = i
      break
    }
  }
  const priorSrc = lastUser >= 0 ? messages.slice(0, lastUser) : messages
  const last = lastUser >= 0 ? messages[lastUser] : undefined
  return {
    prompt: userPromptContent(last),
    prior: priorSrc.map(toEngineMessage).filter((m): m is Message => m !== null),
  }
}

function userPromptContent(msg: unknown): string | ContentBlockParam[] {
  if (!msg || typeof msg !== 'object') return ''
  const content = (msg as { content?: unknown }).content
  if (typeof content === 'string') return content
  if (Array.isArray(content)) return content as ContentBlockParam[]
  return ''
}

function toEngineMessage(raw: unknown): Message | null {
  if (!raw || typeof raw !== 'object') return null
  const msg = raw as { role?: string; content?: unknown }
  if (msg.role === 'assistant') {
    const content = msg.content
    if (typeof content === 'string') return createAssistantMessage({ content })
    if (Array.isArray(content)) return createAssistantMessage({ content: content as never })
    return createAssistantMessage({ content: '' })
  }
  if (msg.role === 'user' || msg.role === 'tool') {
    const content = msg.content
    if (typeof content === 'string' || Array.isArray(content)) {
      return createUserMessage({ content: content as never })
    }
    return createUserMessage({ content: '' })
  }
  return null
}

function thinkingFromRequest(
  request: Record<string, unknown>,
  fallback: ThinkingConfig | undefined,
): ThinkingConfig | undefined {
  const raw = request.thinking
  if (!raw || typeof raw !== 'object') return fallback
  const rec = raw as { type?: string; budget_tokens?: number; budgetTokens?: number }
  if (rec.type === 'disabled' || rec.type === 'none') return { type: 'disabled' }
  if (rec.type === 'enabled') {
    return {
      type: 'enabled',
      budgetTokens: Number(rec.budget_tokens ?? rec.budgetTokens ?? 0),
    }
  }
  if (rec.type === 'adaptive') return { type: 'adaptive' }
  return fallback
}
