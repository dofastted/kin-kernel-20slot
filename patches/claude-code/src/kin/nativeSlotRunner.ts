/**
 * NativeSlotRunner (P3 hello slice).
 *
 * Hosts N QueryEngine slots inside one CLI PID. Jobs arrive on stdin as
 * kin_job_start; Anthropic SSE is forwarded immediately as kin_stream_event.
 * Client-tool parking is a follow-up — this slice streams text jobs.
 */
import { createInterface } from 'node:readline'
import { ask } from '../QueryEngine.js'
import type { ContentBlockParam } from '@anthropic-ai/sdk/resources/index.mjs'
import { leftoverFromSystemPrompt } from './systemLayout.js'
import {
  parseStdinLine,
  slotId,
  writeStdout,
  type KinStdin,
} from './stdioProtocol.js'
import { createFileStateCacheWithSizeLimit } from '../utils/fileStateCache.js'

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

type SlotState = {
  id: string
  abort?: AbortController
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
  const slots = new Map<string, SlotState>()
  for (let i = 0; i < n; i++) {
    const id = slotId(i)
    slots.set(id, { id })
    writeStdout({ type: 'kin_slot_ready', slot_id: id })
  }

  const rl = createInterface({ input: process.stdin, crlfDelay: Infinity })
  for await (const line of rl) {
    const msg = parseStdinLine(line)
    if (!msg) continue
    try {
      await dispatch(host, slots, msg)
    } catch (err) {
      const jobId = 'job_id' in msg ? String(msg.job_id) : 'unknown'
      writeStdout({
        type: 'kin_job_error',
        job_id: jobId,
        error: err instanceof Error ? err.message : String(err),
      })
    }
  }
}

async function dispatch(
  host: NativeHost,
  slots: Map<string, SlotState>,
  msg: KinStdin,
): Promise<void> {
  switch (msg.type) {
    case 'kin_hello':
      return
    case 'kin_cancel': {
      for (const slot of slots.values()) {
        slot.abort?.abort()
      }
      return
    }
    case 'kin_tool_result':
      // P3.5: unblock parked tool.call. Hello slice ignores.
      return
    case 'kin_job_start': {
      const slot = slots.get(msg.slot_id)
      if (!slot) {
        writeStdout({
          type: 'kin_job_error',
          job_id: msg.job_id,
          error: `unknown slot ${msg.slot_id}`,
        })
        return
      }
      await runJob(host, slot, msg.job_id, msg.request)
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

  const leftover = leftoverFromRequest(request) || host.options.systemPrompt
  const prompt = lastUserPrompt(request)
  let stopReason = 'end_turn'
  let usage: unknown = {}
  const readFileCache = createFileStateCacheWithSizeLimit(100)

  for await (const message of ask({
    commands: host.commands as never,
    prompt,
    cwd: host.cwd?.() || process.cwd(),
    tools: host.tools as never,
    verbose: Boolean(host.options.verbose),
    mcpClients: [],
    thinkingConfig: host.options.thinkingConfig as never,
    maxTurns: host.options.maxTurns,
    canUseTool: host.canUseTool as never,
    customSystemPrompt: leftover,
    appendSystemPrompt: undefined,
    userSpecifiedModel: host.options.userSpecifiedModel,
    fallbackModel: host.options.fallbackModel,
    getAppState: host.getAppState as never,
    setAppState: host.setAppState as never,
    getReadFileCache: () => readFileCache,
    setReadFileCache: () => {},
    abortController: abort,
    includePartialMessages: true,
    agents: host.agents as never,
  })) {
    if (abort.signal.aborted) break
    const rec = message as {
      type?: string
      event?: unknown
      event_type?: string
      stop_reason?: string
      usage?: unknown
      message?: { stop_reason?: string; usage?: unknown }
    }
    if (rec.type === 'stream_event' && rec.event) {
      writeStdout({
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

  writeStdout({
    type: 'kin_job_done',
    job_id: jobId,
    slot_id: slot.id,
    stop_reason: stopReason,
    usage,
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

function lastUserPrompt(
  request: Record<string, unknown>,
): string | ContentBlockParam[] {
  const messages = Array.isArray(request.messages) ? request.messages : []
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i] as { role?: string; content?: unknown }
    if (msg?.role !== 'user') continue
    if (typeof msg.content === 'string') return msg.content
    if (Array.isArray(msg.content)) return msg.content as ContentBlockParam[]
  }
  return ''
}
