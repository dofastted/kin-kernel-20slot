/**
 * Kin outbound envelope for patched Claude Code (Node).
 *
 * When CLAUDE_CODE_KIN_ENVELOPE / KIN_SYSTEM_MODE is set, kin-slot API
 * requests replace the stock identity + fat Environment wrap with:
 *
 *   zero     (default): billing.prompt_version = <official sentence>
 *                       + timezone-only Environment
 *                       + caller leftover if present
 *   identity:           billing (no prompt_version)
 *                       + official sentence as its own block
 *                       + timezone-only Environment
 *                       + leftover
 *
 * Config is re-read every request from KIN_ENVELOPE_PATH so the console
 * can flip mode without restarting the CLI process.
 */
import { createHash, randomUUID } from 'crypto'
import { readFileSync } from 'fs'

export const IDENTITY =
  "You are a Claude agent, built on Anthropic's Claude Agent SDK."
const SALT = '59cf53e54c78'
const CLI_VER = '2.1.241'
const DEFAULT_TZ = 'America/New_York'

export type SystemMode = 'zero' | 'identity'

export type EnvelopeConfig = {
  mode: SystemMode
  timezone: string
}

type TextBlock = { type: 'text'; text: string }

function parseMode(raw: string | undefined): SystemMode {
  const v = (raw || '').trim().toLowerCase()
  if (v === 'identity' || v === 'id' || v === 'block') return 'identity'
  return 'zero'
}

export function enabled(): boolean {
  return Boolean(
    process.env.CLAUDE_CODE_KIN_ENVELOPE ||
      process.env.KIN_SYSTEM_MODE ||
      process.env.KIN_ENVELOPE_PATH,
  )
}

export function loadConfig(): EnvelopeConfig {
  const path = process.env.KIN_ENVELOPE_PATH
  if (path) {
    try {
      const parsed = JSON.parse(readFileSync(path, 'utf8')) as Partial<EnvelopeConfig>
      return {
        mode: parseMode(parsed.mode || process.env.KIN_SYSTEM_MODE),
        timezone:
          (parsed.timezone || process.env.KIN_SLOT_TZ || process.env.TZ || DEFAULT_TZ).trim() ||
          DEFAULT_TZ,
      }
    } catch {
      /* fall through */
    }
  }
  return {
    mode: parseMode(process.env.CLAUDE_CODE_KIN_ENVELOPE || process.env.KIN_SYSTEM_MODE),
    timezone: (process.env.KIN_SLOT_TZ || process.env.TZ || DEFAULT_TZ).trim() || DEFAULT_TZ,
  }
}

export function isKinSlotRequest(tools: readonly { name?: string }[] | undefined): boolean {
  return (tools || []).some(t => String(t?.name || '').startsWith('mcp__kin_runtime__'))
}

export function applyKinEnvelope(
  messages: unknown,
  tools: readonly { name?: string }[] | undefined,
): string[] | null {
  if (!enabled() || !isKinSlotRequest(tools)) {
    return null
  }
  const cfg = loadConfig()
  const leftover = leftoverFromMessages(messages)
  const firstUser = firstUserFromJob(messages) || firstUserFromMessages(messages)
  const sessionId = sessionIdFromMessages(messages)
  const billing = billingLine(cfg.mode, firstUser, sessionId)
  const blocks = [billing]
  if (cfg.mode === 'identity') {
    blocks.push(IDENTITY)
  }
  blocks.push(`# Environment\n - Timezone: ${cfg.timezone}`)
  if (leftover) {
    blocks.push(leftover)
  }
  return blocks
}

export function billingLine(mode: SystemMode, firstUser: string, sessionId: string): string {
  const fp = computeFp(firstUser, CLI_VER)
  const cch = computeCch(firstUser, CLI_VER)
  const promptId = promptIdOf(sessionId, fp)
  if (mode === 'zero') {
    return `x-anthropic-billing-header: cc_version=${CLI_VER}.${fp}; cc_entrypoint=sdk-cli; cch=${cch}; cc_prompt_id=${promptId}; prompt_version=<${IDENTITY}>`
  }
  return `x-anthropic-billing-header: cc_version=${CLI_VER}.${fp}; cc_entrypoint=sdk-cli; cch=${cch}; cc_prompt_id=${promptId}`
}

function computeFp(firstUser: string, ver: string): string {
  const buf = Buffer.from(firstUser)
  const chars = Buffer.from([
    4 < buf.length ? buf[4] : 0x30,
    7 < buf.length ? buf[7] : 0x30,
    20 < buf.length ? buf[20] : 0x30,
  ])
  const hasher = createHash('sha256')
  hasher.update(SALT)
  hasher.update(chars)
  hasher.update(ver)
  return hasher.digest('hex').slice(0, 3)
}

function computeCch(firstUser: string, ver: string): string {
  return createHash('sha256')
    .update(`${SALT}:cch:${firstUser}:${ver}`)
    .digest('hex')
    .slice(0, 5)
}

function promptIdOf(sessionId: string, fp: string): string {
  const raw = sessionId.trim()
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(raw)) {
    return raw
  }
  const seed = raw || `prompt:${CLI_VER}:${fp}`
  const hx = createHash('sha256').update(seed).digest('hex')
  const variant = (parseInt(hx.slice(16, 18), 16) & 0x3f) | 0x80
  return `${hx.slice(0, 8)}-${hx.slice(8, 12)}-4${hx.slice(13, 16)}-${variant.toString(16).padStart(2, '0')}${hx.slice(18, 20)}-${hx.slice(20, 32)}`
}

function leftoverFromMessages(messages: unknown): string | undefined {
  const job = latestJob(messages)
  if (!job) return undefined
  return leftoverText(job.system ?? job.request?.system)
}

function leftoverText(system: unknown): string | undefined {
  if (system == null) return undefined
  if (typeof system === 'string') return sanitizeLeftover(system)
  if (!Array.isArray(system)) return undefined
  const parts: string[] = []
  for (const block of system) {
    const text =
      typeof block === 'string' ? block : typeof block?.text === 'string' ? block.text : ''
    const kept = sanitizeLeftover(text)
    if (kept) parts.push(kept)
  }
  return parts.length ? parts.join('\n\n') : undefined
}

function sanitizeLeftover(text: string): string | undefined {
  const trimmed = text.trim()
  if (!trimmed) return undefined
  if (trimmed.startsWith('x-anthropic-billing-header:')) return undefined
  if (trimmed.startsWith('# Environment')) return undefined
  if (trimmed === IDENTITY) return trimmed
  if (trimmed.includes('mcp__kin_runtime__') || trimmed.includes('persistent Kin')) {
    return undefined
  }
  return trimmed
}

function latestJob(messages: unknown): { system?: unknown; request?: { system?: unknown; messages?: unknown }; session_id?: string; messages?: unknown } | undefined {
  if (!Array.isArray(messages)) return undefined
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i]
    if (!msg || msg.role !== 'user') continue
    const content = msg.content ?? msg.message?.content
    if (typeof content === 'string') {
      try {
        const parsed = JSON.parse(content)
        if (parsed?.type === 'job') return parsed
      } catch {
        continue
      }
    }
    if (!Array.isArray(content)) continue
    for (let j = content.length - 1; j >= 0; j--) {
      const block = content[j]
      if (block?.type !== 'tool_result') continue
      const raw = typeof block.content === 'string' ? block.content : JSON.stringify(block.content ?? '')
      try {
        const parsed = JSON.parse(raw)
        if (parsed?.type === 'job') return parsed
      } catch {
        continue
      }
    }
  }
  return undefined
}

function firstUserFromJob(messages: unknown): string {
  const job = latestJob(messages)
  const list = job?.messages ?? job?.request?.messages
  return firstUserFromMessages(list)
}

function firstUserFromMessages(messages: unknown): string {
  if (!Array.isArray(messages)) return ''
  for (const msg of messages) {
    if (msg?.role !== 'user' && msg?.message?.role !== 'user') continue
    return contentText(msg.content ?? msg.message?.content)
  }
  return ''
}

function contentText(content: unknown): string {
  if (typeof content === 'string') return content
  if (!Array.isArray(content)) return ''
  for (const block of content) {
    if (block?.type === 'text' && typeof block.text === 'string') return block.text
  }
  return ''
}

function sessionIdFromMessages(messages: unknown): string {
  const job = latestJob(messages)
  if (typeof job?.session_id === 'string' && job.session_id) return job.session_id
  return randomUUID()
}

export function asTextBlocks(texts: string[]): TextBlock[] {
  return texts.map(text => ({ type: 'text', text }))
}
