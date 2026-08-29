export type KinStdin =
  | {
      type: 'kin_hello'
      slots: number
      system_layout: string
      timezone: string
    }
  | {
      type: 'kin_job_start'
      job_id: string
      slot_id: string
      request: Record<string, unknown>
    }
  | {
      type: 'kin_tool_result'
      job_id: string
      slot_id: string
      tool_use_id: string
      content: unknown
    }
  | { type: 'kin_cancel'; job_id: string }

export type KinStdout =
  | { type: 'kin_slot_ready'; slot_id: string }
  | {
      type: 'kin_stream_event'
      job_id: string
      slot_id: string
      event: unknown
    }
  | {
      type: 'kin_job_done'
      job_id: string
      slot_id: string
      stop_reason: string
      usage?: unknown
    }
  | { type: 'kin_job_error'; job_id: string; error: string }

export function parseStdinLine(line: string): KinStdin | null {
  const trimmed = line.trim()
  if (!trimmed) return null
  try {
    const parsed = JSON.parse(trimmed) as KinStdin
    if (!parsed || typeof parsed.type !== 'string') return null
    if (!parsed.type.startsWith('kin_')) return null
    return parsed
  } catch {
    return null
  }
}

export function writeStdout(frame: KinStdout): void {
  process.stdout.write(JSON.stringify(frame) + '\n')
}

export function slotId(index: number): string {
  return `s${String(index).padStart(2, '0')}`
}
