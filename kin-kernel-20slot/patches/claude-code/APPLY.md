# Native native_messages construction (protocol v2, stateless)

Baseline: `claude-code-best/claude-code` `77a7934`.

## Copy

```
src/kin/systemLayout.ts
src/kin/stdioProtocol.ts
src/kin/nativeMessagesRunner.ts
```

into the CLI tree. Apply:

```
print.ts.hook.patch
main.ts.native.patch
claude.ts.layout.patch
subagent-token-streaming.patch   # still useful on mcp_slot rollback
```

Do **not** use the old `kinEnvelope.ts` billing forge (`cch` / `2.1.241.fp`). Node omits `cch`; `cc_version` comes from `getAttributionHeader()`.

## Env

```
CLAUDE_CODE_SYSTEM_LAYOUT=zero|identity
CLAUDE_CODE_TIMEZONE=America/New_York
CLAUDE_CODE_KIN_NATIVE_SLOTS=<n>    # 1-20; enables native_messages host loop, skips structuredIO + MCP connect
```

## Kernel

```
KIN_EXECUTION_MODE=native_slot
KIN_RELAY_MODE=off
KIN_SYSTEM_MODE=zero
KIN_SLOT_TZ=America/New_York
KIN_SOCKS5=...
```

Native spawn must **not** write `mcp.json` and must **not** send `kin_hello` on boot —
protocol v2 has no `kin_hello` handshake at all (see below).

Official `-p` peeks stdin and, after the first byte, waits for EOF forever —
a live job pipe would hang before `runHeadless` / `kin_slot_ready`. This is why
`getInputPrompt()` short-circuits under `CLAUDE_CODE_KIN_NATIVE_SLOTS`.

## Architecture: stateless native_messages

The CLI holds **no tools, no agents, no canUseTool, no cross-job state**. Each
slot is a plain `{ id, phase, jobId?, abort?, task? }` record. A job is exactly
one `queryKinMessagesWithStreaming()` call: the caller (Rust) supplies the full
`messages` / `system` / `tools` / `tool_choice` / `thinking` / sampling params
per request, and the CLI routes them through the real `queryModel` pipeline
with `tools: []` + `extraToolSchemas` so it never executes a tool itself. Tool
execution, continuation across turns, and cancellation bookkeeping all live in
Rust (`.trellis/tasks/08-30-native-slot-stateless`).

Slot state machine is **three states**, not the old four:

```
idle → running → cancelling → idle
```

There is no `parked` state — nothing in native_messages waits on tool results
inside the CLI, because the CLI never runs tools. Cancel follows a strict
7-step protocol: receive `kin_cancel` → validate `job_id`+`slot_id` →
`AbortController.abort()` → await generator exit → release HTTP body → set
slot idle → send `kin_cancel_ack`. Rust may only re-enqueue that slot after
step 7 (i.e. after observing `kin_cancel_ack`).

The stdin reader must **not** `await` a job — `startJob` fires the job as a
detached task per slot so multiple slots overlap concurrently.

## Protocol v2 (`stdioProtocol.ts`)

`KIN_PROTOCOL_VERSION = 2`. No `kin_hello`, no `kin_tool_result`, no
`kin_job_parked` — those are protocol v1 relics from the old `mcp_slot` /
`nativeSlotRunner.ts` design and do not exist in v2.

```
KinStdin  = kin_job_start | kin_cancel
KinStdout = kin_host_ready | kin_slot_ready | kin_stream_event
          | kin_job_done | kin_job_error | kin_cancel_ack
```

`kin_host_ready` is the boot capability handshake:
`capabilities: ['multi_slot', 'native_sse', 'stateless']`, plus an optional
`config_hash` field (Go console computes and compares this against
`RuntimeProfile` to detect drift; see design.md §6 — not yet implemented on
the Go side as of this writing).

Recycle a slot only on `kin_job_done` / `kin_job_error` / `kin_cancel_ack` —
there is no `kin_job_parked` to special-case anymore.

## Status

CLI-side (this directory) is implemented and verified: `bun run typecheck`
clean, `bun run check` (biome) clean, `bun test` shows only pre-existing
unrelated failures (MACRO bundling macro, WorkflowsPanel test key warning,
deep-link protocol test — none touch native_messages code).

Rust-side (`execution_mode.rs`, `native_protocol.rs`, `mod.rs`) is
implemented and verified: `ExecutionMode::NativeMessages` added alongside
`McpSlot`/`NativeAgent`; protocol v2 wire types match `stdioProtocol.ts`
exactly (no `Hello`/`ToolResult`/`JobParked`); `StreamAssembler` wired into
`complete_job()` so `stream:false` responses and job-done frames carry real
assembled `content` instead of `vec![]`; `park_native_job()` deleted;
`resume()` under native modes delegates to a new shared `submit_fresh()`
(treats continuation as a brand-new job, since tenant/tool_use_id/message-
merge validation already happened in `api.rs`/`session.rs`); `kin_host_ready`
is now validated (`validate_host_ready()`) against expected
`protocol_version`/`slot_count`/`system_layout`/`timezone`/`capabilities`
before slots are registered; `MAX_JOB_BYTES` metering now falls back to a
frame's own `job_id` when no `parent` field is present (needed for
native_messages frames); `x-kin-native-slot` diagnostic response header
added via new `Provider::session_slot()`. `cargo build` clean, `cargo test
--bin kin-kernel provider::multiplex_cli` 108/108 passing (no `McpSlot`
regression). `config_hash` is round-tripped, logged, and (once the Go side
below landed) compared against `KIN_DESIRED_CONFIG_HASH`.

Go console (`config_hash` / `RuntimeProfile`, design.md §6) is **implemented
and verified**: `RuntimeProfile.ConfigHash()` normalizes via a
`map[string]any` round-trip (key-sorted, whitespace-free JSON) before
SHA-256; `PUT`/`GET /api/v1/runtime-profile` validate and serve it. Rust
`validate_host_ready()` now compares the CLI's echoed `config_hash` against
`KIN_DESIRED_CONFIG_HASH` (set from the Go-computed hash at process
startup — no runtime re-fetch, changes require drain + restart) and
`/readyz` returns `503 {"reason":"config_hash_mismatch"}` on drift. `go
build/vet/test` clean; `cargo test --bin kin-kernel provider::multiplex_cli`
109/109, `config::` 2/2, `api::` 3/3, all passing; clippy flat at 57
warnings (no new lint debt).

First real end-to-end native_messages smoke test passed against the live
Anthropic API (Docker + SOCKS5 harness, `claude-haiku-4-5-20251001`): a
`kin_job_start` with `thinking:{type:"disabled"}` produced a full real
`kin_stream_event` sequence (`message_start` → `content_block_delta` ×N →
`message_stop`) and a `kin_job_done` with non-zero `usage`. This also
surfaced and fixed a real protocol defect: `runJob()`'s `assistant` branch
did not check `isApiErrorMessage` on the synthetic error-wrapper message
that `getAssistantMessageFromError()` produces for real API failures (4xx/
5xx), so a genuine API error (e.g. a 400 from an invalid `thinking` budget)
was silently swallowed and reported as a normal `kin_job_done` — Rust had
no way to observe the failure. Fixed by checking `isApiErrorMessage` and
emitting `kin_job_error` with the extracted error text instead; verified
by replaying the same failing job and observing `kin_job_error` in place
of the previous silent `kin_job_done`.

S3 functional acceptance (AC1–AC15) has not been run — needs systematic
coverage beyond this one smoke test. Default `KIN_EXECUTION_MODE` remains
`mcp_slot` until S3 acceptance passes on native.
