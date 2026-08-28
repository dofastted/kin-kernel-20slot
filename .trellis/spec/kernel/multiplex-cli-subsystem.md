# Multiplex CLI Subsystem

`provider/multiplex_cli/` implements `KIN_ISOLATION=subagent-pool`: **one** Claude
Code OS process hosts up to 20 concurrent logical request "slots" via MCP-blocked
background subagents, instead of spawning one process per request. This is the only
isolation mode that lets a single kernel serve real concurrency against a stock
Claude CLI (see `docs/DELIVERY_STATUS.md` "已知限制": the stock CLI is one process /
one stdin turn — 5 truly concurrent CLI processes cost ~210MB each, so multiplexing
inside one process is the only way to approach 20-way concurrency without a
proportional memory bill).

## Slot State Machine (`slot.rs`)

```
Booting -> ReadyBlocked -> Running -> WaitingTool -> Running -> ... -> Draining -> Dead
```

`Slot{id, parent_tool_use_id, phase, tenant_id, session_id, job_id, jobs_completed,
bytes_used, created_at, last_change}`. `cas(from, to)` is the only way to transition
phase — a compare-and-swap, not a plain assignment, so two racing callers can't both
believe they own the same slot.

`bind_job()` only succeeds from `ReadyBlocked`, and **enforces same-tenant
stickiness** if the slot is already bound to a tenant/session. `unbind_ready()`
deliberately does **not** clear `tenant_id`/`session_id` — read its doc-comment:
"Keep tenant+session sticky. Clearing them would let another tenant inherit leftover
subagent context." This is a security-relevant invariant, not an oversight: a slot
that goes idle between turns for the same session must not be handed to a different
tenant while any conversational context could still be resident in the CLI
subagent's memory.

`should_retire()` checks `max_jobs`/`max_lifetime`/idle-while-tenant-bound — slots are
recycled rather than kept forever, bounding both memory growth and the blast radius
of any single subagent's accumulated state.

## Memory Admission Control (`memory_guard.rs`)

Exact RSS admission bands for the 20-slot single-process runtime (module
doc-comment):

| RSS | Admission |
|---|---|
| < 3.0 GiB | `Allow` — admit all 20 slots |
| 3.0–3.5 GiB | `AllowSmall` — keep in-flight, refuse new *large* requests |
| 3.5–3.75 GiB | `Drain` — no new requests |
| > 3.75 GiB | `Reject` — 503 |

4 GiB is the cgroup hard cap, so `Reject` at 3.75 GiB leaves headroom before an OOM
kill. `MemoryLimits::production()` codifies these as `soft=3GiB, drain=3500MiB,
reject=3750MiB, max_inflight_payload=256MiB, max_pending=100,
large_request_bytes=256MiB`; override via `KIN_MEM_SOFT_BYTES` /
`KIN_MEM_DRAIN_BYTES` / `KIN_MEM_REJECT_BYTES` / `KIN_MAX_INFLIGHT_PAYLOAD` /
`KIN_MAX_PENDING`.

`MemoryGuard::admit(request_bytes)` checks, in order: `max_pending` ->
`max_inflight_payload` -> the admission band (large requests are specifically
blocked once in `AllowSmall`, even though small ones are still allowed).

`snapshot()` reads **kernel + Claude CLI RSS from `/proc/{pid}/statm`** by default —
not cgroup `memory.current`. Read the doc-comment before "fixing" this to use
cgroup accounting: the sandbox cgroup often includes unrelated sibling processes, so
cgroup-based accounting would false-drain on load unrelated to this kernel instance.
Set `KIN_MEM_OBSERVED=cgroup` only when you have confirmed the cgroup is scoped
exclusively to this kernel + CLI pair.

**Anti-pattern**: adding a new resource check that reads `memory.current` unconditionally
would silently break this isolation guarantee for any deployment where the cgroup is
shared — always gate cgroup-based RSS reads behind the same opt-in env var.

## Signed Continuation Tokens (`continuation.rs`)

`ContinuationToken{process_generation, slot_id, job_id, logical_session_id,
tool_call_id, expires_at, nonce}`. `issue()` generates a `Uuid::new_v4()` nonce and a
TTL-based expiry. `encode()`/`decode()` produce/parse the `kct_<hex_payload>.<hex_mac>`
wire format.

**Important**: `mac()` is a **hand-rolled, non-standard mixing function** — despite
the "MAC" terminology in the type name, it is *not* HMAC-SHA256 or any other
standardized MAC construction. It cycles the secret bytes into a 32-byte buffer,
XORs/rotates/multiplies the payload bytes in, then runs 4 rounds of
rotate-left-5+add+XOR-with-secret. If you touch this code:

- Do not describe it as "HMAC" or "cryptographically signed" in new comments/docs
  without the caveat that it's a custom scheme.
- Its entire security value depends on `secret` being non-empty and unpredictable —
  `mac()` returns an all-zero MAC when `secret.is_empty()`. Any code path that can
  leave the multiplex secret unset in production is a security bug, not a
  configuration nicety.
- `matches_runtime()` checks `process_generation` equality and returns
  `KernelError::ContinuationLost` on mismatch — a token from a previous CLI process
  generation (e.g. after a restart) is correctly rejected rather than silently
  resumed against a different process.

This is the canonical local example for
`.trellis/spec/guides/code-reuse-thinking-guide.md`'s "don't roll your own crypto
without flagging it" pattern — reference it there rather than duplicating this
explanation if that guide is extended.

## MCP JSON-RPC Server (`mcp_server.rs`)

`spawn(runtime, bind)` starts an axum router serving `/mcp` (POST for JSON-RPC calls,
GET for the SSE upgrade path) and `/healthz`. This is a **second, independent** HTTP
server inside the kernel process — separate from the main client-facing router in
`api.rs` — used only for the kernel <-> CLI-subagent MCP protocol.

`tools/list` exposes exactly 4 tools: `slot_wait`, `client_tool`, `kin_done`,
`kin_fail`. `tools/call` dispatches to `runtime.mcp_slot_wait` /
`runtime.mcp_client_tool` / `runtime.mcp_kin_done` / `runtime.mcp_kin_fail`, branching
to `tool_sse()` (SSE response) if the client's `Accept` header includes
`text/event-stream`, otherwise a synchronous JSON-RPC response.

**Critical, easy-to-break invariant** (from the doc-comment on
`progress_notification()`): Claude Code 2.1.x Zod-validates
`notifications/progress.params.progressToken` as `string | number`. A **missing**
token throws inside the CLI's notification handler and **drops the MCP HTTP
connection** — meaning an idle `slot_wait` call would never receive its job. This is
why `tool_sse()` sends periodic progress-notification-or-comment keepalives (every
15s) while a dispatched future is pending, and why the keepalive conditionally
echoes back a `progressToken` only if the client supplied one — **never send a
`notifications/progress` frame with a null/absent `progressToken` if the original
call included one**, and never assume the CLI tolerates a malformed progress
notification; a regression here silently kills long-idle slots rather than
returning a visible error.

## Job Flow Summary

1. `bootstrap.rs` writes the root prompt instructing the CLI to spawn exactly N
   `kin-slot` background agents (`supervisor.rs::kin_slot_agents()` defines the agent
   prompt/tools), then `wait_ready()` polls `runtime.ready_slots()` until N slots
   report ready or a timeout elapses.
2. `supervisor.rs::spawn()` launches the actual CLI child process: writes
   `.credentials.json` (via `write_oauth_file()`) and `mcp.json` pointing at the
   in-process MCP server, sets `CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS`/
   `CLAUDE_CODE_MAX_TOOL_USE_CONCURRENCY` to the slot count, and applies proxy env via
   `apply_proxy_env()` (SOCKS5-only enforcement — see `provider-adapters.md`).
3. Each ready slot repeatedly calls the MCP `slot_wait` tool; `scheduler.rs::SlotScheduler::pick()`
   assigns a `Job` to a `ReadyBlocked` slot — sticky-first by
   `"{tenant}\u{1f}{session}"` key, else first-available filtered by tenant match
   (a `ReadyBlocked` slot already bound to a different tenant is skipped, not stolen).
4. `stream_decoder.rs::decode()` routes incoming CLI NDJSON frames by
   `parent_tool_use_id`: frames with no parent are root-supervisor traffic
   (`Decoded::Root`); an `AgentSpawn` frame identifies a new subagent's tool_use id;
   everything else with a matching parent is `Decoded::Routed` to that slot's stream.
5. `job_stream.rs::JobStream::ingest()` translates routed frames into Anthropic SSE
   events — **read the module doc-comment**: CLI 2.1.241 does not set
   `parent_tool_use_id` on `stream_event` token-stream frames (only root-level), so
   subagent text arrives as complete `assistant`/`user` frames, not incremental
   deltas. `JobStream` forwards those as whole stage-level SSE blocks. **Never
   re-chunk a complete string into fake incremental deltas** to simulate streaming —
   one of the 8 inline tests in `mod.rs` exists specifically to guarantee this
   ("no fake chunking guarantee").
6. `pending_call.rs::PendingCalls` holds the `oneshot` channel registries
   (`slot_wait`/`client_tool`/`done`) that connect the async MCP handlers to the
   synchronous-looking job dispatch logic in `mod.rs`.
7. On `kin_done`/`kin_fail`, the slot's `Job` completes, its continuation token
   (if any) is finalized, and the slot returns to `ReadyBlocked` (or `Draining`/`Dead`
   per `should_retire()`).

## Testing Without a Live CLI

`replay.rs` supports fully offline load/soak testing by replaying a recorded CLI
NDJSON trace (`Trace::from_ndjson()` or `Trace::synthetic()`) against N virtual
sessions, in `PayloadMode::Shared` (sessions share the same underlying bytes — bounds
gateway memory) or `PayloadMode::Independent` (each session independently parses its
own copy). This never talks to a real Claude process or spends tokens — use it for
soak/regression testing multiplex behavior at scale instead of spinning up 20 real
CLI slots.

`MultiplexCliProvider::simulated(slot_count)` (in `mod.rs`) is the synchronous unit-test
constructor — see `quality-guidelines.md` for the testing pattern this establishes.
