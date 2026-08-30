# Multiplex CLI Subsystem

`provider/multiplex_cli/` is the only execution path: **one** patched Claude Code
process hosts up to 20 concurrent stateless request "slots", instead of spawning one
process per request. That is the only way a single kernel serves real concurrency
against the CLI (see `docs/DELIVERY_STATUS.md` "已知限制": the stock CLI is one
process / one stdin turn — 5 truly concurrent CLI processes cost ~210MB each, so
multiplexing inside one process is the only way to approach 20-way concurrency
without a proportional memory bill).

The kernel never originates an upstream request. The CLI keeps its own TLS, its own
request fingerprint, and its own SSE decoding; Rust writes jobs to its stdin and
reads frames from its stdout. Everything that used to compensate for Rust *not*
being on the wire — the Messages relay, the SSE tap, the source arbiter, the
request↔job correlator, the MCP slot loop, per-turn process isolation — was deleted
in `08-30-patch-only-consolidation`. Do not reintroduce a second data plane: if a
feature seems to need one, it belongs in the CLI patch (`patches/claude-code/`).

`execution_mode` is a fixed string (`native_messages`) reported by `/healthz`,
`/readyz` and `/internal/v1/envelope`, and it is a field of the Go control plane's
`RuntimeProfile`, hence an input to `config_hash`. There is no `KIN_EXECUTION_MODE`,
`KIN_ALLOW_NATIVE_AGENT`, `KIN_ISOLATION` or `KIN_RELAY_*` env var any more; the
constant lives in `api.rs::EXECUTION_MODE` and the console's
`server.go::executionMode`, and the two must stay byte-identical or the three-way
`config_hash` check fails.

## Native Protocol (`native_protocol.rs`)

stdin (`KinStdin`): `kin_job_start{job_id, slot_id, request}`,
`kin_cancel{job_id, slot_id?}`.
stdout (`KinStdout`): `kin_host_ready{protocol_version, slots, system_layout,
timezone, capabilities, config_hash?}`, `kin_slot_ready{slot_id}`,
`kin_stream_event{job_id, slot_id, event}`, `kin_job_done{job_id, slot_id,
stop_reason, usage}`, `kin_job_error{job_id, slot_id?, error}`,
`kin_cancel_ack{job_id, slot_id}`.

Invariants:

- **Every frame carries its own ids.** No `parent_tool_use_id` heuristics, no
  correlation tokens. A `kin_stream_event`/`kin_job_done` whose `slot_id` disagrees
  with the job's bound slot is dropped with a warning, never routed on guess.
- **Do not write `kin_hello` at boot.** Official `-p` peeks stdin and then waits for
  EOF; a boot handshake hangs the CLI before `kin_slot_ready` is ever emitted.
- **`validate_host_ready()` is a gate, not a log line.** Protocol version, slot
  count, envelope mode/timezone, required capabilities and (when
  `KIN_DESIRED_CONFIG_HASH` is set) the echoed `config_hash` must all match before
  any slot is registered. A `config_hash` mismatch sets the flag `/readyz` turns
  into a 503 — never downgrade it to a warning.
- **Only cancel a job the CLI still owns.** The CLI drops its slot to idle the
  instant it emits `kin_job_done` / `kin_job_error`, and its `kin_cancel`
  handler returns silently for a job it no longer owns — no
  `kin_cancel_ack`. So `abort_terminal_job(job_id, cli_owns_job)` takes
  `false` after any CLI-side terminal frame (re-register the slot locally) and
  `true` only while the CLI is still streaming (client gone / overflow /
  stall), where the ack is the authoritative release. Getting this wrong leaks
  the slot forever: two failed jobs took a live 2-slot runtime to
  `no_capacity`. Regression test:
  `cli_side_job_error_frees_the_slot_without_a_cancel_ack`.
- **`MAX_LINE_BYTES` / `MAX_JOB_BYTES` are per-job.** Metering keys on `job_id` so a
  runaway job stops being decoded without starving the other slots sharing the CLI
  process.

## Slot State Machine (`slot.rs`)

```
Booting -> ReadyBlocked -> Running -> ReadyBlocked -> ... -> Dead
```

`Slot{id, phase, tenant_id, session_id, job_id, jobs_completed, created_at,
last_change}`. `ReadyBlocked` means idle-and-registered with the scheduler (the name
predates the MCP loop it was blocking in). There is no in-CLI tool parking: a
`tool_use` turn ends the job and its continuation arrives as a **new** job, so
`SlotPhase::WaitingTool` no longer exists. HTTP-level tool waiting still exists one
layer up, in `session.rs`/`scheduler.rs`.

`bind_job()` only succeeds from `ReadyBlocked`, and **enforces same-tenant
stickiness** if the slot is already bound to a tenant/session. `unbind_ready()`
deliberately does **not** clear `tenant_id`/`session_id` — read its doc-comment:
"Keep tenant+session sticky. Clearing them would let another tenant inherit leftover
subagent context." This is a security-relevant invariant, not an oversight: a slot
that goes idle between turns for the same session must not be handed to a different
tenant while any conversational context could still be resident in the CLI's memory.

`should_retire()` checks `max_jobs`/`max_lifetime`/idle-while-tenant-bound — slots are
recycled rather than kept forever, bounding both memory growth and the blast radius
of any single slot's accumulated state.

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

## Job Flow Summary

1. `supervisor.rs::spawn()` launches the CLI child: writes `.credentials.json` (via
   `cli_auth`), applies proxy env through `apply_proxy_env()` (HTTP CONNECT only —
   SOCKS5 without a bridge is a hard error, see `provider-adapters.md`), passes
   `native_cli_args()` plus `CLAUDE_CODE_KIN_NATIVE_SLOTS`,
   `CLAUDE_CODE_SYSTEM_LAYOUT`, `CLAUDE_CODE_TIMEZONE` and, when configured,
   `CLAUDE_CODE_KIN_CONFIG_HASH`. There is no `--agents`, no `--mcp-config` and no
   `ANTHROPIC_BASE_URL` redirect.
2. The CLI emits `kin_host_ready`; after `validate_host_ready()` passes, the runtime
   registers `slots` slots and `bootstrap::wait_ready()` returns once
   `runtime.ready_slots()` reaches the configured count.
3. `submit_fresh()` admits the request against `MemoryGuard`, picks a slot via
   `scheduler.rs::SlotScheduler::pick()` — sticky-first by `"{tenant}\u{1f}{session}"`,
   else first-available filtered by tenant match (a `ReadyBlocked` slot bound to
   another tenant is skipped, never stolen) — retries within `KIN_SUBMIT_WAIT_MS`
   instead of 503-ing on the slot re-entry gap, then writes `kin_job_start`.
4. `handle_cli_frame()` decodes stdout frames: `kin_stream_event.event` is forwarded
   to the client verbatim **and** fed to `StreamAssembler` so a `stream:false` client
   and `complete_job()` get real `{id,name,input}` content blocks.
5. `kin_job_done` / `kin_job_error` finish the job: `complete_job()` builds the
   `MessageResponse` from the assembler (CLI-sent empty `stop_reason`/`usage` fall
   back to the assembled values), `finish_sent_job()` frees the slot after the
   terminal frame is actually delivered, and `register_native_ready()` returns it to
   `ReadyBlocked` unless `should_retire()` says otherwise.
6. `resume()` is a thin wrapper: continuation/tenant/tool_use_id validation and the
   history merge already happened in `api.rs` → `session.rs`, so a resume is
   structurally a fresh job (`submit_fresh` with `resumed: false`).
7. Client disconnect / overflow / stall sets a `JobSink` terminal;
   `abort_terminal_job()` sends `kin_cancel` and only re-registers the slot locally
   if that write fails (the CLI's `kin_cancel_ack` is the normal path).

## Testing Without a Live CLI

`MultiplexCliProvider::simulated(slot_count)` / `Runtime::start_simulated()` wire an
in-memory `tokio::io::duplex` pipe and run `simulated_cli()` on the far end. The
simulator speaks the real `kin_*` protocol, so tests exercise `write_cli_stdin()`,
`decode_stdout()` and `handle_native_frame()` rather than a bespoke fake. Request
text selects the reply shape: `[use_tool:NAME]` ends the turn on `tool_use`,
`[web_search]` emits `server_tool_use` + `web_search_tool_result` + text, anything
else answers with one text block (`slot {slot_id} :: {text}`).

Rules when extending it:

- `message_start` is emitted **before** `simulate_latency` — a client must see the
  turn open without waiting for the answer (there is a test for that).
- Each job runs in its own task, so N submissions really overlap; the 20-slot test
  asserts both wall time and `peak_running`.
- Cancel semantics must stay faithful: a `kin_cancel` for a job the simulator
  no longer runs is dropped **without** an ack, exactly like the real runner.
  Acking unconditionally would hide slot-release bugs.
- The simulator echoes the `config_hash` it was handed, exactly like the real CLI.
  It therefore cannot fake a stale-CLI mismatch — that path is covered by
  `validate_host_ready_rejects_config_hash_mismatch` and
  `readyz_returns_503_on_config_hash_mismatch`, not by the simulator.
- Do not fake-chunk a complete string into synthetic `text_delta`s to imitate
  streaming; emit the frames the CLI would emit.

See `quality-guidelines.md` for the broader testing pattern.
