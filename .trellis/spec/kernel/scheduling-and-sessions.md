# Scheduling and Sessions

## P2C Worker-Lease Scheduler (`kernel/src/scheduler.rs`)

This is the top-level scheduler used by every provider **except** multiplex's
internal slot picker (`provider/multiplex_cli/scheduler.rs::SlotScheduler` — a
different type with a different job; see `multiplex-cli-subsystem.md`). Do not
confuse the two `scheduler.rs` files when reading stack traces or adding tests.

`Worker{id, generation: AtomicU64, capacity, counters: Mutex<Counters>,
healthy/draining: AtomicBool, latency_ewma_micros/error_ewma_ppm}` — each worker
tracks its own health and load signals so the scheduler can make P2C ("power of two
choices") decisions without a central load table.

`WorkerLease` is RAII: acquiring a lease increments the worker's active count, and
`Drop` normally releases it — **except** `park_waiting()`, which moves the lease from
active into a `waiting_tool` bucket instead of releasing on drop. This is how a
worker stays reserved for a client across a tool-result round trip without counting
against the scheduler's "how many requests can this worker still take" budget the
same way an active request does — see `docs/CAPACITY.md` §3 for why `waiting_tool`
must be tracked and reported separately from `active`.

`Scheduler::acquire(preferred)`:
1. Tries the `preferred` worker first (sticky-first — reuse the same worker for
   repeat traffic from the same tenant/session when possible).
2. Falls back to P2C double-pick: sample two workers, choose by load signal.

`Scheduler::resume(index, generation)` validates the worker's current `generation`
still matches (a worker that restarted has a new `generation` — a stale-generation
resume must fail, not silently attach to a different worker instance) and that it is
`healthy`, then moves the reservation from `waiting_tool` back to `active`.

## Session Directory (`kernel/src/session.rs`)

`SessionDirectory` is an in-memory `Mutex<HashMap>` keyed by a unit-separator-joined
`"{tenant}\u{1f}{session}"` string (chosen specifically so tenant/session values
containing arbitrary characters, including `:` or `/`, cannot collide or be spoofed
by concatenation — do not switch this to a plain `format!("{tenant}:{session}")` join).

`SessionRecord{worker_index, worker_generation, phase, expected_tool_use_ids,
continuation_token, pending_request, reserved_worker, expires_at}` is the per-session
state. `mark_waiting()` validates the serialized record size against
`config.max_session_bytes` before committing — a session record is rejected rather
than silently truncated if it grows too large (guards against unbounded tool_result
payloads bloating in-memory state).

`resume()` enforces:
- **Single-use token** — a continuation token is consumed atomically; replay is
  rejected.
- **Exact tool_use_id set match** — the `tool_result` blocks in the resume request
  must match the *sorted and deduplicated* `expected_tool_use_ids` exactly, not a
  subset or superset.
- **Not expired** — checked against `expires_at`.

`sweep_expired()` runs on a 5-second interval, spawned from `main.rs`, and is paired
with `scheduler.expire_waiting(worker_index, worker_generation)` so an expired
session's `waiting_tool` reservation is also released from the scheduler side — these
two cleanup calls must stay paired; adding a new expiry path that clears the session
without also calling `expire_waiting` will leak a scheduler reservation.

## The Simple Continuation-Token Protocol

`session.rs` issues plain opaque tokens shaped `cont_<uuid>` — a random UUID with no
embedded structure or signature. This is **a different, independent system** from
`provider/multiplex_cli/continuation.rs`'s `ContinuationToken`, which is
cryptographically signed (with a custom MAC, not HMAC — see
`multiplex-cli-subsystem.md`) and encodes `process_generation`/`slot_id`/`job_id` in
its payload. Non-multiplex providers (`mock`, `anthropic_api`, `LocalCliProvider`) use
the plain `session.rs` token; only `MultiplexCliProvider` uses the signed one. When
reasoning about "what does a continuation token contain," always check which
provider issued it — do not assume the `session.rs` shape applies to multiplex
tokens or vice versa.

## Isolation Modes (`kernel/src/config.rs`)

```rust
pub enum IsolationMode {
    ProcessPerTurn,   // KIN_ISOLATION=process — one Claude child per turn
    ResetAndReuse,    // KIN_ISOLATION=session-reset — reuse one child, /clear between turns
    Multiplexed,      // KIN_ISOLATION=subagent-pool — one CLI process, N MCP-blocked slots
}
```

`FromStr` is alias-tolerant (`"process"`/`"one-shot"`/`"process-per-turn"` all map to
`ProcessPerTurn`, etc.) — when adding a new alias, add it to the match in `config.rs`,
do not normalize aliases at call sites.

**Hard invariant**: `Config::from_env()` rejects `KIN_SLOTS_PER_WORKER > 20` with an
explicit error ("Claude official subagent cap is 20"). Do not raise this constant
without confirming the underlying Claude Code CLI's subagent concurrency limit has
actually changed — this is an external platform constraint, not an internal tuning
knob.

Default worker/slot counts differ by mode: `ProcessPerTurn` defaults to
`worker_count=4, slots_per_worker=5`; every other mode defaults to `worker_count=1,
slots_per_worker=20` (because non-`ProcessPerTurn` modes model one OS process as one
worker with many logical slots, not many OS processes).

## Verification

```bash
cargo test --manifest-path kernel/Cargo.toml scheduler::
cargo test --manifest-path kernel/Cargo.toml session::
bash scripts/smoke.sh   # exercises the full tool_use -> continuation -> tool_result round trip against mock
```
