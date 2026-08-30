# Directory Structure

## Layout

```
kin-kernel-20slot/service/kernel/src/
├── main.rs              # entry point: Config -> Scheduler/SessionDirectory -> provider select -> axum::serve
├── config.rs             # Config::from_env(), tunable constants
├── state.rs               # AppState{config, scheduler, sessions, provider} — axum State payload
├── error.rs                # KernelError enum + IntoResponse (HTTP status/code/retryable mapping)
├── model.rs                  # MessageRequest/MessageResponse/ContentBlock/StopReason + OpenAI Chat* family
├── stream.rs                   # StreamAssembler (SSE event -> ContentBlock accumulation), SSE parsing helpers
├── scheduler.rs                  # P2C Scheduler, Worker, WorkerLease (top-level; NOT the multiplex-internal one)
├── session.rs                     # SessionDirectory, SessionRecord, the simple continuation-token protocol
├── api.rs                          # axum router, /v1/messages + /v1/chat/completions handlers, SSE encoding
└── provider/
    ├── mod.rs                        # Provider trait, boot()/collect_stream() shared helpers
    ├── mock.rs                        # MockProvider — deterministic, no external calls
    ├── anthropic.rs                    # AnthropicProvider — official Messages API, forces stream:true upstream
    └── multiplex_cli/                    # the only CLI path: one patched CLI process, up to 20 stateless slots
        ├── mod.rs                          # MultiplexCliProvider, Runtime, simulated_cli(), decode_stdout()
        ├── native_protocol.rs                # kin_* stdin/stdout frames + stdout byte caps
        ├── job.rs                             # Job{job_id, slot_id, request} + new_id()
        ├── slot.rs                             # Slot state machine (SlotPhase, bind_job(), should_retire())
        ├── memory_guard.rs                       # RSS admission bands (Allow/AllowSmall/Drain/Reject)
        ├── scheduler.rs                           # SlotScheduler — sticky + ready-queue picker, distinct from top-level scheduler.rs
        ├── supervisor.rs                            # spawns the patched CLI child, writes .credentials.json, proxy env
        ├── bootstrap.rs                              # wait_ready(): polls ready_slots() until the CLI registered N slots
        └── envelope.rs                                # console-managed system layout + timezone envelope
```

## Module Organization Rule

Each file under `provider/` owns exactly one `Provider` implementation (or, for
`multiplex_cli/`, one cohesive subsystem of a single implementation). When adding a
new provider, add a new `provider/<name>.rs`, implement the `Provider` trait from
`provider/mod.rs`, and add a match arm in `main.rs`'s provider-selection block —
do not branch on provider name inside `api.rs` or `model.rs`. `api.rs`, `model.rs`,
`stream.rs`, `scheduler.rs`, and `session.rs` are provider-agnostic; provider-specific
protocol quirks (e.g. multiplex's signed continuation tokens vs. `session.rs`'s plain
`cont_<uuid>` strings) stay inside the owning provider module.

## Naming Conventions

- Provider structs are named `<Name>Provider` (`MockProvider`, `AnthropicProvider`,
  `MultiplexCliProvider`) and all report a `name()` string that matches the
  `KIN_PROVIDER` value that selects them — `MultiplexCliProvider` reports
  `"local_cli"` because that is still the operator-facing value for "drive the local
  Claude CLI"; the per-turn-process provider that used to share the name is gone.
- Errors are constructed via `KernelError` variants, never `anyhow`/`Box<dyn Error>`,
  inside request-handling code (see `error-handling.md`).

## Where New Code Goes

- New HTTP routes/endpoints: `api.rs`, wired into `router()`.
- New request/response fields: `model.rs`, keep the `extra: serde_json::Map` catch-all
  so unknown fields still round-trip.
- New provider-agnostic scheduling policy: `scheduler.rs`.
- New provider-agnostic session/continuation behavior: `session.rs`.
- Anything only relevant to driving the local Claude CLI: under
  `provider/multiplex_cli/`. Anything that would need Rust to originate an upstream
  request or re-decode the CLI's stream belongs in the CLI patch instead
  (`patches/claude-code/`), not in a second in-kernel data plane.
