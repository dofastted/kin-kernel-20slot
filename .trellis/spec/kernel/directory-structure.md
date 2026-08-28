# Directory Structure

## Layout

```
kin-kernel-20slot/service/kernel/src/
├── main.rs              # entry point: Config -> Scheduler/SessionDirectory -> provider select -> axum::serve
├── config.rs             # Config::from_env(), IsolationMode enum + FromStr, tunable constants
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
    ├── local_cli.rs                     # LocalCliProvider — blocking one-child-per-session CLI driver
    └── multiplex_cli/                    # subagent-pool isolation: one CLI process, up to 20 MCP-blocked slots
        ├── mod.rs                          # MultiplexCliProvider, Runtime, simulate_worker(), decode_stdout()
        ├── slot.rs                          # Slot state machine (SlotPhase, cas(), bind_job())
        ├── memory_guard.rs                   # RSS admission bands (Allow/AllowSmall/Drain/Reject)
        ├── continuation.rs                    # ContinuationToken — signed, hand-rolled MAC (see multiplex-cli-subsystem.md)
        ├── mcp_server.rs                       # HTTP/SSE MCP JSON-RPC server (slot_wait/client_tool/kin_done/kin_fail)
        ├── scheduler.rs                         # SlotScheduler — sticky + ready-queue picker, distinct from top-level scheduler.rs
        ├── supervisor.rs                         # spawns the Claude CLI child, writes .credentials.json + mcp.json
        ├── bootstrap.rs                           # writes the root "spawn N kin-slot agents" prompt, waits for ready slots
        ├── job_stream.rs                           # CLI NDJSON frame -> Anthropic SSE event translation (no fake chunking)
        ├── stream_decoder.rs                       # routes NDJSON frames by parent_tool_use_id (root vs. subagent)
        ├── pending_call.rs                          # oneshot-channel registries: slot_wait / client_tool / done
        └── replay.rs                                # offline NDJSON trace replay for load/soak testing (no live CLI)
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
  `LocalCliProvider`, `MultiplexCliProvider`) and all report a `name()` string that
  matches the `KIN_PROVIDER` value that selects them — note that both `LocalCliProvider`
  and `MultiplexCliProvider` report `"local_cli"`; they are disambiguated only by
  `KIN_ISOLATION` in `main.rs` (`Multiplexed` -> `MultiplexCliProvider`, anything else
  -> `LocalCliProvider`).
- Errors are constructed via `KernelError` variants, never `anyhow`/`Box<dyn Error>`,
  inside request-handling code (see `error-handling.md`).

## Where New Code Goes

- New HTTP routes/endpoints: `api.rs`, wired into `router()`.
- New request/response fields: `model.rs`, keep the `extra: serde_json::Map` catch-all
  so unknown fields still round-trip.
- New provider-agnostic scheduling policy: `scheduler.rs`.
- New provider-agnostic session/continuation behavior: `session.rs`.
- Anything only relevant to driving the local Claude CLI in multiplex mode: under
  `provider/multiplex_cli/`, not `provider/local_cli.rs` — the two providers are
  intentionally independent implementations (see `provider-adapters.md` for why they
  are not merged despite sharing patterns like credential-file writing).
