# Provider Adapters

## The `Provider` Trait (`kernel/src/provider/mod.rs`)

Every backend the kernel can drive implements `Provider`. `boot()` has a default
no-op implementation — read its doc-comment before overriding it: multiplex boots
eagerly (spawns the Claude CLI process at startup, not on first request) specifically
so an HTTP client cancelling the *first* request cannot abort slot spawn and leave the
runtime half-initialized. `collect_stream()` is a shared buffering helper available to
any provider that needs to turn a `Stream` into a complete response outside the SSE
fast path.

Exactly 4 providers exist, selected by `KIN_PROVIDER` in `main.rs`:

| `KIN_PROVIDER` | Struct | File |
|---|---|---|
| `mock` | `MockProvider` | `provider/mock.rs` |
| `anthropic_api` | `AnthropicProvider` | `provider/anthropic.rs` |
| `local_cli` (non-multiplexed isolation) | `LocalCliProvider` | `provider/local_cli.rs` |
| `local_cli` (`KIN_ISOLATION=subagent-pool`) | `MultiplexCliProvider` | `provider/multiplex_cli/mod.rs` |

> **Schema note**: `contracts/kernel-config.schema.json` also lists `openai_api` as a
> valid `provider` enum value and `process_per_session` as a valid `isolation.mode`
> value. Neither has a corresponding Rust implementation — `main.rs`'s provider match
> only has arms for `mock`/`anthropic_api`/`local_cli`, and `config.rs`'s
> `IsolationMode` only has `ProcessPerTurn`/`ResetAndReuse`/`Multiplexed`. Treat these
> two schema values as **not yet implemented**, not as documented behavior, when
> writing route-policy configs or new code against the schema.

## Capability Differences

Providers report different capabilities via `capabilities()`; do not assume every
provider supports resume, native tool-wait, or cancel receipts. `AnthropicProvider` is
the least-capable: `resume: false, native_tool_wait: false, cancel_receipt: false`
(only `streaming`/`multiplex_slots` are true), because the official Anthropic
Messages API has no session/resume concept — every call is stateless from Anthropic's
perspective. `MultiplexCliProvider` reports the richest capability set, matching
its stateful CLI-backed model. **Check `capabilities()` in any code that assumes
resume or tool-wait, rather than assuming all providers behave like mock/multiplex.**

`MockProvider` (`provider/mock.rs`) is the deterministic reference implementation:
`mock_turn()` — a `tool_result` message produces `"tool result accepted for {id}:
{json}"` + `StopReason::EndTurn` (this exact string is what `scripts/smoke.sh`
asserts on); a `[use_tool:NAME]` pattern in the request text produces a `ToolUse`
block + `StopReason::ToolUse` if `NAME` is declared in `request.tools` (else
`InvalidRequest`); otherwise it echoes back a context-embedding string
(tenant/session/worker/generation/resumed) useful for verifying scheduler/session
plumbing without a real model. `mock_events()` produces a full synthetic Anthropic SSE
sequence with 24-byte text chunking — **this is the reference shape any new
provider's SSE output must match** for `StreamAssembler` to reassemble it correctly.

## `AnthropicProvider` (`provider/anthropic.rs`)

`from_env()` requires `ANTHROPIC_API_KEY`, builds a `reqwest` client with the API key
as a *sensitive* header, `anthropic-version: 2023-06-01`, a custom user-agent, a 300s
timeout, and redirects disabled. `execute_stream()` forces `stream: true` upstream
regardless of what the client requested (the official API's streaming and
non-streaming code paths differ enough that always-stream simplifies the adapter to
one path — buffering happens client-side via `into_sse()`/`collect_stream()` if the
client asked for non-streaming). 429 responses are special-cased into
`KernelError::ProviderRateLimited { retry_after }` before generic error mapping.

## `LocalCliProvider` vs. `MultiplexCliProvider`

Both drive a local Claude Code CLI binary and both register as `"local_cli"`, but they
are architecturally independent, not variants of shared code:

- **`LocalCliProvider`** (`provider/local_cli.rs`, 738 lines): blocking
  `std::process::Child` via `spawn_blocking`, one child process per session. A
  `SessionTable{parked, busy}` polling mutex (15ms backoff, 30s deadline) manages
  reuse; LRU eviction of parked children at capacity. Matches `IsolationMode::ProcessPerTurn`
  and `IsolationMode::ResetAndReuse`.
- **`MultiplexCliProvider`** (`provider/multiplex_cli/`): a single long-lived CLI OS
  process hosting up to `KIN_SLOTS_PER_WORKER` (capped at 20 — "Claude official
  subagent cap is 20", enforced in `config.rs`) MCP-blocked background slots. Matches
  `IsolationMode::Multiplexed` only. See `multiplex-cli-subsystem.md` for the full
  internal architecture.

### Repeated pattern: credential + proxy setup

`provider/local_cli.rs` and `provider/multiplex_cli/supervisor.rs` independently
implement the same `.credentials.json` writing (`claudeAiOauth` blob, `0600`
permissions, demo-fallback fake tokens when `KIN_CLAUDE_AI_OAUTH_JSON` is unset) and
the same SOCKS5-only proxy enforcement (`apply_proxy_env()` rejects a bare `KIN_SOCKS5`
value and requires `KIN_HTTPS_PROXY` pointing at an HTTP CONNECT bridge, since the
Claude CLI cannot use SOCKS5 directly as `HTTPS_PROXY`). This is a known, accepted
duplication between two independent provider implementations, not an oversight to
"fix" by merging the providers — but if you are **adding a third** CLI-driving
provider, extract this into a shared helper instead of copying it a third time (see
`.trellis/spec/guides/code-reuse-thinking-guide.md`'s "3+ times" rule).

The Go control plane has the same SOCKS5-only invariant independently:
`control/internal/broker/oauth.go`'s `NormalizeSOCKS5()` rejects bare
`HTTPS_PROXY`/upgrades `socks5://` to `socks5h://`. This is a deliberate
cross-language architectural invariant (Claude CLI credential exchange must always go
through a real SOCKS5 proxy, never a bare HTTPS forward proxy), not independent
implementation detail — keep both sides in sync if the proxy policy changes.

## Adding a New Provider

1. Implement `Provider` in a new `provider/<name>.rs` (or `provider/<name>/mod.rs` if
   it needs a subsystem like multiplex).
2. Add a `capabilities()` that honestly reflects what the backend supports — do not
   claim `resume`/`native_tool_wait`/`cancel_receipt` unless implemented.
3. Add a unit test using a synthetic/simulated backend, following
   `MultiplexCliProvider::simulated()` — see `quality-guidelines.md`.
4. Add a match arm in `main.rs`; if the new provider also needs an isolation mode,
   add the variant to `config.rs`'s `IsolationMode` first.
5. Update `contracts/kernel-config.schema.json`'s `provider` enum **only if** the
   value is genuinely implemented — do not add speculative enum values (see the
   `openai_api`/`process_per_session` note above for why that drifts).
