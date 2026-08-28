# Logging Guidelines

## The Local Pattern

The kernel uses `tracing` with a JSON formatter, initialized once in `main.rs`:

```rust
// kernel/src/main.rs
tracing_subscriber::fmt()
    .with_env_filter(
        EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("kin_kernel=info,tower_http=info")),
    )
    .json()
    .init();
```

Default level is `info` for both the kernel's own target and `tower_http` (the
`api.rs` `router()` request/trace layer). Override with the standard `RUST_LOG` env
var, which `EnvFilter::try_from_default_env()` reads.

Logging is sparse and intentional — this is not a codebase that logs every function
entry/exit. Actual call sites (`kernel/src/main.rs`, `provider/multiplex_cli/mod.rs`):

- `info!` for lifecycle milestones: provider boot success, MCP server bind, CLI
  supervisor alive.
- `warn!` for recoverable anomalies: provider boot failure (kernel still starts —
  `main.rs` boots the provider in a background non-blocking task, see comment on
  `Provider::boot()` in `provider/mod.rs`), CLI process exit, `wait()` failure, a
  finished job whose client channel is full/closed.
- `error!` is used sparingly; most failure paths return `KernelError` and let
  `IntoResponse` carry the failure to the client instead of also logging it.
- Raw CLI stdout lines are forwarded through `tracing::info!(target: "kin_kernel::claude", "{line}")`
  in `provider/multiplex_cli/mod.rs` — a dedicated target so operators can filter CLI
  chatter independently of kernel logs via `RUST_LOG=kin_kernel::claude=debug` or
  similar.

## Structured Fields

Use `tracing`'s field syntax (`key = value`, or `%value`/`?value` for
Display/Debug), not string interpolation of structured data into the message:

```rust
tracing::warn!(%status, "claude process exited");
tracing::info!(mcp = %mcp_addr, "kin mcp listening");
tracing::info!(pid = supervised.pid, "claude supervisor alive");
```

The Go control plane (`control/`) follows the equivalent structured pattern with
`log/slog`, not `tracing` — see `.trellis/spec/control/logging-guidelines.md`.

## What NOT to Log

Per `docs/SECURITY.md`'s forbidden-patterns list and `docs/RUNBOOK.md` §6: never log
OAuth tokens, `claudeAiOauth` blobs, or `x-api-key`/`Authorization` header values —
not to stdout, not to a crash dump, not to chat transcripts. `provider/anthropic.rs`
builds the `x-api-key` header via `reqwest`'s sensitive-header marking so it is
excluded from `reqwest`'s own debug logging. `provider/multiplex_cli/supervisor.rs`'s
`write_oauth_file()` writes the credential JSON straight to a `0600` file — it never
routes the OAuth payload through a log call.

Do not log full request/response bodies containing user prompt text at `info` level;
this matches the same "no user prompt in logs" rule stated for HTTP error messages in
`docs/API_AND_STATE.md` §2 and `error-handling.md`.

## Common Mistakes

- Interpolating a secret or full upstream body into a `tracing::info!("{...}", body)`
  format string defeats field-based redaction — always pass secrets-adjacent data as
  a named field you control, or omit it entirely, never as free text.
- Logging at `error!` for expected, retryable conditions (e.g. a 429 from the
  provider) adds noise; those are surfaced to the client via `KernelError`'s
  `retryable` flag instead (see `error-handling.md`).
