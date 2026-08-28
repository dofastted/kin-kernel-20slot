# Kernel (Rust Data Plane) Guidelines

> Coding guidance for `kin-kernel-20slot/service/kernel/` — the Rust `axum` process that
> terminates client HTTP/SSE traffic, runs P2C scheduling, and drives one of four
> `Provider` implementations (`mock`, `anthropic_api`, `local_cli`, `multiplex_cli`).

This directory documents the kernel **as it is implemented today**. For the
as-built vs. aspirational split, see
`kin-kernel-20slot/service/docs/SOURCE_AND_PRINCIPLES.md` (as-built) vs.
`kin-kernel-20slot/service/docs/ARCHITECTURE.md` (target design — Postgres/Redis/
secret-manager infra that does not exist in this codebase yet). Rules below only
describe what the Rust source actually does.

## Guidelines Index

| Guide | Covers |
|---|---|
| [Directory Structure](./directory-structure.md) | Module layout of `kernel/src`, where to add new code |
| [API and Schema](./api-and-schema.md) | `api.rs` HTTP/SSE layer, `model.rs` request/response schema, `stream.rs` SSE assembly |
| [Provider Adapters](./provider-adapters.md) | `Provider` trait, `mock`/`anthropic_api`/`local_cli` implementations, provider selection |
| [Scheduling and Sessions](./scheduling-and-sessions.md) | P2C worker-lease scheduler, `SessionDirectory`, isolation modes, the simple continuation-token protocol |
| [Multiplex CLI Subsystem](./multiplex-cli-subsystem.md) | The `subagent-pool` isolation mode: slot state machine, memory admission control, signed continuation tokens, MCP server |
| [Error Handling](./error-handling.md) | `KernelError`, HTTP status/code/retryable mapping |
| [Logging Guidelines](./logging-guidelines.md) | `tracing` usage, JSON logs, what never gets logged |
| [Quality Guidelines](./quality-guidelines.md) | `cargo test`/`clippy` conventions, forbidden patterns found in this codebase |

## Verification

```bash
cd kin-kernel-20slot/service
cargo fmt --check --manifest-path kernel/Cargo.toml
cargo clippy --all-targets --manifest-path kernel/Cargo.toml -- -D warnings
cargo test --all-targets --manifest-path kernel/Cargo.toml
make static-check   # scripts/validate.py: required-file presence check
make smoke          # scripts/smoke.sh: end-to-end tool-loop round trip against mock
```
