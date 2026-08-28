# Quality Guidelines (Kernel)

## Required Checks

```bash
cd kin-kernel-20slot/service
cargo fmt --check --manifest-path kernel/Cargo.toml
cargo clippy --all-targets --manifest-path kernel/Cargo.toml -- -D warnings
cargo test --all-targets --manifest-path kernel/Cargo.toml
```

These are exactly the `fmt`/`test-rust`/part-of-`verify` targets in the top-level
`Makefile`. Run `make verify` for the full static-check + fmt + test-rust + test-go +
clippy + `go vet` sweep before considering kernel work done.

## Testing Requirements

Every provider and every non-trivial protocol module has inline `#[cfg(test)] mod
tests` in the same file — there is no separate `tests/` integration directory for the
kernel. Examples already in the codebase:

- `kernel/src/stream.rs`: `concatenates_text_deltas`, `parses_sse_data_block` —
  assert `StreamAssembler` behavior directly against synthetic SSE JSON, not against
  a live provider.
- `kernel/src/provider/multiplex_cli/mod.rs`: 8 tests covering concurrency isolation,
  resume independence, stale-generation rejection, 20-parallel-slot uniqueness,
  memory-guard rejection, `message_start` latency decoupling, no-fake-chunking
  guarantee, and web-search sentinel-leak prevention. `MultiplexCliProvider::simulated(slot_count)`
  exists specifically so these tests never spawn a real Claude CLI process.
- `kernel/src/provider/multiplex_cli/continuation.rs`: one round-trip + tamper +
  generation-mismatch test for the signed token.
- `kernel/src/provider/multiplex_cli/memory_guard.rs`: two tests confirming exact RSS
  admission-band boundaries.

**Pattern to follow**: when adding provider or protocol behavior, add a synthetic,
no-network, no-subprocess unit test in the same file, following the
`simulate_worker()` / `MultiplexCliProvider::simulated()` model — do not require a
live Claude CLI binary or live Anthropic API key for `cargo test` to pass.

## Forbidden Patterns

- **No `unwrap()`/`expect()` on request-path data.** Parsing of client-controlled
  JSON (headers, body fields) must go through `Option`/`Result` combinators that
  degrade to `KernelError::InvalidRequest`, mirroring `model.rs`'s lenient
  `MessageContent` deserializer and `stream.rs`'s `.and_then(Value::as_str).unwrap_or("")`
  style for *provider*-controlled JSON (frames from the CLI/API are read
  defensively with `unwrap_or` defaults, never `unwrap()`, because provider output
  is not a fully trusted contract either).
- **No blocking I/O on the async runtime** outside of `spawn_blocking`.
  `provider/local_cli.rs` deliberately drives `std::process::Child` inside
  `tokio::task::spawn_blocking` rather than switching everything to
  `tokio::process` — follow that pattern for any new blocking-API integration rather
  than blocking the shared Tokio executor.
- **No rolling your own crypto without saying so.** `provider/multiplex_cli/continuation.rs`'s
  `mac()` is a hand-rolled, non-standard mixing function (not HMAC). If you touch it,
  keep the doc-comment explicit that it is not a standard MAC and that its security
  depends entirely on `secret` being non-empty and unpredictable (`mac()` returns an
  all-zero MAC if `secret.is_empty()` — never let that config path go unchecked in
  production).
- **No provider-name branching outside `main.rs` and the provider's own module.**
  `api.rs` calls `provider.execute_stream(...)` through the `Provider` trait; it does
  not match on `provider.name()`.

## Code Review Checklist

- [ ] New `Result`-returning code uses `KernelError`, not a local error type or
      `anyhow`.
- [ ] New provider logic has a `#[cfg(test)]` unit test that runs without a live CLI
      or live upstream API.
- [ ] Any new blocking syscall/process spawn on the request path is wrapped in
      `spawn_blocking` (or lives inside `provider/multiplex_cli`'s already-async
      `tokio::process` usage).
- [ ] No secret (OAuth token, API key) is passed to a `tracing::*!` call as free text.
- [ ] `cargo clippy -- -D warnings` passes — warnings are treated as errors in `make verify`.
