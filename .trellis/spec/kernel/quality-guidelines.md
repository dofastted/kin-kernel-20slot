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
- `kernel/src/provider/multiplex_cli/mod.rs`: tests covering concurrency isolation,
  tool_use resume independence, 5- and 20-parallel-slot behavior, memory-guard
  rejection, `message_start` latency decoupling, no-fake-chunking, web-search frame
  forwarding, per-job stdout metering, sink overflow/stall terminals and
  `kin_host_ready` config_hash validation. `MultiplexCliProvider::simulated(slot_count)`
  drives `simulated_cli()` over an in-memory pipe so these tests exercise the real
  `kin_*` protocol without spawning a Claude CLI process.
- `kernel/src/provider/multiplex_cli/native_protocol.rs`: frame round-trip and
  handshake tests for the stdin/stdout contract.
- `kernel/src/provider/multiplex_cli/memory_guard.rs`: tests confirming exact RSS
  admission-band boundaries.

**Pattern to follow**: when adding provider or protocol behavior, add a synthetic,
no-network, no-subprocess unit test in the same file, following the
`simulated_cli()` / `MultiplexCliProvider::simulated()` model — do not require a
live Claude CLI binary or live Anthropic API key for `cargo test` to pass.

## Forbidden Patterns

- **No `unwrap()`/`expect()` on request-path data.** Parsing of client-controlled
  JSON (headers, body fields) must go through `Option`/`Result` combinators that
  degrade to `KernelError::InvalidRequest`, mirroring `model.rs`'s lenient
  `MessageContent` deserializer and `stream.rs`'s `.and_then(Value::as_str).unwrap_or("")`
  style for *provider*-controlled JSON (frames from the CLI/API are read
  defensively with `unwrap_or` defaults, never `unwrap()`, because provider output
  is not a fully trusted contract either).
- **No blocking I/O on the async runtime** outside of `spawn_blocking`. The CLI child
  is driven through `tokio::process` + `tokio::io`; if you integrate a blocking API,
  wrap it in `tokio::task::spawn_blocking` rather than blocking the shared executor.
- **No rolling your own crypto.** The kernel currently signs nothing: the signed
  multiplex continuation token and its HMAC module were deleted with the MCP path
  (`session.rs`'s opaque `cont_<uuid>` + server-side lookup is the whole protocol).
  If a future feature needs authenticated tokens, add one HMAC-SHA256 module with
  domain separation and a hard failure on an empty secret — never a hand-rolled MAC,
  never a per-module helper.
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
