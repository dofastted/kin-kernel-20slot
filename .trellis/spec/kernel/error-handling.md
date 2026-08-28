# Error Handling

## The Local Pattern

All fallible request-handling code returns `Result<T, KernelError>`
(`kernel/src/error.rs`). `KernelError` is a single flat `thiserror::Error` enum —
there is no per-module error type and no `anyhow`/`Box<dyn Error>` in request paths.
`main.rs` startup code is the one exception: `Config::from_env()` and
`AnthropicProvider::from_env()` return `Box<dyn std::error::Error>` because they run
once before the axum server exists and have no HTTP response to produce.

```rust
// kernel/src/error.rs
#[derive(Debug, Error)]
pub enum KernelError {
    #[error("{0}")]
    InvalidRequest(String),
    #[error("{0}")]
    UnsupportedFeature(String),
    #[error("no compatible capacity is currently available")]
    NoCapacity,
    #[error("runtime overloaded")]
    Overloaded { retry_after: Option<String> },
    #[error("{0}")]
    ContinuationMismatch(String),
    #[error("the bound runtime is no longer available")]
    ContinuationLost,
    #[error("provider error: {0}")]
    Provider(String),
    #[error("provider rate limit reached")]
    ProviderRateLimited { retry_after: Option<String> },
    #[error("internal error")]
    Internal,
}
```

`impl IntoResponse for KernelError` is the single place that maps every variant to an
HTTP status, a stable `code` string, and a `retryable` bool, and optionally sets
`Retry-After`:

| Variant | Status | code | retryable |
|---|---|---|---|
| `InvalidRequest` | 400 | `invalid_request` | false |
| `UnsupportedFeature` | 501 | `unsupported_feature` | false |
| `NoCapacity` | 503 | `no_capacity` | true |
| `Overloaded` | 503 | `overloaded` | true (+ `Retry-After` if set) |
| `ContinuationMismatch` | 409 | `continuation_mismatch` | false |
| `ContinuationLost` | 409 | `continuation_lost` | false |
| `Provider` | 502 | `provider_error` | true |
| `ProviderRateLimited` | 429 | `provider_rate_limited` | true (+ `Retry-After` if set) |
| `Internal` | 500 | `internal_error` | false |

The response body is a stable envelope (`{"type": "error", "error": {code, message,
retryable}}`), documented alongside the HTTP-level error code table in
`docs/API_AND_STATE.md` §2 — that table is the source of truth for expected status
codes; `error.rs` is the source of truth for the exact `code` strings.

## Propagation

Handlers in `api.rs` use `?` to bubble `KernelError` up to axum, which calls
`into_response()` automatically because `KernelError: IntoResponse`. Provider
implementations (`provider/*.rs`) construct `KernelError::Provider(...)` for upstream
failures and `KernelError::ProviderRateLimited { .. }` specifically on HTTP 429 from
the real Anthropic API (`provider/anthropic.rs`'s `execute_stream()` special-cases
429 before generic error mapping).

## Common Mistakes / Anti-Patterns

- **Don't add a new error type per module.** A new failure mode is a new
  `KernelError` variant with its own `#[error(...)]` message and its own arm in
  `into_response()`'s match — not a locally-defined struct that gets converted at the
  call site.
- **Don't invent new HTTP status codes ad hoc.** If a new variant needs a status not
  already in the table above, add it to the match in `error.rs` so the mapping stays
  in one place, not scattered across `api.rs` handlers.
- **Don't put upstream/provider error bodies or secrets into `message`.** Per
  `docs/API_AND_STATE.md` §2: "错误消息不得包含 provider token、完整上游 body 或用户
  prompt" (error messages must not contain provider tokens, full upstream response
  bodies, or user prompt text). `provider/anthropic.rs` deliberately does not forward
  the raw Anthropic error body into `KernelError::Provider`.
