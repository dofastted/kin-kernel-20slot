# Logging Guidelines (Control)

## The Local Pattern

`main.go` initializes exactly one `slog.Logger`, JSON-formatted, at startup:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
```

and passes it down explicitly through constructors (`api.New(store, reconciler,
snapshotTTL, logger)`, `reconcile.New(store, heartbeatTimeout, logger)`) — there is
no global/package-level logger. When adding a new component that needs to log,
accept a `*slog.Logger` as a constructor parameter and store it on the struct,
matching this pattern; do not call `slog.Default()`/`slog.Info()` package-level
functions from inside `internal/*` packages.

This mirrors the Rust kernel's single-init-in-`main`, explicit JSON-formatted
`tracing` setup — see `../kernel/logging-guidelines.md`. Keep the two sides
structurally equivalent (one process-wide structured JSON logger, injected rather
than global) even though the libraries differ.

## Actual Call Sites

Logging here is sparse, matching the kernel's philosophy — not every function
call is traced:

- `internal/api/server.go`'s `requestLog` middleware: one `logger.Info("http
  request", "method", r.Method, "path", r.URL.Path, "duration_ms", ...)` per
  request. This is the *only* per-request log line; individual handlers do not
  additionally log success.
- `internal/reconcile/reconcile.go`'s `Reconcile()`: one `logger.Warn("kernels
  marked unhealthy", "kernel_ids", stale)` — **only** emitted when
  `MarkStale` actually found newly-stale kernels; a clean reconcile pass produces
  no log line. Do not add an `Info`-level "reconcile ran, nothing stale" line —
  it would fire every `reconcileInterval` (default 5s) for no operational value.

## Structured Fields

Always pass `slog`'s key-value pairs, never format a value into the message
string:

```go
logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration_ms", ms)
logger.Warn("kernels marked unhealthy", "kernel_ids", stale)
```

not `logger.Info(fmt.Sprintf("http request %s %s", method, path))`. This keeps
log output machine-parseable as JSON and matches the field-based approach used on
the kernel side (`tracing`'s `key = value` syntax).

## What NOT to Log

Same rule as the kernel side (see `../kernel/logging-guidelines.md`): never log
OAuth access/refresh tokens or full credential blobs. `internal/broker/oauth.go`
enforces this at the type level, not just by convention — `RefreshResult.Oauth`
is tagged `json:"-"` (excluded from any JSON serialization, including if it were
ever accidentally passed to a JSON-based log handler), and only `Redact()`'d
fingerprints (`AccessFP`/`RefreshFP` — first 8 + `…` + last 6 chars) are meant to
appear in responses or logs. If you add a new log call anywhere in `broker/`,
never pass `payload.AccessToken`/`payload.RefreshToken` or the raw
`refreshToken`/`socks5` request field directly — pass `Redact(value)` or omit it.

`server_test.go`'s `TestSessionKeyExchangeGone` encodes this as an actual test
assertion (the raw session key string must not appear anywhere in the response
body) — treat that test as the enforcement mechanism for this rule, and extend it
rather than relying on manual review alone if you add a new credential-adjacent
response field.

## Common Mistakes

- Logging inside a hot loop (e.g. per-kernel inside `MarkStale`'s iteration)
  instead of once per `Reconcile()` call with the aggregated list — keep
  reconciler logging aggregate, not per-item, to avoid log volume scaling with
  kernel count.
- Adding a logger parameter to `internal/store/memory.go` — the store is
  deliberately silent; state-change visibility comes from the caller (`api`
  handlers, `reconcile`) logging around the store call, not the store logging
  itself. Keep `store.Memory` a pure data structure with no logging dependency.
