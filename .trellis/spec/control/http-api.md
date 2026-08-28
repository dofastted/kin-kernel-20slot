# HTTP API (`internal/api/server.go`)

## Route Table

```
GET  /healthz
GET  /api/v1/kernels
POST /api/v1/kernels
POST /api/v1/kernels/{id}/heartbeat
POST /api/v1/kernels/{id}/drain
GET  /api/v1/route-policies/{name}
PUT  /api/v1/route-policies/{name}
GET  /api/v1/snapshots/current
POST /api/v1/reconcile
POST /api/v1/credentials/exchange
POST /api/v1/credentials/refresh
```

Routes are registered in `routes()` on a plain `http.ServeMux` using Go 1.22+
method-prefixed patterns (`"POST /api/v1/kernels"`) and `{id}`/`{name}` wildcards
read back via `r.PathValue(...)`. Do not introduce a third-party router — the
standard-library mux already covers every pattern this service needs.

`Server.Handler()` wraps the mux in exactly one middleware, `requestLog`, which logs
method/path/duration after every request. Add new cross-cutting behavior (auth,
rate limiting, etc.) as an additional wrapper here, not by duplicating logic inside
every handler.

## Request Decoding: Strict vs. Lenient

Two decode helpers exist, and the choice is deliberate — do not use the wrong one
for a new handler:

- **`decodeJSON`** (`registerKernel`, `putPolicy`): `DisallowUnknownFields()` +
  rejects trailing data after the first JSON object. Use for kernel/registry
  admin bodies where a typo'd field should fail loudly rather than be silently
  ignored.
- **`decodeJSONAllowUnknown`** (`exchange`, `refresh`): permissive decode with only
  the 1MiB `MaxBytesReader` cap. Used specifically for `broker.ExchangeRequest`
  because that struct intentionally has *more* fields than any single call site
  sends (`session_key` vs `sessionKey` vs `cookie` vs `refresh_token` — see
  `oauth-broker.md`), so strict unknown-field rejection would break legitimate
  clients using an alternate field name.

Both helpers cap the request body at `1<<20` (1MiB) via `http.MaxBytesReader` —
match this when adding a new POST/PUT handler; there is no global body-size
middleware, each handler opts in individually.

## Validation

`validateRegistration`/`validatePolicy` run *after* decode, before any store
mutation. Both delegate character-class checking to `validName()`: lowercase
letters, digits, `-._`, 1–128 chars. This is deliberately restrictive (no
uppercase, no spaces) because kernel IDs and policy names are used as map keys
and log fields — keep new identifier-shaped fields on the same `validName()` check
rather than inventing a looser pattern per field.

`putPolicy` cross-checks the path `{name}` against the body's `Name` field (filling
it in if the body omitted it, rejecting if they conflict) — this is the pattern for
any future `PUT /resource/{id}` handler: path is authoritative, body may omit the
identifying field but must not contradict it.

## Error Envelope

```go
writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
```

produces:

```json
{"type":"error","error":{"code":"invalid_request","message":"...","retryable":false}}
```

`retryable` is derived automatically from the status code (`status >= 500`) —
never set it by hand, and never invent a new envelope shape for a new handler.
This mirrors the Rust kernel's `ErrorEnvelope`/`ErrorBody` shape in
`kernel/src/error.rs` (see `../kernel/error-handling.md`) — keep the two envelopes
structurally aligned since clients may talk to both services.

`writeJSON`/`writeError` are the only two response-writing functions in this
package; add a third only if a genuinely new response shape (not just a new
status code) is needed.

## Credential Endpoints Are Intentionally Not Symmetric

`exchange` **always** returns `410 Gone` (`rejected_interchange`) — the
cookie/`sessionKey` → OAuth interchange path is explicitly unimplemented (see
`oauth-broker.md`'s `Alignment` table). `refresh` is the only endpoint that
actually calls upstream, and only after `SessionKeyPresent()` confirms the
request is not attempting the same rejected path via the `refresh` route. Do not
"fix" `exchange` to also accept `refresh_token` bodies — the split exists so a
client that tries the legacy cookie flow gets an unambiguous, non-silent
rejection at its own endpoint.

## Testing Pattern

`server_test.go` spins up a real `httptest.NewServer(New(...).Handler())` and
drives it with the standard `net/http` client rather than mocking `Server`'s
internals — e.g. `TestRegisterAndListKernel` POSTs a real JSON body and asserts on
the parsed response, `TestSessionKeyExchangeGone` asserts both the `410` status
*and* that the raw session key string never appears in the response body (a
secret-leak regression check, not just a status check). Follow this
black-box-over-HTTP pattern for new handler tests rather than calling handler
methods directly — see `quality-guidelines.md`.
