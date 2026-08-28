# Quality Guidelines (Control)

## Required Checks

```bash
cd kin-kernel-20slot/service
gofmt -l control          # must produce no output — this is the fmt target's Go half
cd control
go vet ./...
go test ./...
```

These are exactly the Go half of the top-level `Makefile`'s `fmt`/`test-go`
targets and part of `verify`. Run `make verify` (from `kin-kernel-20slot/service/`)
for the combined Rust+Go sweep before considering control-plane work done — see
`../kernel/quality-guidelines.md` for the Rust half.

## Testing Requirements

Every package with non-trivial logic has a co-located `_test.go` file — no
separate integration-test directory, matching the kernel's inline-test
convention:

- `internal/api/server_test.go`: spins up a **real** `httptest.NewServer(New(...).Handler())`
  and drives it over actual HTTP with `net/http`'s client — `TestRegisterAndListKernel`
  (round-trip a real kernel registration), `TestSessionKeyExchangeGone` (asserts
  both the `410` status code *and* that the raw session key never appears in the
  response body). **Pattern to follow**: test through the real HTTP handler, not
  by calling `(*Server).registerKernel` directly — this exercises routing,
  decoding, and the error envelope together, the same way a real client would.
- `internal/broker/oauth_test.go`: `TestRejectSessionKey` (detection +
  non-echo), `TestRefreshGrantUsesOfficialBody` (points `Refresher.TokenURL` at a
  local `httptest.NewServer` to assert the exact outgoing request shape —
  `grant_type`, `client_id`, forwarded `refresh_token` — without any real network
  call), `TestSOCKS5Required`, `TestSpawnEnvPinsSameProxy` (asserts
  `env["ALL_PROXY"] == env["HTTPS_PROXY"]` and that no `CLAUDE_CODE_OAUTH_TOKEN`
  key leaks into the spawned CLI's env — a regression guard, not just a happy-path
  check).
- `internal/store/memory_test.go`: `TestMarkStale` — direct, no-HTTP unit test
  against `store.Memory`, appropriate here since the store has no I/O to fake.

**Pattern to follow**: prefer testing through the real boundary a caller would
use (`httptest` + real HTTP for anything reachable via `api.Server`; a local
`httptest` upstream for anything that calls out via `broker.Refresher`) over
mocking internal methods. Every test in this codebase either exercises real HTTP
plumbing or is a pure in-memory unit test — there is no mocking framework
dependency to introduce.

## Forbidden Patterns

- **No global logger or global mutable state.** Every component that logs takes
  a `*slog.Logger` via its constructor (see `logging-guidelines.md`); every
  component that stores state takes a `*store.Memory` via its constructor. Do
  not add a package-level `var` for either.
- **No new external dependency without a strong reason.** `control/go.mod` has
  exactly one non-stdlib dependency, `golang.org/x/net` (for the SOCKS5 dialer).
  Reach for the standard library (`net/http`, `encoding/json`, `log/slog`) before
  adding a router, JSON, or logging library.
- **No leaking real secrets into JSON responses or logs.** Any struct field
  carrying a raw token/credential must be tagged `json:"-"` (see
  `RefreshResult.Oauth` in `oauth.go`) or passed through `broker.Redact()` before
  it reaches a response body or log call.
- **No silently-lossy JSON decoding on admin-facing endpoints.** `registerKernel`/
  `putPolicy` use `decodeJSON`'s `DisallowUnknownFields()` + single-object check —
  keep new admin-facing POST/PUT handlers on this strict decoder rather than the
  permissive `decodeJSONAllowUnknown` (see `http-api.md` for when the permissive
  one is actually appropriate).

## Code Review Checklist

- [ ] New handler uses `writeJSON`/`writeError` for all responses — no ad hoc
      `w.Write`/`json.NewEncoder` calls outside those two helpers.
- [ ] New request struct with a secret-shaped field either omits it from JSON
      (`json:"-"`) or the handler explicitly redacts before responding/logging.
- [ ] New store mutation increments `Memory.revision` if it should be visible to
      snapshot pollers (see `store-and-reconcile.md`) — heartbeat-only updates are
      the deliberate exception.
- [ ] New background task follows the `Run(ctx, interval)` + `go` from `main.go`
      shape, respecting `ctx.Done()` for shutdown.
- [ ] `gofmt -l control` and `go vet ./...` are clean; new logic has a test that
      runs without a live network dependency (or explicitly spins up its own
      local `httptest` server if it needs one).
