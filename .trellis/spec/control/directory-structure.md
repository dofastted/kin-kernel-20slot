# Directory Structure

## Layout

```
kin-kernel-20slot/service/control/
├── go.mod                          # module kin.local/kin-control, go 1.27.0, one dep: golang.org/x/net
├── cmd/kin-control/main.go          # entry point: env config -> store -> reconciler -> api.Server -> http.Server
└── internal/
    ├── model/model.go                # Kernel, KernelRegistration, RoutePolicy, Snapshot — shared wire/domain types
    ├── store/
    │   ├── memory.go                   # Memory — in-memory kernel/policy store, the ONLY persistence layer that exists
    │   └── memory_test.go
    ├── reconcile/reconcile.go          # Reconciler — periodic + on-demand stale-kernel marking
    ├── broker/
    │   ├── oauth.go                      # Refresher — real OAuth refresh_token exchange, sessionKey rejection, SOCKS5 policy
    │   └── oauth_test.go
    └── api/
        ├── server.go                     # Server — http.Handler, all routes, request validation, error envelope
        └── server_test.go
```

Everything under `internal/` is intentionally unexported outside the module — this is
a single deployable binary (`cmd/kin-control`), not a library other services import.
Do not add a `pkg/` directory to make internals importable elsewhere unless a second
consumer binary is actually being added to this module.

## Module Organization

One package per responsibility, matching the table in `index.md`. When adding a new
concern:

- New control-plane HTTP endpoint -> `internal/api/server.go`, following the existing
  route + validate + store-call + JSON-response pattern (see `http-api.md`).
- New kernel/policy state or query -> `internal/store/memory.go`, keeping all
  mutation behind `Memory`'s mutex (see `store-and-reconcile.md`).
- New credential/OAuth behavior -> `internal/broker/oauth.go`.
- New periodic background task -> `internal/reconcile/reconcile.go`, following
  `Reconciler.Run(ctx, interval)`'s ticker-loop shape, started from `main.go` via
  `go reconciler.Run(ctx, reconcileInterval)`.

## Naming Conventions

- Types shared between packages (wire format + domain state) live in
  `internal/model`, not duplicated per package. `api/server.go` and `store/memory.go`
  both import `internal/model` rather than each defining their own `Kernel` struct.
- Constructors are `New(...)` per package (`store.NewMemory()`, `reconcile.New(...)`,
  `api.New(...)`), not `NewMemoryStore()`/`NewReconciler()` — the package name already
  disambiguates.
