# In-Memory Store and Reconciliation

## `internal/store/memory.go`

`Memory{mu sync.RWMutex, kernels map[string]model.Kernel, policies
map[string]model.RoutePolicy, revision uint64}` is the **entire** persistence
layer for this service today — there is no database, no file-backed snapshot, no
WAL. A process restart loses all registered kernels and route policies (kernels
re-register on their own heartbeat loop; route policies do not currently have an
external source of truth to reload from). Do not write code elsewhere that
assumes state survives a restart.

Every method takes the lock for its own duration and returns copies (not
pointers into the map) — e.g. `ListKernels()` builds a fresh sorted slice under
`RLock`. This means callers can safely mutate a returned `Kernel`/`RoutePolicy`
value without corrupting store state, and there is no risk of a caller holding a
reference across a later mutating call. Keep new methods on this pattern: take
the lock, copy out, return; never return a pointer into `m.kernels`/`m.policies`.

`revision` is a single monotonic `uint64`, incremented on every kernel upsert,
drain, and policy write (but **not** on heartbeat — a heartbeat updates
`LastHeartbeat`/`Status` without bumping `revision`, since a heartbeat is not a
config-shape change). `Server.snapshot()` reads `Revision()` to stamp
`model.Snapshot.Revision` — this is what a route-policy-consuming client polls to
detect "did anything I care about change" without diffing the full kernel/policy
lists. If you add a new field to `Kernel`/`RoutePolicy` that route-policy clients
need to react to, bump `revision` on its write path the same way `PutPolicy`/
`UpsertKernel`/`SetDraining` do — a mutation that forgets to bump `revision` is
invisible to snapshot pollers.

`MarkStale(now, timeout)` is the only method that mutates *based on a predicate*
rather than a single explicit ID — it walks every kernel and marks
`KernelUnhealthy` if `now.Sub(LastHeartbeat) > timeout`, returning the list of
newly-marked IDs (an already-unhealthy kernel is skipped, not re-added to the
return list, so callers can log "these just went stale" rather than "these are
currently stale").

Sort order is `ID`/`Name` string-ascending everywhere (`ListKernels`,
`ListPolicies`, `MarkStale`'s returned slice) — deterministic list output was a
deliberate choice for stable API responses and stable test assertions; don't
introduce map-iteration-order-dependent output in a new list method.

## `internal/reconcile/reconcile.go`

`Reconciler{store, heartbeatTimeout, logger}` has exactly one piece of logic:
`Reconcile(now)` calls `store.MarkStale(now, heartbeatTimeout)` and logs a single
`Warn` with the stale kernel IDs if any were found (no log line at all if none —
avoid logging a no-op reconcile pass). `Result{RanAt, StaleKernelIDs}` is returned
both to `Server.reconcileNow()`'s `POST /api/v1/reconcile` (on-demand) and thrown
away by `Run()`'s ticker loop (periodic) — the periodic path relies on the `Warn`
log for observability, not a return value, since nothing consumes `Run()`'s loop
output.

`Run(ctx, interval)` is a plain `time.NewTicker` select loop, exiting on
`ctx.Done()`. It is started exactly once, from `main.go`:

```go
go reconciler.Run(ctx, reconcileInterval)
```

alongside `signal.NotifyContext`-based graceful shutdown — `ctx` cancellation on
SIGINT/SIGTERM stops the ticker loop cleanly. If you add a second periodic
background task, follow this same shape (a `Run(ctx, interval)` method started
with `go` from `main.go`, not a package-level goroutine started from `init()` or
a constructor) so shutdown ordering stays explicit and visible in one place.

## Why On-Demand *and* Periodic

`POST /api/v1/reconcile` exists so an operator (or a test, or `scripts/smoke.sh`-
style tooling) can force an immediate stale-check without waiting for the next
tick — useful right after intentionally killing a kernel in a manual test. It
calls the exact same `Reconcile()` method the ticker calls; there is no separate
"forced" code path to keep in sync.
