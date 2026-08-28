# Control (Go Control Plane) Guidelines

> Coding guidance for `kin-kernel-20slot/service/control/` — the Go `net/http`
> service that tracks kernel registration/heartbeat/health, serves route-policy
> snapshots, and performs the real OAuth `refresh_token` exchange over SOCKS5.

This is a small, deliberately thin service: module `kin.local/kin-control`, Go
1.27.0, a single external dependency (`golang.org/x/net`) per `control/go.mod`. No
ORM, no database driver, no Postgres/Redis client exist in this codebase yet — the
in-memory store (`internal/store/memory.go`) is the only persistence layer that
actually exists today. `docs/DELIVERY_STATUS.md`'s "生产前 P0" table lists a Postgres
migration as a pre-production gap, not something already implemented; do not write
guidance here as if Postgres integration already exists.

## Guidelines Index

| Guide | Covers |
|---|---|
| [Directory Structure](./directory-structure.md) | `control/` package layout |
| [HTTP API](./http-api.md) | `internal/api/server.go` routes, request validation, error envelope |
| [OAuth Broker](./oauth-broker.md) | `internal/broker/oauth.go` — real `refresh_token` exchange, `sessionKey` rejection, SOCKS5 policy |
| [In-Memory Store and Reconciliation](./store-and-reconcile.md) | `internal/store/memory.go`, `internal/reconcile/reconcile.go` |
| [Logging Guidelines](./logging-guidelines.md) | `log/slog` usage |
| [Quality Guidelines](./quality-guidelines.md) | `go test`/`go vet`/`gofmt` conventions, test patterns already in the repo |

## Verification

```bash
cd kin-kernel-20slot/service
gofmt -l control        # must produce no output
cd control && go vet ./...
go test ./...
```
