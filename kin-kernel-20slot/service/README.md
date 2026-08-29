# Kin Gateway v2

Rust 数据面 + Go 控制面。对照现网：`portunex-server` 的粘性/P2C 进 Rust；`isthmus` 的「驱动本机 Claude Code、把 agent loop 伪装成无状态 API」进 `local_cli` provider。

**先读 [docs/SOURCE_AND_PRINCIPLES.md](docs/SOURCE_AND_PRINCIPLES.md)**（as-built 源码地图）。设计对照：[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

## 组件

| 目录 | 作用 |
|---|---|
| `kernel/` | Rust 热路径：Messages/Chat、P2C、continuation、mock / Anthropic API / local_cli |
| `control/` | Go：kernel 注册、心跳、drain、route snapshot、`refresh_token` 换票 |
| `contracts/` | OpenAPI、配置 schema |
| `deploy/` | Compose / Kubernetes 基线 |
| `scripts/` | smoke、静态校验、HTTP CONNECT → SOCKS5 桥 |
| `docs/` | 架构与运维 |

默认 `KIN_PROVIDER=mock`，不碰真实凭据。`local_cli` 订阅票写隔离 `.credentials.json`（`claudeAiOauth`）；setup-token 另走 `CLAUDE_CODE_OAUTH_TOKEN`。Go 对 sessionKey 换票固定 410。

```mermaid
flowchart TB
    C["API clients"] --> K["Rust kernel"]
    K --> S["Session lease"]
    K --> W["mock / anthropic_api / local_cli"]
    G["Go control"] --> K
    G -->|"refresh_token + SOCKS5"| T["platform.claude.com"]
    W --> P["Official Claude / Anthropic"]
```

## 本地启动

Rust 1.98、Go 1.27，或 Docker Compose。

```bash
make compose-up
make smoke
```

分开跑：

```bash
cd kernel && cargo run          # mock
cd control && go run ./cmd/kin-control
```

订阅 CLI（真 Claude Code）：

```bash
export KIN_PROVIDER=local_cli
export KIN_ISOLATION=subagent-pool
export KIN_WORKER_COUNT=1
export KIN_SLOTS_PER_WORKER=20
export KIN_CLAUDE_BIN=/path/to/claude
export KIN_SOCKS5='socks5h://user:pass@host:port'
export KIN_HTTPS_PROXY=http://127.0.0.1:18080
export KIN_CLAUDE_AI_OAUTH_JSON="$(cat oauth.json)"  # claudeAiOauth blob
# 或者 setup-token（inference-only，不能 refresh）：
# export KIN_CLAUDE_CODE_OAUTH_TOKEN='sk-ant-oat01-...AA'
python3 scripts/http_to_socks.py &
cargo run --manifest-path kernel/Cargo.toml
```

| 服务 | 默认地址 | 用途 |
|---|---|---|
| Rust kernel | `0.0.0.0:8080` | `/v1/messages`、`/v1/chat/completions`、健康检查 |
| Go control | `0.0.0.0:9090` | 注册、心跳、drain、策略、`/api/v1/credentials/refresh` |
| CONNECT 桥 | `127.0.0.1:18080` | CLI `HTTPS_PROXY` → 同一条 SOCKS5 |

`stream: true` 返回官方 SSE；`stream: false` 仍出站流式，内核拼完整 JSON。

## 连续 tool loop

```bash
curl -i http://127.0.0.1:8080/v1/messages \
  -H 'content-type: application/json' \
  -H 'x-tenant-id: demo' \
  -d '{
    "model":"mock-agent",
    "max_tokens":256,
    "messages":[{"role":"user","content":"[use_tool:get_weather]"}],
    "tools":[{"name":"get_weather","description":"weather","input_schema":{"type":"object"}}]
  }'
```

保留 `x-kin-session-id` 与 `x-kin-continuation`，下一轮带 `tool_result`。完整命令：`scripts/smoke.sh`。

## 边界

- 不做 sessionKey → OAuth 的 Cookie 换票。
- 不做 UA/TLS 指纹伪装、公网 SNAT 轮换。
- stock CLI 做不到「1 个 OS 进程 × 20 并行 loop」；subagent-pool = 最多 20 个进程，**一 session 一 `--session-id`**。
- 生产前缺口见 [docs/DELIVERY_STATUS.md](docs/DELIVERY_STATUS.md)。

## 阅读顺序

1. [docs/SOURCE_AND_PRINCIPLES.md](docs/SOURCE_AND_PRINCIPLES.md)
2. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
3. [docs/API_AND_STATE.md](docs/API_AND_STATE.md)
4. [docs/CREDENTIALS.md](docs/CREDENTIALS.md)
5. [docs/CAPACITY.md](docs/CAPACITY.md)
6. [docs/SECURITY.md](docs/SECURITY.md)
7. [docs/RUNBOOK.md](docs/RUNBOOK.md)
