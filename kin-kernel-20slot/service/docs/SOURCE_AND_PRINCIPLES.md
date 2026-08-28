# 源码地图与技术原理（As-built）

对照现网：`portunex-server` 做转发/粘性/P2C；`isthmus` 驱动本机 Claude Code CLI，把有状态 agent loop 伪装成无状态 HTTP。本仓库拆成 **Rust 数据面内核** + **Go 控制面** + **脚本/面板**。

本文描述**已经写进源码的行为**，不是愿景。设计对照见 [ARCHITECTURE.md](ARCHITECTURE.md)。

## 1. 一句话

一个 Rust kernel 拉起最多 N 个真实 `claude` 子进程；每个入站 `session_id` 对应一个 `--session-id`；CLI 用 `claudeAiOauth` 走订阅额度出站；官方永远是流式；用户非流式时内核把官方流拼成完整 JSON。Go 不在推理热路径上，只做 kernel 编排和 `refresh_token` 换票。

```mermaid
flowchart TB
  Client["Client /v1/messages"] --> Kernel["kin-kernel Rust"]
  Panel["Control panel"] --> Control["kin-control Go"]
  Control -->|"refresh_token + SOCKS5"| Token["platform.claude.com/v1/oauth/token"]
  Control -->|"secret handle / metadata"| Kernel
  Kernel --> Sched["P2C + slot lease"]
  Sched --> CLI["claude CLI stream-json"]
  CLI --> Bridge["HTTP CONNECT bridge"]
  Bridge --> Socks["SOCKS5 egress"]
  Socks --> Anthropic["api.anthropic.com"]
```

## 2. 源码地图

| 路径 | 语言 | 职责 |
|---|---|---|
| [service/kernel/src/main.rs](../kernel/src/main.rs) | Rust | 进程入口：选 provider、绑调度器、HTTP serve |
| [service/kernel/src/api.rs](../kernel/src/api.rs) | Rust | `/v1/messages`、`/v1/chat/completions`、SSE/JSON |
| [service/kernel/src/scheduler.rs](../kernel/src/scheduler.rs) | Rust | sticky + P2C，active / waiting_tool 容量 |
| [service/kernel/src/session.rs](../kernel/src/session.rs) | Rust | continuation token、tenant/session CAS |
| [service/kernel/src/stream.rs](../kernel/src/stream.rs) | Rust | 官方 SSE / stream_event 拼装 |
| [service/kernel/src/provider/local_cli.rs](../kernel/src/provider/local_cli.rs) | Rust | 真 Claude Code：spawn、stdin 驱动、session 隔离 |
| [service/kernel/src/provider/anthropic.rs](../kernel/src/provider/anthropic.rs) | Rust | 官方 Messages API，出站强制 `stream:true` |
| [service/kernel/src/provider/mock.rs](../kernel/src/provider/mock.rs) | Rust | 契约测试，合成 Anthropic 事件 |
| [service/control/cmd/kin-control/main.go](../control/cmd/kin-control/main.go) | Go | 控制面入口 |
| [service/control/internal/api/server.go](../control/internal/api/server.go) | Go | 注册/心跳/drain/策略/换票 API |
| [service/control/internal/broker/oauth.go](../control/internal/broker/oauth.go) | Go | `refresh_token` + 固定 SOCKS5；拒绝 sessionKey |
| [service/control/internal/reconcile/reconcile.go](../control/internal/reconcile/reconcile.go) | Go | stale kernel 回收 |
| [service/control/internal/store/memory.go](../control/internal/store/memory.go) | Go | demo 内存 desired state |
| [scripts/kin-node-kernel/](../../scripts/kin-node-kernel/) | Node | Rust kernel 的协议孪生，面板实验室用 |
| [service/scripts/smoke.sh](../scripts/smoke.sh) | Shell | mock 两轮 tool loop |
| [service/scripts/http_to_socks.py](../scripts/http_to_socks.py) | Python | CLI 不能直连 SOCKS5，CONNECT 桥接到同一条 SOCKS5 |
| [src/routes/](../../src/routes/) | TS | 控制面板：凭据、容量、CLI 实验室、审计 |

## 3. Rust 内核

### 3.1 模块

```
api ──► scheduler.acquire / resume
     ──► provider.execute_stream
     ──► session.mark_waiting | mark_ready
     ──► SSE 透传 或 collect_stream 拼 JSON
```

Provider 由 `KIN_PROVIDER` 选择：`mock` | `anthropic_api` | `local_cli`。

### 3.2 请求契约

入口：`POST /v1/messages`（Anthropic）或 `POST /v1/chat/completions`（OpenAI 子集）。

| Header | 作用 |
|---|---|
| `x-tenant-id` | 租户（demo 可自报；生产必须从认证注入） |
| `x-kin-session-id` | 会话。缺省生成 UUID。映射为 CLI `--session-id` |
| `x-kin-continuation` | tool_result 绑回等待中的 loop |

响应头：`x-kin-session-id`、`x-kin-continuation`（tool_use 时）、`x-kin-slot`、`x-kin-pid`、`x-kin-generation`。

流式：`stream: true` → `text/event-stream`（`message_start` / `content_block_delta` / `message_stop` / `kin.done`）。
非流式：内核仍消费官方流，拼成一条 `message` JSON。

### 3.3 调度与 slot

一个 kernel 进程 = 一个调度器。默认 `KIN_ISOLATION=subagent-pool`：

- `KIN_WORKER_COUNT=1`（一个 runtime）
- `KIN_SLOTS_PER_WORKER=20`（逻辑并发上限，对应现网 `--max-procs=20`）

Stock Claude CLI **不能**把多条 HTTP 请求写进同一个 stdin。stdin 属于 Root Agent 主会话，消息会排队。并行来自：

> **1 个 Claude OS 进程 + N 个后台 `kin-slot` Subagent + Rust Streamable HTTP MCP**。每个 Slot 阻塞在 `slot_wait()`；请求到达后 Rust 唤醒空闲 Slot。分流靠 stream-json 的 `parent_tool_use_id`。默认 POC 为 5 Slot。

源码：[`kernel/src/provider/multiplex_cli/`](../kernel/src/provider/multiplex_cli/)。`KIN_ISOLATION=process` 仍走旧的每请求子进程。

| `KIN_ISOLATION` | 行为 |
|---|---|
| `process` | 每 turn 新进程，end_turn 退休 |
| `session-reset` | 同 session 复用，非 resume 先 `/clear` |
| `subagent-pool` | session 表保活，默认 1×20 |

Sticky：同一 session 优先回到上次 worker。P2C 在兼容候选里比 utilization / latency EWMA / error EWMA。

tool_use 时 worker 从 `active` 转到 `waiting_tool`，容量仍被占住，直到 continuation 消费或 TTL 回收。

### 3.3.1 内嵌 Messages Relay（subagent-pool 专属）

CLI 2.1.x 不对 subagent 发 `stream_event` 增量帧，stdout 只有整块文本。Relay 让用户流改从 Anthropic upstream SSE 拿逐 token 正文；stdout 降级为控制面与兜底。

源码：[`kernel/src/provider/multiplex_cli/relay/`](../kernel/src/provider/multiplex_cli/relay/)（server 反代 / upstream 出站 / correlate 关联 / sse_tap 解码过滤 / arbiter 正文仲裁 / metrics）。签名统一在 [`signing.rs`](../kernel/src/provider/multiplex_cli/signing.rs)（HMAC-SHA256，`kin/kct/v1` 与 `kin/krc/v1` 域隔离）。

| `KIN_RELAY_MODE` | 行为 |
|---|---|
| 未设置 / `off` | 不启动 Relay、不注入 `ANTHROPIC_BASE_URL`，与旧行为一致 |
| `observe` | CLI 经 Relay 出站；用户正文仍来自 stdout；tap 只累计 SHA-256 摘要对比（`digest_mismatch`） |
| `authoritative` | upstream text/thinking delta 是正文权威；stdout 正文被抑制，仅兜底 |
| 非法值 | 启动即报错退出，禁止静默降级 |

关键机制：

- **启动顺序**：`observe`/`authoritative` 下 Relay 先起并通过 `/healthz` 自检，失败则内核不 Ready、CLI 不启动。回滚 = 改回 `off` 重启（CLI 启动后无法撤销已注入的 Base URL）。
- **双消费者**：upstream 字节原样流回 CLI（网络背压驱动）；tap 走独立有界队列（256 条 / 2 MiB），溢出只 poison 用户支路并计 `tap_dropped`，**永不阻塞 CLI 消费**。上游非 2xx 原样回 CLI 且不进 tap。
- **请求↔job 关联**：`slot_wait` 的 job 响应携带签名 `relay_context`（`krc_` token）；它随 subagent transcript 出现在每次内部 `/v1/messages` body 里，Relay 流式扫描（跨 chunk、单 token ≤2 KiB）并做五重校验（HMAC / generation / job 存在 / slot_id 一致 / slot 仍归属）。无有效关联的请求只转发不 tap。
- **SourceArbiter**：`NoBody → UpstreamActive | StdoutFallback → Completed` 单向；UpstreamActive 中 tap poison → 显式失败终止（不降级、不伪成功）。最终正文优先级：权威 upstream 累计 > stdout > `kin_done.fallback_content`。
- **网络指纹取舍**：Anthropic 侧看到的 TLS/HTTP 客户端是 Rust `reqwest/rustls`，不再是 CLI/Node 指纹——Relay 做的是"应用层请求特征对齐"（headers/body 原样），不是完整网络指纹透传。

`/healthz` 的 `relay` 字段暴露 `{relay_mode, relay_healthy, tap_dropped, digest_mismatch}`。

### 3.4 local_cli 出站

`spawn_parked` 参数（[local_cli.rs](../kernel/src/provider/local_cli.rs)）：

```
claude -p
  --output-format stream-json
  --input-format stream-json
  --verbose --include-partial-messages --replay-user-messages
  --permission-mode acceptEdits
  --no-session-persistence
  --session-id <uuid>
  --model <request.model>
```

工作目录 = `CLAUDE_CONFIG_DIR` = `/tmp/kin-cli/<tenant>/<cli-session-uuid>/`，写入 `.credentials.json`（0600）：

```json
{ "claudeAiOauth": { "accessToken", "refreshToken", "expiresAt", "scopes": [
  "user:profile", "user:inference", "user:sessions:claude_code",
  "user:mcp_servers", "user:file_upload"
] } }
```

**不**设置 `CLAUDE_CODE_OAUTH_TOKEN` / `ANTHROPIC_API_KEY`。那是 inference-only / API key 路径，不是订阅 CLI。

CLI 读 stdin NDJSON `{"type":"user","session_id":"...","message":{...}}`，stdout 吐 `stream_event`（内嵌官方 SSE）→ `assistant` → `result`。内核把 `stream_event.event` 立刻推给 HTTP 客户端。

### 3.5 代理

Claude CLI 把 `HTTPS_PROXY` 当 HTTP CONNECT，**不能**把 `socks5://` 塞进去。

正确链路：

```
KIN_SOCKS5=socks5h://user:pass@host:port     # Go 换票直连
python3 service/scripts/http_to_socks.py     # 读同一条 SOCKS5
KIN_HTTPS_PROXY=http://127.0.0.1:18080       # 注入 CLI
```

换票和推理必须同一条 SOCKS5，否则 egress IP 漂、账号风控。

### 3.6 流式拼装

[stream.rs](../kernel/src/stream.rs) 的 `StreamAssembler`：

- `content_block_delta.text_delta` 追加文本
- `input_json_delta` 拼 tool_use.input
- `assistant` 帧覆盖为权威 content（tool_use 边界）
- `result` 补 usage；用户非流式时 `finish()` 成完整 `MessageResponse`

实测（同 prompt、sonnet-5）：用户 SSE 首字约 2.3s，整段约 5.9s；非流式整包约 6.6s。官方 CLI 单路完成约 6.8s。内核单路开销大约 +1s。一个 Claude 进程 RSS ~210MB。

## 4. Go 控制面

不代理推理。职责：

| 路由 | 行为 |
|---|---|
| `POST /api/v1/kernels` | 注册 observed kernel |
| `POST /api/v1/kernels/{id}/heartbeat` | 续活 |
| `POST /api/v1/kernels/{id}/drain` | 摘流 |
| `PUT /api/v1/route-policies/{name}` | 租户/模型/隔离策略 |
| `GET /api/v1/snapshots/current` | 版本化 desired+observed |
| `POST /api/v1/reconcile` | 立刻扫 stale |
| `POST /api/v1/credentials/exchange` | **410**。sessionKey / Cookie 换票不做 |
| `POST /api/v1/credentials/refresh` | `refresh_token` + 必填 `socks5` |

### 4.1 换票对齐

Portunex 五步（sessionKey Cookie → authorize → token → bootstrap → grove）**整条拒绝**。官方 Claude Code 从不 POST sessionKey；它打开浏览器 `claude.com/cai/oauth/authorize`，callback 后落 `claudeAiOauth`。

Go 只实现 CLI 源码 `refreshOAuthToken` 那一跳：

```
POST platform.claude.com/v1/oauth/token
grant_type=refresh_token
client_id=9d1c250a-e61b-44d9-88ed-5944d1962f5e
scope=user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload
```

出站 `DialContext` 绑调用方给的那条 SOCKS5。响应只回 fingerprint，不把 access/refresh 打进日志。bootstrap / Grove 交给随后拉起的 CLI。

操作员把 `/login` 得到的 `claudeAiOauth` 交给 secret manager；kernel 领取后写隔离 `CLAUDE_CONFIG_DIR`。

## 5. 脚本

| 脚本 | 原理 |
|---|---|
| [scripts/kin-node-kernel/engine.mjs](../../scripts/kin-node-kernel/engine.mjs) | Node 版 lease + spawn。面板 `/cli` 实验室走它，协议与 Rust 对齐 |
| [scripts/kin-node-kernel/protocol.mjs](../../scripts/kin-node-kernel/protocol.mjs) | argv、user frame、assistant 映射、redact |
| [scripts/kin-node-kernel/mock-claude.mjs](../../scripts/kin-node-kernel/mock-claude.mjs) | 无登录时的 stream-json 替身；chunk 出 `stream_event` |
| [scripts/kin-node-kernel/mcp-bridge.mjs](../../scripts/kin-node-kernel/mcp-bridge.mjs) | tool_result 经 MCP `result.json` 绑回真 CLI |
| [scripts/kin-node-kernel/lease.mjs](../../scripts/kin-node-kernel/lease.mjs) | 凭据 lease 句柄，不把 token 暴露给 UI |
| [service/scripts/http_to_socks.py](../scripts/http_to_socks.py) | HTTP CONNECT → SOCKS5h，供 CLI `HTTPS_PROXY` |
| [service/scripts/smoke.sh](../scripts/smoke.sh) | mock：tool_use → continuation → tool_result |
| [service/scripts/validate.py](../scripts/validate.py) | OpenAPI / schema / compose 静态校验 |
| [service/Makefile](../Makefile) | `verify` = 静态 + cargo test + go test |

面板相关（Grok preview 脚手架，不在推理热路径）：`scripts/kin-kernel-plugin.mjs`、`scripts/preview.mjs`。

## 6. 控制面板

| 路由 | 文件 | 作用 |
|---|---|---|
| `/` | [src/routes/index.tsx](../../src/routes/index.tsx) | 拓扑总览 |
| `/credentials` | [src/routes/credentials.tsx](../../src/routes/credentials.tsx) | OAuth/API key/WIF metadata，lease，拒绝 sessionKey 导入 |
| `/cli` | [src/routes/cli.tsx](../../src/routes/cli.tsx) | 对着 Node kernel 打一轮，看 argv / pid / frames |
| `/lab` | [src/routes/lab.tsx](../../src/routes/lab.tsx) | continuation 两跳 |
| `/capacity` | [src/routes/capacity.tsx](../../src/routes/capacity.tsx) | slot / P2C 可视化 |
| `/control` | [src/routes/control.tsx](../../src/routes/control.tsx) | kernel 注册与 snapshot |
| `/audit` | [src/routes/audit.tsx](../../src/routes/audit.tsx) | 动作日志（无 secret） |

前端凭据对象见 [src/lib/kernel/credentials.ts](../../src/lib/kernel/credentials.ts)：`authMode: claude_ai_oauth`，带可选 `socks5`，不存明文 token。

## 7. 现网对照

| 现网 | 本仓库 |
|---|---|
| portunex-server 粘性/P2C | `scheduler.rs` |
| isthmus stream-json 驱动 CLI | `local_cli.rs` |
| `--max-procs=20` 逻辑槽 | `KIN_SLOTS_PER_WORKER=20` |
| `--single-process-subagents` 一进程 20 loop | **stock CLI 做不到**；改为每 session 一进程，上限 20 |
| `--no-strict-isolation` 共用 session | **已改**：每请求独立 `--session-id` |
| `--continuous-tool-loop` | `x-kin-continuation` + parked stdin |
| sessionKey 五步换票 | Go **410**；只用官方 `refresh_token` |
| 89 容器 SNAT | 不实现 IP 轮换；一条 SOCKS5 固定出口 |

## 8. 启动（操作员）

```bash
# 控制面
cd service/control && go run ./cmd/kin-control

# SOCKS5 桥（CLI 用）
export KIN_SOCKS5='socks5h://user:pass@host:port'
python3 service/scripts/http_to_socks.py

# 内核
export KIN_PROVIDER=local_cli
export KIN_ISOLATION=subagent-pool
export KIN_WORKER_COUNT=1
export KIN_SLOTS_PER_WORKER=20
export KIN_CLAUDE_BIN=/path/to/claude
export KIN_HTTPS_PROXY=http://127.0.0.1:18080
export KIN_CLAUDE_AI_OAUTH_JSON='{"accessToken":"...","refreshToken":"...","expiresAt":0,"scopes":[...]}'
cargo run --manifest-path service/kernel/Cargo.toml
```

mock 演示不需要 CLI/凭据：`KIN_PROVIDER=mock` 后跑 `make -C service smoke`。

## 9. 明确不做

- sessionKey / Cookie 换 CLI 票
- 模仿 Chrome / axios / CLI 三套 UA 去打非公开 authorize
- 把一个订阅座转售成多租户公网 API
- 承诺 stock CLI「1 进程 = 20 并行 loop」
