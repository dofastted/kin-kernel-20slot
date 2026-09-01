# Kin Gateway 槽服务对接契约

日期：2026-09-01。Gateway 侧 SSOT 是 `kin-gateway/docs/INTERNAL_CONTRACT.md`；本文记录 Rust 已实现的对接面。

## 1. 运行拓扑

Rust VM 容器命令：

```text
/usr/local/bin/kin-kernel --gateway-worker --config /run/kin/kernel.json
```

不变量：

- `kin-kernel` 是容器 PID 1。
- 容器只挂载 Rust binary，不挂载或启动 Go worker。
- Unix socket 只有 `/run/kin/kernel.sock`，没有 `worker.sock`。
- Rust 同时拥有 inference、credential、identity、usage、telemetry。
- Node 只观察 health，不用 `docker exec` 启动 sidecar。
- 实际 `vm.runtime.engine` 是请求路由 SSOT；Rust 不健康时槽位停止调度，不回退 Go。

Go 模式是独立拓扑：PID 1 为 `kin-worker`，没有 `kin-kernel` 或 `kernel.sock`。两种模式共享凭证磁盘格式，但不同时运行。

## 2. `kernel.json`

```json
{
  "vm_id": "vm-05",
  "socket_path": "/run/kin/kernel.sock",
  "credential_path": "/home/kincli/.claude/credentials.json",
  "proxy_url": "socks5h://user:pass@host:port",
  "proxy_required": true,
  "anthropic_base_url": "https://api.anthropic.com",
  "oauth_token_url": "https://platform.claude.com/v1/oauth/token",
  "internal_token": "<secret>",
  "delivery_mode": "realtime",
  "refresh_skew_seconds": 300,
  "request_timeout_seconds": 0,
  "first_byte_timeout_seconds": 600,
  "idle_timeout_seconds": 180,
  "max_request_bytes": 33554432,
  "max_response_bytes": 67108864,
  "max_event_bytes": 33554432,
  "test_endpoints": false,
  "runtime_kind": "docker",
  "telemetry": { "enabled": false, "identity": {}, "headers": {} }
}
```

校验：路径必须绝对；`proxy_required=true` 时代理必填；代理只允许 `socks5` / `socks5h`；生产 endpoint host 固定为 `api.anthropic.com` 和 `platform.claude.com`；只有 `test_endpoints=true` 放行 mock host；delivery mode 只允许 `realtime|verified`。

## 3. 鉴权与错误

全部 `/internal/*` 使用 HTTP-over-UDS，并要求：

```text
X-Kin-Internal-Token: <kernel.json.internal_token>
```

鉴权失败返回 401 `internal_auth_failed`。内部错误形状：

```json
{
  "ok": false,
  "error": {
    "type": "worker_error",
    "code": "<stable-code>",
    "message": "<sanitized>"
  }
}
```

错误和日志不得包含 token、代理密码、完整上游错误正文或用户 prompt。

## 4. 内部端点

| 方法 | 路径 | 行为 |
|---|---|---|
| GET | `/internal/health` | engine/version、代理、脱敏凭证、credential state、telemetry、uptime |
| GET | `/internal/identity` | OS、machine-id、hostname、kernel、arch、timezone、locale |
| GET | `/internal/credential/status` | 只读脱敏状态 |
| POST | `/internal/credential/import` | OAuth/setup-token/API key 导入与原子写盘 |
| POST | `/internal/credential/ensure?force=` | 唯一 refresh 入口 |
| GET | `/internal/oauth/usage` | `ensure(false)` 后经槽 SOCKS5 请求官方 usage |
| POST | `/internal/telemetry/reload` | 重读 config 中 telemetry，不重启 PID 1 |
| POST | `/internal/telemetry/touch` | 激活 10 分钟活动会话 |
| POST | `/internal/v1/messages` | Gateway 推理信封，流式或非流式 |

模型目录由 Gateway `model_policy` 管理。Rust 不实现 `/internal/v1/models`，Gateway 也不从槽服务读取 catalog。

## 5. Messages 信封

```json
{
  "body": { "model": "...", "messages": [], "system": [], "stream": true },
  "headers": { "anthropic-version": "...", "anthropic-beta": "...", "user-agent": "..." },
  "stream": true,
  "delivery_mode": "realtime"
}
```

不变量：

- 信封不含 Authorization；Rust 在 `ensure(false)` 后装配当前槽凭证。
- 上游 hop 强制 SSE；`body.stream` 与信封 `stream` 一致。
- 调用方 Cookie、Authorization、X-Api-Key 不透传。
- 上游非 2xx 保留状态和 Anthropic 错误体，但剥敏感响应头。
- 上游 401 不触发 refresh，也不触发 Go fallback。

交付模式：

- 非流式：缓冲完整 message JSON，终态 `verified`。
- `stream=true, verified`：收到 `message_stop` 后才提交；不完整返回 502 `upstream_terminal_invalid`。
- `stream=true, realtime`：上游响应头到达即提交；后续中断产生 `incomplete` trailer 与脱敏 SSE error。

元数据：`X-Kin-Usage`、`X-Kin-Model`、`X-Kin-Stop-Reason`、`X-Kin-Event-Count`、`X-Kin-Terminal-State`。

## 6. 凭证

Rust 是 Rust VM 唯一 refresh owner。详细文件与锁协议见 [CREDENTIALS.md](CREDENTIALS.md)。关键规则：

- `credentials.json`、`.lock`、`kinGeneration` 与 Go/Claude 现有格式兼容。
- fresh fast path 后，临期 refresh 必须持文件锁重读并二次判断。
- refresh-token rotation 原子落盘。
- OAuth/setup-token/API key 全支持。
- 没有后台 refresh；推理前 `ensure(false)`；401 不 force refresh。

## 7. Telemetry

Rust telemetry 在 PID 1 进程内运行，不启动子进程。

- `telemetry.enabled=true` 且 official user identity 与 machine identity 同时存在才生效。
- 缺任一身份时状态为 `waiting_official_identity`。
- `/internal/telemetry/touch` 激活 10 分钟会话。
- event logging 每 10 秒 batch。
- GrowthBook 首次活动立即请求，此后每 6 小时。
- 全部经当前槽 SOCKS5。
- 失败只更新 telemetry 状态，不影响 credential、health 的进程存活或 inference。

## 8. 控制面切换语义

- 全局 `routing.inference.engine` 只影响新建 VM 和现有 VM 下次启动。
- 单 VM 显式切换立即重建目标 PID 1。
- 目标 health 成功后才保存 policy 与 runtime。
- 目标失败时重建并验证旧拓扑。
- 请求路径不启动进程、不改拓扑、不跨引擎 fallback。

## 9. 验收

1. `cargo fmt`、Clippy `-D warnings`、完整测试、release build 通过。
2. Docker Rust VM 中 PID 1 是 `kin-kernel`，无 Go binary、Go 进程和 `worker.sock`。
3. import/status/ensure/messages/usage/identity/telemetry 均通过 `kernel.sock`。
4. OAuth rotation 与 generation 原子更新，内部响应无 secret。
5. mock telemetry event batch 与 GrowthBook 都经槽 SOCKS5 发出。
6. Gateway Go→Rust→Go 成功；目标 Rust health 失败能恢复健康 Go 拓扑。
