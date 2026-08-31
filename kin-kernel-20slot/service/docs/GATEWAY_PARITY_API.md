# Kin Gateway 对接补齐 API（gap 文档）

> 目标读者：本仓（kin-kernel-20slot）内核开发者。
> 背景：Rust kernel 将成为 kin-gateway（Node 控制面 + 每槽数据面）的**主驱动数据面**，逐步接管现有 Go worker 的全部职责。本文列出内核当前缺失、必须补齐的 API 与行为，按优先级分级。
> 契约 SSOT：kin-gateway 仓 `docs/INTERNAL_CONTRACT.md`（下称 §N 均指该文档章节）。本文只列缺口与形状摘要，字段冲突时以 SSOT 为准。

## 0. 现状与终态

| 能力域 | kernel 现状 | 终态（Rust 主驱动） |
|---|---|---|
| 推理入口 | 公网 HTTP `:8080` `/v1/messages`、`x-tenant-id`；网关侧默认仍 hop **Go worker** | 每槽 Unix socket + internal token + envelope hop（§8.2/§8.4），仅 `inference.engine=rust` 才切 |
| 凭证 | env `KIN_CLAUDE_AI_OAUTH_JSON` / setup-token；refresh 在 Go control / 网关 Go worker | Rust 为**唯一 refresh owner**（文件、锁、generation、rotation）；M1 未切 |
| 出站 | 全进程单一 `KIN_SOCKS5` / `KIN_HTTPS_PROXY` | 每槽独立 SOCKS5（认证 socks5h），host allowlist |
| 调度 | P2C + session lease + continuation | 保留；网关粘性/配额/failover 仍在 Node，逐步下沉（P2） |
| 槽服务 | 无 | health / identity / oauth usage probe / telemetry sidecar |
| 配置 owner | 内存/SQLite typed 配置；Node BFF 默认关闭 | 配齐 `KIN_CONTROL_*` 后 routing/model/slot/proxy 由 Go control 拥有 |

---

## P0 — gateway worker 模式（不齐则无法接管推理 hop）

> M1 已实现：P0.1、P0.2、P0.3、P0.5 单槽、P0.6 推理 hop 错误码、过渡期只读凭证。P0.4 refresh owner 未做。

### P0.1 传输与鉴权

- 新增启动模式：`kin-kernel --gateway-worker --config <worker.json>`。
- 监听 Unix socket（`socket_path`），仅接受携带 `X-Kin-Internal-Token` 的请求；失败 → 401 `{"ok":false,"error":{"type":"worker_error","code":"internal_auth_failed","message":"..."}}`（§8.2）。
- SIGTERM 优雅退出：in-flight 流按 realtime 中断语义收尾（见 P0.3）。

### P0.2 推理信封 `POST /internal/v1/messages`

请求（§8.4）：

```json
{
  "body":   { "model": "...", "messages": [], "system": [], "stream": true },
  "headers": { "anthropic-version": "...", "anthropic-beta": "...", "user-agent": "..." },
  "stream": true,
  "delivery_mode": "realtime"
}
```

- `body` 不变造：上游 hop 原样发送，仅强制 `stream:true` 与信封一致。
- `Authorization` **只在内核装配**（本槽凭证）；上游头白名单：`Accept` `Accept-Language` `Anthropic-*` `Content-Type` `User-Agent` `X-App` `X-Claude-Code-Session-Id` `X-Client-Request-Id` `X-Stainless-*`；调用方 `Cookie` / `X-Api-Key` 删除。
- 上游非 2xx：原样状态码 + Anthropic 错误体透传，剥 `Authorization` / `Set-Cookie` / `X-Api-Key`。
- 大小限制走 worker.json：`max_request_bytes` / `max_response_bytes` / `max_event_bytes`。

### P0.3 交付语义与元数据

- `delivery_mode=realtime`：Anthropic HTTP 响应头到达即 commit；此后中断 → trailer `incomplete` + 合成 `event: error`。
- `delivery_mode=verified`：等到 `message_stop` 再重放 SSE；流不完整 → 502 `upstream_terminal_invalid`。
- 元数据头/trailer（网关记账依赖，缺一不可）：
  - `X-Kin-Usage`：JSON，Sub2API 口径 —— input / output / cache read / cache write（**含 5m 与 1h 分档**）。
  - `X-Kin-Model`（upstream 模型）、`X-Kin-Stop-Reason`、`X-Kin-Event-Count`、`X-Kin-Terminal-State`。
- `stream.rs` StreamAssembler 已扩展 Sub2API usage 聚合；fixture 字段级覆盖 cache write 总量与 5m/1h 分档。

### P0.4 凭证生命周期（refresh owner 迁移进内核）

网关铁律（§8.5–8.6，全部必须实现）：

- 存储：`credential_path` 指向的 `credentials.json`（`claudeAiOauth` 形状）+ `credentials.json.lock` 文件锁 + `generation` 单调递增。锁协议必须与现网 Go worker 完全一致（过渡期两实现互斥依赖同一把锁）。
- **没有后台定时刷新**；`ensure` 是唯一换票入口。上游 401 **不**强制换票。临期窗口 `refresh_skew_seconds`（默认 300s）内才换；换票前持锁重读 + 临期二次检查；refresh rotation（响应带新 refresh_token 必须原子落盘）。
- refresh 出站：仅 `platform.claude.com`，经本槽 SOCKS5；`test_endpoints=true` 才允许非生产 host。
- sessionKey 换票：维持固定 410（现有行为，不变）。

需新增端点：

| 端点 | 语义 |
|---|---|
| `POST /internal/credential/import` | 体：`{type: "oauth"\|"setup-token"\|"apikey", access_token?, refresh_token?, api_key?, base_url?, auth_scheme?, expires_at?/expires_in?, email?, account_uuid?, org_uuid?, scopes?}`。setup-token：无 refresh、长期 oat、临期报错不换；apikey（`sk-ant-api03…`）：不 refresh、不可打 usage 探测 |
| `POST /internal/credential/ensure?force=` | 返回 `{ok, refreshed, shared, credential}`；fresh 且非 force 不换 |
| `GET /internal/credential/status` | 脱敏凭证元数据（不含 token 明文） |

`credential_state` 状态机：`missing | refreshable | expired_refreshable | expired | refresh_window | fresh`（+ 失败类 `last_error_class: fatal(invalid_grant/缺 refresh/revoked) | retryable`）。精确枚举以 SSOT §8.6 为准。

### P0.5 每槽出站

- worker.json `proxy_url`（`socks5://` / `socks5h://`，带认证）替代进程级 `KIN_SOCKS5`/`KIN_HTTPS_PROXY`；`proxy_required=true` 且未配 → 拒绝启动。
- 出站 host allowlist：`api.anthropic.com`（推理）、`platform.claude.com`（refresh）。禁止其他 egress。
- 多槽聚合模式（一进程 N 槽）下必须做到**每槽独立** proxy / 凭证 / 指纹；做不到之前只发单槽模式（`slots=1`）。

### P0.6 错误码域

内核错误必须落入网关 failover 可识别的错误域（`{"ok":false,"error":{"type":"worker_error","code":...}}`）：

`internal_auth_failed` · `needs_refresh`（过渡期只读模式用，见 §兼容）· `upstream_terminal_invalid` · `upstream_stream_incomplete` · 上游透传类（保留原状态码）。message ≤300 字符，不含 token / 完整上游 body / 用户 prompt（与本仓错误模型一致）。

---

## P1 — 槽服务（接管 Go worker 剩余职责）

### P1.1 `GET /internal/health`

```json
{ "ok": true, "status": "ready|degraded", "vm_id": "...",
  "proxy_configured": true, "proxy_required": true,
  "credential": { "type": "oauth", "has_access": true, "has_refresh": true,
    "expires_at": 0, "ttl_seconds": 0, "generation": 0,
    "email": "", "account_uuid": "", "org_uuid": "",
    "needs_refresh": false, "credential_state": "fresh" },
  "delivery_mode": "realtime", "uptime_seconds": 0,
  "last_error": "", "last_error_class": "",
  "worker_version": "...", "runtime_kind": "docker" }
```

### P1.2 `GET /internal/identity`

槽位身份采集（§8.7）：`/etc/os-release`、`/etc/machine-id`、`TZ`/`LC_ALL`/`LANG` →
`{schema_version, runtime_kind, hostname, os_id, os_pretty, kernel_release, arch, goos, machine_id, timezone, locale, collected_at(RFC3339), worker_version}`。官方 `device_id` / `session_id` 由网关另行覆盖，此接口不伪造。

### P1.3 `GET /internal/oauth/usage`

经本槽 SOCKS5 打 Anthropic 官方用量接口，回额度刻度；API key 凭证 → 400 `usage_unsupported`。网关用它写面板额度，不允许打 `/v1/models`（会 401 烧 refresh，全局禁止）。

### P1.4 遥测 sidecar

对齐 Go worker `telemetry --config` 行为：经槽 SOCKS5 直传 `POST /api/event_logging/batch` + GrowthBook `/api/eval`；**仅在拿到官方 userID/machineID 后启用**（否则 `{enabled:false, reason:"waiting_official_identity"}`）；不伪造身份、不 hop 推理路径。

### P1.5 官方 CC 初装协作

初装流程由网关 Node 驱动（wipe → 物化 `~/.claude/.credentials.json` → hello → usage → seed → 指纹接管 → 遥测 sync）。内核需要：

- 凭证物化后能感知文件变更（每请求重读已覆盖）；
- 初装期间不持锁阻塞 CLI 的凭证读写；
- 提供 reload/bounce 安全性：进程重启后从文件恢复全部状态（无内存必需态）。

---

## P2 — 逻辑对齐（网关逻辑逐步下沉，另立任务，此处只记方向）

- 协议转换全集：OpenAI Chat / Completions（旧）/ Responses → Anthropic；model 策略、thinking、1M/beta、web_search。内核已有 `/v1/chat/completions` 子集，需补齐到网关 `src/lib/protocol/` 等价。
- 调度下沉：网关账号池（sticky 键、WRR、tier 并发帽、cooldown、failover attempt 语义）与内核 P2C/lease 的合并方案。
- 持久化事件流：内核输出 attempt/usage 结构化事件，供网关 SQLite `request_logs` 记账（内核自身不落库）。
- `local_cli` / `native_slot` 在网关拓扑下的定位（与网关「推理不跑 CLI」现原则的取舍）。

---

## 配置增量（worker.json）

内核 gateway-worker 模式读取网关下发的 worker.json（§8.1），至少支持：

`vm_id` · `socket_path` · `credential_path` · `proxy_url` · `proxy_required` · `anthropic_base_url` · `oauth_token_url` · `internal_token` · `delivery_mode` · `refresh_skew_seconds` · `request_timeout_seconds` · `first_byte_timeout_seconds` · `idle_timeout_seconds` · `max_request_bytes` · `max_response_bytes` · `max_event_bytes` · `test_endpoints` · `runtime_kind`。Go 独有的 `telemetry` 在 M1 由 Rust 忽略。

未知字段必须忽略（向前兼容）。配置变更依赖进程 bounce（`reloadSlot`），无热重载要求。

## 兼容与切换（过渡期）

- **现网默认 Go**：`routing.inference.engine` 与槽 `inference_engine` 缺省均为 `go`。Rust 只在显式配置且 `KIN_KERNEL_BIN` 可用时 hop；`fallback_to_go` 默认 true。
- 网关侧有 engine 开关（全局 `routing.inference.engine` + 每槽覆盖），Go worker 与 Rust kernel 并行期共存于同一槽容器。
- **凭证 owner 互斥**：每槽同一时刻只有一个 refresh owner。过渡期内核可运行「只读凭证」降级模式（读文件、临期返回 `needs_refresh` 交网关触发 Go ensure）；owner 切到 Rust 后 Go worker 降为只读或停用。两实现共享同一 lock/generation 协议兜底误配。
- 回退路径永远保留：owner + engine 一次配置切回 Go。

## 验收基准

1. 对拍 fixture：同一 mock SSE 流，Rust 与 Go worker 输出的 `X-Kin-Usage` 等元数据字段级 diff = 0。
2. 网关 e2e：`/v1/messages` + `/v1/chat/completions` × realtime/verified 四组合通过（mock 票 + 真实票）。
3. 凭证演练：import（oauth / setup-token / apikey 三型）→ ensure（fresh 不换 / 临期换 / rotation 落盘 / invalid_grant fatal）→ 进程 kill -9 后从文件+lock 恢复。
4. 安全断言：任何响应/日志不出现 access/refresh token；出站仅 allowlist host；401 不触发换票。
