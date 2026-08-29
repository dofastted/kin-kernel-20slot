# 交付成熟度与生产差距

完整架构 + 可运行参考实现。公网多租户前必须补 P0。

## 已实现

| 组件 | 能力 |
|---|---|
| Rust ingress | `/v1/messages`、OpenAI Chat 子集；`stream:true` SSE，`stream:false` 拼官方流 |
| Scheduler | sticky-first、P2C、active/waiting 分离、generation |
| Continuation | tenant/session/token/tool-id 绑定、单次消费 |
| Isolation | `process` / `session-reset` / `subagent-pool`（每 session 一 `--session-id`，上限 N 进程） |
| Providers | mock；Anthropic Messages 出站强制流式；`local_cli` 真 Claude Code（订阅 `claudeAiOauth` 或 setup-token） |
| Egress | `KIN_HTTPS_PROXY` HTTP CONNECT；Go `refresh_token` 直连同一条 SOCKS5 |
| Go control | 注册、心跳、stale reconcile、drain、route policy、快照；sessionKey 410 |
| Contracts/deploy | OpenAPI、JSON Schema、Compose、Kubernetes、smoke、静态校验 |
| 面板/脚本 | Node kernel 孪生、CLI 实验室、http_to_socks 桥 |

## 生产前 P0

- Redis CAS/Lua 替换内存 SessionDirectory。
- Postgres desired-state/audit 替换 Go 内存 store。
- 客户认证、tenant 注入、RBAC、mTLS；禁止公网自报 `x-tenant-id`。
- signed snapshot + last-known-good。
- 客户端断开取消 CLI、首字节重试边界。
- tenant/model RPM、ITPM、OTPM、waiting-tool quota。
- Secret manager/WIF；禁止长期 env 里放 oauth JSON。
- OpenTelemetry、Prometheus、低基数审计。
- 压测、chaos、镜像扫描、供应商条款评审。

## 已知限制（原型已验证）

- stock Claude CLI：一进程一轮 stdin，不能 1 pid × 20 并行 loop。5 并发 = 5 个 ~210MB 进程。
- 空闲 session 保活会堆 RSS；需要淘汰策略（已有 LRU 上限，无空闲 TTL）。
- sessionKey 五步换票不做；订阅票用官方 `/login` 的 `claudeAiOauth` + `refresh_token`；setup-token 走 `CLAUDE_CODE_OAUTH_TOKEN`（inference-only，不能 refresh）。

## 验证

本环境已跑通：`cargo test`、`go test`、SOCKS5 握手、官方 CLI 对话、Rust kernel 流式/非流式、5 并发 session 隔离。静态校验：`make -C service static-check`。
