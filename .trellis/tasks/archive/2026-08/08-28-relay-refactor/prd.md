# PRD — 内嵌 Messages Relay 与 upstream 权威流重构（核心档）

## 背景

用户提供 `plan/重构plan.md` 作为技术路线参考（基于旧附件代码撰写）。本任务以当前
仓库实际代码为准。架构师已确认范围为**核心档**，并补入流控完整性、启动顺序、
灰度回滚三项硬性要求。完成代码调整后交接测试员做真实环境测试。

## 目标与用户价值

让 20-slot 单 PID 多路复用运行时的**用户流正文来自 Anthropic upstream SSE 逐 token**
（CLI 2.1.241 不对 subagent 发 `stream_event` 增量帧，现状用户只能拿到整块文本）。
stdout 降级为控制面与兜底。

## 已确认事实（仓库核查，2026-08-28）

- `multiplex_cli/` 已实现单 PID + 20 slot、CAS 状态机、唯一 stdout decoder、
  MCP `slot_wait`/`client_tool`/`kin_done`/`kin_fail`、memory admission、
  签名 continuation。plan 的 P0/P1 已被现有代码覆盖，不在本期范围。
- `api.rs` 已是真实时 SSE；热路径无 `unbounded_channel`。
- 已确认缺口：`Runtime::emit`（`mod.rs:651`）用 `try_send` 静默丢事件
  （`traces/replay-stats.json`：dropped=1,125,600）；正文权威源是 stdout 整块帧。
- 仓库无任何 Relay / upstream tap / 请求关联 / CanonicalEvent / Gemini 代码。
- 本机已具备 cargo 1.95 / go 1.25 工具链。

## 架构师决策（2026-08-28）

1. **范围＝核心档**：Relay + upstream tap + 请求关联 + 降级链 + 流控修复。
   R4（CanonicalEvent/Gemini）、R5（Go capability）拆后续任务；但 kernel 本地状态
   接口先暴露 `relay_mode/relay_healthy/tap_dropped` 字段，Go 暂忽略。
2. **Relay 是旁路 tap，不是劫持返回**：同一批上游字节原样回 CLI（CLI 支路由网络
   转发直接驱动），同时解析写入用户 `JobStream`。Relay/tap 故障不得阻断 CLI 消费。
3. **关联机制**：不新造 `kin_context`；在现有 `slot_wait` 结果上增加签名的
   `relay_context`（job_id/slot_id/generation/nonce/mac）。Relay 按 `job_id`
   **动态查询**当前事件接收器（resume 会替换接收器，禁止请求开始时缓存旧 Sender）。
4. **SourceArbiter 降级链**（不引入完整 CanonicalEvent）：
   `NoBody → UpstreamActive | StdoutFallback → Completed`。
   优先级：有效关联的 upstream SSE > stdout 完整 assistant frame >
   `kin_done.fallback_content`。turn 一旦进入 `UpstreamActive` 不得中途切 stdout。
5. **内部多轮拼接（最小实现）**：一个用户请求可触发多次内部 `/v1/messages`；
   内部 `message_start/stop` 不外泄为多个外层响应，`content_block_delta` 拼入当前
   外层 JobStream，MCP 内部工具不外泄，WebSearch 阶段事件保留，最终只产生一组
   外层 `message_delta/message_stop`。
6. **流控修复（必须同步做）**：文本 delta 不允许静默丢弃；有界等待；客户端持续
   过慢时显式终止该用户 SSE（不得返回正文残缺的表面成功）；用户 tap 独立有界队列，
   溢出只影响用户支路并产生显式错误与指标，不阻塞 CLI。
7. **启动顺序**：Relay 先启动并通过健康检查，再启动 Claude CLI（注入
   `ANTHROPIC_BASE_URL`）；Relay 未就绪则不启动 CLI 或安全退回 `off`。
8. **灰度模式与默认值（已定稿）**：默认 `off`（合入零行为变化）。
   配置语义：未设置/`off` 完全不启动 Relay、不注入 `ANTHROPIC_BASE_URL`；
   `observe`/`authoritative` 下 Relay 是必需组件，未就绪则内核不 Ready、不启动
   CLI；非法值配置错误直接退出，**禁止静默降级为 off**（防"测试通过但没走
   Relay"假阳性）。回滚 = 改回 `off` 并重启。
   验收顺序：off 基线零修改通过 → 显式 observe 摘要对比 → 显式 authoritative。
9. **安全**：请求正文、鉴权、beta header 保持 CLI 原始特征；禁止记录 OAuth token
   与完整敏感请求。
10. **代码边界**：新代码收敛在 `provider/multiplex_cli/relay/`
    （mod/server/upstream/correlate/sse_tap/arbiter/metrics）；改动
    `supervisor.rs`（启动顺序、env）、`mod.rs`（动态路由、流控）、
    `job_stream.rs`（来源标记、去重、降级状态）、`mcp_server.rs`（`kin_done` 语义）、
    `config.rs`（Relay 配置与灰度）。

## 需求

- R1: 内嵌 Messages Relay（127.0.0.1 loopback，先起后 CLI，健康检查，敏感信息不落日志）。
- R2: `relay_context` 签名关联；无有效关联的请求只回 CLI，不进用户流。
- R3: SourceArbiter 权威优先级与单向降级；正文不重复、不截断。
- R6: 内部多轮拼接最小实现 + 流控修复（delta 零静默丢弃、慢客户端显式断流、
  tap 独立有界队列）。
- R7: 灰度三模式与回滚；kernel 状态接口暴露 `relay_mode/relay_healthy/tap_dropped`。

## 验收标准（架构师定稿）

- 单 PID、20 slot 保持不变。
- 普通回答在 `kin_done` 前收到多个自然的 upstream `text_delta`。
- 20 并发无串流、无重复正文、无错绑 slot。
- `tool_result` continuation 恢复原 `job_id/slot_id`。
- WebSearch 阶段事件正常，内部 MCP 工具不外泄。
- observe 模式下三种关联场景（首轮 / WebSearch 内部多轮 / `tool_result` resume
  后）在真实 CLI 上均能找到正确 `krc_` token（关联假设的实机证明）。
- 上游正文与最终 stdout/`kin_done` 摘要一致。
- 成功响应中丢失的文本 delta 数量 = 0。
- 慢客户端收到显式断流错误，不得收到残缺的成功响应。
- Relay 未就绪不启动 CLI，或安全退回 `off`。
- Relay/tap 故障不阻断 CLI 对上游响应的消费。
- 20 并发仍满足 2–4 GB VM 目标。

## Out of Scope

- R4 CanonicalEvent / Gemini 出口（后续任务）。
- R5 Go 心跳 capability 消费端（后续任务；本期只在 kernel 侧暴露字段）。
- DELIVERY_STATUS 生产 P0（Redis/Postgres/认证/Secret manager 等）。
- sessionKey 换票流程变更。

## 分工

- 本任务：代码实现 + 本地可验证测试（单测/回放/mock 上游）。
- 测试员：真实 Claude CLI + 真实 Anthropic 上游的端到端与并发/内存验收。

## Open Questions

- 无（范围、默认模式、配置语义均已由架构师定稿；见 design.md / implement.md）。
