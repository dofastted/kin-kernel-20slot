# 收敛为 patch 单一路线

## Goal

删除 `relay` / `mcp_slot` / `native_agent` / `local_cli` 四条旧执行路线，使 `native_messages`（patch 模式）成为唯一路径。目标是减少复杂性与认知负担，而非新增能力。

## Background

`08-30-native-slot-stateless` 已完成并全部通过验收（AC1–AC19，144 单测 + 真实 API 端到端）。该任务的 Non-Goals 曾要求「不改 mcp_slot 路径，保持回退可用」「不改 relay」——那是在 native_messages 尚未验证时的**保守条款**，用途是保底回退。

前提已经改变：

- native_messages 走通了真流式、工具续接、20 并发、取消回收、安全隔离，并已切为默认（AC19）。
- patch 模式（CLI 侧补丁 + 原装 Anthropic 请求）在结构上优于 relay 模式：relay 由 Rust 发起上游 TLS，不属于 CLI 原装网络路径，需要维护 SSE 解码/事件仲裁/上下文关联三套机制；patch 模式下这些全部消失。

因此本任务**显式推翻**前任务的两条 Non-Goals。这是决策变更，不是缺陷修复。

## 现状规模

| 目标 | 行数 | 性质 |
|---|---|---|
| `provider/multiplex_cli/relay/`（8 文件） | 3702 | Rust 自建上游数据面 |
| `provider/local_cli.rs` | 747 | 每请求单进程隔离模式 |
| `provider/multiplex_cli/mcp_server.rs` | 293 | `mcp_slot` 的 MCP JSON-RPC 服务端 |
| `ExecutionMode::{McpSlot, NativeAgent}` + 门禁 | ~60 | 模式分支 |
| `mod.rs` 内 `is_native()` 分支 | 7 处 | 双路线并存产生的条件逻辑 |

## Constraints

- **C1 `sse_tap.rs` 不可整体删除。** 其中 `EventFilter` / `FilterPolicy::CLI` / `KIN_SYNTH_MARKER` 被 `job_stream.rs` 依赖，而 `job_stream` 在 native_messages 路径上活跃。必须先拆分：保留 CLI 策略，删除 `FilterPolicy::RELAY`、`TapQueue`、`TapBinding` 等 relay 专属部分。
- **C2 不得降低现有验收水位。** 删除后 `cargo test --all-targets` 必须全绿，`cargo clippy --all-targets -- -D warnings` 必须零诊断，`cargo fmt --check` 干净。
- **C3 不碰 `gateway_worker`。** 它是 PR #1 的独立在途功能，与本任务正交。
- **C4 删除即彻底。** 不保留注释掉的代码、不保留「以后可能用」的孤儿函数；被删路径的配置项（`KIN_RELAY_MODE` / `KIN_ISOLATION` 等）同步从 env example、README、spec 中移除。
- **C5 凭证与 SOCKS 口令不得进入日志、产物或提交。**

## Requirements

### R1 · 拆分 sse_tap，删除 relay

将 `EventFilter` 及其 CLI 策略提升为独立模块（如 `provider/multiplex_cli/event_filter.rs`），删除 relay 专属类型，随后整体删除 `relay/` 目录与 `mod.rs` 的 relay 接线（`RelayHandle` / `relay::spawn` / `confirm_healthy` / `tap_binding` / `UpstreamClient`）。

### R2 · 删除 mcp_slot

删除 `mcp_server.rs`、`Runtime::mcp_slot_wait` 及相关 job 分发路径、`SlotPhase::WaitingTool`（若删 mcp_slot 后不再被写入则一并删除）、`session.rs::Phase::WaitingTool` 的对应处理。

### R3 · 删除 native_agent

`ExecutionMode` 收敛为单一形态或直接删除该枚举。AC18 的 opt-in 门禁（`KIN_ALLOW_NATIVE_AGENT` / `check_opt_in`）随被门禁对象一起删除——门禁存在的意义是防止 NativeAgent 被误用，对象消失后门禁即无意义。

### R4 · 删除 local_cli

删除 `provider/local_cli.rs` 与 `main.rs` 的分发分支。若 `IsolationMode` 的 `ProcessPerTurn` / `ResetAndReuse` 变体随之无人使用，一并删除，`config.rs` 相应收敛。连带解决：该文件两个测试依赖仓库中**从未存在过**的 `mock-claude.mjs`。

### R5 · 配置面与文档同步

从 `configs/kernel.env.example`、`service/README.md`、`.trellis/spec/kernel/multiplex-cli-subsystem.md` 移除已删路径的配置项与描述；`APPLY.md` 记录本次收敛。spec 中「Messages Relay」「MCP JSON-RPC Server」等章节随实现删除。

## Non-Goals

- 不改 `native_messages` 的协议与行为。本任务是纯删除 + 必要的依赖拆分，不应改变任何现存的通过性行为。
- 不动 `gateway_worker`（PR #1 在途）。
- 不删 `anthropic` / `mock` provider——它们与 execution_mode 正交，`mock` 是测试基础设施。

## Acceptance Criteria

### 删除完成度

- [x] AC1 `provider/multiplex_cli/relay/` 目录不存在；全仓无 `relay::` 引用
- [x] AC2 `mcp_server.rs` 不存在；无 `mcp_slot_wait` / `slot_wait` MCP 工具残留
- [x] AC3 `ExecutionMode::{McpSlot, NativeAgent}` 及 `KIN_ALLOW_NATIVE_AGENT` 门禁不存在
- [x] AC4 `provider/local_cli.rs` 不存在；`main.rs` 无 `local_cli` 分支
- [x] AC5 `KIN_RELAY_MODE` / `KIN_EXECUTION_MODE` / `KIN_ISOLATION` 等已废配置项从代码与 env example 中移除

### 不回退

- [x] AC6 `cargo test --all-targets` 全绿，且**保留了所有仍适用的测试**（不得靠删测试达成绿）
- [x] AC7 `cargo clippy --all-targets -- -D warnings` 零诊断，且不新增 `allow(dead_code)`
- [x] AC8 `cargo fmt --check` 干净
- [x] AC9 二进制实机可启动，`/healthz` 正常响应
- [!] AC10 **阻塞（缺凭证，非代码问题）**：真实 patched CLI 已用本次改动后的内核拉起并完成
  `kin_host_ready` 握手（protocol_version=2, slots=2, capabilities=[multi_slot,
  native_sse, stateless]），`/readyz` 返回 200、`/internal/v1/slots` 显示 capacity=2，
  证明 S1–S5 后的代码能驱动真 CLI。但 `/v1/messages` 返回
  `provider error: Not logged in · Please run /login`：本机
  `~/.claude/.credentials.json` 只有 MCP OAuth 条目，没有 Claude 订阅
  `claudeAiOauth` blob，`ANTHROPIC_API_KEY` / `CLAUDE_CODE_OAUTH_TOKEN` 均未设置。
  需要 `KIN_CLAUDE_AI_OAUTH_JSON`（订阅 blob）或 `KIN_CLAUDE_CODE_OAUTH_TOKEN`
  （setup-token）后重跑一次单槽 hello + 一次 tool_use 续接即可闭合。
  替代证据：模拟 CLI 走同一条 `write_cli_stdin` / `decode_stdout` /
  `handle_native_frame` 路径的逐 token 输出与 tool_use 续接均已在测试与实机
  `/v1/messages`（流式与非流式）验证。

### 度量

- [x] AC11 记录删除前后的 `src/` 总行数与文件数，写入 APPLY.md

## Open Questions（已在实施中回答）

- **OQ1 → 是（仅 SlotPhase）。** 删除 mcp_slot 后 `SlotPhase::WaitingTool` 只剩一个测试
  helper 在写，编译器确认无生产写入方，已连同 `Draining` / `Slot::cas` /
  `parent_tool_use_id` 一起删除。`session.rs` 侧的等待状态**保留**：HTTP 层的
  tool_use 续接（`scheduler.rs` 的 `waiting_tool` 容量、`session.rs::mark_waiting`）
  与 CLI 内部 parking 无关，仍然需要。
- **OQ2 → 是，两个枚举都退化并已删除。** `ExecutionMode` 在删掉 `McpSlot`/`NativeAgent`
  后只剩一个变体，直接删除枚举与 `KIN_EXECUTION_MODE`；`IsolationMode` 在删掉
  `local_cli` 后只剩 `Multiplexed`，同样删除枚举与 `KIN_ISOLATION`。两处对外报告的
  字符串改为常量（`api.rs::EXECUTION_MODE` / `ISOLATION`），因为 `execution_mode` 是
  `config_hash` 三方契约的一部分，payload 形状不能变。

## 实施偏差（据实记录）

1. **S3 的删除范围比计划大。** 计划只写了 `mcp_server.rs` + MCP 分发；编译器证明
   `job_stream.rs` / `event_filter.rs` / `stream_decoder.rs` / `pending_call.rs` /
   `continuation.rs` / `signing.rs` / `replay.rs` 的唯一生产消费者都在 mcp_slot 路径上，
   保留它们会触发 `dead_code`（AC7 禁止 `allow(dead_code)`），因此一并删除。
   C1 约束依然被遵守：`event_filter` 在 S1 拆出、S2 保持 native 路径可编译，直到
   S3 证明它整体无人使用。
2. **模拟器是移植而非删除。** 旧 `simulate_worker()` 跑的是 MCP 循环，删掉它会带走
   20 槽并发/续接/web_search/背压等 10 个仍然适用的测试。改为 `simulated_cli()`：
   in-memory duplex 上跑真 `kin_*` 协议，测试因此覆盖 `write_cli_stdin` +
   `decode_stdout` + `handle_native_frame`，比原来更接近真实路径。
3. **Go `RuntimeProfile.execution_mode` 保留。** `config_hash` 是整个 profile 的
   SHA-256，内核只做字符串比较；删字段会让所有已发的 hash 失效且无收益，因此只收紧
   取值为 `native_messages`。闸门 B 以实机 `/readyz 200` 验证（详见 APPLY.md）。
