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

- [ ] AC1 `provider/multiplex_cli/relay/` 目录不存在；全仓无 `relay::` 引用
- [ ] AC2 `mcp_server.rs` 不存在；无 `mcp_slot_wait` / `slot_wait` MCP 工具残留
- [ ] AC3 `ExecutionMode::{McpSlot, NativeAgent}` 及 `KIN_ALLOW_NATIVE_AGENT` 门禁不存在
- [ ] AC4 `provider/local_cli.rs` 不存在；`main.rs` 无 `local_cli` 分支
- [ ] AC5 `KIN_RELAY_MODE` / `KIN_EXECUTION_MODE` / `KIN_ISOLATION` 等已废配置项从代码与 env example 中移除

### 不回退

- [ ] AC6 `cargo test --all-targets` 全绿，且**保留了所有仍适用的测试**（不得靠删测试达成绿）
- [ ] AC7 `cargo clippy --all-targets -- -D warnings` 零诊断，且不新增 `allow(dead_code)`
- [ ] AC8 `cargo fmt --check` 干净
- [ ] AC9 二进制实机可启动，`/healthz` 正常响应
- [ ] AC10 native_messages 真流式行为不变：以真实 API 复跑一次单槽 hello + 一次 tool_use 续接，逐 token 输出与工具参数正确

### 度量

- [ ] AC11 记录删除前后的 `src/` 总行数与文件数，写入 APPLY.md

## Open Questions

- OQ1 删除 `mcp_slot` 后 `SlotPhase::WaitingTool` 与 `session.rs::Phase::WaitingTool` 是否完全无写入方？需在实施时以编译器验证，而非静态推测。
- OQ2 `IsolationMode` 是否会退化为单变体枚举？若是，应直接删除该枚举而非留一个恒真的类型。
