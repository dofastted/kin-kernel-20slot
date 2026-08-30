# 收敛为 patch 单一路线 · 执行计划

每个 S 阶段独立编译通过、独立提交。任一阶段测试未全绿则停止，不进入下一阶段。

---

## S1 · 拆分 sse_tap（搬迁，不删除）

- [ ] 1.1 新建 `provider/multiplex_cli/event_filter.rs`
- [ ] 1.2 迁移 `EventFilter`、`FilterPolicy`（仅 CLI 策略）、`KIN_SYNTH_MARKER`、索引重映射逻辑及其单测
- [ ] 1.3 `job_stream.rs` 的 `use super::relay::sse_tap::{...}` 改指向 `event_filter`
- [ ] 1.4 `mod.rs` 注册新模块

**验证**：`cargo test --all-targets` 全绿；`grep -rn "relay::" src/ | grep -v "^src/provider/multiplex_cli/relay/"` 只剩 `mod.rs` 的启动接线与测试引用。

⛳ **闸门 A**：若仍有 `job_stream` 之外的模块依赖 relay 内部类型，先补拆，不得进入 S2。

---

## S2 · 删除 relay

- [ ] 2.1 删 `relay/` 全部 8 文件
- [ ] 2.2 删 `mod.rs` 接线：`relay: OnceLock<RelayHandle>`、`relay::spawn`、`confirm_healthy`、`UpstreamClient`、`tap_binding()`、`relay_snapshot()`
- [ ] 2.3 删 `config.rs::RelayMode` 与 `KIN_RELAY_MODE`、`relay_addr`、`relay_upstream`
- [ ] 2.4 删 `api.rs` 中 `/healthz` 的 relay 字段
- [ ] 2.5 清理随之失效的测试（仅限测试对象已删除者）

**验证**：`cargo test --all-targets` 全绿；`cargo clippy -- -D warnings` 零诊断；全仓 `grep -c "relay"` 仅剩文档/注释。

---

## S3 · 删除 mcp_slot

- [ ] 3.1 删 `mcp_server.rs`
- [ ] 3.2 删 `Runtime::mcp_slot_wait` 及 MCP job 分发路径
- [ ] 3.3 删 `supervisor::write_mcp_config` 与 CLI 启动参数中的 `--agents` / mcp 配置注入
- [ ] 3.4 **验证 OQ1**：编译器确认 `SlotPhase::WaitingTool` 无写入方后删除该变体；同步处理 `session.rs::Phase::WaitingTool`

**验证**：全量测试绿；`/healthz` 仍可响应。

---

## S4 · 删除 native_agent，收敛 ExecutionMode

- [ ] 4.1 删 `ExecutionMode::NativeAgent`、`check_opt_in`、`NATIVE_AGENT_OPT_IN*`
- [ ] 4.2 删 Go `server.go::validateExecutionMode` 的 native 分支与对应测试
- [ ] 4.3 **验证 OQ2**：`ExecutionMode` 是否退化为单变体 → 是则删除枚举、`KIN_EXECUTION_MODE`、`reported_execution_mode()`
- [ ] 4.4 内联 `mod.rs` 中 7 处 `is_native()` 分支为恒真路径
- [ ] 4.5 **若 `config_hash` 含 `execution_mode`**：同步 Go RuntimeProfile 与 CLI `kin_host_ready` 回传，三方一起改

**验证**：Rust 全量测试绿 + `go test ./...` 全绿 + `go vet` 干净；三方 `config_hash` 一致（`/readyz` 不失败）。

⛳ **闸门 B**：4.5 是三方契约变更，改错会让 `/readyz` 持续失败。必须实机验证一次 `/readyz` 返回正常。

---

## S5 · 删除 local_cli

- [ ] 5.1 删 `provider/local_cli.rs`、`provider/mod.rs` 的 `pub mod local_cli`
- [ ] 5.2 删 `main.rs` 的 `"local_cli"` 分发分支
- [ ] 5.3 **验证 OQ2b**：`IsolationMode` 是否退化 → 是则删枚举与 `KIN_ISOLATION`
- [ ] 5.4 确认依赖不存在的 `mock-claude.mjs` 的两个测试随文件一起消失

**验证**：全量测试绿；`KIN_PROVIDER` 仅剩 `mock` / `anthropic_api` / multiplex 路径。

---

## S6 · 文档与配置收敛

- [ ] 6.1 `configs/kernel.env.example`：移除 `KIN_RELAY_MODE` / `KIN_EXECUTION_MODE` / `KIN_ALLOW_NATIVE_AGENT` / `KIN_ISOLATION`（按实际删除结果）
- [ ] 6.2 `service/README.md` 同步
- [ ] 6.3 `.trellis/spec/kernel/multiplex-cli-subsystem.md`：删「Messages Relay」「MCP JSON-RPC Server」「Execution Modes」章节，改写为单一路线描述
- [ ] 6.4 `APPLY.md` 记录本次收敛与 **AC11 度量**（删除前后 `src/` 行数与文件数）

---

## 最终验收

- [ ] 全量 `cargo test --all-targets` 绿 + `clippy -D warnings` 零诊断 + `fmt --check` 干净
- [ ] `go test ./...` + `go vet ./...` 绿
- [ ] **AC10 真实 API 复跑**：单槽 hello 逐 token 输出正确；tool_use 续接工具参数正确
- [ ] AC11 度量数字写入 APPLY.md

---

## 回滚点

| 位置 | 动作 |
|---|---|
| 任意阶段 | `git revert <该阶段 commit>` |
| S2 之后 | **无法用环境变量回退到 relay**——旧路径已物理删除，这是预期结果 |
| S4 之后 | config_hash 契约已变，回滚需三方同步 revert |

## 本任务不含

- `gateway_worker`（PR #1 在途，正交）
- `anthropic` / `mock` provider
- native_messages 自身的任何行为变更
