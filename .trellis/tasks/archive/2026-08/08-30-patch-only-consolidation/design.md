# 收敛为 patch 单一路线 · 技术设计

## 1. 为什么 patch 优于 relay

| 维度 | relay 模式 | patch 模式（native_messages） |
|---|---|---|
| 上游 TLS 发起方 | Rust | Claude CLI（原装 SDK） |
| 请求指纹 | Rust 重建，需手工对齐 UA/beta/attribution | CLI 原生产生，天然一致 |
| SSE 处理 | Rust 解码 → 事件仲裁 → 重新编码 | CLI 逐帧写 stdout，Rust 直通 |
| 上下文关联 | 需 HMAC 签名的 `RelayContextToken` 在旁路里回关 | 帧自带 `job_id`/`slot_id`，无需关联 |
| 失败面 | 解码器、仲裁器、关联器、上游客户端各自可错 | 只有一条 stdout 管道 |

relay 的三大机制（`sse_tap` 解码、`arbiter` 仲裁、`correlate` 关联）都是为「Rust 不知道 CLI 在做什么」而存在的补偿设计。patch 模式下 CLI 显式上报 `job_id`，这些补偿全部失去存在理由。

## 2. 依赖拆解（关键路径）

删除不是纯减法，存在一处真实耦合：

```
job_stream.rs (native_messages 活跃路径)
  └── use super::relay::sse_tap::{EventFilter, FilterPolicy, KIN_SYNTH_MARKER}
        └── EventFilter::with_policy(alloc, FilterPolicy::CLI)
```

`sse_tap.rs`（1235 行）内部有两类东西：

| 归属 | 内容 | 处置 |
|---|---|---|
| **通用** | `EventFilter`、`FilterPolicy::CLI`、`KIN_SYNTH_MARKER`、索引重映射 | 提升为 `event_filter.rs` |
| **relay 专属** | `FilterPolicy::RELAY`、`TapQueue`、`TapBinding`、`TapBudget`、`SseDecoder`、`drain_tap_chunks` | 删除 |

### 拆分顺序（必须先拆后删）

```
1. 新建 provider/multiplex_cli/event_filter.rs
2. 迁移 EventFilter + CLI 策略 + 相关测试
3. job_stream.rs 改 use 指向新模块
4. 编译通过 → 此时 relay 仅剩自身内部引用
5. 删除 relay/ 目录 + mod.rs 接线
```

第 4 步是安全闸门：若此时仍有外部引用 relay，说明拆分不完整，不得继续。

## 3. 删除批次

按「依赖倒序」执行，每批独立编译通过、独立提交，便于二分定位问题。

### S1 · 拆 sse_tap（准备）
新建 `event_filter.rs`，`job_stream.rs` 转向。**此批不删任何东西**，只做搬迁，风险最低。

### S2 · 删 relay
删 `relay/` 8 文件、`mod.rs` 接线、`RelayMode`（config.rs）、`KIN_RELAY_MODE`。
预期同时消失：`relay_addr` / `relay_upstream` / `relay_snapshot()` / `/healthz` 的 relay 字段。

### S3 · 删 mcp_slot
删 `mcp_server.rs`、`mcp_slot_wait` 及其 job 分发、MCP 配置写入（`write_mcp_config`）、`--agents` 参数注入。
验证 OQ1：`SlotPhase::WaitingTool` 写入方是否归零 → 归零则删。

### S4 · 删 native_agent + ExecutionMode 收敛
删 `NativeAgent` 变体、`check_opt_in`、`KIN_ALLOW_NATIVE_AGENT`、Go 侧 `validateExecutionMode` 的 native 分支。
若 `ExecutionMode` 退化为单变体 → 直接删除枚举与 `KIN_EXECUTION_MODE`，`is_native()` 分支全部内联为恒真路径（7 处）。

### S5 · 删 local_cli
删 `provider/local_cli.rs`、`main.rs` 分支。验证 OQ2：`IsolationMode` 是否退化 → 是则删。
连带清除依赖不存在的 `mock-claude.mjs` 的两个测试。

### S6 · 文档与配置收敛
`kernel.env.example` / `README.md` / spec / `APPLY.md`。

## 4. 风险与对策

| 风险 | 对策 |
|---|---|
| 删除时误伤 native_messages 行为 | 每批执行后跑全量 `cargo test`；S6 后以**真实 API** 复跑 hello + tool_use 续接（AC10） |
| 靠删测试凑绿 | 每批记录测试数增减并说明原因；只允许删「测试对象已不存在」的测试 |
| `ExecutionMode` 收敛后 config_hash 三方校验失配 | `config_hash` 计算若含 `execution_mode` 字段，需同步 Go 侧 RuntimeProfile 与 CLI 回传，三方一起改，否则 `/readyz` 会失败 |
| 大规模删除产生 CRLF 噪音掩盖真实改动 | 每次提交用 `git diff --ignore-space-at-eol` 复核真实内容行数并在 commit message 中注明 |

## 5. 回滚

每批一个独立 commit，`git revert` 即可。S2 之后不可通过设置环境变量回退到 relay——旧路径已物理删除，这是本任务的**预期结果**而非缺陷。真正需要回退时走 git。

## 6. 度量

删除前基线（本设计撰写时）：

```
relay/          3702 行 / 8 文件
local_cli.rs     747 行
mcp_server.rs    293 行
replay.rs        429 行（保留，cfg(test)）
mod.rs          4229 行（预期缩减，非删除）
```

预期净删除 ≈ 4700 行 + `mod.rs` 内 7 处双路线分支。实际数字在 AC11 中据实记录。
