# native_messages 无状态执行模式

## Goal

新增 `execution_mode=native_messages`：Claude CLI 只负责构造并发出「原装」Anthropic Messages 请求，Rust 负责调度、会话重建与协议转换，客户端负责客户端工具。最终以它替换 `native_slot`，并把默认值从 `mcp_slot` 切过去。

## Background

`e467415` 已达成 `plan/原型.md` 的 system 布局（§1）、槽位生命周期移出模型（§3 前半）、显式 `job_id/slot_id` stdout 协议（§4）、CONNECT→SOCKS 隧道。单 PID × 2 slot 真实并发实测通过。

未闭环的是工具续接与取消。根因是 CLI 侧引入了完整 `QueryEngine`（agent loop），而原型只要求「使用调用方 system/messages/tools 向 Anthropic 请求，原样输出 StreamEvent」。多出来的 agent loop 是以下全部缺陷的来源。

### 已确认的阻断项

| ID | 问题 | 证据 |
|---|---|---|
| P0-1 | `clientToolStub()` 用 `as unknown as Tool` 绕过接口，缺 `mapToolResultToToolResultBlockParam()` | `toolExecution.ts:1367` 无条件调用该方法，首个客户端工具返回即 `TypeError` |
| P0-2 | `kin_cancel_ack` 在 `runJob.finally` 前发送 | `nativeSlotRunner.ts:140` 立即 ack；Rust 收到即重新入队，Node 槽可能仍 `running/parked` |
| P0-3 | cancel / tool_result 未严格校验 `(job_id, slot_id, tool_use_id)` | `nativeSlotRunner.ts:130-158` 只查 slot_id，迟到帧可取消新任务 |
| P0-4 | 本地工具与客户端同名工具冲突处理错误 | `mergeTools()` 的 `hostNames.has(name)` 跳过 stub，`Read/Bash/Edit` 落到宿主执行 |
| **P0-5** | **宿主工具全量暴露 + 权限无条件放行（安全）** | `mergeTools()` 返回 `[...hostTools, ...stubs]`；`print.ts.hook.patch` 写死 `canUseTool: () => ({behavior:'allow'})`。任意调用方 prompt 可在服务器执行 Bash |
| P1-1 | native resume 有两个 `message_start` 来源 | `mod.rs:1754` emit 在 native 分支前且无 `is_native()` 守卫 |
| P1-2 | `kin_job_parked` 只带 ID，Rust 合成 `name:"tool", input:{}` | `mod.rs:1207`；非流式/OpenAI 聚合与会话记录不准确 |
| P1-3 | `HostReady` 只记录不验证 | `mod.rs:1100-1126` 直接注册槽，protocol/slots/timezone/layout 不匹配也放行 |
| P1-4 | native stdout 未按 `job_id` 应用 `MAX_JOB_BYTES` | `mod.rs:2344` 计量键是 `parent_id()`，只认 `parent_tool_use_id`；native 帧顶层是 `job_id`，完全不进计量分支 |
| **P1-5** | **`FileStateCache` 跨 job 复用** | `nativeSlotRunner.ts:269` 槽回收后下一租户继承上一租户文件读缓存 |
| **P1-6** | **上游流式失败时静默降级为非流式** | `claude.ts:2589`；用户会看到整块正文而非逐 token。需 `CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1` |

P0-5 / P1-5 / P1-6 为本次新增，其余八条来自既有 review 并已逐条核实成立。

## 决策

`native_slot` 不再继续修补，改为并存三模式：

| 模式 | 定位 |
|---|---|
| `mcp_slot` | 现有稳定回退，不动 |
| `native_agent` | 当前 QueryEngine 实验路线，**立即停止对外暴露**（避免宿主工具执行），保留为未来可选的「服务端代理执行工具」模式 |
| `native_messages` | 新的产品目标路线 |

## Requirements

### R1 · CLI 侧新增 Kin 专用薄封装

在 `claude.ts` 增加 `queryKinMessagesWithStreaming()`，内部仍调 `queryModelWithStreaming()`：

- `tools: []`，调用方 tool schema 进 `options.extraToolSchemas`（CLI 不产生任何本地工具执行器）
- `max_tokens` → `options.maxOutputTokensOverride`
- **补充 `Options` 尚未暴露的 `top_p` / `top_k` / `stop_sequences`**（见 Constraints C1）
- 进程级设置 `CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1`
- `SystemAPIErrorMessage` 映射为 `kin_job_error`
- 只把 `stream_event.event` 写 stdout
- `message_delta` 记录 usage / stop_reason
- generator **完全退出后**才发 `kin_job_done`

### R2 · 槽状态机三态

每槽只保留 `slot_id` / `job_id` / `AbortController` / 当前异步任务 / `idle | running | cancelling`。

删除：`QueryEngine`、`FileStateCache`、`Tool` bridge、`parked`、`toolWaiters`、`canUseTool`、宿主 Bash/Read/Edit、跨 HTTP 存活的 agent loop。

**cancel 必须严格按七步**：

```
1. 收到 kin_cancel
2. 校验 job_id + slot_id
3. AbortController.abort()
4. 等待 generator 退出
5. 释放 HTTP stream/body
6. 槽置 idle
7. 发送 kin_cancel_ack
```

Rust 只能在第 7 步之后重新入队。

### R3 · 工具循环新语义

> 不再恢复同一个内存中的 Claude Agent loop，而是恢复同一个**逻辑 Messages 会话**。

第一回合：Rust 下发完整 `system/messages/tools` → CLI 请求 → 原生 SSE 逐帧回 → 遇客户端 `tool_use` 以 `stop_reason=tool_use` 正常结束 → **槽立即释放** → Rust 保存 assistant tool_use 与 continuation 状态。

第二回合：客户端提交 `tool_result` → Rust 验证 continuation 与全部 `tool_use_id` → 合并完整历史 → 分配**任意**空闲槽重新执行一次 Messages 请求。

若客户端只提交 `tool_result`，Rust 用保存的 `pending_request` 补齐历史；若客户端自带完整历史，验证后直接使用。

### R4 · 字段所有权

| 所有者 | 字段 |
|---|---|
| 调用方 / Rust | messages、调用方 system、tools、tool_choice、thinking、max_tokens、采样参数 |
| Claude CLI | Authorization、UA、billing/identity、账号 metadata、必需 beta、retry、prompt cache |
| Go 控制台 | system layout、时区、SOCKS、模型/工具/beta 白名单、slot 数、资源上限 |

调用方**不得**覆盖 CLI metadata、Authorization、UA、强制 beta。

### R5 · Rust 侧

- `tool_use` 是正常终态，不进 `WaitingTool`
- continuation 不再保留 worker/slot
- `StreamAssembler` 从原生 SSE 保存真实 `{id, name, input}`
- native 帧按顶层 `job_id` 统计 `MAX_JOB_BYTES`
- Anthropic 入口原样输出；OpenAI/Gemini 从统一 `CanonicalEvent` 转换
- 仅 `kin_job_done` 后回收槽
- 新增 `x-kin-native-slot: s00` 响应头，仅用于诊断
- P1-1 / P1-3 一并修复

### R6 · 协议 v2

`KIN_PROTOCOL_VERSION = 2`，capabilities 改为 `['multi_slot', 'native_sse', 'stateless']`。

删除 stdin `kin_tool_result` / `kin_hello`，删除 stdout `kin_job_parked`。新增 `config_hash` 字段于 `kin_host_ready`。

### R7 · Go 控制台 RuntimeProfile

不可变 profile：`execution_mode` / `system_layout` / `timezone` / `slot_count` / `socks5` / `allowed_models` / `allowed_server_tools` / `allowed_betas` / `max_body_bytes` / `max_output_tokens`。

对规范化 JSON 计算 `config_hash`：Go 下发 desired → Rust `/healthz` 返回 applied → CLI `kin_host_ready` 回传相同 hash → **三者不一致则 `/readyz` 失败**。

system/env/SOCKS 变更走 drain → 重启 CLI → generation+1，禁止任务中途热改。

### R8 · 本机测试环境

干净 worktree 应用补丁并构建 `dist/cli-node.js`；setup-token 经 `KIN_CLAUDE_CODE_OAUTH_TOKEN`；SOCKS5 经 `KIN_SOCKS5` + `http_to_socks.py`；`TZ` 与 `CLAUDE_CODE_TIMEZONE` 同设为与 SOCKS 出口一致的 IANA 名。产物落 `测试结果/<日期>-native-messages/`。

## Non-Goals

- 不实现服务端代执行宿主工具。该能力留给 `native_agent` 模式，本任务不对外暴露。
- 不改 `mcp_slot` 路径，保持回退可用。
- 不改 relay。`native_messages` 下 relay 关闭。

## Acceptance Criteria

### 功能

- [x] AC1 单槽 hello：客户端逐 token 收到 `text_delta`，顺序与上游 SSE 一致
- [x] AC2 `Read`/`Bash` 被作为**客户端 tool_use 返回**，虚拟机上无任何执行记录
- [x] AC3 tool_result 第二 HTTP 回合成功续接，且**可换槽**继续
- [x] AC4 双工具乱序 tool_result 续接正确
- [x] AC5 WebSearch 原生 server tool SSE 正常
- [x] AC6 5 并发 + 单任务取消：被取消槽正确回收，其余正常完成
- [x] AC7 20 个短请求重叠：单 CLI PID 不变（`gen=1`），无 `slot busy`
- [x] AC8 「1 个长上下文 + 19 个短请求」RSS 在 2–4 GB（实测远低于该上限，见 APPLY.md 说明）
- [x] AC9 测试标准 01–07 全 PASS（06 沿用既有口径）

### 安全与正确性

- [x] AC10 出站 `tools` 数组严格等于调用方声明，无 Bash/Read/Edit
- [x] AC11 出站 `system` 只有 billing + `# Environment`（+ 调用方 leftover），无默认长 prompt、无 Kin MCP transcript
- [x] AC12 上游流式失败时**不降级为非流式**（`kin_job_error` 而非整块正文）
- [x] AC13 单 job stdout 超 `MAX_JOB_BYTES` 被截断，不影响同 PID 其他 job
- [x] AC14 `config_hash` 三方不一致时 `/readyz` 失败
- [x] AC15 20 并发下各 job 的 `top_p`/`top_k`/`stop_sequences` 互不串扰

### 工程

- [ ] AC16 `cargo test --all-targets` + `cargo clippy -- -D warnings` 通过
- [x] AC17 槽状态机单测：正常完成 / 取消七步时序 / job-slot 不匹配丢弃 / 并发不串槽（取消七步时序仅单测覆盖 fallback 分支，主分支见 APPLY.md 说明）
- [ ] AC18 `native_agent` 不对外暴露（配置层拒绝或显式 opt-in 门禁）
- [ ] AC19 默认 `KIN_EXECUTION_MODE` 切到 `native_messages`（AC1–AC18 全绿后）

## Constraints

- **C1** `top_p`/`top_k`/`stop_sequences` **不可**经 `CLAUDE_CODE_EXTRA_BODY` 传递——它是进程级 env（`claude.ts:282`），单 PID 20 并发会串 job。必须扩 `Options` 类型并在请求体构造处（`claude.ts:1788`）补字段。
- **C2** `normalizeMessagesForAPI(messages, [])` 的空 `availableToolNames` 会剥离 `tool_reference` block（`messages.ts:1839`）。该 block 是 Claude Code 私有类型，第三方客户端不发，风险可控但需在验收中确认。
- **C3** baseline 为 `claude-code-best/claude-code` `77a7934`，补丁须能干净应用。
- **C4** 不伪造 `cch` / `cc_version`；attribution 一律来自 `getAttributionHeader()`。
- **C5** setup-token 为 inference-only、不可 refresh、无 `user:sessions:claude_code` 等 scope。
- **C6** 凭证与 SOCKS 口令不得进入日志、产物或提交。

## Open Questions

- OQ1 `stream:false` 聚合：CLI 仍逐事件输出，Rust 沿用 `job_stream.rs`；需确认 `tool_use` block 被正确累积进非流式响应。
- OQ2 20 并发是否共享单个 Anthropic SDK client；若共享 keep-alive 池需确认连接数上限与 `http_to_socks.py` 承载。
