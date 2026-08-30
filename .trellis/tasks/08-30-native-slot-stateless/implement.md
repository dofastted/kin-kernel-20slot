# Implement — native_messages

按你给定的 10 步实施顺序展开。⛳ 标记的是需你确认后才继续的 gate。

---

## S0 · 立即止血 + 测试环境

### S0.1 停止对外暴露 `native_agent`（优先于一切）

当前 `native_slot` 会执行宿主 Bash/Read/Edit。在动任何新代码前先加门禁：

```rust
// execution_mode.rs
"native" | "native_slot" | "native_agent" => {
    if !env_truthy("KIN_ALLOW_NATIVE_AGENT") {
        return Err("native_agent 需显式 KIN_ALLOW_NATIVE_AGENT=1（会执行宿主工具）".into())
    }
    Ok(Self::NativeAgent)
}
```

**验证**：不带该 env 时 `KIN_EXECUTION_MODE=native_slot` 启动即退出并给出明确原因。（AC18）

### S0.2 干净 CLI worktree

`/mnt/x/project/claude-code` 当前 HEAD `d010f772`、工作树脏、无 `src/kin/`。不在这棵树上改。

```bash
cd /mnt/x/project/claude-code
git worktree add /mnt/x/project/claude-code-kin 77a7934
```

> CLAUDE.md 规定跨界 Git 走 Windows `git.exe`；执行前按 `wsl-windows-git-bridge` skill 确认路由。

### S0.3 gitignore + 凭证

`.gitignore` 当前**没有** `.local/` 规则，凭证会被误提交。先加：

```bash
printf '\n# --- Local credentials (never commit) ---\n.local/\n' >> .gitignore
```

再写入（内容需你提供）：

```bash
# .local/native.env
KIN_CLAUDE_CODE_OAUTH_TOKEN=sk-ant-oat01-...AA
KIN_SOCKS5=socks5h://user:pass@host:port
KIN_SLOT_TZ=America/New_York        # 必须与 SOCKS 出口一致的 IANA 名
```

**验证**：`git check-ignore -v .local/native.env` 命中；经 `http_to_socks.py` 桥 `curl -I https://api.anthropic.com` 有响应。

⛳ **Gate 0**：环境通过后回报，再开始改代码。

---

## S1 · CLI 侧 `native_messages`（对应你的第 2 步）

### S1.1 `claude.ts` 补采样参数（真补丁，见 design §3.2）

- `Options` 加 `topP` / `topK` / `stopSequences`
- 请求体构造处（`claude.ts:1788`）补 `top_p` / `top_k` / `stop_sequences`

**不可**用 `CLAUDE_CODE_EXTRA_BODY`——进程级 env，20 并发会串。

### S1.2 新增 `queryKinMessagesWithStreaming()`

签名与内部实现见 design §3.1。

### S1.3 重写 `nativeSlotRunner.ts` → `nativeMessagesRunner.ts`

- 删：`QueryEngine`、`mergeTools`、`clientToolStub`、`waitToolResult`、`toolUseIds`、`matchToolUseId`、`allClientToolUseIds`、`FileStateCache`、`ToolWaiter`、`parked`
- `SlotState` = `{ id, phase: 'idle'|'running'|'cancelling', jobId?, abort?, task? }`
- cancel 严格七步（design §4）
- 启动时设 `process.env.CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK = '1'`

### S1.4 `stdioProtocol.ts` v2

`KIN_PROTOCOL_VERSION = 2`；capabilities `['multi_slot','native_sse','stateless']`；删 `kin_tool_result`/`kin_hello`/`kin_job_parked`；`kin_host_ready` 加 `config_hash`。

### S1.5 `print.ts.hook.patch`

只传 `options`，删 `canUseTool`/`tools`/`agents`/`commands`/`getAppState`/`setAppState`/`cwd`。

**验证**：`bun run build` 通过；`grep -c "QueryEngine\|canUseTool\|hostTools" src/kin/*.ts` 为 0。

⛳ **Gate 1**

---

## S2 · Rust 侧适配

1. `execution_mode.rs` 三模式（`McpSlot` / `NativeAgent` / `NativeMessages`）
2. P1-1 `mod.rs:1754` — `message_start` 移入非 native 分支
3. P1-3 `mod.rs:1100` — `HostReady` 实校验（含 `config_hash`）
4. P1-4 `mod.rs:2344` — 计量键 fallback 到顶层 `job_id`
5. 删 `park_native_job()`；`native_messages` 不使用 `SlotPhase::WaitingTool`
6. `resume()` 改为 continuation 校验 + 历史合并 + 任意空闲槽重下 `JobStart`（design §5.2）
7. `StreamAssembler` 累积真实 `{id,name,input}`（design §5.4）
8. `api.rs` 加 `x-kin-native-slot` 诊断头
9. `native_protocol.rs` 删 `ToolResult`/`JobParked`，`HostReady` 加 `config_hash`

### S2.1 单测（AC17）

正常完成 / 取消七步时序 / job-slot 不匹配丢弃 / 并发不串槽。

**验证**：`cargo test --all-targets && cargo clippy --all-targets -- -D warnings`

⛳ **Gate 2**

---

## S3 · 功能验收

严格按你的实施顺序 2–8 步，产物落 `测试结果/<日期>-native-messages/<step>/`。

| 步 | 内容 | AC |
|---|---|---|
| S3.1 | 单槽 hello | AC1 |
| S3.2 | **`Read`/`Bash` 返回为客户端 tool_use，VM 无执行记录** | AC2 · AC10 |
| S3.3 | tool_result 第二回合，验证换槽继续 | AC3 |
| S3.4 | 双工具乱序 tool_result | AC4 · C2 |
| S3.5 | WebSearch 原生 server tool SSE | AC5 |
| S3.6 | 出站 `system` 抓包审计 | AC11 |
| S3.7 | 上游流式失败不降级 | AC12 |
| S3.8 | 5 并发 + 单任务取消 | AC6 |
| S3.9 | 20 短请求重叠 | AC7 · AC15 |
| S3.10 | 1 长上下文 + 19 短请求 → RSS | AC8 |
| S3.11 | `MAX_JOB_BYTES` 截断 | AC13 |
| S3.12 | 测试标准 01–07 | AC9 |

S3.2 是安全项，必须在 S3.8 扩量前完成。S3.10 用「1 长 + 19 短」压 RSS，避免真实 token 浪费。

⛳ **Gate 3**：S3.1–S3.8 全 PASS 后再跑扩量项。

---

## S4 · Go `config_hash`（第 9 步）

RuntimeProfile 定义 + 规范化 JSON hash + 三方比对 + `/readyz` 失败路径（design §6）。

**验证**：故意改一个字段使三方不一致 → `/readyz` 返回失败（AC14）。

⛳ **Gate 4**

---

## S5 · 切默认值（第 10 步）

AC1–AC18 全绿后，`ExecutionMode::default()` 改为 `NativeMessages`，更新 `APPLY.md` / `kernel.env.example` / `README.md` / `.trellis/spec/kernel/multiplex-cli-subsystem.md`。

⛳ **Gate 5**：切默认值前须你明确确认。

---

## 回滚点

| 位置 | 动作 |
|---|---|
| 任意阶段 | `KIN_EXECUTION_MODE=mcp_slot` |
| S1 后 | worktree `git checkout src/kin src/services/api/claude.ts` |
| S2 后 | `git revert`；协议 v2↔v1 不兼容，Rust 与 CLI 须同步回滚 |

## 本任务不含

- `native_agent` 的 8 条缺陷修复（该模式冻结、不对外暴露，未来若启用再单独立项）
- 慢客户端显式失败在 `native_messages` 下的复测
