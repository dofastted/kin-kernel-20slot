# 交接清单 — Relay 重构真实环境验收（给测试员）

代码状态：阶段 A–D 已提交（`da9e594` → `5f65fc6` + 本次 E/F 提交）。本地已验证：
62/64 `cargo test`（2 个失败为基线既有 `local_cli` PID 断言，与本任务无关）、
clippy 零新增、fmt 全仓干净、mock 上游端到端（CLI 支路逐位 digest 一致、tap
过滤、krc_ 跨 chunk、5xx 直通不 tap）、off 模式 `make smoke` 零修改通过。

**未在真实环境验证过的假设（你的核心任务）**：

1. CLI 对 `ANTHROPIC_BASE_URL=http://127.0.0.1:<port>` 的接受度（http 而非 https）。
2. **"CLI 每次内部 /v1/messages 都携带 slot_wait tool_result 里的 krc_ token"**
   —— 目前只有 transcript 结构推断 + mock 证明。若不成立，关联机制需重新设计，
   authoritative 不得上线。
3. 真实 Anthropic SSE 事件流经过滤表后的用户流形态（thinking/web_search 等）。
4. 网络指纹变化（reqwest/rustls 替代 CLI/Node 出站）对账号侧的影响。

## 验收顺序（严格按序）

### 第 1 步：off 基线

```bash
# 不设 KIN_RELAY_MODE，按 README 的 local_cli 流程启动
```

- [ ] 现有行为无回归（对话、WebSearch、客户端工具循环、continuation）。
- [ ] `curl :8080/healthz | jq .relay` = `null`；进程无额外监听端口。

### 第 2 步：observe

```bash
export KIN_RELAY_MODE=observe
```

- [ ] 启动日志确认 relay 先起、healthz 通过后 CLI 才启动。
- [ ] 故障注入：`KIN_RELAY_UPSTREAM=http://127.0.0.1:1`（不可达）→ 内核启动失败，
      CLI 不启动（不允许静默回 off）。
- [ ] `jq .relay` = `{relay_mode:"observe", relay_healthy:true, ...}`。
- [ ] 用户流行为与 off 完全一致（正文仍 stdout 整块）。
- [ ] **关联三场景**（假设 2 的实机证明，全部要在 relay 日志/metrics 里确认
      correlate 命中）：
      - [ ] 首轮普通请求
      - [ ] 含 WebSearch 的多轮内部请求
      - [ ] 客户端工具 `tool_result` resume 之后的请求
- [ ] 跑若干真实请求后 `digest_mismatch` = 0。非零 → 停止，不切第 3 步，
      带 digest 开工单。
- [ ] Claude PID 数 = 1，slot capacity = 20。

### 第 3 步：authoritative

```bash
export KIN_RELAY_MODE=authoritative
```

- [ ] 普通回答在 `kin_done` 前收到**多个自然的 upstream text_delta**（逐 token，
      非一次整块）——这是本次重构的核心验收。
- [ ] 正文与 observe 阶段 stdout 版本语义一致；无重复、无截断。
- [ ] WebSearch 阶段事件正常；`mcp__kin_runtime__*` 内部工具不出现在用户流。
- [ ] 客户端工具循环 + continuation 恢复原 job/slot。
- [ ] 20 并发（可复用 `scripts/conc20_hello.py`）无串流、`tap_dropped`=0、
      成功响应文本零缺失。
- [ ] 慢客户端（人为限速读 SSE）收到显式错误或异常 EOF，绝无缺字的
      `message_stop` 伪成功。
- [ ] VM 总 RSS 2–4 GB；首 token 相对 observe 的额外延迟 < 50ms。

### 回滚

任一步失败：`export KIN_RELAY_MODE=off`（或删除该 env）+ 重启 kernel。
CLI 启动后无法动态撤销 Base URL，必须重启。

## 观测点

- `GET :8080/healthz` → `.relay.{relay_mode, relay_healthy, tap_dropped, digest_mismatch}`
- 运维手册：`service/docs/RUNBOOK.md` §4.1（各故障处置）
- 日志纪律：relay 不落 token/正文；若发现日志里有 authorization/正文内容，按缺陷上报。

## 验收标准原文

见 `.trellis/tasks/08-28-relay-refactor/prd.md`「验收标准」一节。
