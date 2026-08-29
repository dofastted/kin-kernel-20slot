# 交接清单 — Relay 重构真实环境验收（给测试员）

代码状态：`378ad7d` 静态验收二轮缺陷已在当前工作树修复，尚未提交，尚未签收。
`authoritative` 禁止上线，直到 observe 阶段真实环境采证完成并确认通过。

本地基线：上一轮阶段 A-D/E/F 已完成，覆盖 relay strict boot、off/observe/
authoritative 模式、tap 非阻塞、5xx 直通不 tap、digest 比对、慢客户端显式失败、
krc_ 跨 chunk、HMAC 签名 token、mock 上游端到端。历史本地结果为 62/64
`cargo test`，2 个失败是既有 `local_cli` PID 断言，与 relay 无关。

## 三轮修复摘要（authoritative 实测三 FAIL）

- FAIL-1 无逐 token：根因是 arbiter 竞态——stdout 整块帧先于首个 tap delta 到达
  时 turn 被单向锁死 StdoutFallback，upstream delta 全被抑制。修复为『延迟仲裁』：
  authoritative 且 tap 已附着时，stdout 正文帧（含配套 stop）暂存不转发；首个
  tap delta 升级 UpstreamActive 后丢弃暂存；tap 始终无产出则在 kin_done 释放暂存
  兜底（预算 2 MiB，超限立即回退）。observe/off 行为不变。
- FAIL-2 20 并发串流：根因是 root supervisor 带 --forward-subagent-text，其
  transcript 内嵌多个存活 job 的 krc_ token，原「取最后一个」把 root 响应 tap 进
  错误 session。修复为『恰好一个存活』关联：签名有效 token 按 job_id 去重后逐个
  做运行时存活校验，恰好 1 个才关联；0 → miss；≥2 → ambiguous（新指标
  correlate_ambiguous），纯转发不 tap。
- FAIL-3 18/20（503）：slot 完成到重入 slot_wait 的空隙撞上瞬时并发。submit 对
  NoCapacity 做有界重试（50ms 间隔，KIN_SUBMIT_WAIT_MS 默认 2000ms），超时才 503。
- 慢客户端测不到是 FAIL-1 的衍生（正文一次塞满 socket 缓冲）；逐 token 生效后
  stall 检测自然可测，复测时无需改测试方法。

### 三 FAIL 复测步骤

- [ ] 逐 token：短句/长句/80 词段落均出现多个自然 `content_block_delta`（非单块）。
- [ ] 首 token 时间重新采集（此前 1209ms 实为整块 stdout 帧时间，不可比）。
- [ ] 20 并发：20/20 成功、无串流、无空 200、无 503；观察
      `.relay.correlate_ambiguous` 应随 root 内部请求增长（这是 root 流量被正确
      排除的证据），`correlate_hit` 仍随用户 turn 增长。
- [ ] 慢客户端（依赖逐 token 生效）：5s 停读 → 显式错误或异常 EOF，无伪成功。

## 二轮修复摘要

- F-1：新增 provider boot 状态门 `Booting -> Ready | Failed`，`/readyz` 在
  boot 未完成或失败时强制 503，原因分别为 `booting`、`boot_failed`。
- F-2：relay 模式下 CLI 启动前增加上游 preflight：relay spawn、`/healthz`
  通过后，对 `KIN_RELAY_UPSTREAM` 发轻量 GET；连接或代理错误直接 boot failed，
  不启动 CLI。非 2xx 状态不算失败。
- F-3：relay metrics 新增 `correlate_hit`、`correlate_miss`、
  `tap_response_started`；关联成功只用 `job_id`、`slot_id`、`turn_id` 打
  debug 日志，不记录 token、正文或 headers。
- F-4：tap poison 改为 turn-local。resume 会创建新 turn 的独立 poison flag；
  旧 turn 的迟到 `TapQueue::poison()` 不再影响新 turn。
- F-5：`ContextScanner` 扫描阶段即验 `krc_` HMAC，只保存最后一个签名有效
  token；运行时 generation/job/slot 校验仍在关联查找阶段完成。
- 上一轮 P0 清单简述：relay 不能静默降级到 off；CLI 支路不被 tap 阻塞；
  upstream 5xx 必须原样透传且不 tap；authoritative 已切到 upstream body 后
  禁止中途回退 stdout；relay 日志不得包含 token、正文或 authorization。

## 未在真实环境验证过的假设（你的核心任务）

1. CLI 对 `ANTHROPIC_BASE_URL=http://127.0.0.1:<port>` 的接受度（http 而非 https）。
2. **CLI 每次内部 `/v1/messages` 都携带 slot_wait tool_result 里的 `krc_` token**。
   目前只有 transcript 结构推断 + mock 证明。若不成立，关联机制需重新设计，
   `authoritative` 不得上线。
3. 真实 Anthropic SSE 事件流经过滤表后的用户流形态（thinking/web_search 等）。
4. 网络指纹变化（reqwest/rustls 替代 CLI/Node 出站）对账号侧的影响。

## 验收顺序（严格按序）

### 第 1 步：off 基线

```bash
# 不设 KIN_RELAY_MODE，按 README 的 local_cli 流程启动
```

- [ ] 现有行为无回归（对话、WebSearch、客户端工具循环、continuation）。
- [ ] `curl :8080/healthz | jq .relay.relay_mode` = `"off"`；进程无额外 relay
      监听端口。

### 第 2 步：observe

```bash
export KIN_RELAY_MODE=observe
export RUST_LOG=kin_kernel=debug,tower_http=info
```

- [ ] 启动日志确认 relay 先起、relay `/healthz` 通过、upstream preflight 通过后，
      CLI 才启动。
- [ ] 故障注入：`KIN_RELAY_UPSTREAM=http://127.0.0.1:1`（不可达）→ provider
      boot failed，`/readyz` 返回 503 且 reason 为 `boot_failed`，CLI 不启动，
      不允许静默回 off。
- [ ] `curl :8080/healthz | jq .relay` 包含
      `{relay_mode:"observe", relay_healthy:true, correlate_hit, correlate_miss, correlate_ambiguous,
      tap_response_started, tap_dropped, digest_mismatch}`。
- [ ] 用户流行为与 off 完全一致（正文仍 stdout 整块）。
- [ ] **关联三场景**（假设 2 的实机证明）：
      首轮普通请求、含 WebSearch 的多轮内部请求、客户端工具 `tool_result`
      resume 之后的请求。
- [ ] 三场景采证方式：每个场景后记录 `.relay.correlate_hit` 增量、
      `.relay.correlate_miss` 是否异常增长、`.relay.tap_response_started` 增量；
      同时保存 debug 日志中的 `relay correlated request` 行，日志行只能包含
      `job_id`、`slot_id`、`turn_id`。
- [ ] 跑若干真实请求后 `digest_mismatch` = 0。非零 → 停止，不切第 3 步，
      带 digest 开工单。
- [ ] Claude PID 数 = 1，slot capacity = 20。

### 第 3 步：authoritative

```bash
export KIN_RELAY_MODE=authoritative
export RUST_LOG=kin_kernel=debug,tower_http=info
```

- [ ] 只有在第 2 步 observe 三场景采证全部通过后，才允许进入本步。
- [ ] 普通回答在 `kin_done` 前收到**多个自然的 upstream text_delta**（逐 token，
      非一次整块）——这是本次重构的核心验收。
- [ ] 正文与 observe 阶段 stdout 版本语义一致；无重复、无截断。
- [ ] WebSearch 阶段事件正常；`mcp__kin_runtime__*` 内部工具不出现在用户流。
- [ ] 客户端工具循环 + continuation 恢复原 job/slot。
- [ ] 20 并发（可复用 `scripts/conc20_hello.py`）无串流、`tap_dropped`=0、
      成功响应文本零缺失。
- [ ] 慢客户端（人为限速读 SSE）收到显式错误或异常 EOF，绝无缺字的
      `message_stop` 伪成功。
- [ ] VM 总 RSS 2-4 GB；首 token 相对 observe 的额外延迟 < 50ms。

### 回滚

任一步失败：`export KIN_RELAY_MODE=off`（或删除该 env）+ 重启 kernel。
CLI 启动后无法动态撤销 Base URL，必须重启。

## 观测点

- `GET :8080/readyz` → provider boot 未完成时 `reason=booting`；boot 失败时
  `reason=boot_failed`；boot 成功但无容量时 `reason=no_capacity`。
- `GET :8080/healthz` → `.relay.{relay_mode, relay_healthy, correlate_hit,
  correlate_miss, tap_response_started, tap_dropped, digest_mismatch}`。
- debug 日志：`relay correlated request` 只能携带 `job_id`、`slot_id`、`turn_id`。
- 运维手册：`service/docs/RUNBOOK.md` §4.1（各故障处置）。
- 日志纪律：relay 不落 token/正文/authorization；若发现日志里有这些内容，
  按缺陷上报。

## 验收标准原文

见 `.trellis/tasks/08-28-relay-refactor/prd.md`「验收标准」一节。
