# Implement — 内嵌 Messages Relay 与 upstream 权威流（核心档）

按序执行；每步末尾的验证命令通过后再进下一步。工作目录：
`kin-kernel-20slot/service`（cargo 命令均带 `--manifest-path kernel/Cargo.toml`）。

## 阶段 A：流控重构（独立正确性修复，不依赖 Relay）

- [ ] A1. `mod.rs`：引入 per-job `JobSink`（有界 data 队列 + 独立 terminal 信号）
  + egress task。
  - `JobSink { data_tx（256 条且 2 MiB 字节预算）, terminal: OnceLock<Terminal> }`；
    `Terminal { Overflow, ClientTooSlow, ClientGone, Done, Failed }`，原子终态、
    首写获胜（overflow / kin_done / 客户端取消只能一个成为终态）。
  - 共享生产者（`decode_stdout` 路径的 `emit`）改为 try_send 进 data 队列；
    区分 `TrySendError::Full`（溢出规则）与 `Closed`（egress 已退，停止生产）。
  - 溢出规则：阶段事件计数丢弃；**文本/thinking delta → set terminal(Overflow)**。
  - egress task：每次循环先查 terminal 再取数据；`send().await` +
    `KIN_CLIENT_STALL_SECS`（默认 30s）超时 → terminal(ClientTooSlow)。
  - 失败终态后：停发缓存正文；可写则发显式错误事件，否则直接关 SSE（异常
    EOF）；永不再发 `message_stop`/`Finished`；并在下一个 MCP 安全点终止/退休
    该 job（pending client_tool 以错误 resolve，slot 走 retire 路径，不得被
    永久占住）。
  - `resume` 路径：替换 egress 持有的用户 StreamTx；JobSink 与 terminal 保持。
- [ ] A2. `replay.rs`：更新统计（dropped 语义拆分为 `stage_dropped` /
  `jobs_aborted_slow_client`），调整/新增断言：成功完成的 job 文本 delta
  丢失数必须为 0。
- [ ] A3. 回归验证：

```bash
cargo test --all-targets --manifest-path kernel/Cargo.toml
cargo clippy --all-targets --manifest-path kernel/Cargo.toml -- -D warnings
```

新增单测：慢消费者收到显式错误或异常 EOF 而非静默缺字成功；快消费者全量收到
delta；队列 Full 时 Overflow 终态仍能送达（不经数据队列）；不读数据的客户端
绝不收到成功结束帧。

## 阶段 B：配置、签名模块与骨架

- [ ] B1. `config.rs`：`RelayMode { Off, Observe, Authoritative }`，
  `KIN_RELAY_MODE` 解析，非法值返回 Err（进程退出）；`KIN_RELAY_ADDR` /
  `KIN_RELAY_UPSTREAM` / `KIN_CLIENT_STALL_SECS` 进 `MultiplexConfig`。
- [ ] B2. `signing.rs`：HMAC-SHA256 统一 `sign/verify`（`hmac` + `sha2` 依赖，
  常量时间 `verify_slice`）；域隔离 `kin/kct/v1` / `kin/krc/v1`；共用 Runtime
  secret。**迁移 `continuation.rs`** 到该模块（线格式 `kct_` 不变；重启换
  secret，无兼容负担）；删除自制 `mac()`。单测：域隔离（kct 签名不能验过 krc）、
  空 secret 拒绝、篡改检测。
- [ ] B3. 建 `relay/` 模块骨架：`mod.rs`（RelayHandle、spawn、healthz）、
  `metrics.rs`（AtomicU64 计数器组）。单测：mode 解析（含非法值报错）。
- [ ] B4. 验证：`cargo test` + `KIN_RELAY_MODE=bogus cargo run` 启动即错。

## 阶段 C：Relay 数据面

- [ ] C1. `server.rs`：loopback axum 反代，全 path 透传，header 白名单/hop-by-hop
  剔除，**流式** body 转发（`Bytes` 引用计数；请求体一边转发一边喂扫描器，
  全程不 `to_bytes()` 缓存）。日志纪律：不落 token/正文。
- [ ] C2. `upstream.rs`：reqwest 流式出站；代理选择（KIN_SOCKS5 直连 socks5h，
  否则 KIN_HTTPS_PROXY）；上游非 2xx 原样回 CLI。
  依赖调整：reqwest 增加 `socks` + `stream` feature。
- [ ] C3. `correlate.rs`：`krc_` token 生成（`signing.rs`，域 `kin/krc/v1`）、
  **增量流式扫描**（跨 chunk 边界保留尾部候选、单 token 上限 2 KiB）、五重
  校验（HMAC 常量时间 / generation / job 存在 / slot_id 一致 / slot 仍归属该
  job）、按 job_id 动态查询。
  单测：多 token 取最后有效；旧 generation 拒绝；签名有效但 job 已完成 → 排除；
  slot 已易主 → 排除；token 跨 chunk 边界切开仍识别；超长候选放弃；伪 token
  验签失败忽略；无 token → 纯转发标记。
- [ ] C4. `mcp_server.rs` / `mod.rs::mcp_slot_wait`：job 响应注入 `relay_context`
  （仅 mode != off 时）。
- [ ] C5. `sse_tap.rs`：增量 SSE 帧解码（`\n\n` 边界、未完成帧 1 MiB 上限）、
  per-job tap 有界队列（256 条/2 MiB）、事件过滤表（信封吞、`mcp__kin_runtime__*`
  吞、ping 吞、server_tool 转发、block index 重编号）。
  单测：跨 chunk 边界帧、过滤表逐项、溢出 → TapPoisoned。
- [ ] C6. 用 mock 上游（本地 axum 返回录制 SSE fixture，可由
  `traces/` 或 `scripts/` 现有素材改造）跑通：CLI 支路字节逐位一致（digest 对比）、
  tap 输出事件序列符合过滤表。
- [ ] C7. 验证：`cargo test` + clippy。

## 阶段 D：Arbiter 与 boot 接线

- [ ] D1. `arbiter.rs`：状态机（NoBody/UpstreamActive/StdoutFallback/Completed/
  poisoned 终止）、单向降级约束、迟到事件拒绝、双侧摘要（SHA-256）累计。
  单测：升级后 stdout 正文被抑制；UpstreamActive 中 tap 溢出 → 失败终止而非降级；
  observe 模式强制 StdoutFallback。
- [ ] D2. `job_stream.rs`：来源标记与去重挂接 arbiter；`complete_job` 最终正文
  优先级：upstream 累计 > stdout text > fallback_content；usage 采用 upstream
  累加值（有则优先）。
- [ ] D3. `mod.rs::start_claude` + `supervisor.rs`：mode != off 时 Relay 先起、
  healthz 自检失败则启动失败；`SpawnSpec.anthropic_base_url` → env 注入。
  确认 `NO_PROXY=127.0.0.1` 已覆盖 CLI→Relay 回环（现有 apply_proxy_env 已设）。
- [ ] D4. 状态暴露：`Provider::relay_snapshot()` 默认 None；multiplex 实现返回
  `relay_mode/relay_healthy/tap_dropped/digest_mismatch`；`api.rs` `/status`
  加 `relay` 字段。
- [ ] D5. 验证：`cargo test` + clippy + `make static-check`。

## 阶段 E：端到端（mock CLI + mock 上游，不花真实 token）

- [ ] E1. `off` 模式基线：现有 smoke / 回放全通过，`/status.relay.relay_mode=off`，
  确认未监听 relay 端口、CLI env 无 ANTHROPIC_BASE_URL。

```bash
make static-check && make smoke
cargo test --all-targets --manifest-path kernel/Cargo.toml
```

- [ ] E2. `observe` 模式：mock-claude 经 Relay 出站到 mock 上游；用户正文仍
  stdout；digest 对比记录产生；Relay 不健康时内核启动失败（不静默回 off）。
  关联覆盖（mock 层面）：首轮请求、WebSearch/内部多轮后续请求、客户端
  `tool_result` resume 后的请求，三种场景都能找到正确 `krc_`。
  ——同一组场景写入 F2 交接清单，要求测试员在**真实 CLI** 上复验（"CLI 每次
  内部请求都携带 krc_" 是待实机证明的假设，mock 通过不算证明）。
- [ ] E3. `authoritative` 模式：用户流收到 upstream 逐 token delta；内部 MCP
  工具不外泄；`kin_done` 收尾唯一一组 message_delta/message_stop；
  tool_result continuation 恢复原 job/slot；20 并发回放无串流、文本零丢失。
- [ ] E4. 故障注入：上游 5xx / tap 溢出 / 慢客户端 → CLI 支路不受阻、用户侧
  显式错误、无伪成功。

## 阶段 F：收尾

- [ ] F1. 文档：`docs/SOURCE_AND_PRINCIPLES.md` + `docs/RUNBOOK.md` 增补 Relay
  段落（模式语义、回滚步骤）；`.trellis/spec/kernel/multiplex-cli-subsystem.md`
  增补 relay 小节并**改写"自制 MAC"告诫段落**（已迁 HMAC-SHA256，
  spec update，步骤 3.3）。
- [ ] F2. 交接测试员的清单（写入 task 目录 `handover.md`）：
  真实 CLI 验收顺序 off→observe→authoritative、验收标准对照表（prd.md）、
  观测点（/status.relay、digest_mismatch、tap_dropped）、回滚操作。
- [ ] F3. 最终全量检查：

```bash
cargo fmt --check --manifest-path kernel/Cargo.toml
cargo clippy --all-targets --manifest-path kernel/Cargo.toml -- -D warnings
cargo test --all-targets --manifest-path kernel/Cargo.toml
make static-check && make smoke
cd control && go test ./...   # 确认未破坏 Go 侧
```

## 风险点与回滚

| 风险 | 缓解/回滚 |
|---|---|
| A 阶段改 `emit` 破坏现有 8 个内联测试 | A 阶段单独提交；先跑全量测试再进 B |
| "CLI 每次内部请求携带 krc_" 假设不成立 | mock 只做行为定义；observe 阶段真实 CLI 三场景复验（E2/F2）；不成立则关联机制需另行设计，authoritative 不得上线 |
| `krc_` 扫描误匹配用户正文中的相似字符串 | HMAC 常量时间验签 + 五重校验兜底；单测覆盖伪 token |
| continuation 签名迁移破坏现有 token 测试 | 线格式不变，仅算法替换；重启换 secret 本无跨进程兼容；同步更新 `stale_generation_is_continuation_lost` 等测试 |
| 网络指纹变化：Anthropic 侧看到 reqwest/rustls 而非 CLI/Node 的 TLS 指纹、ALPN、连接池 | 已确认的架构取舍（design §10.5）：本方案是"应用层请求特征对齐"；风险由测试员在 observe 阶段用真实上游观察账号侧表现 |
| reqwest 新 feature（socks/stream）+ hmac/sha2 拉重依赖 | `default-features=false` 基础上最小增量；`cargo tree` 检查 |
| CLI 对 loopback Base URL 的证书/协议行为未知 | Relay 走 http://127.0.0.1（mock 上游验证）；真实行为由测试员在 observe 阶段确认 |
| 回放统计语义变化影响既有报告对比 | REPORT.md 基线不改写；新统计字段另名记录 |

提交切分建议：A（流控）→ B+C（relay 数据面）→ D（arbiter+接线）→ E/F（验证与文档），
每段独立可回滚。
