# Design — 内嵌 Messages Relay 与 upstream 权威流（核心档）

前置阅读：`prd.md`（范围与验收）、`.trellis/spec/kernel/multiplex-cli-subsystem.md`（现有不变式）。

## 1. 边界与总体结构

新代码收敛在 `provider/multiplex_cli/relay/`，不触碰 `api.rs` 的对外协议、
`local_cli.rs`、Go control（本期只在 kernel 状态接口加只读字段）。

```text
provider/multiplex_cli/signing.rs   # 统一 HMAC-SHA256 sign/verify（kct_/krc_ 域隔离）
provider/multiplex_cli/relay/
├── mod.rs          # RelayHandle：启动/健康/共享状态；RelayMode 定义
├── server.rs       # loopback axum listener（127.0.0.1，端口 0 随机或 KIN_RELAY_ADDR）
├── upstream.rs     # reqwest 出站到 Anthropic（或 KIN_RELAY_UPSTREAM 覆盖，测试用 mock）
├── correlate.rs    # krc_ 流式扫描/验签 + job 动态查找
├── sse_tap.rs      # 增量 SSE 解码（跨 chunk 边界安全），内部工具过滤
├── arbiter.rs      # SourceArbiter：NoBody/UpstreamActive/StdoutFallback/Completed
└── metrics.rs      # tap_dropped / digest_mismatch / relay_requests 等原子计数
```

同步修改（均在现有文件内做小切口）：

| 文件 | 改动 |
|---|---|
| `config.rs` | `RelayMode` 解析（`KIN_RELAY_MODE`）；非法值 → 启动报错退出 |
| `continuation.rs` | 签名实现迁移到 `signing.rs`（HMAC-SHA256，域 `kin/kct/v1`），线格式 `kct_` 前缀不变 |
| `mod.rs` | 启动顺序接入 Relay；`emit` 流控重构（见 §6）；job 动态路由查询接口给 correlate 用 |
| `supervisor.rs` | `SpawnSpec` 增加 `anthropic_base_url: Option<String>`；设 `ANTHROPIC_BASE_URL` |
| `job_stream.rs` | 来源标记（upstream/stdout/fallback）、正文去重、arbiter 状态挂接 |
| `mcp_server.rs` | `slot_wait` 响应注入 `relay_context`；`kin_done` 最终正文按 arbiter 取值 |

## 2. 配置语义（架构师定稿，不得偏离）

| `KIN_RELAY_MODE` | 启动语义 |
|---|---|
| 未设置 / `off` | 完全不启动 Relay、不注入 `ANTHROPIC_BASE_URL`，行为与现状 100% 一致 |
| `observe` | Relay 是必需组件；Relay 未通过健康检查则 **内核不 Ready、不启动 Claude CLI** |
| `authoritative` | 同样严格启动；tap、关联、正文仲裁全部启用 |
| 非法值 | `Config::from_env` 返回错误，进程退出；**禁止静默降级为 off** |

理由：显式设置新模式后系统不能偷偷退回 `off`，否则出现"测试通过但实际没走
Relay"的假阳性；CLI 启动后无法动态撤销已注入的 Base URL，回滚 = 改配置重启。

辅助配置：

- `KIN_RELAY_ADDR`：默认 `127.0.0.1:0`（随机端口，启动后回读实际端口）。
- `KIN_RELAY_UPSTREAM`：默认 `https://api.anthropic.com`；测试时指向 mock。
- Relay 出站代理：优先 `KIN_SOCKS5`（reqwest 原生支持 socks5h），否则
  `KIN_HTTPS_PROXY`。注意与 CLI 路径的区别：CLI 只能用 HTTP CONNECT 桥，而
  Relay 是 Rust 出站，可直连 SOCKS5。现有 `apply_proxy_env` 的
  `NO_PROXY=127.0.0.1` 保证 CLI→Relay 的 loopback 不被代理劫持。

## 3. 启动顺序（`Runtime::start_claude`）

```text
mode == off:
  现有流程不变（不 touch）。

mode != off:
  1. mcp_server::spawn（现有）
  2. relay::spawn(runtime, cfg) → 绑定 loopback，回读端口
  3. 健康自检：GET http://127.0.0.1:{port}/healthz，失败 → Err，内核不 Ready
  4. supervisor::spawn，SpawnSpec.anthropic_base_url = Some(relay_url)
     → cmd.env("ANTHROPIC_BASE_URL", relay_url)
  5. 其余（stdout decoder、bootstrap）不变
```

Relay 与 MCP server 同为进程内 axum listener，随 kernel 生命周期存活；CLI 退出
不影响 Relay（重启 CLI 时 generation 递增，旧 relay_context 因 generation 不匹配
被拒——复用 `continuation.rs` 的 generation 语义）。

## 4. 请求↔job 关联（correlate.rs）

### 注入路径

`mcp_slot_wait` 的 job 响应 JSON 增加一个字段：

```json
"relay_context": "krc_<hex_payload>.<hex_mac>"
```

payload = `{job_id, slot_id, generation, nonce}`。签名**不复用**现有自制
`continuation.rs::mac()`（XOR/rotate 自制算法、非常量时间校验，不适合作为外部
可控 body 的认证边界）。新建统一签名模块 `provider/multiplex_cli/signing.rs`：

- HMAC-SHA256（`hmac` + `sha2` crate），库提供的常量时间 `verify_slice`。
- `kct_`（continuation）与 `krc_`（relay_context）共用 Runtime secret、共用同一
  套 `sign/verify` 实现。
- 域隔离：HMAC 输入前缀分别为 `kin/kct/v1` 与 `kin/krc/v1`。
- **顺便迁移 continuation.rs 到该模块**——Runtime 重启本来就更换 secret，无线上
  兼容负担；spec 中"自制 MAC"的告诫段落随 3.3 spec update 改写。

原理：`slot_wait` 的返回值是 MCP tool_result，会进入 kin-slot subagent 的对话
transcript；CLI 为该 subagent 发出的每次 `/v1/messages` 请求体都携带这段
transcript，因此 Relay 能在请求体中扫到 `krc_` token。**不需要 CLI 配合注入
任何自定义 header。**

> ⚠️ 待实机证明的假设："CLI 每次内部请求都携带 slot_wait tool_result（含
> krc_ token）" 目前只由 transcript 结构推断 + mock 验证。observe 阶段必须在
> 真实 CLI 上确认（见识别路径末尾的 observe 验收项）。

### 识别路径

Relay 收到请求后，**流式扫描**——body 一边转发上游一边增量扫描，禁止
`to_bytes()` 缓存完整请求体（20 × 1M 上下文并发会重造内存问题）：

1. 增量扫描器跨 HTTP chunk 边界安全（保留尾部不完整候选片段，上限 = token
   最大长度）；单 token 最大长度 2 KiB，超长即放弃该候选。
2. 每个候选 `krc_[0-9a-f]+\.[0-9a-f]+` 做 HMAC 常量时间验签。
3. "最后一个有效 token" 必须**同时**满足：
   - HMAC 正确；
   - `process_generation` 与当前一致；
   - `job_id` 当前存在于 `runtime.jobs`；
   - token 中 `slot_id` 与 `job.slot_id` 一致；
   - 该 slot 当前仍归属该 job（slot.job_id == token.job_id）。
   旧 transcript 中历史 token 即使签名有效，也因 job 已不活跃 / slot 已易主而
   被排除。
4. 按 `job_id` **动态**查 `runtime.events`（每次事件发送时查，不缓存 Sender）——
   resume 会替换接收器，缓存旧 Sender 会把恢复后的 token 发进已关闭的通道。
5. 无有效 context（root supervisor 流量、slot 引导、maintenance）→ 纯转发，
   不 tap 进任何用户流。

observe 模式验收必须覆盖三种关联场景均能找到正确 `krc_`：
首轮请求、WebSearch/内部多轮后续请求、客户端 `tool_result` resume 之后的请求。

### 失效与安全

- generation 不匹配 → 视为无关联，纯转发（CLI 重启后旧 slot 残留请求不会
  错绑新 job）。
- nonce 仅作唯一性；不做一次性消费（同一 job 的多轮内部请求都携带同一 context，
  多次匹配是预期行为）。
- 日志只记 `job_id/slot_id/generation` 与 body 长度；**禁止记录 authorization /
  x-api-key / OAuth token / 请求正文**。

## 5. Relay 数据面（server.rs / upstream.rs / sse_tap.rs）

### 转发（对 CLI 保持透明）

- 只代理 `POST /v1/messages*`（含 query/beta 后缀路径按原样透传 path+query）；
  其他 path（`/v1/models` 等）同样原样转发——Relay 是通用反代，不理解语义的
  路径也必须可用，避免 CLI 功能性退化。
- Header 白名单式透传：剔除 hop-by-hop（`host`/`connection`/`content-length`
  重算/`transfer-encoding`），其余（`authorization`、`x-api-key`、
  `anthropic-version`、`anthropic-beta`、`user-agent` 等）原样转发，保持 CLI
  原始请求特征。
- 请求体不解析重构，**流式**转发：chunk 到达即转发上游，同时喂给增量 `krc_`
  扫描器（见 §4），全程不缓存完整 body。

### 双消费者（核心不变式）

```rust
// 伪代码：CLI 支路由网络转发直接驱动；tap 绝不阻塞它
while let Some(chunk) = upstream_body.next().await {
    let bytes: Bytes = chunk?;                 // 引用计数，零拷贝 clone
    cli_response_tx.send(bytes.clone()).await; // 主路：背压来自 CLI 读取
    tap.offer(&bytes);                          // 旁路：try_send 进有界队列
}
```

- CLI 支路：axum streaming body，upstream→CLI 的背压天然由网络传导。
- tap 支路：每个关联到 job 的响应有独立有界队列（事件 256 条 + 字节预算 2 MiB，
  取先到者）；`offer` 用 try_send，溢出即：
  `metrics.tap_dropped += 1` → 该 turn 的 arbiter 标记 `TapPoisoned` →
  用户 SSE 以显式错误终止（见 §6），**CLI 支路完全不受影响**。
- tap 消费端（per-job forwarder task）做增量 SSE 解码（`sse_tap.rs`：按
  `\n\n` 帧边界缓冲，未完成帧上限 1 MiB，超限同 TapPoisoned 处理），产出
  Anthropic 事件 JSON 交给 arbiter。
- upstream 连接失败/非 2xx：原样把状态与 body 回给 CLI（CLI 自己重试），该
  turn 的 tap 记为不可用 → arbiter 走 StdoutFallback。**Relay 故障永不阻断
  CLI 消费上游响应**。

### 内部工具过滤与多轮拼接（sse_tap.rs + arbiter.rs）

upstream SSE 里会出现 kin-slot 的内部 MCP 工具调用（`mcp__kin_runtime__*` 的
`tool_use` block）与多次内部 `/v1/messages` 的完整信封。tap→用户流转换规则：

| upstream 事件 | 处理 |
|---|---|
| `message_start` / `message_stop` / `message_delta` | 吞掉信封（外层信封由 HTTP 层已发/由 kin_done 收尾）；`message_delta.usage` 累加进 job 级 usage |
| `content_block_start/delta/stop`（text / thinking） | 重编号 block index 后转发（复用 `JobStream.next_index` 语义） |
| `tool_use` block，name 以 `mcp__kin_runtime__` 开头 | 整块吞掉（内部工具不外泄） |
| `server_tool_use` / `web_search_tool_result` | 转发（阶段事件保留） |
| 其他 `tool_use`（客户端工具） | 转发（与现有 client_tool 流程一致的外显方式） |
| `ping` | 吞掉 |

最终只由 `kin_done` 触发一组外层 `message_delta`（含累计 usage + stop_reason）
和 `message_stop` —— 这维持现有 `complete_job` 的收尾职责不变。

## 6. 流控重构（`mod.rs::emit` 及 per-job egress）

现状缺陷：`emit` 对用户 `StreamTx` 用 `try_send` 静默丢弃
（`traces/replay-stats.json` dropped=1,125,600），逐 token 场景不可接受。

新结构 —— 每 job 一个 `JobSink`：

```rust
struct JobSink {
    data_tx: mpsc::Sender<StreamItem>,   // 有界：256 条 且 2 MiB 字节预算，先到者生效
    terminal: Arc<OnceLock<Terminal>>,   // 独立终止信号，不走数据队列
}
enum Terminal { Overflow, ClientTooSlow, ClientGone, Done, Failed(KernelError) }
```

```text
生产者（共享，不得阻塞）            per-job JobSink                消费者
stdout decoder ──try_send──┐   ┌───────────────────┐   send().await（带超时）
                            ├─→ │ data 队列 256/2MiB │ ─→ 用户 StreamTx（api.rs）
relay tap      ──try_send──┘   │ terminal: OnceLock │
                                └───────────────────┘   egress task（每 job 一个）
```

- 共享路径（stdout decoder / relay 转发循环）只做 try_send 进 data 队列，
  永不 await —— 一个慢 job 不能拖住其他 19 个 slot 的解码。
- **终止信号独立于数据队列**：数据队列 Full 时错误事件本身也进不了队列，所以
  overflow/取消/完成一律通过 `terminal`（原子 OnceLock，首个写入者获胜）通知
  egress；egress 每次循环先检查 terminal 再取数据。overflow、`kin_done`、
  客户端取消只能有一个成为终态。
- 生产者必须区分 `TrySendError::Full`（溢出，按下述规则处理）与 `Closed`
  （egress 已退出，直接停止生产，不计为溢出错误）。
- per-job egress task 用 `send().await` + 超时（`KIN_CLIENT_STALL_SECS`，默认
  30s）推给用户 StreamTx；超时 → set terminal(ClientTooSlow)。
- 溢出规则（生产者 try_send Full 时）：
  - 阶段/控制事件：计数（`stage_dropped`）+ 丢弃（可容忍）。
  - **文本/thinking delta：set terminal(Overflow)**。
- egress 观察到失败终态（Overflow/ClientTooSlow/Failed）后：
  1. 立即标记 job 用户流失败，停止发送缓存正文；
  2. 用户 StreamTx 可写时发送显式错误事件，否则直接关闭 SSE（客户端看到
     异常 EOF）——对完全不读数据的客户端只能保证"错误帧或异常 EOF"，
     **绝不伪装成成功结束**；
  3. 永远不再发送 `message_stop` / `StreamItem::Finished`；
  4. CLI 网络支路继续消费上游，不受任何影响；
  5. runtime 在下一个 MCP 安全点（该 job 的 `client_tool` 挂起返回、
     `slot_wait` 重入或 `kin_done`/`kin_fail`）终止/退休该 job——不能让无人
     可恢复的 `client_tool` 永久占住 slot（等价于对该 job 的 pending client_tool
     以错误 resolve 并让 slot 走 retire/ReadyBlocked 路径）。
- `resume`：替换 egress task 持有的用户 StreamTx；JobSink 与 terminal 保持。
- 该重构对 `off` 模式同样生效（这是无关 Relay 的正确性修复），回放测试的
  dropped 语义随之改变：`replay.rs` 统计拆分为 `stage_dropped` /
  `jobs_aborted_slow_client`，成功 job 的文本 delta 丢失数断言为 0。

## 7. SourceArbiter（arbiter.rs）

每 job 一个状态机，由 egress task 持有：

```text
NoBody ──首个有效关联 upstream text/thinking delta──→ UpstreamActive
NoBody ──首个 stdout 正文帧（observe/off 模式，或 authoritative 下 tap 不可用）──→ StdoutFallback
UpstreamActive ──kin_done──→ Completed
StdoutFallback ──kin_done──→ Completed
UpstreamActive ──✗──→ StdoutFallback   （禁止：中途切换会重复/截断正文）
任意 ──tap 溢出且已 UpstreamActive──→ 失败终止（显式错误，不降级）
Completed：拒绝迟到事件；kin_done 做 usage/stop 汇总
```

- 模式与初始行为：`off` 无 arbiter（编译进但不实例化 tap 路径）；`observe`
  强制 StdoutFallback 供用户流、tap 仅累计摘要；`authoritative` 按上表。
- 正文抑制：进入 `UpstreamActive` 后，stdout 的 assistant 完整文本帧不再进入
  用户流，仅累入 stdout 摘要供对比；WebSearch 等阶段事件源也切到 upstream
  （stdout 侧同 id 事件按 `JobStream.seen` 去重丢弃）。
- 摘要对比：job 完成时计算 upstream 正文 SHA-256 与 stdout/`kin_done.final_digest`
  正文 SHA-256，不一致 → `metrics.digest_mismatch += 1` + warn 日志（只记
  digest，不记正文）。observe 模式的核心产出就是这个对比。
- 最终 `MessageResponse.content` 取值优先级（`complete_job` 改造）：
  UpstreamActive 的累计正文 > `JobStream.text`（stdout）> `kin_done.fallback_content`。

## 8. 状态暴露

`Provider` trait 增加默认方法 `relay_snapshot() -> Option<Value>`（默认 None）；
`MultiplexCliProvider` 返回：

```json
{ "relay_mode": "off|observe|authoritative",
  "relay_healthy": true,
  "tap_dropped": 0,
  "digest_mismatch": 0 }
```

`api.rs` 的 `/status` 响应加 `"relay": ...` 字段。Go 本期不消费（R5 后续任务）。

## 9. 兼容与回滚

- 默认 `off`：合入后零行为变化；现有 smoke / 回放 / 20-slot 测试零修改通过
  （流控重构除外——见 §6 末尾，`replay.rs` 统计字段调整属于正确性修复的一部分，
  需同步更新其断言）。
- 回滚 = `KIN_RELAY_MODE=off` + 重启 kernel（CLI 随之以无 Base URL 注入重启）。
- 不改 continuation 线协议（`kct_` token 原样）；`krc_` 是新增且只在 kernel↔CLI
  闭环内流转，不出现在对外 API。

## 10. 关键权衡记录

1. **body 流式扫描 vs 自定义 header 关联**：CLI 不可控，无法让它加 header；body
   扫描利用"slot_wait tool_result 必然随 transcript 上行"这一结构性事实（待实机
   证明，见 §4），零 CLI 侵入。扫描是增量的，不缓存完整 body。
2. **HMAC-SHA256 统一签名 vs 复用自制 `mac()`**：`krc_` 的输入来自外部可控的
   请求 body，自制 XOR/rotate 算法且非常量时间校验不能作为该认证边界；引入
   `hmac`/`sha2` 依赖，`kct_`/`krc_` 共 secret、域隔离（`kin/kct/v1`、
   `kin/krc/v1`），continuation 一并迁移（重启换 secret，无兼容负担）。
3. **per-job egress task + 独立 terminal 信号 vs 直接 send().await**：直接
   await 会让共享 stdout decoder 被单一慢客户端阻塞；错误走数据队列则队列
   Full 时错误自身也送不进去——所以数据与终止信号分离，终态原子唯一。
4. **不做完整 CanonicalEvent**：arbiter + tap 过滤表以 Anthropic 事件 JSON 为
   载体即可满足本期；CanonicalEvent 留给 R4（Gemini 出口）时一并设计。
5. **网络指纹取舍（架构师确认）**：引入 Rust Relay 后，CLI 仍负责生成 Messages
   body、OAuth header、beta、tools 和 system prompt，但 Anthropic 侧看到的
   TLS/HTTP 客户端变为 Rust `reqwest/rustls`——不再是 Claude CLI/Node 的 TLS
   指纹、ALPN 与连接池行为。本方案实现的是**"应用层请求特征对齐"**，不是
   "完整网络指纹仍由 CLI 出站"。`KIN_SOCKS5=socks5h://` 出站需启用 reqwest 的
   `socks` feature。
