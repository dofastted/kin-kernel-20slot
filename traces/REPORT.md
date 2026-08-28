# SPCX 长对话录制 + 20 Slot 回放报告

日期: 2026-08-28  
模型: `claude-sonnet-5`  
隔离: 1 Claude OS PID + 20 MCP Subagent slots  
真实长请求: **1**（不是 20）

## 测试拆分与结果

| 测试 | 真实长请求 | 结果 |
| --- | --- | --- |
| 长对话录制（SPCX 研报 4 轮） | 1 | 4 轮全部 200；11 次 WebSearch；stdout 1409 帧 |
| 20 路 Trace 回放 | 0 | Shared 20×2 + Independent 20×50 通过；peak_inflight=20 |
| 20 Slot 极短真实并发 | 20 个 hello | 墙钟 2.10s；20/20 流式；18/20 独立文本 |
| 1 长 + 19 MCP hold | 1（录制期间） | 录制时其余 slot 保持 `slot_wait`；service RSS 几乎不涨 |

## 长对话内容（session `spcx-long-01`）

| 轮 | 主题 | 状态 | 字符 | 说明 |
| --- | --- | --- | --- | --- |
| 1 overview | 检索 SPCX ETF 结构/持仓/表现 | 200 / 64.6s | 2821 | 4×WebSearch，SSE 完整 |
| 2 outlook | 12 个月情景与风险 | 200 | 10421 | 多轮 WebSearch；客户端中断后从 stdout 回收 |
| 3 related | 关联公司 / Tuttle / SPAK / 发起人 | 200 | 7241 | stdout 回收 |
| 4 report | 机构口径研报七段结构 | 200 / 101.2s | 9526 | SSE 完整，`kin_done` |

关键事实（模型检索结论，截至 2026-08-28）：原 SPCX（The SPAC and New Issue ETF, Tuttle Capital）已于 2026-04-07 更名为 **SPCK**；`SPCX` 代码被重新分配给 2026-06-12 上市的 SpaceX 普通股。研报正文见 `transcript.md`。

## 内存（kernel + Claude RSS，不含整个 sandbox cgroup）

Admission 按 **service RSS = kernel+Claude** 分类。Sandbox `memory.current` ≈ 3.3 GiB，含 cargo/CLI/日志，不能代表 4 GiB 服务配额。

| 点 | 含义 | service RSS | Claude RSS | kernel RSS | admission |
| --- | --- | --- | --- | --- | --- |
| **M0** | 20 slot 空闲 | 276.7 MiB | 261.8 MiB | 14.9 MiB | allow |
| **ML** | 1 长会话结束后 + 19 hold | 283.4 MiB | 267.8 MiB | 15.6 MiB | allow |
| **M1** | 20 极短真实并发之后 | 312.9 MiB | 297.0 MiB | 15.8 MiB | allow |
| **MR** | 20 路 × 50 轮本地回放 | 网关进程内 JobStream；见 replay-stats | — | — | — |

$$
\Delta_{long}=ML-M0 \approx 7\ \text{MiB}
$$

$$
M_{estimate}=M1+20\times\Delta_{long} \approx 453\ \text{MiB}
$$

远低于 4 GiB。这是 **单进程内一份长上下文的残差外推**，不能证明 Claude 同时持有 20 份百万 token 上下文。阶梯真实测试（1/2/5 长上下文）仍建议后续做。

录制峰值 Claude RSS ≈ 290 MiB；20 hello 后升到 297 MiB。cgroup 全程 ~3.2–3.3 GiB（sandbox），与服务无关。

## 回放（ReplayProvider，0 Claude token）

`response.ndjson`：1409 帧 Claude stream-json（已脱敏）。

Independent 模式 20 并发 × 50 轮：

```
finished:        1000
events_emitted:  1_222_000
dropped:         1_125_600   # stalled/slow/disconnected try_send
peak_inflight:   20
frames:          1409
```

- Shared：`Arc<[Value]>` 零拷贝读  
- Independent：每路 `serde_json::from_slice`，模拟最坏解析  
- 虚拟 ID：`session/slot/job/parent_tool_use_id/connection_id` 互不串号  
- 慢客户端 / 卡住 / 断连：channel cap=2，背压用 `try_send` 丢事件并计数  

内存未随 50 轮线性堆积（测试结束 RSS 回落到 cargo test 基线；1000/1000 finished）。

## 20 路真实极短并发

墙钟 **2.104s**，单路 1.57–2.08s 重叠 → 不是回放假并发。

- 20/20 HTTP 200 且 `message_start`  
- 18 路独立正文 `hello-00` … `hello-17`  
- 2 路（18/19）流式信封完整但 text 为空（4 个 SSE 事件）  

## 修复（本次为了录制能跑通）

1. **MCP `notifications/progress` 必须带 `progressToken`**。缺 token 时 Claude 2.1.x Zod 抛错并掐掉 MCP HTTP，idle `slot_wait` 收不到 job，SSE 被 ping 吊住。  
2. Provider **boot 与首个 HTTP 解耦**，避免客户端 30s 超时取消 20-slot 初始化。  
3. Admission 默认看 kernel+Claude RSS，不再被 sandbox cgroup 误判成 `allow_small/drain`。

## 产物（均已脱敏，无 OAuth/Cookie/Authorization）

| 文件 | 内容 |
| --- | --- |
| `request.ndjson` | 4 轮原始输入 |
| `response.ndjson` | Claude stdout stream-json，1409 帧 |
| `sse.ndjson` | HTTP SSE（完整捕获的轮次） |
| `timing.json` | 相对时间、TTFT、SSE 类型 |
| `metadata.json` | tool_use / parent / web_search=11 / usage |
| `memory.csv` | cgroup / RSS / PSS 0.5s 采样 |
| `transcript.md` | 可读 4 轮中文研报 |
| `replay-stats.json` | 20×50 independent |
| `conc20-hello.json` | 20 短请求明细 |
| `m0.json` `ml.json` `m1.json` | 内存快照 |

## 结论

当前 20 并发 + 2–4 GiB 目标 **仍然成立**：

- 1 PID / 20 slot 已真实并发（2.1s 内 20 个短流）  
- 1 条长对话 + tool loop 已录制并可 20 路本地复放  
- 服务 RSS 在 280–330 MiB，外推 20 份同等长上下文约 0.45 GiB（乐观）  
- 不要用 20 次真实长对话烧 token；下一步若要收紧上限，做 1/2/5 长上下文阶梯即可  
