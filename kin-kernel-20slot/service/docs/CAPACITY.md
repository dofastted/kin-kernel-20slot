# 容量与压测模型

## 1. 先区分四个量

| 指标 | 含义 |
|---|---|
| OS process | 宿主机看到的进程数 |
| logical slot | runtime 声明可同时承载的独立 request lease |
| active turn | 当前正在生成/执行的请求 |
| waiting tool | 已保留 stateful runtime、等待客户端 tool_result 的会话 |

因此 `89 × 20 = 1780` 只能表示逻辑 slot 上限，不能直接当作可持续并发或吞吐承诺。

## 2. 有效并发上限

可持续并发取以下最小值：

\[
C_{effective}=\min(C_{slot}, C_{cpu}, C_{mem}, C_{rpm}, C_{itpm}, C_{otpm}, C_{egress})
\]

使用 Little's Law 粗估 provider 限额对应并发：

\[
C_{rpm} \approx \frac{RPM \times L_{avg}}{60}
\]

\[
C_{itpm} \approx \frac{ITPM \times L_{avg}}{60 \times tokens_{in,avg}}
\]

输出 token 同理。最终还要乘以 0.6–0.75 的安全利用率，给突发、长尾和重试留空间。

## 3. waiting_tool 的成本

如果 stateful adapter 在工具等待期间占用 slot：

\[
C_{available}=C_{slot}-N_{waiting\_tool}-N_{draining}-N_{unhealthy}
\]

这正是 continuous tool loop 容易“看起来低 CPU、却无容量”的地方。必须分别展示 active 与 reserved waiting，设置：

- tool wait TTL；
- tenant 最大 waiting 数；
- 全局最大 waiting 比例；
- drain deadline；
- stateless transcript fallback（仅 adapter 支持时）。

## 4. warm pool

`min-procs=0` 在极致密度下减少空闲资源，但会把首次请求延迟暴露给用户。建议：

- 以 route/model 维度维护 `min_warm`；
- 根据过去 5–15 分钟到达率和启动 p95 预测；
- 保持 10–20% warm headroom，而不是固定每实例 20；
- 新版本 rollout 先预热再接流量。

## 5. 压测阶段

1. 单 slot 正确性：流式、取消、tool wait/resume、超时。
2. 单 kernel 梯度：1/5/10/20/40 并发，记录 CPU、RSS、first-token、queue。
3. 多 kernel：验证 sticky、P2C 均衡和 Redis CAS。
4. provider 限额：注入 429/retry-after，确认不发生重试风暴。
5. 长等待：30% 请求停在 tool wait，观察 reservation 回收。
6. chaos：kill runtime/kernel、断 Redis、控制面停机、配置回滚。

## 6. 上线阈值示例

示例只作为初始 guardrail：

| 指标 | 阈值 |
|---|---|
| kernel target utilization | 65% |
| queue p95 | < 100 ms |
| first token p95 | 按模型基线 + 20% |
| waiting_tool / slots | < 25% 持续 5 分钟 |
| provider 429 | < 0.5% 持续 5 分钟 |
| continuation mismatch | < 0.01% |
| config apply failure | 0 |

autoscaler 同时看 queue 和 provider headroom。provider 已 429 时增加 kernel/slot 没有意义，应降低 admission 或申请正式限额。

