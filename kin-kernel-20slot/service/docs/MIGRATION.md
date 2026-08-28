# 从现网链路迁移到 v2

## 阶段 0：只观测

- 给现有 portunex/isthmus 增加统一 request/session/slot id 指标。
- 分离 active、waiting_tool、queued、process 四类数量。
- 记录 model pin、sticky 命中、P2C 选择原因、429 和 tool wait age。
- 不改变流量和凭据。

退出条件：可以解释任意请求落到哪个实例/slot，以及为什么。

## 阶段 1：Rust kernel shadow

- Rust kernel 只做协议解析、策略评估和调度影子计算，不转发真实请求。
- 将其 P2C 结果与现网 portunex 对比。
- 用合成请求验证 canonical schema 和 error mapping。

退出条件：shadow 决策差异可解释，错误率和资源开销符合目标。

## 阶段 2：mock 与正式 API canary

- 先启用 mock provider 跑完整 tool loop。
- 接入正式 provider API adapter，使用独立测试 workspace 和正式 API key/WIF。
- 1% 无状态流量 canary，不承载真实 CLI subscription credential。

退出条件：流式、usage、取消、429、幂等和审计全部通过。

## 阶段 3：session/continuation 切换

- Redis session directory 双写，先读旧逻辑。
- 校验 continuation token、slot generation 和 TTL。
- 切换读取后保留快速回退；stateful 会话不跨系统迁移。

退出条件：无跨租户绑定、无永久 reservation、drain 可控。

## 阶段 4：Go 控制面接管

- 导入 route policy、slot group 和 tenant quota，不导入明文 token。
- kernel 读取 signed snapshot，控制面故障演练。
- 逐步接管 drain、rollout 和 autoscale。

退出条件：控制面停机 30 分钟数据面不受影响，last-known-good 可验证。

## 阶段 5：移除高风险链路

- 停止 sessionKey -> OAuth 自动换票和任何浏览器/CLI 指纹模拟。
- 将 SNAT 收敛为标准 egress allowlist，不再与账号身份绑定。
- 清理共享 session id、共享 CLI home、明文 credential 副本。
- 撤销历史 token，完成 secret rotation 和审计封存。

## 回滚原则

- 配置通过 revision 回滚，不回滚数据库 schema。
- 首字节后请求不跨系统重放。
- stateful tool wait 会话允许自然完成或明确失败，不做隐式迁移。
- 任何凭据异常立即 fail closed，不回退到 cookie/sessionKey 链路。

