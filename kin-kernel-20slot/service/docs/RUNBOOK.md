# 运维手册

## 1. 健康检查

| 检查 | 成功条件 |
|---|---|
| kernel `/healthz` | 进程事件循环正常 |
| kernel `/readyz` | 有兼容 slot、配置未过期、必需 store 可用 |
| control `/healthz` | HTTP 与 reconcile loop 正常 |
| snapshot age | 小于有效期 50% |
| Redis lease round-trip | p95 在本区域目标内 |

`healthz` 不代表可接流量；负载均衡器应使用 `readyz`。

## 2. Drain

1. 控制面把 kernel/slot group 标成 draining。
2. 停止新 session；已有 sticky 请求按策略继续。
3. 等待 active turn 到 0。
4. 等待 tool loop 到短 deadline；向 adapter 发 cancel。
5. 使 continuation 失效并记录明确原因。
6. 停止进程，generation 递增后才可重新注册。

不要直接 kill 带有 waiting_tool 的 runtime，除非接受 `continuation_lost`。

## 3. Provider 429

- 读取并遵守 `retry-after`；
- 提高该 provider/model 的 rate-pressure 分数；
- 减少 admission，而不是增加 slot；
- 检查 prompt cache、平均 token、组织 workspace 限额；
- 不切换账号或源 IP 来规避限额。

## 4. Redis 故障

- 新 continuation fail closed；
- stateful 本地 loop 保持到短 grace period，等待 Redis 恢复；
- 无状态单轮请求只有在策略允许时降级；
- 禁止在每个 kernel 本地新建互不一致的 sticky mapping；
- 恢复后运行 reservation reconciliation。

## 4.1 Messages Relay（`KIN_RELAY_MODE`）

灰度顺序固定为 `off → observe → authoritative`；每档验证通过再切下一档。

| 场景 | 处置 |
|---|---|
| 内核起不来且日志含 `relay healthz` | Relay 未就绪时 CLI 不会启动（预期行为）。检查 `KIN_RELAY_ADDR` 端口占用与 `KIN_RELAY_UPSTREAM` 可达性；无法立刻修复则改回 `off` 重启 |
| `KIN_RELAY_MODE` 拼写错误 | 启动即退出（预期，禁止静默降级）；修正后重启 |
| `/healthz` 的 `relay.tap_dropped` 增长 | 某些 turn 的用户 tap 溢出；对应用户流会收到显式错误而非缺字成功。CLI 支路不受影响。持续增长说明客户端消费过慢或事件突发过大 |
| `relay.digest_mismatch` 非零 | upstream 正文与 stdout/`kin_done` 摘要不一致；observe 阶段出现说明关联或过滤有 bug，**不要切 authoritative**，带 digest（无正文）开工单 |
| 回滚 | 设 `KIN_RELAY_MODE=off` 并重启 kernel。CLI 启动后无法动态撤销已注入的 `ANTHROPIC_BASE_URL`，必须重启 |

日志纪律：Relay 不记录 authorization/x-api-key/OAuth token/请求或响应正文；排障只用 job_id/slot_id/generation/digest。

## 5. Control plane 故障

- kernel 继续使用 last-known-good；
- 禁止 snapshot 过期后无限运行，设置 grace window；
- 暂停 rollout、secret rotation 和自动扩缩；
- 修复 Postgres/控制面后确认 revision 单调再恢复发布。

## 6. Credential 异常

- 立即将 credential reference 标为 quarantined；
- 断开新流量，不把错误日志中的 token 复制到工单；
- 通过 secret manager rotate/revoke；
- 查询 audit 中的 lease 领取者和时间，不读取用户 prompt；
- 不回退到 session cookie 或历史 token 副本。

## 7. 常用验证

```bash
make static-check
make test-rust
make test-go
make compose-up
make smoke
make compose-down
```

生产发布前还应执行依赖审计、容器扫描、负载测试和 chaos 套件。

