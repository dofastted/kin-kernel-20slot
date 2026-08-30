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

## 4.1 CLI 数据面（patch 单一路线）

CLI 自己发起上游 TLS，内核只读它的 stdout。没有 relay、没有 tap、没有
`ANTHROPIC_BASE_URL` 改写，`KIN_RELAY_MODE` / `KIN_EXECUTION_MODE` /
`KIN_ISOLATION` 已随代码删除。

| 场景 | 处置 |
|---|---|
| `/readyz` 返回 `config_hash_mismatch` | CLI 上报的 `kin_host_ready.config_hash` 与 `KIN_DESIRED_CONFIG_HASH` 不一致，多为 CLI 进程用旧 profile 启动。对齐控制面 `GET /api/v1/runtime-profile` 的 hash 后 drain + 重启，禁止把校验关掉 |
| `/readyz` 长时间 `booting` | CLI 没有发出 `kin_host_ready` 或 slot 数不符。查 `/tmp/kin-live/claude.multiplex.stderr.log`，确认 `CLAUDE_CODE_KIN_NATIVE_SLOTS` 与 `KIN_SLOTS_PER_WORKER` 一致 |
| 某个 job 卡住不出 token | 该 job 的 stdout 超过 `MAX_JOB_BYTES` 后会停止解码（按 job_id 独立计量，不影响其他 slot）。查该 job_id 的 stdout 体量与客户端消费速度 |
| 客户端收到显式错误而非缺字成功 | 预期行为：sink 溢出/客户端过慢会设置 terminal 并报错，不会静默截断 |
| 回滚 | 旧路线已物理删除，环境变量无法退回 relay/mcp_slot；需要回退走 git revert 对应批次 commit |

日志纪律：不记录 authorization/x-api-key/OAuth token/请求或响应正文；排障只用
job_id/slot_id/config_hash。

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

