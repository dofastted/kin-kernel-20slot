# e28132d observe 采证

- commit: `e28132d6b0e583653e29522b3c8060d65610432e`
- 时间: 2026-08-28T20:19:10Z
- 路径: Kin observe relay → SOCKS5 → api.anthropic.com
- 采证目录: `/workspace/测试结果/2026-08-28-201911-e28132d-observe`

## 清单

- F-2 故障注入 `KIN_RELAY_UPSTREAM=http://127.0.0.1:1`: PASS reason=`boot_failed` cli_spawned=False
  - 0.3s 内 `/readyz` 503 `{reason:boot_failed}`；relay 环回仍 200，`relay_mode` 仍为 `observe`（未静默回 off）
  - 日志: `relay upstream preflight: error sending request for url (http://127.0.0.1:1/)`
- `/readyz` boot 期间 `reason=booting`: PASS（live boot 采到 6 个 503 样本，期间 relay 环回已 200）
- relay 先于 CLI: PASS（relay_first `before_cli=true`；随后 1 个 Claude PID）
- SOCKS5-only CLI (无 HTTPS_PROXY, BASE_URL loopback): PASS
  - `ANTHROPIC_BASE_URL=http://127.0.0.1:18082`；无 `HTTPS_PROXY`/`HTTP_PROXY`；无 `ANTHROPIC_API_KEY`；无 `CLAUDE_CODE_OAUTH_TOKEN`
- healthz.relay 字段: `{"relay_mode": "observe", "relay_healthy": true, "correlate_hit": 5, "correlate_miss": 72, "tap_response_started": 5, "tap_dropped": 0, "digest_mismatch": 0}`
- digest_mismatch: 0
- Claude PID 数: 1；slots_per_worker: 20

## 三场景

| 场景 | HTTP | 文本/工具 | hitΔ | missΔ | tapΔ |
|---|---:|---|---:|---:|---:|
| hello | 200 | observe-hello | 1 | 3 | 1 |
| web_search | 200 | 今天 UTC 日期是 2026 年 8 月 28 日（星期五） | 2 | 12 | 2 |
| client_tool | 200/200 | get_weather → resume `The current weather in Tokyo is 26°C.` | 2 | 5 | 2 |

- hello 2.6s；web_search 10.7s（含 WebSearch 工具块）；client_tool first 2.0s + resume 2.4s，continuation 恢复同一 job、`turn_id` 0→1
- `correlate_hit` 累计 5 = 5 条 `relay correlated request` 日志，一一对应
- `correlate_miss` 累计 72：**不视为关联失败**。boot 完成时已 52（CLI 对自定义 `ANTHROPIC_BASE_URL` 的 GET `/` 探测 404，与上一轮抓包一致）；三场景增量 3+12+5=20，同属探测/非 slot_wait POST。真实 `/v1/messages` 均 hit

## 关联日志 (`relay correlated request`) n=5

仅含 `job_id` / `slot_id` / `turn_id`：

- hello turn 0 — `job_4874c263…` / `slot_a96f4dee…`
- web_search turn 0 ×2（同 job `job_c38c8db8…` / `slot_3739a076…`，搜索+作答两轮内部 POST）
- client_tool turn 0 + turn 1 — `job_a662e587…` / `slot_1d1a2d14…`（resume 新 turn，F-4 未串毒）

authorization/token 泄漏行: 0（kernel.log 无 `sk-ant-` / Authorization / Bearer / accessToken）

## 结论

**observe 采证通过**（handover 第 2 步）。

authoritative（第 3 步）**未启动**。测试员确认本包后可进入：自然 `text_delta`、WebSearch 事件、20 并发、慢客户端显式失败、RSS。回滚仍是 `KIN_RELAY_MODE=off` + 重启。
