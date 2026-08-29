# adc1450 authoritative 三 FAIL 复测

- commit: `adc1450b24bb281ddc931843399219f8315eef18`
- 时间: 2026-08-29T05:59:25Z
- 模式: KIN_RELAY_MODE=authoritative · stall=3s · submit_wait=3000ms · SOCKS5 → api.anthropic.com

## 清单

- boot authoritative + relay_healthy: PASS
- FAIL-1 短句多个 text_delta: FAIL
- FAIL-1 长句多个 text_delta: FAIL
- FAIL-1 80词段落多个 text_delta: FAIL
- hello 正文含 observe-hello: PASS
- 正文无空/失败: PASS
- WebSearch 正常且用户流无 mcp__kin_runtime: PASS
- client_tool + continuation resume: PASS
- FAIL-2 20 并发无串流、无空 200: FAIL
- FAIL-3 20/20 成功无 503: FAIL
- tap_droppedΔ=0: PASS
- 慢客户端显式失败，非缺字 message_stop: FAIL
- RSS < 4GB: PASS
- digest_mismatch=0: PASS
- 无 mcp 内部工具泄漏: PASS

- hello TTFB: `2029ms` n_delta=1 text='observe-hello'
- stream n_delta=1 max_delta=108 first=2253ms
- para n_delta=1 max_delta=734 first=4682ms
- conc20 ok=17/20 unique=17 crosstalk=2 empty200=3 503=0 tapΔ=0 ambiguous=0
- RSS service=286978048 claude=266891264 (0.27 GiB)
- slow: explicit_fail=False fake_success=False err=None
- final_relay: {"correlate_ambiguous": 0, "correlate_hit": 28, "correlate_miss": 101, "digest_mismatch": 0, "relay_healthy": true, "relay_mode": "authoritative", "tap_dropped": 0, "tap_response_started": 28}

## 结论

**authoritative 采证未完全通过**。
