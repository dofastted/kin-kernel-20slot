# e28132d authoritative 采证

- commit: `e28132d6b0e583653e29522b3c8060d65610432e`
- 时间: 2026-08-28T20:29:53Z
- 模式: KIN_RELAY_MODE=authoritative · stall=3s · SOCKS5 → api.anthropic.com

## 清单

- boot authoritative + relay_healthy: PASS
- 普通回答多个自然 text_delta（非一次整块）: FAIL
- hello 正文语义对齐 observe（observe-hello）: PASS
- 正文无空/失败: PASS
- WebSearch 正常且用户流无 mcp__kin_runtime: PASS
- client_tool + continuation resume: PASS
- 20 并发无串流 tap_droppedΔ=0: FAIL
- 慢客户端显式失败，非缺字 message_stop: FAIL
- RSS < 4GB: PASS
- 首 token 相对 observe 额外延迟 < 50ms: PASS
- digest_mismatch=0: PASS
- 无 mcp 内部工具泄漏: PASS

- observe TTFB: `1986ms` n_delta=1
- auth hello TTFB: `1209ms` n_delta=1 extra=-777
- stream n_delta=1 max_delta=108 natural=False
- conc20 ok=18/20 unique=18 crosstalk=1 tapΔ=0
- RSS service=291487744 claude=271175680 (0.27 GiB)
- slow: explicit_fail=False fake_success=False err=None

## 结论

**authoritative 采证未完全通过**。
