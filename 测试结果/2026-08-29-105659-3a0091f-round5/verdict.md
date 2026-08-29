# 3a0091f 五轮最小化复测 S1→S4

- commit: `3a0091f2c1b65d2b8dab94d81cdccbfcbf51c588`
- 时间: 2026-08-29T10:56:56Z
- 模式: authoritative · SOCKS5 72.1.181.43 via 127.0.0.1:18080
- 录制: `05-roleplay`（S1/S3 普通对话）+ `07-forced-weather`（S2 工具回路）

## 步骤

- S1: **PASS** — synth_like=True status=200 n_cbd=14 n_td=11 max_delta=12 first=3139ms text_len=68 Δtap_dropped=0
- S2: **PASS** — resume_status=200 text_len=19 same_job=True jobs_first=['job_a60db5e165de4c028c6eac60ee7ed91b'] jobs_resume=['job_a60db5e165de4c028c6eac60ee7ed91b'] slots_first=['slot_a07c8c7172d44d12a8c04afd59adb68a'] slots_resume=['slot_a07c8c7172d44d12a8c04afd59adb68a']
- S3: **PASS** — ok=3/3 empty=[] 503=[] fail=[] xtalk=[]
- S4: **PASS** — Δhit=6 ambiguous=0 digest=0 Δtap_dropped=0 before={'correlate_ambiguous': 0, 'correlate_hit': 0, 'correlate_miss': 47, 'digest_mismatch': 0, 'relay_healthy': True, 'relay_mode': 'authoritative', 'tap_dropped': 0, 'tap_response_started': 0} after={'correlate_ambiguous': 0, 'correlate_hit': 6, 'correlate_miss': 59, 'digest_mismatch': 0, 'relay_healthy': True, 'relay_mode': 'authoritative', 'tap_dropped': 0, 'tap_response_started': 6}

## 结论

**五轮最小化通过**。
