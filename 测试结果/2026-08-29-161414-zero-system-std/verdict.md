# 测试标准 (kernel + 0-inject system)

栈：`KIN_RELAY_MODE=authoritative` · 官方 Claude Code 2.1.251 · setup-token · SOCKS5 72.1.181.43 · 2 slot。
出站默认头：billing `2.1.241.<fp>` + `cc_entrypoint=sdk-cli` + `prompt_version=<You are a Claude agent…>` + `# Environment / America/New_York`。无独立 identity 块。

## 客户端流式

| 题 | HTTP | 耗时 | `text_delta` | 工具 | 结果 |
|---|---:|---:|---:|---|---|
| 01-tools-list | 200 | 1.6s | 2 | — | PASS |
| 02-vision | 200 | 21.7s | 15 | WebSearch | PASS |
| 03-web-search | 200 | 46.8s | 34 | WebSearch×2 | PASS |
| 04-identity | 200 | 9.4s | 16 | — | PASS |
| 05-roleplay | 200 | 2.6s | 4 | — | PASS |
| 06-prompt-leak | 200 | 4.3s | 8 | — | FAIL `Claude Code` 出现在拒答 |
| 07-forced-weather | 200 | 2.3s | 0 | `get_weather` | PASS |

04 正文自称 Claude / Anthropic，不再报 Claude Agent SDK。07 无可见正文，`stop_reason=tool_use`，`{"city":"东京","unit":"celsius"}`。

05 出站第三块为调用方 leftover「你是一个高速收费员。」

## Relay

本轮客户端 token 来自 relay 对上游 SSE 的 tap，不是 CLI stdout。官方 2.1.251 无 `--forward-subagent-partials`。0 注入改写也在 relay。**不能移除中间层。**
