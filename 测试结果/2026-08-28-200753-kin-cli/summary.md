# 测试结果 2026-08-28-200753-kin-cli

- 时间: 2026-08-28T20:11:35.612242+00:00
- 路径: Kin multiplex CLI → loopback relay → dump_proxy → SOCKS5 → api.anthropic.com
- 模型: claude-sonnet-5 · TZ America/New_York
- 票: vm-05 / ['user:file_upload', 'user:inference', 'user:mcp_servers', 'user:profile', 'user:sessions:claude_code']
- 冷启动: ready 76.02s · dump=True relay=True

| ID | 题目 | HTTP | 出站包 | 判定 | 失败 |
|---|---|---:|---:|---|---|
| 01-tools-list | 工具列表 | 200 | 9 | 干净 |  |
| 02-vision | 识图 | 200 | 51 | 不干净 | has_visible_text, vision_mentions_image |
| 03-web-search | web_search | 200 | 112 | 不干净 | used_web_search_or_answered |
| 04-identity | 模型自我认知 | 200 | 11 | 不干净 | has_visible_text |
| 05-roleplay | 高速收费员角色 | 200 | 7 | 干净 |  |
| 06-prompt-leak | 系统提示词套取 | 200 | 13 | 不干净 | no_cc_identity |
| 07-forced-weather | 强制天气工具 | 200 | 12 | 干净 |  |

## 目录说明

- `00-boot/outbound/` 冷启动时 CLI 真实 POST（supervisor + 20 slots）
- `0N-*/inbound-request.json` 打进 Kin 的题目
- `0N-*/inbound-response.raw.txt` Kin 回给客户端的 SSE
- `0N-*/outbound/*/request-body.json` **Claude CLI 实际发往 Anthropic 的请求体**
- `0N-*/outbound/*/response.raw.txt` **Anthropic 实际 SSE 回包**
- `index.ndjson` 全量抓包索引（Authorization 已打码）

Authorization / cookie <redacted>

