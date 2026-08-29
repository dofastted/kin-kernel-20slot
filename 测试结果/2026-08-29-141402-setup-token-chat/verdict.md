# setup-token 单次对话验证

- 时间: 2026-08-29T14:14:02Z
- 用法: `CLAUDE_CODE_OAUTH_TOKEN`（`claude setup-token` / inference-only）
- 不写完整 `claudeAiOauth` refresh；不启 kernel；不并行
- 出口: SOCKS5 72.1.181.43 via 127.0.0.1:18080
- 票: 去掉 UI 后缀 `Storethistokensecurely.Youwon` 后的 oat01…AA

## 结果

- rc=0 elapsed=4.892s
- text='setup-ok'
- is_error=None
- ok=PASS

## 使用方式变更

1. 导出 type=`setup-token` 时走 env `CLAUDE_CODE_OAUTH_TOKEN`，不是 `.credentials.json` + refresh。
2. Kin supervisor：setup-token 注入 `CLAUDE_CODE_OAUTH_TOKEN`；订阅票只写 credentials 并 env_remove。
3. 本地 `--mcp-config` HTTP MCP 仍可用；2-slot multiplex 已在 `*-142605-v2-cli-node` 跑通。
