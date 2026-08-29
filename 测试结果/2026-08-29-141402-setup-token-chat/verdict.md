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
2. Kin supervisor 改为：若 `KIN_CLAUDE_CODE_OAUTH_TOKEN` 非空则注入该 env，否则继续 `env_remove`（完整订阅票）。
3. `cli-node.sh` 同步注入。MCP / `user:sessions:claude_code` 此票没有，不能当 20-slot multiplex 用。
