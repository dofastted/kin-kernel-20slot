# 测试结果

按日期时间分目录。

## `*-e28132d-authoritative` — handover 第 3 步 authoritative 采证

- `verdict.md` 结论
- `report.json`：hello/stream/web_search/tool、conc20、slow-client
- `observe-ttfb.json`：切模式前 observe 首 token 基线
- `kernel.log`

## `*-e28132d-observe` — handover 第 2 步 observe 采证

e28132d：`/readyz` 门控、upstream preflight、关联三场景、SOCKS5-only。

- `verdict.md` 结论
- `fault-inject.json` + `fault-inject.kernel.log`：`KIN_RELAY_UPSTREAM=http://127.0.0.1:1` → `boot_failed`、CLI 不启动
- `report.json`：boot 时间线、三场景 hit/miss/tap 增量
- `kernel.log`：`relay correlated request`（仅 job/slot/turn）
- `claude.debug.log`：CLI debug（无 token）

## `*-kin-cli` — Kin CLI 出站抓包

```
入站 /v1/messages
  → Kin multiplex (20 slots)
  → Claude CLI 真实 POST
  → loopback relay → dump_proxy → SOCKS5
  → api.anthropic.com
```

目录内：

- `00-boot/outbound/` 冷启动包
- `01`–`07` 标准套件：`inbound-*` 是打进 Kin 的题和回包；`outbound/*/request-body.json` 是 CLI 实际请求体；`response.raw.txt` 是 Anthropic SSE
- `summary.md` 总表

## 入库说明

- 已剔除 CLI 对 `GET /` 的探测包（404）和 `zz-idle/`，只保留真实 `POST /v1/messages` 请求体/回包与 observe/authoritative 采证。
- `Authorization` / SOCKS / `sk-ant-*` 已打码。
- `*.log` 在仓库根 gitignore 中；本目录日志用 `git add -f` 入库。
