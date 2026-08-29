# 测试结果

按日期时间分目录。

## `*-142605-v2-cli-node` — Node + setup-token，V2 parented 流 × Kin（2026-08-29）

patched `cli-node.js`（Node 24.20，非 bun）+ setup-token + 2-slot authoritative。

- CLI stdout：433 `stream_event`，258 带非空 `parent_tool_use_id`（2 个 kin-slot），0 ProgressMessage
- parented delta **全是 `input_json_delta`**（`slot_wait` / `kin_done` 参数），**0 条 parented `text_delta`**
- HTTP FAIL-1：hello n=5 / 长句 n=16 / 80 词 n=117，max 4~12 字 — 来自 `kin_done.text` 合成，不是模型 token
- `verdict.md` / `report.json` / `stream-analysis.json` / `cli-node.stdout.ndjson` / `kernel.log`

## `*-141402-setup-token-chat` — setup-token 单次对话

`CLAUDE_CODE_OAUTH_TOKEN` 直连 CLI `-p`，SOCKS 72.1.181.43，回复 `setup-ok`（4.9s）。不启 kernel。

## `*-133604-v2-cli-node` — 首次 Node V2 parented 证据

boot 时 12 条 parented `stream_event`（后续 OAuth 作废轮次见同日前缀目录，不入库）。

## `*-123900-v2-cli-partials` — claude-code-best bun 路径 V2 sink

59 条 parented `stream_event`；HTTP 仍 kin_done 合成。

## `*-3a0091f-round5` — 五轮最小化 S1→S4（通过）

剥 `accept-encoding` 后 tap 吃到明文 SSE；`kin_done` `input_json_delta` 合成多片 `text_delta`。

- `verdict.md`：S1–S4 全 PASS
- `report.json`：S1 n_td=11 / max_delta=12；S2 同 job/slot；S3 3/3；S4 Δhit=6 tap_dropped=0
- `kernel.log`

## `*-f28de67-round4` — 四轮最小化 S1 FAIL

tap 挂上但客户端仍整块 stdout（后续定位为压缩字节流）。S2–S4 未跑。

- `verdict.md` / `report.json` / `kernel.log`

## `*-adc1450-authoritative` — 三轮 FAIL 复测（仍 FAIL）

延迟仲裁后仍 n_delta=1；conc20 17/20 串流+空 200。

- `verdict.md` / `report.json` / `kernel.log`

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
- setup-token 与 OAuth JSON **不入库**。
