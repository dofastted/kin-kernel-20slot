# 测试结果

按日期时间分目录。

## `*-161414-zero-system-std` — 0 注入出站头 + 测试标准（2026-08-29）

出站 `system` 改为 billing（`cc_version=2.1.241.<fp>` + `prompt_version=<身份句>`）+ `# Environment` 时区；**默认不写 identity 块**，05 才带调用方 leftover。

栈：kernel `authoritative` relay + 官方 Claude Code **2.1.251** 原生二进制 + setup-token + SOCKS5 `72.1.181.43:5437`，2 slot。客户端 SSE 来自 relay tap（本轮未关 relay，不是 CLI stdout 权威）。

| 题 | 结果 | 客户端 `text_delta` | 出站 kinds |
|---|---|---:|---|
| 01 工具列表 | PASS | 2 | billing+environment |
| 02 识图 | PASS | 15 | billing+environment |
| 03 web_search | PASS | 34 | billing+environment |
| 04 自我认知 | **PASS** | 16 | billing+environment（无 identity 块） |
| 05 收费员 | PASS | 4 | billing+environment+append |
| 06 套取 | FAIL `no_cc_identity` | 8 | 拒答里出现题面词 `Claude Code` |
| 07 强制天气 | PASS | 0（`tool_use`） | `get_weather` 东京/celsius |

结论：04 不再报 Agent SDK，0 注入头生效。Relay **不能拆**——改写出站 `system` 就在这一层；官方 2.1.251 也没有 `--forward-subagent-partials`。T4（关 relay、CLI stdout 权威）本轮未跑。

- `verdict.md` / `results.json` / 各题 `outbound/request-body.json` + `client.json`

## `*-153229-t1-native-text` — prompt 修复后首次原生 parented `text_delta`

`b9efab7` kin-slot prompt：先普通 assistant text，再 metadata-only `kin_done`。Node 24 + setup-token + 2-slot。

- parented `text_delta` **4**（hello 2 + fox 2），正文完整
- 同 response 内 `kin_done` 只有 `job_id` / `stop_reason`，**无 `text` 字段**
- stdout tee 整 64KiB 截断，slot 再入 `slot_wait` 未采到
- `verdict.md` / `report.json` / `stream-analysis.json` / `cli-node.stdout.ndjson` / `kernel.log`

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
