# Node + setup-token：patched CLI 流式 subagent × Kin 实测

- 时间: 2026-08-29T14:26:05Z
- CLI: claude-code-best 2.8.4 `dist/cli-node.js` / Node v24.20.0（进程名 claude，exe=/tmp/node24/bin/node）
- 鉴权: setup-token → `CLAUDE_CODE_OAUTH_TOKEN`（inference-only）
- Kin: accept kernel 2-slot authoritative · SOCKS5 72.1.181.43
- 不并行：单 CLI、场景串行

## 结论

**HTTP 多 delta 通过；原生 subagent `text_delta` 仍没有。**  
patched Node CLI 已经把 `background:true` kin-slot 的 SSE 打到 stdout（带 `parent_tool_use_id`），Kin 也能按 parent 分槽。但 slot 按设定把正文放进 `kin_done` 的 **tool input JSON**，不发 assistant `text_delta`。用户 SSE 的小块 `text_delta` 来自 Kin 对 `kin_done.text` 的合成（4~12 字），不是模型逐 token。

## 清单

| 项 | 结果 |
|---|---|
| kernel ready authoritative | PASS（~69s boot） |
| 运行时 Node 24（非 bun） | PASS（exe=node24；comm=claude 是 CLI 改名） |
| SOCKS 出口 72.1.181.43 | PASS |
| stdout 多个 stream_event | PASS 433 |
| parent_tool_use_id 非空 | PASS 258 / 2 个 Agent toolu |
| ProgressMessage / agent_stream_event | PASS 0 |
| FAIL-1 hello 多 delta | PASS n=5 text=observe-hello |
| FAIL-1 长句 | PASS n=16 max=11 |
| FAIL-1 80 词 | PASS n=117 max=12 |
| 原生 parented **text_delta** | **FAIL 0** |
| parented delta 实际类型 | input_json_delta × 213（slot_wait / kin_done 参数） |
| Kin 用户流来源 | kin_done.text 合成（tap correlate_hit=4） |

## CLI tee

- parented block_start 工具名：`mcp__kin_runtime__slot_wait`、`mcp__kin_runtime__kin_done`
- kin_done JSON 里已经带完整 `"text":"observe-hello"` 等，是 **tool 参数流**，不是 assistant 正文流
- supervisor 根上有 7 条 text_delta（“I'll spawn 2 background kin-slot…”），与用户请求无关

## 含义

V2 patch（onStreamEvent + sink + parent id）在 Node + `background:true` **有效，且已被 Kin 使用**（demux / 合成 SSE）。  
要变成「slot 正文逐 token」，需要改 kin-slot prompt：允许 assistant text_delta，或让 Rust 消费 `kin_done` 的 `input_json_delta` 做增量，而不是等工具完成再切块。
