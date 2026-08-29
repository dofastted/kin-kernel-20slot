# V2 CLI 后台 stream_event 验收

- CLI: claude-code-best `77a7934` + V2 补丁（`onStreamEvent` 旁路 + StructuredIO drain）
- Kin: `--forward-subagent-partials` + `CLAUDE_CODE_FORWARD_SUBAGENT_PARTIALS=1`
- 2 persistent `background:true` kin-slot

## 第一项验收

| 项 | 结果 |
|---|---|
| 原始 stdout 多个 `stream_event` | PASS（256 条；其中 59 条带 parent） |
| subagent `parent_tool_use_id` 非空 | PASS（2 个 slot toolu） |
| 不经过 `agent_stream_event` / `progress` | PASS（0） |
| 根会话 `--include-partial-messages` 仍带 `parent=null` | 预期（197 条），不是 V2 出口 |

## Rust 转发

HTTP S1：`n_text_delta=9` `max_delta=11`，用户可见流仍是 **tap 对 kin_done JSON 的合成**。
subagent stdout 里 **没有 `text_delta`**（thinking / input_json_delta / signature），因此原生 CLI 增量还不能替代 kin_done 合成。
