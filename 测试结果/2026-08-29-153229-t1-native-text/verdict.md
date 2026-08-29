# T1 native text_delta (2026-08-29-153229)

- CLI: Node 24 `cli-node.js` · setup-token · SOCKS5 72.1.181.43 · 2 slots
- Kin: `b9efab7` prompt（先普通 assistant text，再 metadata-only `kin_done`）
- 判定对象: CLI stdout parented `stream_event`（不拿 HTTP 当主结论）

## 结论

**prompt 就是阻塞点。改完后 subagent 开始发原生 `text_delta`。**

同一条 parented assistant response 的顺序是：

```
content_block_start text
text_delta "observ"
text_delta "e-hello"
content_block_start kin_done
input_json_delta {"job_id":"...","stop_reason":"end_turn"}
```

fox 同结构：`text_delta "The"` + `text_delta "<整句>"`，然后 `kin_done` 只有 job_id / stop_reason，**没有 `text` 字段**。

## 清单

| 项 | 结果 |
|---|---|
| kernel ready authoritative | PASS（~5s boot，Node 仍热） |
| parented `text_delta` >= 3 | **PASS 4**（hello 2 + fox 2） |
| 正文内容 | `observe-hello` + 完整 fox 句 |
| `kin_done` 被调用 | PASS ×2 |
| `input_json_delta` 仍在 | PASS 30（slot_wait / kin_done 元数据） |
| `kin_done` JSON 不含正文 | **PASS**（无 `"text"` 字段） |
| ProgressMessage | PASS 0 |
| slot 再进 `slot_wait` | **未采到**（stdout tee 卡在整 64KiB，第二条 kin_done JSON 被截断后进程被杀） |

HTTP 仅作对照：hello n_delta=2 first=8.0s text=`observe-hello`；fox n_delta=2。用户流已是原生 text，不再靠 `kin_done.text` 合成。

## 含义

下一笔不该再改 Claude Code SSE 导出。T1 已证明模型会按新 prompt 吐普通 assistant text。T2 的内部 MCP 过滤 / index 重映射可以在这次抓包上对照（CLI 已有 text + kin_done 同 response）。slot 再入 `slot_wait` 需要 tee 完整 flush 后再杀进程。
