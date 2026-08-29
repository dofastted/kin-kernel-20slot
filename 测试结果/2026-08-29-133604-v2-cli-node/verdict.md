# V2 CLI Node runtime 复测（不用 bun）

- 时间: 2026-08-29T13:36:03Z
- CLI: claude-code-best 2.8.4 `dist/cli-node.js` via `node --use-env-proxy`
- Kin: `3a0091f2c1b65d2b8dab94d81cdccbfcbf51c588` authoritative · 2 kin-slot · SOCKS5 72.1.181.43 via 127.0.0.1:18080
- 凭证: 仅 kernel multiplex 路径，未做闲聊

## 清单

- kernel ready authoritative: FAIL
- 子进程 comm=node + cli-node.js: FAIL
- 无 cli-bun.js: PASS
- stdout 多个 stream_event: PASS
- subagent parent_tool_use_id 非空: PASS
- FAIL-1 短句多个 text_delta: FAIL
- FAIL-1 长句多个 text_delta: FAIL
- FAIL-1 80词段落多个 text_delta: FAIL
- hello 含 observe-hello: FAIL

- hello n_delta=None first=Nonems text=None
- stream n_delta=None max=None text=''
- para n_delta=None max=None first=Nonems
- tee stream_event=152 parented=12 empty_parent=140
- runtimes: []

## 结论

**Node runtime 采证未完全通过**。

