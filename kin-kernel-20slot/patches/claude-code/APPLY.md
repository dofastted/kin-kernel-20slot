# Native 0-inject construction (P0–P3)

Baseline: `claude-code-best/claude-code` `77a7934`.

## Copy

```
src/kin/systemLayout.ts
src/kin/stdioProtocol.ts
src/kin/nativeSlotRunner.ts
```

into the CLI tree. Apply:

```
print.ts.hook.patch
main.ts.native.patch
claude.ts.layout.patch
subagent-token-streaming.patch   # still useful on mcp_slot rollback
```

Do **not** use the old `kinEnvelope.ts` billing forge (`cch` / `2.1.241.fp`). Node omits `cch`; `cc_version` comes from `getAttributionHeader()`.

## Env

```
CLAUDE_CODE_SYSTEM_LAYOUT=zero|identity
CLAUDE_CODE_TIMEZONE=America/New_York
CLAUDE_CODE_KIN_NATIVE_SLOTS=1    # P3 host loop; skip structuredIO
```

## Kernel

```
KIN_EXECUTION_MODE=native_slot
KIN_RELAY_MODE=off
KIN_SYSTEM_MODE=zero
KIN_SLOT_TZ=America/New_York
KIN_SOCKS5=...
```

Native spawn must **not** write `mcp.json` and must **not** send `kin_hello` on boot.
Official `-p` peeks stdin and, after the first byte, waits for EOF forever —
a live job pipe would hang before `runHeadless` / `kin_slot_ready`.

The stdin reader must **not** `await` a job. Each slot is an independent
state machine (`idle|running|parked`) with its own QueryEngine and cache.
`kin_host_ready` is the capability handshake (`multi_slot`, `tool_parking`,
`native_sse`). Recycle a slot only on `kin_job_done` / `kin_job_error` /
`kin_cancel_ack` — never on `kin_job_parked`.

Default `KIN_EXECUTION_MODE` remains `mcp_slot` until 2-slot overlap,
tool continuation, and 测试标准 01–07 pass on native.
