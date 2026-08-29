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

P3 hello slice: text jobs only. `kin_tool_result` parking is P3.5.
