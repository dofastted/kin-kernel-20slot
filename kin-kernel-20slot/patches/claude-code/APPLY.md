# Claude Code native envelope + subagent streaming

Baseline: `claude-code-best/claude-code` `77a7934` (2.8.4, Node).

Goal:

```
patched cli-node.js  -->  HTTPS_PROXY (http_to_socks)  -->  SOCKS5  -->  api.anthropic.com
       ^                                                         |
       +---- stdout stream_event (parent_tool_use_id) <----------+
```

Relay stays **off**. Kin kernel demuxes CLI stdout.

## 1. Subagent token streaming

```bash
cd claude-code
patch -p1 < ../kin-kernel-20slot/patches/claude-code/subagent-token-streaming.patch
```

Source: `attachments/subagent-token-streaming.patch`. Yields subagent `stream_event` through the existing progress channel; `queryHelpers.normalizeMessage` re-emits SDK `stream_event` with `parent_tool_use_id`.

## 2. Envelope (system + env)

Copy `src/utils/kinEnvelope.ts` into the CLI tree.

In `src/services/api/claude.ts`, around the wrap that always prepends attribution + identity (search `getCLISyspromptPrefix`), replace with:

```ts
import { applyKinEnvelope } from '../../utils/kinEnvelope.js'

const kinBlocks = applyKinEnvelope(messagesForAPI, filteredTools)
if (kinBlocks) {
  systemPrompt = asSystemPrompt(kinBlocks)
} else {
  systemPrompt = asSystemPrompt(
    [
      getAttributionHeader(),
      getCLISyspromptPrefix({
        isNonInteractive: options.isNonInteractiveSession,
        hasAppendSystemPrompt: options.hasAppendSystemPrompt,
      }),
      ...systemPrompt,
      ...(advisorModel ? [ADVISOR_TOOL_INSTRUCTIONS] : []),
      ...(injectChromeHere ? [CHROME_SEARCH_EXTRA_TOOLS_INSTRUCTIONS] : []),
    ].filter(Boolean),
  )
}
```

Only kin-slot requests (`mcp__kin_runtime__*` tools) are rewritten. Supervisor traffic is untouched.

## 3. Build Node CLI

```bash
bun run build   # or the repo's Node dist target
# KIN_CLAUDE_BIN=/path/to/cli-node.js  (wrapper: exec node cli-node.js "$@")
```

## 4. Kernel env (console-managed)

```bash
export KIN_RELAY_MODE=off          # native CLI -> SOCKS, default
export KIN_SYSTEM_MODE=zero        # or identity
export KIN_SLOT_TZ=America/New_York
export KIN_SOCKS5='socks5h://user:pass@host:port'
# KIN_HTTPS_PROXY is auto-started on 127.0.0.1:18080 when relay=off and SOCKS is set

# console
curl -s localhost:18090/internal/v1/envelope
curl -s -X PUT localhost:18090/internal/v1/envelope \
  -H 'content-type: application/json' \
  -d '{"mode":"identity","timezone":"America/New_York"}'
```

Patched CLI re-reads `KIN_ENVELOPE_PATH` every request, so mode/TZ changes apply without respawn.
