# API and Schema

## HTTP Surface (`kernel/src/api.rs`)

`router()` wires exactly 5 routes plus body-limit/request-id/trace middleware layers:

- `POST /v1/messages` — Anthropic Messages-shaped endpoint.
- `POST /v1/chat/completions` — OpenAI Chat-shaped endpoint.
- `GET /healthz`, `GET /readyz` — liveness/readiness (see `docs/RUNBOOK.md` §1: `healthz`
  does not imply capacity; load balancers must use `readyz`).
- `GET /slots` — introspection endpoint (demo/ops visibility; `docs/API_AND_STATE.md`
  §1 flags the equivalent `x-kin-slot` response header as something to strip on public
  production deployments).

Both `/v1/messages` and `/v1/chat/completions` are handled by the same shared
`dispatch()` function — request/response shape differences are normalized at the edge
(`model.rs`'s `normalize_openai_tools()` and `impl From<ChatRequest> for
MessageRequest`), not by duplicating handler logic per format. **This is the pattern
to follow**: any new client-facing wire format should convert into `MessageRequest`
at the boundary and reuse `dispatch()`, not fork a parallel request-handling path.

### Continuation headers

Per `docs/API_AND_STATE.md` §1:

- `x-kin-session-id` — session-sticky key, must be tenant-bound.
- `x-kin-continuation` — required on any `tool_result` follow-up call; single-use.

`ActiveTurn::begin()` in `api.rs` branches on whether `x-kin-continuation` is present:
continuation-token path resumes the bound worker/session via `Scheduler::resume()` +
`SessionDirectory::resume()`; the fresh-request path acquires a new lease via
`Scheduler::acquire()`. `ActiveTurn::complete()` branches the other way on the
response: a `StopReason::ToolUse` result calls `mark_waiting()` + `park_waiting()` to
keep the worker reserved for the client's tool result; any other stop reason calls
`mark_ready()` to release it.

### Streaming

`into_sse()` builds the SSE response: a 12-second ping keepalive, per-format event
translation (Anthropic event names vs. OpenAI `chat.completion.chunk` via
`stream::openai_chunk()`), and a final `kin.done` event carrying session id,
continuation token, provider pid, and worker generation — this is Kin-specific
metadata, not part of either upstream vendor's wire format, and exists so operators/
clients can correlate a completed SSE stream back to the exact worker instance.

### Validation

`validate_request()` in `api.rs` checks model/messages/tools/`max_tokens` bounds and a
cross-cutting rule: any `tool_result` content block byte size is checked against
`config::MAX_TOOL_RESULT_BYTES` (4 MiB). This is the one place request-shape
validation happens — handlers do not re-validate downstream. New request fields that
need bounds checking belong in `validate_request()`, not scattered `if` checks inside
`dispatch()` or provider code.

## Schema (`kernel/src/model.rs`)

`MessageRequest` is the canonical internal request shape. It has a flatten
catch-all `extra: serde_json::Map<String, Value>` field so unrecognized JSON keys
round-trip instead of being silently dropped — follow this pattern for any new
request field you are not ready to model explicitly yet.

`MessageContent` is untagged (`Text` string or `Blocks` array) with a lenient custom
deserializer — client requests are not required to use the verbose block-array form
for plain text.

`ContentBlock` is a 9-variant tagged enum: `Text`, `Image`, `Document`, `ToolUse`,
`ToolResult`, `Thinking`, `RedactedThinking`, `ServerToolUse`,
`WebSearchToolResult`. When adding a new content block kind, add a variant here (not
a parallel ad hoc `Value` field elsewhere) so `stream.rs`'s `StreamAssembler` and
`map_assistant()` have one place to extend.

`MessageResponse` / `StopReason` (8 variants) / `Usage` mirror the Anthropic Messages
response shape. The OpenAI-compatible `Chat*` type family
(`ChatRequest`/`ChatResponse`/etc.) converts to/from these via explicit `From` impls
in `model.rs` — OpenAI-shape handling is a translation layer over the Anthropic-shape
core model, not a second independent schema implementation.

Two inline roundtrip tests in `model.rs` are the reference examples for testing new
schema additions — assert JSON serialize/deserialize symmetry, not just one direction.

## SSE Assembly (`kernel/src/stream.rs`)

`StreamAssembler` is the shared cross-provider primitive that turns a sequence of
Anthropic-shaped SSE events into a `MessageResponse`. It is used by:

- `provider/anthropic.rs`'s `pump_anthropic_sse()` — dual-purpose: forwards live
  events to the client channel **and** feeds them through `assembler.apply_event()`,
  then `assembler.finish(request)` produces the terminal response.
- `multiplex_cli/mod.rs`'s `handle_native_frame()` — forwards each
  `kin_stream_event.event` to the client verbatim **and** feeds it to a per-job
  `StreamAssembler`, whose `parts()` builds the terminal response in
  `complete_job()`.

There is exactly one accumulation primitive: `StreamAssembler`. The old
`multiplex_cli/job_stream.rs` demuxer (index remapping + internal-tool swallowing for
MCP-shaped frames) was deleted with the MCP path — do not reintroduce a second
event-translation layer; the CLI already emits well-formed Anthropic SSE.

`apply_event()` handles `content_block_start` (text/thinking/image/tool_use/
server_tool_use/web_search_tool_result variants), `content_block_delta`
(`text_delta`/`thinking_delta`/`input_json_delta` — tool JSON is accumulated as a
string in `tool_json[index]` and only parsed into `Value` on `content_block_stop`,
because partial JSON is not valid JSON), and `message_delta` (usage + stop reason).

`parse_sse_block()` is the shared SSE frame parser (`data:` line joining, `[DONE]`
sentinel handling) — reuse it rather than re-implementing SSE line parsing.
