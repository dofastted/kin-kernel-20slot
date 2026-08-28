# OAuth Broker (`internal/broker/oauth.go`)

## What This Package Does and Does Not Do

Read the package doc-comment first — it states the scope precisely: the official
Claude Code CLI login flow (browser authorize URL + localhost callback) is
**not implemented here**; this package implements only the `refresh_token` grant
(same request body as the CLI's `src/services/oauth/client.ts`) plus explicit
rejection of the cookie/`sessionKey`-based interchange some third-party tools use
to mint tokens without a real login.

The `Alignment` table (`var Alignment = []map[string]string{...}`) documents, hop
by hop, which parts of the real OAuth flow are implemented (`kin: "implemented"`
— only step 2b, `refresh_token`) versus rejected (steps 0, 1, 2, 4 — all
cookie/session-key-dependent or requiring interactive browser consent). This
table is returned verbatim in `RejectSessionKey()`'s response body so operators
hitting a `410` can see exactly what is and isn't supported without reading Go
source. **When touching this file, keep `Alignment` accurate** — it is
user-facing documentation embedded in the API response, not an internal comment.

## Session-Key Rejection

`SessionKeyPresent(req)` checks `SessionKey + SessionKeyAlt + Cookie` for the
substring `"sk-ant-sid"` or a case-insensitive `"sessionkey="` — this is a
heuristic content check, not a strict field-presence check, because
`ExchangeRequest` accepts the value under three different possible field names
(`session_key`, `sessionKey`, `cookie`) depending on which client convention a
caller follows. If you add a fourth field alias for the same secret shape, add it
to this substring check, not a new field-presence branch.

`RejectSessionKey()` returns `{"ok": false, "error": {code: "rejected_interchange",
...}, "alignment": Alignment}` — always `410 Gone`, never `400`, because the
interchange is permanently unsupported, not merely malformed in this instance.

## `refresh_token` Grant (`Refresher.Refresh`)

This is a real, working OAuth `refresh_token` exchange against
`https://platform.claude.com/v1/oauth/token` (or `Refresher.TokenURL` override for
tests) — not a stub. Request body: `{grant_type: "refresh_token", refresh_token,
client_id, scope}`, matching the CLI's own client body exactly (see the package
doc-comment reference to `src/services/oauth/client.ts`). `DefaultClientID` and
`ClaudeAIScope` are the CLI's real published values — do not change them without
confirming against the current CLI source, since a mismatched `client_id`/`scope`
will be rejected upstream.

Response handling: if the upstream response omits `refresh_token` or `scope`,
the request's own values are echoed back rather than left empty (refresh-token
rotation is optional server-side, so a missing field means "unchanged," not
"cleared"). `expires_in == 0` falls back to an 8-hour assumed lifetime rather than
producing an already-expired token.

**Never expose raw tokens in a response `Redact()` doesn't cover.**
`RefreshResult.Oauth.AccessToken`/`RefreshToken` carry the real secret values (for
handing to the CLI process — see `SpawnEnv`) but that field is tagged `json:"-"`
so it never serializes into the HTTP response; only `AccessFP`/`RefreshFP`
(`Redact()`'d fingerprints: first 8 + `…` + last 6 chars) go out over the wire. If
you add a new field carrying a real secret to `RefreshResult`, tag it `json:"-"`
the same way — do not rely on remembering to redact at the call site.

## SOCKS5 Pinning

`NormalizeSOCKS5(raw)`: rejects empty input, upgrades bare `socks5://` to
`socks5h://` (DNS resolution happens proxy-side, not client-side — required so a
`.onion`-style or internal-only hostname behind the proxy resolves correctly),
and rejects anything that isn't `socks5`/`socks5h` scheme with a host.
`Refresher.RequireSOCKS5` (set `true` in `api.New()`'s default `&broker.Refresher{}`)
makes a missing/invalid SOCKS5 value a hard error on `/credentials/refresh` — this
is the same cross-language invariant as the Rust kernel's `apply_proxy_env()` (see
`../kernel/provider-adapters.md`'s "Repeated pattern: credential + proxy setup"):
**the token refresh and the later CLI process spawn must share exactly one
egress path.** `SpawnEnv()` sets `ALL_PROXY`/`HTTPS_PROXY`/`HTTP_PROXY` to the
same `socks5` value passed to `Refresh()`, enforcing this at the type level — do
not add a code path that refreshes over one proxy and spawns the CLI with
another.

`HTTPClient(socks5)` builds an `http.Client` with a `proxy.SOCKS5` dialer via
`golang.org/x/net/proxy` — the module's only external dependency — when
`RequireSOCKS5` is true or a socks5 value was given; `Transport` can be overridden
for tests to bypass the network entirely (see `oauth_test.go`'s
`TestRefreshGrantUsesOfficialBody`, which points `TokenURL` at an `httptest`
server with `RequireSOCKS5: false`).

## Testing Pattern

`oauth_test.go` covers exactly the properties that matter for a credential
broker, and is the template for new tests in this package:
`TestRejectSessionKey` (detection + non-echo), `TestRefreshGrantUsesOfficialBody`
(asserts the exact upstream request shape against a local `httptest` server, and
that the fingerprint never contains the real token substring),
`TestSOCKS5Required` (missing proxy is a hard error when required; normalization
round-trip), `TestSpawnEnvPinsSameProxy` (asserts `ALL_PROXY == HTTPS_PROXY` and
that no `CLAUDE_CODE_OAUTH_TOKEN` env leaks in — a regression guard against
accidentally injecting a setup-token env var the CLI would trust unauthenticated).
When adding broker behavior, add both a positive-path test and a
secret-non-leakage assertion, not just a status-code check.
