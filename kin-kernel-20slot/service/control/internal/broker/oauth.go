// Official OAuth ticket rotation for Kin.
//
// Portunex exchange.py mints CLI tokens from a claude.ai sessionKey cookie
// (Chrome impersonation + PKCE authorize + axios token + Grove patch).
// That path is not implemented here. The official Claude Code client never
// sends sessionKey: it opens a browser authorize URL and listens on
// localhost/callback, then stores claudeAiOauth {access, refresh, scopes}.
//
// This package:
//   - 410 on session_key / cookie interchange
//   - refresh_token grant (same body as src/services/oauth/client.ts)
//   - pins one SOCKS5 for refresh and for later CLI spawn (ALL_PROXY)
package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/proxy"
)

const (
	DefaultClientID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
	DefaultTokenURL = "https://platform.claude.com/v1/oauth/token"
	ClaudeAIScope   = "user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload"
)

// Alignment is returned on 410 so operators see which hops are official.
var Alignment = []map[string]string{
	{
		"step":     "0 GET /api/organizations + Cookie sessionKey",
		"cli":      "never. orgUUID is an optional query on the browser authorize URL",
		"portunex": "Chrome 146 + chrome146 TLS impersonation",
		"kin":      "rejected",
	},
	{
		"step":     "1 POST /v1/oauth/{org}/authorize + Cookie sessionKey",
		"cli":      "GET claude.com/cai/oauth/authorize in a real browser, PKCE, redirect to localhost",
		"portunex": "JSON POST with CLI scopes, UA Chrome 146",
		"kin":      "rejected",
	},
	{
		"step":     "2 POST /v1/oauth/token authorization_code",
		"cli":      "axios JSON after the browser returns code; redirect_uri is localhost or manual callback",
		"portunex": "same grant, axios UA, omit expires_in, redirect_uri is platform callback",
		"kin":      "rejected (no cookie-minted code)",
	},
	{
		"step":     "2b POST /v1/oauth/token refresh_token",
		"cli":      "refreshOAuthToken in src/services/oauth/client.ts",
		"portunex": "not this hop; they mint a new refresh from the cookie",
		"kin":      "implemented, via the pinned SOCKS5",
	},
	{
		"step":     "3 GET /api/claude_cli/bootstrap",
		"cli":      "Bearer + anthropic-beta oauth-2025-04-20 + UA claude-code/<version>",
		"portunex": "same headers, UA pinned to 2.1.241",
		"kin":      "left to the spawned CLI",
	},
	{
		"step":     "4 PATCH grove_enabled",
		"cli":      "interactive Grove notice, not auto-accepted",
		"portunex": "auto PATCH true",
		"kin":      "rejected",
	},
}

type ExchangeRequest struct {
	SessionKey    string `json:"session_key"`
	SessionKeyAlt string `json:"sessionKey"`
	Cookie        string `json:"cookie"`
	Socks5        string `json:"socks5"`
	SecretRef     string `json:"secret_ref"`
	RefreshToken  string `json:"refresh_token"`
	AccessToken   string `json:"access_token"`
	Scopes        string `json:"scope"`
}

type ClaudeAiOauth struct {
	AccessToken      string   `json:"accessToken"`
	RefreshToken     string   `json:"refreshToken"`
	ExpiresAt        int64    `json:"expiresAt"`
	Scopes           []string `json:"scopes"`
	SubscriptionType string   `json:"subscriptionType,omitempty"`
	RateLimitTier    string   `json:"rateLimitTier,omitempty"`
}

type RefreshResult struct {
	OK         bool          `json:"ok"`
	AuthMode   string        `json:"authMode"`
	Socks5     string        `json:"socks5"`
	Oauth      ClaudeAiOauth `json:"-"`
	ExpiresIn  int           `json:"expires_in"`
	HasRefresh bool          `json:"has_refresh"`
	AccessFP   string        `json:"access_token_fp"`
	RefreshFP  string        `json:"refresh_token_fp"`
	Scope      string        `json:"scope"`
}

type Refresher struct {
	TokenURL      string
	ClientID      string
	RequireSOCKS5 bool
	Transport     http.RoundTripper
}

func (r *Refresher) tokenURL() string {
	if r.TokenURL != "" {
		return r.TokenURL
	}
	return DefaultTokenURL
}

func (r *Refresher) clientID() string {
	if r.ClientID != "" {
		return r.ClientID
	}
	return DefaultClientID
}

func NormalizeSOCKS5(raw string) (string, error) {
	proxyURL := strings.TrimSpace(raw)
	if proxyURL == "" {
		return "", fmt.Errorf("socks5 is required so refresh and CLI share one egress")
	}
	if strings.HasPrefix(proxyURL, "socks5://") && !strings.HasPrefix(proxyURL, "socks5h://") {
		proxyURL = "socks5h://" + strings.TrimPrefix(proxyURL, "socks5://")
	}
	if !strings.Contains(proxyURL, "://") {
		proxyURL = "socks5h://" + proxyURL
	}
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return "", fmt.Errorf("socks5 url invalid")
	}
	if parsed.Scheme != "socks5" && parsed.Scheme != "socks5h" {
		return "", fmt.Errorf("socks5h://host:port required")
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("socks5 host required")
	}
	return proxyURL, nil
}

func Redact(value string) string {
	if value == "" {
		return ""
	}
	if len(value) <= 12 {
		return value[:min(4, len(value))] + "…"
	}
	return value[:8] + "…" + value[len(value)-6:]
}

func SessionKeyPresent(req ExchangeRequest) bool {
	blob := req.SessionKey + req.SessionKeyAlt + req.Cookie
	return strings.Contains(blob, "sk-ant-sid") || strings.Contains(strings.ToLower(blob), "sessionkey=")
}

func RejectSessionKey() map[string]any {
	return map[string]any{
		"ok": false,
		"error": map[string]any{
			"code":      "rejected_interchange",
			"message":   "sessionKey → OAuth interchange is not implemented. Use official /login claudeAiOauth + refresh_token.",
			"retryable": false,
		},
		"alignment": Alignment,
	}
}

func (r *Refresher) DialContext(socks5 string) (func(ctx context.Context, network, addr string) (net.Conn, error), error) {
	normalized, err := NormalizeSOCKS5(socks5)
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(normalized)
	if err != nil {
		return nil, err
	}
	var auth *proxy.Auth
	if parsed.User != nil {
		password, _ := parsed.User.Password()
		auth = &proxy.Auth{User: parsed.User.Username(), Password: password}
	}
	dialer, err := proxy.SOCKS5("tcp", parsed.Host, auth, proxy.Direct)
	if err != nil {
		return nil, err
	}
	contextDialer, ok := dialer.(proxy.ContextDialer)
	if ok {
		return contextDialer.DialContext, nil
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.Dial(network, addr)
	}, nil
}

func (r *Refresher) HTTPClient(socks5 string) (*http.Client, error) {
	if r.Transport != nil {
		return &http.Client{Timeout: 30 * time.Second, Transport: r.Transport}, nil
	}
	if r.RequireSOCKS5 || socks5 != "" {
		dial, err := r.DialContext(socks5)
		if err != nil {
			return nil, err
		}
		return &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				DialContext:     dial,
				Proxy:           nil,
				IdleConnTimeout: 30 * time.Second,
			},
		}, nil
	}
	return &http.Client{Timeout: 30 * time.Second}, nil
}

func (r *Refresher) Refresh(ctx context.Context, refreshToken, scope, socks5 string) (*RefreshResult, error) {
	if refreshToken == "" {
		return nil, fmt.Errorf("refresh_token required")
	}
	if r.RequireSOCKS5 {
		if _, err := NormalizeSOCKS5(socks5); err != nil {
			return nil, err
		}
	}
	client, err := r.HTTPClient(socks5)
	if err != nil {
		return nil, err
	}
	if scope == "" {
		scope = ClaudeAIScope
	}
	body, _ := json.Marshal(map[string]string{
		"grant_type":    "refresh_token",
		"refresh_token": refreshToken,
		"client_id":     r.clientID(),
		"scope":         scope,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.tokenURL(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token refresh status %d", resp.StatusCode)
	}
	var payload struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
		Scope        string `json:"scope"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	if payload.AccessToken == "" {
		return nil, fmt.Errorf("token refresh missing access_token")
	}
	if payload.RefreshToken == "" {
		payload.RefreshToken = refreshToken
	}
	if payload.Scope == "" {
		payload.Scope = scope
	}
	expiresAt := time.Now().Add(time.Duration(payload.ExpiresIn) * time.Second).UnixMilli()
	if payload.ExpiresIn == 0 {
		expiresAt = time.Now().Add(8 * time.Hour).UnixMilli()
	}
	socksDisplay := ""
	if socks5 != "" {
		socksDisplay = "socks5h://••••"
	}
	return &RefreshResult{
		OK:         true,
		AuthMode:   "claude_ai_oauth",
		Socks5:     socksDisplay,
		ExpiresIn:  payload.ExpiresIn,
		HasRefresh: payload.RefreshToken != "",
		AccessFP:   Redact(payload.AccessToken),
		RefreshFP:  Redact(payload.RefreshToken),
		Scope:      payload.Scope,
		Oauth: ClaudeAiOauth{
			AccessToken:  payload.AccessToken,
			RefreshToken: payload.RefreshToken,
			ExpiresAt:    expiresAt,
			Scopes:       strings.Fields(payload.Scope),
		},
	}, nil
}

func SpawnEnv(configDir, socks5 string) map[string]string {
	return SpawnEnvOpts(SpawnOpts{ConfigDir: configDir, Socks5: socks5})
}

// SpawnOpts builds CLI env. Subscriber oauth leaves CLAUDE_CODE_OAUTH_TOKEN unset.
// Setup-token (claude setup-token / inference-only) sets it.
type SpawnOpts struct {
	ConfigDir  string
	Socks5     string
	SetupToken string
}

func SanitizeSetupToken(raw string) string {
	s := strings.TrimSpace(raw)
	idx := strings.Index(s, "sk-ant-oat01-")
	if idx < 0 {
		return s
	}
	body := s[idx:]
	end := strings.LastIndex(body, "AA")
	if end < 0 {
		return body
	}
	return body[:end+2]
}

func SpawnEnvOpts(opts SpawnOpts) map[string]string {
	env := map[string]string{
		"CLAUDE_CONFIG_DIR":      opts.ConfigDir,
		"CLAUDE_CODE_ENTRYPOINT": "cli",
	}
	if opts.Socks5 != "" {
		env["ALL_PROXY"] = opts.Socks5
		env["HTTPS_PROXY"] = opts.Socks5
		env["HTTP_PROXY"] = opts.Socks5
		env["NO_PROXY"] = "127.0.0.1,localhost"
	}
	if token := SanitizeSetupToken(opts.SetupToken); token != "" {
		env["CLAUDE_CODE_OAUTH_TOKEN"] = token
		env["KIN_CLAUDE_CODE_OAUTH_TOKEN"] = token
	}
	return env
}
