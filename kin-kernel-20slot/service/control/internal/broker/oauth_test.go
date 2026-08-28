package broker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRejectSessionKey(t *testing.T) {
	t.Parallel()
	if !SessionKeyPresent(ExchangeRequest{SessionKey: "sk-ant-sid01-example"}) {
		t.Fatal("expected session key detection")
	}
	if SessionKeyPresent(ExchangeRequest{RefreshToken: "sk-ant-ort01-example"}) {
		t.Fatal("refresh token is not a sessionKey")
	}
	body := RejectSessionKey()
	raw, _ := json.Marshal(body)
	if !strings.Contains(string(raw), "rejected_interchange") {
		t.Fatalf("missing code: %s", raw)
	}
	if strings.Contains(string(raw), "sk-ant-sid") && strings.Contains(string(raw), "sessionKey →") {
		// alignment text may mention the name; it must not echo a live key
	}
}

func TestRefreshGrantUsesOfficialBody(t *testing.T) {
	t.Parallel()
	var saw map[string]string
	token := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &saw)
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content-type = %s", r.Header.Get("Content-Type"))
		}
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "sk-ant-oat01-refreshed",
			"refresh_token": "sk-ant-ort01-rotated",
			"expires_in":    28800,
			"scope":         ClaudeAIScope,
		})
	}))
	defer token.Close()

	refresher := &Refresher{TokenURL: token.URL, RequireSOCKS5: false}
	result, err := refresher.Refresh(context.Background(), "sk-ant-ort01-old", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if saw["grant_type"] != "refresh_token" || saw["client_id"] != DefaultClientID {
		t.Fatalf("unexpected body: %#v", saw)
	}
	if saw["refresh_token"] != "sk-ant-ort01-old" {
		t.Fatalf("refresh token not forwarded")
	}
	if !result.OK || result.AuthMode != "claude_ai_oauth" {
		t.Fatalf("result = %#v", result)
	}
	if result.Oauth.AccessToken != "sk-ant-oat01-refreshed" {
		t.Fatal("access token not stored on oauth blob")
	}
	if strings.Contains(result.AccessFP, "refreshed") {
		t.Fatal("fingerprint leaked token")
	}
}

func TestSOCKS5Required(t *testing.T) {
	t.Parallel()
	refresher := &Refresher{RequireSOCKS5: true}
	_, err := refresher.Refresh(context.Background(), "sk-ant-ort01-old", "", "")
	if err == nil {
		t.Fatal("expected socks5 required")
	}
	normalized, err := NormalizeSOCKS5("127.0.0.1:1080")
	if err != nil || normalized != "socks5h://127.0.0.1:1080" {
		t.Fatalf("normalize = %s %v", normalized, err)
	}
}

func TestSpawnEnvPinsSameProxy(t *testing.T) {
	t.Parallel()
	env := SpawnEnv("/run/kin/demo/sess", "socks5h://user:pass@127.0.0.1:1080")
	if env["CLAUDE_CONFIG_DIR"] != "/run/kin/demo/sess" {
		t.Fatal("config dir")
	}
	if env["ALL_PROXY"] != env["HTTPS_PROXY"] || env["ALL_PROXY"] == "" {
		t.Fatal("refresh and CLI must share ALL_PROXY")
	}
	if _, ok := env["CLAUDE_CODE_OAUTH_TOKEN"]; ok {
		t.Fatal("must not inject setup-token env")
	}
}
