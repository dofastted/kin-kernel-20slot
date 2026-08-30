package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kin.local/kin-control/internal/reconcile"
	"kin.local/kin-control/internal/store"
)

const testInternalToken = "test-internal-token"

func newTestServer(t *testing.T) (*httptest.Server, *store.SQLite) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	controlStore, err := store.OpenSQLite(filepath.Join(t.TempDir(), "kin.db"), "test-db-secret")
	if err != nil {
		t.Fatal(err)
	}
	reconciler := reconcile.New(controlStore.Observed(), 20*time.Second, logger)
	server := httptest.NewServer(New(
		controlStore,
		reconciler,
		time.Hour,
		logger,
		Options{InternalToken: testInternalToken},
	).Handler())
	t.Cleanup(func() {
		server.Close()
		_ = controlStore.Close()
	})
	return server, controlStore
}

func doRequest(t *testing.T, method, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("authorization", "Bearer "+testInternalToken)
	if body != "" {
		req.Header.Set("content-type", "application/json")
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestInternalAuthAndPublicHealth(t *testing.T) {
	t.Parallel()
	server, _ := newTestServer(t)

	health, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", health.StatusCode)
	}

	unauthorized, err := http.Get(server.URL + "/api/v1/kernels")
	if err != nil {
		t.Fatal(err)
	}
	_ = unauthorized.Body.Close()
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.StatusCode)
	}
}

func TestRegisterAndListKernel(t *testing.T) {
	t.Parallel()
	server, _ := newTestServer(t)

	body := `{"id":"kernel-a","address":"http://kernel-a:8080","capacity":20,"provider":"mock","revision":1}`
	response := doRequest(t, http.MethodPost, server.URL+"/api/v1/kernels", body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("register status = %d", response.StatusCode)
	}

	response = doRequest(t, http.MethodGet, server.URL+"/api/v1/kernels", "")
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"id":"kernel-a"`) {
		t.Fatalf("unexpected list response: %s", payload)
	}
}

func TestRuntimeProfilePutAndGet(t *testing.T) {
	t.Parallel()
	server, _ := newTestServer(t)

	body := `{
		"execution_mode": "native_messages",
		"system_layout": "zero",
		"timezone": "America/New_York",
		"slot_count": 20,
		"socks5": "socks5h://127.0.0.1:1080",
		"allowed_models": ["claude-opus-5"],
		"allowed_server_tools": ["web_search"],
		"allowed_betas": ["beta-1"],
		"max_body_bytes": 1048576,
		"max_output_tokens": 8192
	}`
	putResp := doRequest(t, http.MethodPut, server.URL+"/api/v1/runtime-profile", body)
	defer putResp.Body.Close()
	putPayload, _ := io.ReadAll(putResp.Body)
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("put status = %d body = %s", putResp.StatusCode, putPayload)
	}
	if !strings.Contains(string(putPayload), `"config_hash"`) {
		t.Fatalf("expected config_hash in put response: %s", putPayload)
	}

	getResp := doRequest(t, http.MethodGet, server.URL+"/api/v1/runtime-profile", "")
	defer getResp.Body.Close()
	getPayload, _ := io.ReadAll(getResp.Body)
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d body = %s", getResp.StatusCode, getPayload)
	}
	if string(getPayload) != string(putPayload) {
		t.Fatalf("get response should match put response: get=%s put=%s", getPayload, putPayload)
	}
}

func TestRuntimeProfileGetNotFound(t *testing.T) {
	t.Parallel()
	server, _ := newTestServer(t)
	resp := doRequest(t, http.MethodGet, server.URL+"/api/v1/runtime-profile", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestRuntimeProfilePutInvalidSlotCount(t *testing.T) {
	t.Parallel()
	server, _ := newTestServer(t)
	body := `{
		"execution_mode": "native_messages",
		"system_layout": "zero",
		"timezone": "America/New_York",
		"slot_count": 21,
		"max_body_bytes": 1048576,
		"max_output_tokens": 8192
	}`
	resp := doRequest(t, http.MethodPut, server.URL+"/api/v1/runtime-profile", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestDocumentRevisionConflict(t *testing.T) {
	t.Parallel()
	server, _ := newTestServer(t)
	first := `{"schema_version":1,"expected_revision":0,"data":{"enabled":true}}`
	response := doRequest(t, http.MethodPut, server.URL+"/api/v1/config/documents/routing", first)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("first put status = %d", response.StatusCode)
	}

	response = doRequest(t, http.MethodPut, server.URL+"/api/v1/config/documents/routing", first)
	defer response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("conflict status = %d body = %s", response.StatusCode, payload)
	}
}

func TestOperationLifecycleAndIdempotency(t *testing.T) {
	t.Parallel()
	server, _ := newTestServer(t)
	body := `{"kind":"slot.restart","target":"slot-a","payload":{"reason":"config"},"max_attempts":2,"idempotency_key":"restart-slot-a-r1"}`
	first := doRequest(t, http.MethodPost, server.URL+"/api/v1/operations", body)
	firstPayload, _ := io.ReadAll(first.Body)
	_ = first.Body.Close()
	if first.StatusCode != http.StatusAccepted {
		t.Fatalf("enqueue status = %d body = %s", first.StatusCode, firstPayload)
	}
	second := doRequest(t, http.MethodPost, server.URL+"/api/v1/operations", body)
	secondPayload, _ := io.ReadAll(second.Body)
	_ = second.Body.Close()
	if second.StatusCode != http.StatusAccepted || !bytes.Equal(firstPayload, secondPayload) {
		t.Fatalf("idempotent enqueue mismatch: first=%s second=%s", firstPayload, secondPayload)
	}

	claim := doRequest(t, http.MethodPost, server.URL+"/api/v1/operations/claim", `{"worker":"worker-a","lease_seconds":30}`)
	claimPayload, _ := io.ReadAll(claim.Body)
	_ = claim.Body.Close()
	if claim.StatusCode != http.StatusOK || !strings.Contains(string(claimPayload), `"status":"claimed"`) {
		t.Fatalf("claim status = %d body = %s", claim.StatusCode, claimPayload)
	}
}

func TestSessionKeyExchangeGone(t *testing.T) {
	t.Parallel()
	server, _ := newTestServer(t)
	body := `{"session_key":"sk-ant-sid01-example","socks5":"socks5h://127.0.0.1:1080"}`
	response := doRequest(t, http.MethodPost, server.URL+"/api/v1/credentials/exchange", body)
	defer response.Body.Close()
	payload, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusGone {
		t.Fatalf("status = %d body = %s", response.StatusCode, payload)
	}
	if !strings.Contains(string(payload), "rejected_interchange") {
		t.Fatalf("body = %s", payload)
	}
	if strings.Contains(string(payload), "sk-ant-sid01-example") {
		t.Fatal("session key echoed")
	}
}

// The kernel implements exactly one execution mode after the patch-only
// consolidation; the console must not accept a profile the kernel cannot boot.
func TestValidateExecutionModeAcceptsOnlyNativeMessages(t *testing.T) {
	for _, mode := range []string{"native_messages", "NATIVE_MESSAGES", " native_messages "} {
		if err := validateExecutionMode(mode); err != nil {
			t.Fatalf("execution_mode %q must be accepted, got %v", mode, err)
		}
	}
	for _, mode := range []string{"mcp", "mcp_slot", "agent", "native", "native_slot", "native_agent", "host"} {
		if err := validateExecutionMode(mode); err == nil {
			t.Fatalf("deleted execution_mode %q must be rejected", mode)
		}
	}
}

func TestValidateExecutionModeRejectsUnknown(t *testing.T) {
	if err := validateExecutionMode("bogus"); err == nil {
		t.Fatal("unknown execution_mode must be rejected")
	}
}

func TestTypedRoutingAPIHonorsOwnerAndRevision(t *testing.T) {
	t.Parallel()
	server, controlStore := newTestServer(t)

	get := doRequest(t, http.MethodGet, server.URL+"/api/v1/config/routing", "")
	defer get.Body.Close()
	if get.StatusCode != http.StatusOK {
		t.Fatalf("default routing status = %d", get.StatusCode)
	}

	request := `{"expected_revision":0,"data":{"inference":{"engine":"rust"}}}`
	blocked := doRequest(t, http.MethodPut, server.URL+"/api/v1/config/routing", request)
	_ = blocked.Body.Close()
	if blocked.StatusCode != http.StatusConflict {
		t.Fatalf("node-owned routing put status = %d", blocked.StatusCode)
	}
	moveOwnerToGo(t, controlStore, "routing")

	put := doRequest(t, http.MethodPut, server.URL+"/api/v1/config/routing", request)
	defer put.Body.Close()
	putPayload, _ := io.ReadAll(put.Body)
	if put.StatusCode != http.StatusOK {
		t.Fatalf("typed routing put status = %d body = %s", put.StatusCode, putPayload)
	}
	var envelope struct {
		Revision uint64 `json:"revision"`
		Data     struct {
			Inference struct {
				Engine string `json:"engine"`
			} `json:"inference"`
		} `json:"data"`
	}
	if err := json.Unmarshal(putPayload, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Revision == 0 || envelope.Data.Inference.Engine != "rust" {
		t.Fatalf("unexpected routing envelope: %s", putPayload)
	}

	stale := doRequest(t, http.MethodPut, server.URL+"/api/v1/config/routing", request)
	_ = stale.Body.Close()
	if stale.StatusCode != http.StatusConflict {
		t.Fatalf("stale routing put status = %d", stale.StatusCode)
	}
	invalid := doRequest(t, http.MethodPut, server.URL+"/api/v1/config/routing", fmt.Sprintf(`{"expected_revision":%d,"data":{"unknown":true}}`, envelope.Revision))
	_ = invalid.Body.Close()
	if invalid.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid routing put status = %d", invalid.StatusCode)
	}
}

func moveOwnerToGo(t *testing.T, controlStore *store.SQLite, domain string) {
	t.Helper()
	ctx := context.Background()
	owner := store.DomainOwner{}
	for _, next := range []store.PutDomainOwnerInput{
		{Domain: domain, Owner: "node", State: "shadow_import", UpdatedBy: "test"},
		{Domain: domain, Owner: "node", State: "shadow_match", UpdatedBy: "test"},
		{Domain: domain, Owner: "node", State: "frozen", UpdatedBy: "test"},
		{Domain: domain, Owner: "go", State: "go", UpdatedBy: "test"},
	} {
		next.ExpectedRevision = owner.Revision
		var err error
		owner, err = controlStore.PutDomainOwner(ctx, next)
		if err != nil {
			t.Fatal(err)
		}
	}
}
