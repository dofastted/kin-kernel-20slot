package api

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"kin.local/kin-control/internal/reconcile"
	"kin.local/kin-control/internal/store"
)

func TestRegisterAndListKernel(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	memory := store.NewMemory()
	reconciler := reconcile.New(memory, 20*time.Second, logger)
	server := httptest.NewServer(New(memory, reconciler, time.Hour, logger).Handler())
	defer server.Close()

	body := `{"id":"kernel-a","address":"http://kernel-a:8080","capacity":20,"provider":"mock","revision":1}`
	response, err := http.Post(server.URL+"/api/v1/kernels", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("register status = %d", response.StatusCode)
	}

	response, err = http.Get(server.URL + "/api/v1/kernels")
	if err != nil {
		t.Fatal(err)
	}
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	memory := store.NewMemory()
	reconciler := reconcile.New(memory, 20*time.Second, logger)
	server := httptest.NewServer(New(memory, reconciler, time.Hour, logger).Handler())
	defer server.Close()

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
	req, err := http.NewRequest(http.MethodPut, server.URL+"/api/v1/runtime-profile", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("content-type", "application/json")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer putResp.Body.Close()
	putPayload, _ := io.ReadAll(putResp.Body)
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("put status = %d body = %s", putResp.StatusCode, putPayload)
	}
	if !strings.Contains(string(putPayload), `"config_hash"`) {
		t.Fatalf("expected config_hash in put response: %s", putPayload)
	}

	getResp, err := http.Get(server.URL + "/api/v1/runtime-profile")
	if err != nil {
		t.Fatal(err)
	}
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	memory := store.NewMemory()
	reconciler := reconcile.New(memory, 20*time.Second, logger)
	server := httptest.NewServer(New(memory, reconciler, time.Hour, logger).Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/api/v1/runtime-profile")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestRuntimeProfilePutInvalidSlotCount(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	memory := store.NewMemory()
	reconciler := reconcile.New(memory, 20*time.Second, logger)
	server := httptest.NewServer(New(memory, reconciler, time.Hour, logger).Handler())
	defer server.Close()

	body := `{
		"execution_mode": "native_messages",
		"system_layout": "zero",
		"timezone": "America/New_York",
		"slot_count": 21,
		"max_body_bytes": 1048576,
		"max_output_tokens": 8192
	}`
	req, err := http.NewRequest(http.MethodPut, server.URL+"/api/v1/runtime-profile", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("content-type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestSessionKeyExchangeGone(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	memory := store.NewMemory()
	reconciler := reconcile.New(memory, 20*time.Second, logger)
	server := httptest.NewServer(New(memory, reconciler, time.Hour, logger).Handler())
	defer server.Close()

	body := `{"session_key":"sk-ant-sid01-example","socks5":"socks5h://127.0.0.1:1080"}`
	response, err := http.Post(server.URL+"/api/v1/credentials/exchange", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
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
