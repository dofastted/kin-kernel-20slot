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
