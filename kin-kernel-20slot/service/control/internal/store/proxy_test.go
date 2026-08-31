package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestProxySecretWriteFailsClosedWithoutKey(t *testing.T) {
	ctx := context.Background()
	controlStore, _ := openTestSQLite(t, "")
	user := "alice"
	pass := "s3cret"
	_, err := controlStore.PutProxy(ctx, PutProxyInput{
		ID: "px-test1", Scheme: "socks5", Host: "127.0.0.1", Port: 1080, Enabled: true,
		Username: &user, Password: &pass, UpdatedBy: "test",
	})
	if !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("missing secret error = %v", err)
	}
}

func TestProxyStoresAuthOnlyInSecretTable(t *testing.T) {
	ctx := context.Background()
	controlStore, _ := openTestSQLite(t, "db-secret")
	user := "alice"
	pass := "s3cret"
	record, err := controlStore.PutProxy(ctx, PutProxyInput{
		ID: "px-test1", Scheme: "socks5", Host: "127.0.0.1", Port: 1080, Enabled: true,
		Username: &user, Password: &pass, UpdatedBy: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !record.HasAuth || record.SecretRef == "" {
		t.Fatalf("expected secret ref, got %#v", record)
	}
	var leaked int
	if err := controlStore.db.QueryRow(`SELECT COUNT(*) FROM control_proxies
		WHERE CAST(id AS TEXT) || CAST(host AS TEXT) LIKE '%s3cret%' OR CAST(bound_vm_ids_json AS TEXT) LIKE '%alice%'`).Scan(&leaked); err != nil {
		t.Fatal(err)
	}
	if leaked != 0 {
		t.Fatal("plaintext leaked into control_proxies")
	}
	uri, err := controlStore.RevealProxyURI(ctx, "px-test1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(uri, "alice") || !strings.Contains(uri, "s3cret") {
		t.Fatalf("reveal uri = %q", uri)
	}
	listed, err := controlStore.ListProxies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(listed)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "s3cret") || strings.Contains(string(raw), "alice") {
		t.Fatalf("list leaked secret: %s", raw)
	}
}
