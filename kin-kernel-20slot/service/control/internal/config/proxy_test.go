package config

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"kin.local/kin-control/internal/store"
)

func TestProxyPoolImportAndBindLimit(t *testing.T) {
	ctx := context.Background()
	controlStore, err := store.OpenSQLite(filepath.Join(t.TempDir(), "kin.db"), "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	service := NewService(controlStore)
	setGoOwner(t, ctx, controlStore, proxyDomain)

	user := "alice"
	pass := "s3cret"
	added, skipped, err := service.ImportProxies(ctx, []store.PutProxyInput{{
		Host: "10.0.0.1", Port: 1080, Username: &user, Password: &pass,
	}}, "test", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 1 || len(skipped) != 0 {
		t.Fatalf("import added=%d skipped=%d", len(added), len(skipped))
	}
	if added[0].HasAuth == false || added[0].Host != "10.0.0.1" {
		t.Fatalf("imported proxy = %#v", added[0])
	}
	snapshot, err := service.ListProxies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(snapshot)
	if stringContains(body, "s3cret") || stringContains(body, "alice") {
		t.Fatalf("snapshot leaked secret: %s", body)
	}

	_, err = service.PutProxyPool(ctx, PutInput{
		ExpectedRevision: snapshot.Config.Revision,
		Data:             json.RawMessage(`{"bind_limit":1}`),
		UpdatedBy:        "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := service.GetProxyRecord(ctx, added[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = service.BindProxy(ctx, added[0].ID, "vm-1", current.Revision, "test")
	if err != nil {
		t.Fatal(err)
	}
	current, err = service.GetProxyRecord(ctx, added[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = service.BindProxy(ctx, added[0].ID, "vm-2", current.Revision, "test")
	if !errors.Is(err, store.ErrInvalid) {
		t.Fatalf("second bind error = %v", err)
	}
}

func TestRevealFailsClosedWithoutSecret(t *testing.T) {
	ctx := context.Background()
	controlStore, err := store.OpenSQLite(filepath.Join(t.TempDir(), "kin.db"), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	service := NewService(controlStore)
	setGoOwner(t, ctx, controlStore, proxyDomain)
	if _, err := service.RevealProxy(ctx, "px-missing"); !errors.Is(err, store.ErrSecretUnavailable) {
		t.Fatalf("reveal error = %v", err)
	}
}

func stringContains(raw []byte, needle string) bool {
	return len(raw) > 0 && len(needle) > 0 && json.Valid(raw) && (string(raw) != "" && containsFold(string(raw), needle))
}

func containsFold(haystack, needle string) bool {
	return len(needle) > 0 && (haystack == needle || len(haystack) >= len(needle) && (indexOf(haystack, needle) >= 0))
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
