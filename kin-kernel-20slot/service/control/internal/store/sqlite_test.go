package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"kin.local/kin-control/internal/model"
)

func openTestSQLite(t *testing.T, secret string) (*SQLite, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kin.db")
	controlStore, err := OpenSQLite(path, secret)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	return controlStore, path
}

func TestControlMigrationCoexistsWithNodeSchemaMigrations(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "kin.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE schema_migrations (
		version TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		checksum TEXT NOT NULL,
		applied_at TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO schema_migrations(version, name, checksum, applied_at)
		VALUES ('001', 'init', 'node-checksum', '2026-01-01T00:00:00.000Z')`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	controlStore, err := OpenSQLite(path, "secret")
	if err != nil {
		t.Fatal(err)
	}
	defer controlStore.Close()
	var count int
	if err := controlStore.db.QueryRow(`SELECT COUNT(*) FROM schema_migrations
		WHERE version IN ('001', 'control-001')`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("preserved migration count = %d", count)
	}
}

func TestControlMigrationRejectsChecksumMismatch(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "kin.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE schema_migrations (
		version TEXT PRIMARY KEY,
		name TEXT,
		checksum TEXT,
		applied_at TEXT
	)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO schema_migrations(version, name, checksum, applied_at)
		VALUES ('control-001', 'control-001.sql', 'wrong', '2026-01-01T00:00:00.000Z')`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLite(path, "secret"); err == nil {
		t.Fatal("checksum mismatch must reject startup")
	}
}

func TestDocumentPersistsAndRejectsConcurrentRevision(t *testing.T) {
	t.Parallel()
	controlStore, path := openTestSQLite(t, "secret")
	ctx := context.Background()
	first, err := controlStore.PutDocument(ctx, PutDocumentInput{
		Domain:        "routing",
		SchemaVersion: 1,
		Data:          json.RawMessage(`{"model":"claude"}`),
		UpdatedBy:     "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, value := range []string{"a", "b"} {
		value := value
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, updateErr := controlStore.PutDocument(ctx, PutDocumentInput{
				Domain:           "routing",
				SchemaVersion:    1,
				ExpectedRevision: first.Revision,
				Data:             json.RawMessage(`{"value":"` + value + `"}`),
				UpdatedBy:        "test",
			})
			results <- updateErr
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var succeeded int
	var conflicted int
	for result := range results {
		switch {
		case result == nil:
			succeeded++
		case errors.Is(result, ErrConflict):
			conflicted++
		default:
			t.Fatalf("unexpected update error: %v", result)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("succeeded=%d conflicted=%d", succeeded, conflicted)
	}

	if err := controlStore.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLite(path, "secret")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	document, err := reopened.GetDocument(ctx, "routing")
	if err != nil {
		t.Fatal(err)
	}
	if document.Revision <= first.Revision {
		t.Fatalf("revision did not persist: %+v", document)
	}
}

func TestDocumentFallsBackToLastKnownGood(t *testing.T) {
	t.Parallel()
	controlStore, path := openTestSQLite(t, "secret")
	ctx := context.Background()
	document, err := controlStore.PutDocument(ctx, PutDocumentInput{
		Domain:        "notify",
		SchemaVersion: 1,
		Data:          json.RawMessage(`{"enabled":true}`),
		UpdatedBy:     "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlStore.db.Exec(`UPDATE control_documents SET body_json = '{broken'
		WHERE domain = 'notify'`); err != nil {
		t.Fatal(err)
	}
	fallback, err := controlStore.GetDocument(ctx, "notify")
	if err != nil {
		t.Fatal(err)
	}
	if !fallback.Degraded || fallback.Revision != document.Revision {
		t.Fatalf("unexpected fallback: %+v", fallback)
	}
	if err := controlStore.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLite(path, "secret")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !reopened.Health(ctx).UsingDegraded {
		t.Fatal("reopened store must report degraded LKG use")
	}
	if _, err := reopened.db.Exec(`UPDATE control_document_versions SET body_json = '{broken'
		WHERE domain = 'notify'`); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.GetDocument(ctx, "notify"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("all corrupt versions error = %v", err)
	}
}

func TestPolicyProfileOwnerAndSlotPersist(t *testing.T) {
	t.Parallel()
	controlStore, path := openTestSQLite(t, "secret")
	ctx := context.Background()
	policy := model.RoutePolicy{Name: "default", Tenant: "tenant-a", ModelPattern: "claude-*", Provider: "anthropic", SlotGroup: "primary"}
	if _, err := controlStore.PutPolicy(policy); err != nil {
		t.Fatal(err)
	}
	profile := model.RuntimeProfile{
		ExecutionMode:   "native_messages",
		SystemLayout:    "zero",
		Timezone:        "UTC",
		SlotCount:       20,
		MaxBodyBytes:    1 << 20,
		MaxOutputTokens: 8192,
	}
	if err := controlStore.SetRuntimeProfile(profile); err != nil {
		t.Fatal(err)
	}
	owner := DomainOwner{}
	for _, next := range []PutDomainOwnerInput{
		{Domain: "routing", Owner: "node", State: "shadow_import", UpdatedBy: "test"},
		{Domain: "routing", Owner: "node", State: "shadow_match", UpdatedBy: "test"},
		{Domain: "routing", Owner: "node", State: "frozen", UpdatedBy: "test"},
		{Domain: "routing", Owner: "go", State: "go", UpdatedBy: "test"},
	} {
		next.ExpectedRevision = owner.Revision
		var err error
		owner, err = controlStore.PutDomainOwner(ctx, next)
		if err != nil {
			t.Fatal(err)
		}
	}
	if owner.Revision == 0 {
		t.Fatal("owner revision not assigned")
	}
	slot, err := controlStore.PutSlotDesired(ctx, PutSlotDesiredInput{
		SlotID: "slot-a", ApplyState: "pending_restart", Desired: json.RawMessage(`{"model":"claude"}`), UpdatedBy: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	acknowledged, err := controlStore.AcknowledgeSlot(ctx, "slot-a", slot.DesiredRevision, json.RawMessage(`{"ready":true}`), "effective", "")
	if err != nil {
		t.Fatal(err)
	}
	if acknowledged.EffectiveRevision != slot.DesiredRevision || acknowledged.ApplyState != "effective" {
		t.Fatalf("unexpected acknowledged slot: %+v", acknowledged)
	}
	operation, err := controlStore.EnqueueOperation(ctx, EnqueueOperationInput{
		Kind: "slot.restart", Target: "slot-a", Payload: json.RawMessage(`{"reason":"config"}`), MaxAttempts: 2, IdempotencyKey: "persist-slot-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	controlStore.Observed().UpsertKernel(model.KernelRegistration{
		ID: "kernel-a", Address: "http://kernel-a", Capacity: 20, Provider: "mock", Revision: 1,
	}, time.Now())
	if err := controlStore.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLite(path, "secret")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.GetPolicy("default"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := reopened.GetRuntimeProfile(); err != nil || !ok {
		t.Fatalf("profile ok=%v err=%v", ok, err)
	}
	persistedOwner, err := reopened.GetDomainOwner(ctx, "routing")
	if err != nil || persistedOwner.Revision != owner.Revision {
		t.Fatalf("owner=%+v err=%v", persistedOwner, err)
	}
	persistedSlot, err := reopened.GetSlotState(ctx, "slot-a")
	if err != nil || persistedSlot.EffectiveRevision != slot.DesiredRevision {
		t.Fatalf("slot=%+v err=%v", persistedSlot, err)
	}
	persistedOperation, err := reopened.GetOperation(ctx, operation.ID)
	if err != nil || persistedOperation.ID != operation.ID {
		t.Fatalf("operation=%+v err=%v", persistedOperation, err)
	}
	if kernels := reopened.Observed().ListKernels(); len(kernels) != 0 {
		t.Fatalf("observed kernels persisted unexpectedly: %+v", kernels)
	}
}

func TestOperationLeaseReclaimAndAttemptLimit(t *testing.T) {
	t.Parallel()
	controlStore, _ := openTestSQLite(t, "secret")
	ctx := context.Background()
	first, err := controlStore.EnqueueOperation(ctx, EnqueueOperationInput{
		Kind: "slot.restart", Target: "slot-a", Payload: json.RawMessage(`{"reason":"config"}`), MaxAttempts: 2, IdempotencyKey: "slot-a-r1",
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := controlStore.EnqueueOperation(ctx, EnqueueOperationInput{
		Kind: "slot.restart", Target: "slot-a", Payload: json.RawMessage(`{"different":true}`), MaxAttempts: 2, IdempotencyKey: "slot-a-r1",
	})
	if err != nil || duplicate.ID != first.ID {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
	claimed, err := controlStore.ClaimOperation(ctx, "worker-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Attempts != 1 {
		t.Fatalf("first attempts = %d", claimed.Attempts)
	}
	time.Sleep(5 * time.Millisecond)
	reclaimed, err := controlStore.ClaimOperation(ctx, "worker-b", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.ID != first.ID || reclaimed.Attempts != 2 || reclaimed.LeaseOwner != "worker-b" {
		t.Fatalf("unexpected reclaim: %+v", reclaimed)
	}
	if _, err := controlStore.CompleteOperation(ctx, first.ID, "worker-a", "succeeded", json.RawMessage(`{"ok":true}`), ""); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong lease owner completion error = %v", err)
	}
	completed, err := controlStore.CompleteOperation(ctx, first.ID, "worker-b", "succeeded", json.RawMessage(`{"ok":true}`), "")
	if err != nil || completed.Status != "succeeded" {
		t.Fatalf("completed=%+v err=%v", completed, err)
	}

	limited, err := controlStore.EnqueueOperation(ctx, EnqueueOperationInput{
		Kind: "slot.restart", Target: "slot-b", Payload: json.RawMessage(`{"reason":"config"}`), MaxAttempts: 1, IdempotencyKey: "slot-b-r1",
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err = controlStore.ClaimOperation(ctx, "worker-a", time.Millisecond)
	if err != nil || claimed.ID != limited.ID {
		t.Fatalf("limited claim=%+v err=%v", claimed, err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := controlStore.ClaimOperation(ctx, "worker-c", time.Second); !errors.Is(err, ErrNotFound) {
		t.Fatalf("max-attempt claim error = %v", err)
	}
}

func TestSecretCompatibilityAndFailClosed(t *testing.T) {
	t.Parallel()
	withoutSecret, _ := openTestSQLite(t, "")
	if _, err := withoutSecret.PutSecret(context.Background(), "proxy.password", "proxy-a", "plain"); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("missing secret error = %v", err)
	}

	key := deriveSecretKey("node-compatible-secret")
	const nodeCiphertext = "enc:v1:AAECAwQFBgcICQoL:s8NLoGDw4ebTMZfGSsqtPg==:1SMc2b+OPmso5zJP+QekX39ZSA5q"
	plain, err := decryptSecret(key, nodeCiphertext)
	if err != nil {
		t.Fatal(err)
	}
	if plain != "proxy-password-示例" {
		t.Fatalf("node vector plaintext = %q", plain)
	}
	if _, err := decryptSecret(deriveSecretKey("wrong-secret"), nodeCiphertext); err == nil {
		t.Fatal("wrong database secret must fail authentication")
	}
	goCiphertext, err := encryptSecret(key, "proxy-password-示例")
	if err != nil {
		t.Fatal(err)
	}
	plain, err = decryptSecret(key, goCiphertext)
	if err != nil || plain != "proxy-password-示例" {
		t.Fatalf("go roundtrip plain=%q err=%v", plain, err)
	}

	withSecret, _ := openTestSQLite(t, "node-compatible-secret")
	metadata, err := withSecret.PutSecret(context.Background(), "proxy.password", "proxy-a", "plain-password")
	if err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := withSecret.db.QueryRow(`SELECT ciphertext FROM control_secrets WHERE id = ?`, metadata.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored == "plain-password" || len(stored) < len(secretPrefix) || stored[:len(secretPrefix)] != secretPrefix {
		t.Fatalf("secret stored without enc:v1 envelope: %q", stored)
	}
	if _, _, err := withSecret.GetSecret(context.Background(), "proxy.password", "proxy-a"); err != nil {
		t.Fatal(err)
	}
}

func TestDomainOwnerRejectsSkippedTransition(t *testing.T) {
	t.Parallel()
	controlStore, _ := openTestSQLite(t, "secret")
	defer controlStore.Close()
	_, err := controlStore.PutDomainOwner(context.Background(), PutDomainOwnerInput{
		Domain: "routing", Owner: "go", State: "go", UpdatedBy: "test",
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("direct node-to-go transition error = %v", err)
	}
}
