package config

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"kin.local/kin-control/internal/store"
)

func TestRoutingOwnerGateAndApplyLifecycle(t *testing.T) {
	ctx := context.Background()
	controlStore, err := store.OpenSQLite(filepath.Join(t.TempDir(), "kin.db"), "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	service := NewService(controlStore)

	if _, err := service.PutRouting(ctx, PutInput{Data: json.RawMessage(`{}`), UpdatedBy: "test"}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("node-owned routing write error = %v", err)
	}
	setGoOwner(t, ctx, controlStore, routingDomain)

	first, err := service.PutRouting(ctx, PutInput{Data: json.RawMessage(`{"quota":{"warn_ratio":0.7}}`), UpdatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Apply.State != "effective" || first.Apply.EffectiveRevision != first.Revision {
		t.Fatalf("first routing write was not effective: %#v", first.Apply)
	}

	hot, err := service.PutRouting(ctx, PutInput{ExpectedRevision: first.Revision, Data: json.RawMessage(`{"quota":{"warn_ratio":0.65}}`), UpdatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if hot.Apply.State != "effective" {
		t.Fatalf("hot routing update state = %s", hot.Apply.State)
	}
	if _, err := controlStore.ClaimOperation(ctx, "worker", time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("hot routing update enqueued an operation: %v", err)
	}

	pending, err := service.PutRouting(ctx, PutInput{ExpectedRevision: hot.Revision, Data: json.RawMessage(`{"inference":{"engine":"rust"}}`), UpdatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if pending.Apply.State != "pending_restart" || pending.Apply.EffectiveRevision != hot.Revision {
		t.Fatalf("restart routing update state = %#v", pending.Apply)
	}
	operation, err := controlStore.ClaimOperation(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if operation.Kind != "fleet.reload" {
		t.Fatalf("operation kind = %s", operation.Kind)
	}
	if _, err := controlStore.CompleteOperation(ctx, operation.ID, "worker", "succeeded", json.RawMessage(`{"ready":true}`), ""); err != nil {
		t.Fatal(err)
	}
	applied, err := service.GetRouting(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Apply.State != "effective" || applied.Apply.EffectiveRevision != pending.Revision {
		t.Fatalf("completed routing apply = %#v", applied.Apply)
	}
}

func TestStaleSlotOperationCannotAcknowledgeNewRevision(t *testing.T) {
	ctx := context.Background()
	controlStore, err := store.OpenSQLite(filepath.Join(t.TempDir(), "kin.db"), "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	service := NewService(controlStore)
	setGoOwner(t, ctx, controlStore, slotDomain)

	first, err := service.PutSlot(ctx, "slot-1", PutInput{Data: json.RawMessage(`{"tier":"pro"}`), UpdatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	rust, err := service.PutSlot(ctx, "slot-1", PutInput{ExpectedRevision: first.Revision, Data: json.RawMessage(`{"inference_engine":"rust"}`), UpdatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	goRevision, err := service.PutSlot(ctx, "slot-1", PutInput{ExpectedRevision: rust.Revision, Data: json.RawMessage(`{"inference_engine":"go"}`), UpdatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}

	stale, err := controlStore.ClaimOperation(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlStore.CompleteOperation(ctx, stale.ID, "worker", "succeeded", json.RawMessage(`{"engine":"rust"}`), ""); err != nil {
		t.Fatal(err)
	}
	stillPending, err := service.GetSlot(ctx, "slot-1")
	if err != nil {
		t.Fatal(err)
	}
	if stillPending.Apply.State != "pending_restart" || stillPending.Apply.DesiredRevision != goRevision.Revision {
		t.Fatalf("stale operation acknowledged latest slot: %#v", stillPending.Apply)
	}

	latest, err := controlStore.ClaimOperation(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlStore.CompleteOperation(ctx, latest.ID, "worker", "succeeded", json.RawMessage(`{"engine":"go"}`), ""); err != nil {
		t.Fatal(err)
	}
	applied, err := service.GetSlot(ctx, "slot-1")
	if err != nil {
		t.Fatal(err)
	}
	if applied.Apply.State != "effective" || applied.Apply.EffectiveRevision != goRevision.Revision {
		t.Fatalf("latest operation did not acknowledge slot: %#v", applied.Apply)
	}
}

func setGoOwner(t *testing.T, ctx context.Context, controlStore *store.SQLite, domain string) {
	t.Helper()
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
			t.Fatalf("set %s owner: %v", domain, err)
		}
	}
}
func TestSlotBatchReportsPartialFailure(t *testing.T) {
	ctx := context.Background()
	controlStore, err := store.OpenSQLite(filepath.Join(t.TempDir(), "kin.db"), "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	service := NewService(controlStore)
	setGoOwner(t, ctx, controlStore, slotDomain)

	result, err := service.PutSlots(ctx, []string{"invalid slot", "slot-ok"}, PutInput{
		Data: json.RawMessage(`{"tier":"pro"}`), UpdatedBy: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Updated != 1 || len(result.Failed) != 1 || result.Failed[0].SlotID != "invalid slot" {
		t.Fatalf("unexpected batch result: %#v", result)
	}
	if result.Items[0].SlotID != "slot-ok" {
		t.Fatalf("updated slot id = %q", result.Items[0].SlotID)
	}
}
