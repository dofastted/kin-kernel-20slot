package store

import (
	"testing"
	"time"

	"kin.local/kin-control/internal/model"
)

func TestMarkStale(t *testing.T) {
	t.Parallel()
	memory := NewMemory()
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	memory.UpsertKernel(model.KernelRegistration{
		ID:       "kernel-a",
		Address:  "http://kernel-a:8080",
		Capacity: 20,
		Provider: "mock",
	}, now)

	changed := memory.MarkStale(now.Add(30*time.Second), 20*time.Second)
	if len(changed) != 1 || changed[0] != "kernel-a" {
		t.Fatalf("unexpected stale kernels: %#v", changed)
	}
	kernels := memory.ListKernels()
	if kernels[0].Status != model.KernelUnhealthy {
		t.Fatalf("expected unhealthy, got %q", kernels[0].Status)
	}
}

