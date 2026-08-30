package reconcile

import (
	"context"
	"log/slog"
	"time"

	"kin.local/kin-control/internal/store"
)

type Result struct {
	RanAt          time.Time `json:"ran_at"`
	StaleKernelIDs []string  `json:"stale_kernel_ids"`
}

type Reconciler struct {
	store            *store.Memory
	heartbeatTimeout time.Duration
	logger           *slog.Logger
}

func New(memoryStore *store.Memory, heartbeatTimeout time.Duration, logger *slog.Logger) *Reconciler {
	return &Reconciler{
		store:            memoryStore,
		heartbeatTimeout: heartbeatTimeout,
		logger:           logger,
	}
}

func (r *Reconciler) Reconcile(now time.Time) Result {
	stale := r.store.MarkStale(now, r.heartbeatTimeout)
	if len(stale) > 0 {
		r.logger.Warn("kernels marked unhealthy", "kernel_ids", stale)
	}
	return Result{RanAt: now.UTC(), StaleKernelIDs: stale}
}

func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.Reconcile(now)
		}
	}
}
