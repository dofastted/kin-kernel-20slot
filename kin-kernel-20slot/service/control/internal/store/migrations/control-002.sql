CREATE TABLE IF NOT EXISTS control_apply_state (
    domain TEXT PRIMARY KEY,
    desired_revision INTEGER NOT NULL CHECK (desired_revision >= 0),
    effective_revision INTEGER NOT NULL CHECK (effective_revision >= 0),
    apply_state TEXT NOT NULL CHECK (apply_state IN ('effective', 'pending_restart', 'rejected')),
    runtime_hash TEXT NOT NULL,
    kernel_hash TEXT NOT NULL,
    last_error TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_control_apply_state_state
    ON control_apply_state(apply_state, updated_at);
