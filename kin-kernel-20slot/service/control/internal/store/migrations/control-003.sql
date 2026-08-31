CREATE TABLE IF NOT EXISTS control_proxies (
    id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    scheme TEXT NOT NULL CHECK (scheme IN ('socks5')),
    host TEXT NOT NULL,
    port INTEGER NOT NULL CHECK (port >= 1 AND port <= 65535),
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    status TEXT NOT NULL CHECK (status IN ('unknown', 'ok', 'fail', 'dead')),
    bound_vm_ids_json TEXT NOT NULL,
    consecutive_failures INTEGER NOT NULL CHECK (consecutive_failures >= 0),
    latency_ms INTEGER,
    last_probe_at TEXT,
    last_error TEXT,
    secret_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (secret_id) REFERENCES control_secrets(id)
);

CREATE INDEX IF NOT EXISTS idx_control_proxies_status
    ON control_proxies(status, enabled, updated_at);
