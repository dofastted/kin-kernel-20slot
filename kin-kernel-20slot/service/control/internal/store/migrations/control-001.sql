CREATE TABLE IF NOT EXISTS control_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  revision INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO control_meta (id, revision) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS control_documents (
  domain TEXT PRIMARY KEY,
  schema_version INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  body_json TEXT NOT NULL,
  body_sha256 TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  updated_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS control_document_versions (
  domain TEXT NOT NULL,
  revision INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,
  body_json TEXT NOT NULL,
  body_sha256 TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  updated_by TEXT NOT NULL,
  PRIMARY KEY (domain, revision)
);
CREATE INDEX IF NOT EXISTS idx_control_document_versions_latest
  ON control_document_versions (domain, revision DESC);

CREATE TABLE IF NOT EXISTS control_revisions (
  revision INTEGER PRIMARY KEY,
  parent_revision INTEGER NOT NULL,
  domain TEXT NOT NULL,
  change_kind TEXT NOT NULL,
  summary_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  created_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS control_domain_owners (
  domain TEXT PRIMARY KEY,
  owner TEXT NOT NULL CHECK (owner IN ('node', 'go')),
  state TEXT NOT NULL CHECK (state IN ('node', 'shadow_import', 'shadow_match', 'frozen', 'go', 'verified', 'rollback_export', 'rejected')),
  source_hash TEXT,
  revision INTEGER NOT NULL,
  updated_at TEXT NOT NULL,
  updated_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS control_slot_state (
  slot_id TEXT PRIMARY KEY,
  desired_revision INTEGER NOT NULL DEFAULT 0,
  effective_revision INTEGER NOT NULL DEFAULT 0,
  apply_state TEXT NOT NULL CHECK (apply_state IN ('effective', 'pending_restart', 'rejected')),
  desired_json TEXT NOT NULL DEFAULT '{}',
  observed_json TEXT NOT NULL DEFAULT '{}',
  last_error TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS control_operations (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  target TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'claimed', 'running', 'succeeded', 'failed', 'cancelled')),
  payload_json TEXT NOT NULL,
  result_json TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL,
  lease_owner TEXT,
  lease_expires_at TEXT,
  idempotency_key TEXT NOT NULL UNIQUE,
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_control_operations_claim
  ON control_operations (status, lease_expires_at, created_at);

CREATE TABLE IF NOT EXISTS control_secrets (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  ciphertext TEXT NOT NULL,
  key_version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE (kind, owner_id)
);

CREATE TABLE IF NOT EXISTS control_snapshots (
  revision INTEGER PRIMARY KEY,
  payload_json TEXT NOT NULL,
  payload_sha256 TEXT NOT NULL,
  key_id TEXT,
  signature TEXT,
  issued_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('draft', 'published', 'rejected'))
);
