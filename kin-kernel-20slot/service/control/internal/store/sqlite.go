package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

var (
	ErrConflict          = errors.New("revision conflict")
	ErrInvalid           = errors.New("invalid control state")
	ErrCorrupt           = errors.New("stored control state is corrupt")
	ErrSecretUnavailable = errors.New("KIN_DB_SECRET is required")
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type SQLite struct {
	db        *sql.DB
	observed  *Memory
	secretKey []byte
	degraded  atomic.Bool
}

type Health struct {
	Status        string `json:"status"`
	Database      string `json:"database"`
	Revision      uint64 `json:"revision"`
	SecretReady   bool   `json:"secret_ready"`
	UsingDegraded bool   `json:"using_degraded"`
}

func OpenSQLite(path, secret string) (*SQLite, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("sqlite path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve sqlite path: %w", err)
	}
	dsn := sqliteDSN(absolute)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// One Go writer connection keeps connection-local PRAGMAs deterministic.
	// Node may still use its own WAL connection for tables it owns.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if err := applyControlMigrations(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	store := &SQLite{db: db, observed: NewMemory()}
	if secret != "" {
		store.secretKey = deriveSecretKey(secret)
	}
	if err := store.validateCurrentDocuments(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func deriveSecretKey(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

func sqliteDSN(path string) string {
	u := &url.URL{Scheme: "file", Path: filepath.ToSlash(path)}
	query := url.Values{}
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "synchronous(NORMAL)")
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(1)")
	u.RawQuery = query.Encode()
	return u.String()
}

func (s *SQLite) Close() error {
	return s.db.Close()
}

func (s *SQLite) Observed() *Memory {
	return s.observed
}

func (s *SQLite) SecretReady() bool {
	return len(s.secretKey) == 32
}

func (s *SQLite) Health(ctx context.Context) Health {
	health := Health{
		Status:        "ok",
		Database:      "ok",
		SecretReady:   s.SecretReady(),
		UsingDegraded: s.degraded.Load(),
	}
	if err := s.db.PingContext(ctx); err != nil {
		health.Status = "degraded"
		health.Database = "unavailable"
		return health
	}
	revision, err := s.CurrentRevision(ctx)
	if err != nil {
		health.Status = "degraded"
		health.Database = "revision_unavailable"
		return health
	}
	health.Revision = revision
	if health.UsingDegraded {
		health.Status = "degraded"
	}
	return health
}

func (s *SQLite) CurrentRevision(ctx context.Context) (uint64, error) {
	var revision uint64
	if err := s.db.QueryRowContext(ctx, "SELECT revision FROM control_meta WHERE id = 1").Scan(&revision); err != nil {
		return 0, fmt.Errorf("read control revision: %w", err)
	}
	return revision, nil
}

func nextRevision(ctx context.Context, tx *sql.Tx) (uint64, uint64, error) {
	var parent uint64
	if err := tx.QueryRowContext(ctx, "SELECT revision FROM control_meta WHERE id = 1").Scan(&parent); err != nil {
		return 0, 0, fmt.Errorf("read parent revision: %w", err)
	}
	next := parent + 1
	result, err := tx.ExecContext(ctx, "UPDATE control_meta SET revision = ? WHERE id = 1 AND revision = ?", next, parent)
	if err != nil {
		return 0, 0, fmt.Errorf("advance revision: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("read revision update count: %w", err)
	}
	if changed != 1 {
		return 0, 0, ErrConflict
	}
	return parent, next, nil
}

func applyControlMigrations(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY,
		name TEXT,
		checksum TEXT,
		applied_at TEXT
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		raw, readErr := migrationFiles.ReadFile("migrations/" + entry.Name())
		if readErr != nil {
			return fmt.Errorf("read migration %s: %w", entry.Name(), readErr)
		}
		version := strings.TrimSuffix(entry.Name(), ".sql")
		sum := sha256.Sum256(raw)
		checksum := hex.EncodeToString(sum[:])
		var applied string
		err = db.QueryRow("SELECT checksum FROM schema_migrations WHERE version = ?", version).Scan(&applied)
		switch {
		case err == nil:
			if applied != checksum {
				return fmt.Errorf("migration checksum mismatch for %s", entry.Name())
			}
			continue
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("read migration %s state: %w", entry.Name(), err)
		}
		if err := applyMigration(db, version, entry.Name(), checksum, string(raw)); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(db *sql.DB, version, name, checksum, statement string) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", name, err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(statement); err != nil {
		return fmt.Errorf("apply migration %s: %w", name, err)
	}
	if _, err := tx.Exec(
		"INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)",
		version,
		name,
		checksum,
		time.Now().UTC().Format(time.RFC3339Nano),
	); err != nil {
		return fmt.Errorf("record migration %s: %w", name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", name, err)
	}
	return nil
}
