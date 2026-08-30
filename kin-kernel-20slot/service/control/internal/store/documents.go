package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	"kin.local/kin-control/internal/model"
)

var documentDomainPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]{0,127}$`)

type Document struct {
	Domain        string          `json:"domain"`
	SchemaVersion int             `json:"schema_version"`
	Revision      uint64          `json:"revision"`
	ConfigHash    string          `json:"config_hash"`
	Data          json.RawMessage `json:"data"`
	UpdatedAt     time.Time       `json:"updated_at"`
	UpdatedBy     string          `json:"updated_by"`
	Degraded      bool            `json:"degraded"`
}

type PutDocumentInput struct {
	Domain           string
	SchemaVersion    int
	ExpectedRevision uint64
	Data             json.RawMessage
	UpdatedBy        string
	ChangeKind       string
}

type documentRow struct {
	domain        string
	schemaVersion int
	revision      uint64
	body          string
	hash          string
	updatedAt     string
	updatedBy     string
}

func (s *SQLite) PutDocument(ctx context.Context, input PutDocumentInput) (Document, error) {
	return s.putDocumentWithHook(ctx, input, nil)
}

type documentCommitHook func(tx *sql.Tx, revision uint64, now time.Time) error

func (s *SQLite) putDocumentWithHook(ctx context.Context, input PutDocumentInput, hook documentCommitHook) (Document, error) {
	if !documentDomainPattern.MatchString(input.Domain) {
		return Document{}, fmt.Errorf("%w: invalid document domain", ErrInvalid)
	}
	if input.SchemaVersion < 1 {
		return Document{}, fmt.Errorf("%w: schema_version must be positive", ErrInvalid)
	}
	if strings.TrimSpace(input.UpdatedBy) == "" || len(input.UpdatedBy) > 128 {
		return Document{}, fmt.Errorf("%w: updated_by is required and must be at most 128 characters", ErrInvalid)
	}
	if input.ChangeKind == "" {
		input.ChangeKind = "put"
	}
	if len(input.ChangeKind) > 128 || len(input.Data) > 1<<20 {
		return Document{}, fmt.Errorf("%w: change_kind or document data exceeds its size limit", ErrInvalid)
	}
	canonical, hash, err := canonicalDocument(input.Data)
	if err != nil {
		return Document{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Document{}, fmt.Errorf("begin document update: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	current, err := documentRevision(ctx, tx, input.Domain)
	if err != nil {
		return Document{}, err
	}
	if current != input.ExpectedRevision {
		return Document{}, fmt.Errorf("%w: expected %d, current %d", ErrConflict, input.ExpectedRevision, current)
	}
	parent, revision, err := nextRevision(ctx, tx)
	if err != nil {
		return Document{}, err
	}
	now := time.Now().UTC()
	nowText := now.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO control_document_versions
		(domain, revision, schema_version, body_json, body_sha256, updated_at, updated_by)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		input.Domain, revision, input.SchemaVersion, string(canonical), hash, nowText, input.UpdatedBy,
	); err != nil {
		return Document{}, fmt.Errorf("insert document version: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO control_documents
		(domain, schema_version, revision, body_json, body_sha256, updated_at, updated_by)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(domain) DO UPDATE SET
			schema_version=excluded.schema_version,
			revision=excluded.revision,
			body_json=excluded.body_json,
			body_sha256=excluded.body_sha256,
			updated_at=excluded.updated_at,
			updated_by=excluded.updated_by`,
		input.Domain, input.SchemaVersion, revision, string(canonical), hash, nowText, input.UpdatedBy,
	); err != nil {
		return Document{}, fmt.Errorf("upsert current document: %w", err)
	}
	summary, _ := json.Marshal(map[string]any{"body_sha256": hash, "schema_version": input.SchemaVersion})
	if _, err := tx.ExecContext(ctx, `INSERT INTO control_revisions
		(revision, parent_revision, domain, change_kind, summary_json, created_at, created_by)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		revision, parent, input.Domain, input.ChangeKind, string(summary), nowText, input.UpdatedBy,
	); err != nil {
		return Document{}, fmt.Errorf("insert revision: %w", err)
	}
	if hook != nil {
		if err := hook(tx, revision, now); err != nil {
			return Document{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Document{}, fmt.Errorf("commit document update: %w", err)
	}
	return Document{
		Domain:        input.Domain,
		SchemaVersion: input.SchemaVersion,
		Revision:      revision,
		ConfigHash:    hash,
		Data:          canonical,
		UpdatedAt:     now,
		UpdatedBy:     input.UpdatedBy,
	}, nil
}

func (s *SQLite) GetDocument(ctx context.Context, domain string) (Document, error) {
	row, err := scanDocument(s.db.QueryRowContext(ctx, `SELECT domain, schema_version, revision,
		body_json, body_sha256, updated_at, updated_by FROM control_documents WHERE domain = ?`, domain))
	if errors.Is(err, sql.ErrNoRows) {
		return Document{}, ErrNotFound
	}
	if err != nil {
		return Document{}, fmt.Errorf("read current document: %w", err)
	}
	document, err := row.document(false)
	if err == nil {
		return document, nil
	}
	fallback, fallbackErr := s.lastKnownGood(ctx, domain)
	if fallbackErr != nil {
		return Document{}, fmt.Errorf("%w: domain %s has no valid version", ErrCorrupt, domain)
	}
	s.degraded.Store(true)
	fallback.Degraded = true
	return fallback, nil
}

func (s *SQLite) ListDocuments(ctx context.Context, prefix string) ([]Document, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT domain FROM control_documents
		WHERE domain LIKE ? ORDER BY domain`, prefix+"%")
	if err != nil {
		return nil, fmt.Errorf("list documents: %w", err)
	}
	defer rows.Close()
	var domains []string
	for rows.Next() {
		var domain string
		if err := rows.Scan(&domain); err != nil {
			return nil, fmt.Errorf("scan document domain: %w", err)
		}
		domains = append(domains, domain)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate documents: %w", err)
	}
	result := make([]Document, 0, len(domains))
	for _, domain := range domains {
		document, err := s.GetDocument(ctx, domain)
		if err != nil {
			return nil, err
		}
		result = append(result, document)
	}
	return result, nil
}

func (s *SQLite) lastKnownGood(ctx context.Context, domain string) (Document, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT domain, schema_version, revision, body_json,
		body_sha256, updated_at, updated_by FROM control_document_versions
		WHERE domain = ? ORDER BY revision DESC`, domain)
	if err != nil {
		return Document{}, err
	}
	defer rows.Close()
	for rows.Next() {
		row, scanErr := scanDocument(rows)
		if scanErr != nil {
			return Document{}, scanErr
		}
		document, documentErr := row.document(true)
		if documentErr == nil {
			return document, nil
		}
	}
	if err := rows.Err(); err != nil {
		return Document{}, err
	}
	return Document{}, ErrCorrupt
}

func (s *SQLite) validateCurrentDocuments(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "SELECT domain FROM control_documents ORDER BY domain")
	if err != nil {
		return fmt.Errorf("list current documents: %w", err)
	}
	var domains []string
	for rows.Next() {
		var domain string
		if err := rows.Scan(&domain); err != nil {
			_ = rows.Close()
			return err
		}
		domains = append(domains, domain)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, domain := range domains {
		document, err := s.GetDocument(ctx, domain)
		if err != nil {
			return err
		}
		if document.Degraded {
			s.degraded.Store(true)
		}
	}
	return nil
}

func canonicalDocument(raw json.RawMessage) (json.RawMessage, string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, "", fmt.Errorf("%w: invalid document JSON: %v", ErrInvalid, err)
	}
	if value == nil {
		return nil, "", fmt.Errorf("%w: document data must not be null", ErrInvalid)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, "", err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, "", fmt.Errorf("canonicalize document: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return canonical, hex.EncodeToString(sum[:]), nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("%w: document contains multiple JSON values", ErrInvalid)
	}
	return fmt.Errorf("%w: invalid document suffix: %v", ErrInvalid, err)
}

func documentRevision(ctx context.Context, tx *sql.Tx, domain string) (uint64, error) {
	var revision uint64
	err := tx.QueryRowContext(ctx, "SELECT revision FROM control_documents WHERE domain = ?", domain).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read document revision: %w", err)
	}
	return revision, nil
}

func scanDocument(scanner interface{ Scan(...any) error }) (documentRow, error) {
	var row documentRow
	err := scanner.Scan(
		&row.domain,
		&row.schemaVersion,
		&row.revision,
		&row.body,
		&row.hash,
		&row.updatedAt,
		&row.updatedBy,
	)
	return row, err
}

func (r documentRow) document(degraded bool) (Document, error) {
	canonical, hash, err := canonicalDocument(json.RawMessage(r.body))
	if err != nil || hash != r.hash {
		return Document{}, ErrCorrupt
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, r.updatedAt)
	if err != nil {
		return Document{}, ErrCorrupt
	}
	return Document{
		Domain:        r.domain,
		SchemaVersion: r.schemaVersion,
		Revision:      r.revision,
		ConfigHash:    r.hash,
		Data:          canonical,
		UpdatedAt:     updatedAt,
		UpdatedBy:     r.updatedBy,
		Degraded:      degraded,
	}, nil
}

func (s *SQLite) PutPolicy(policy model.RoutePolicy) (model.RoutePolicy, error) {
	ctx := context.Background()
	domain := "route-policy/" + policy.Name
	expected := uint64(0)
	if current, err := s.GetDocument(ctx, domain); err == nil {
		expected = current.Revision
	} else if !errors.Is(err, ErrNotFound) {
		return model.RoutePolicy{}, err
	}
	raw, err := json.Marshal(policy)
	if err != nil {
		return model.RoutePolicy{}, err
	}
	_, err = s.PutDocument(ctx, PutDocumentInput{
		Domain:           domain,
		SchemaVersion:    1,
		ExpectedRevision: expected,
		Data:             raw,
		UpdatedBy:        "legacy-route-policy-api",
		ChangeKind:       "route_policy_put",
	})
	return policy, err
}

func (s *SQLite) GetPolicy(name string) (model.RoutePolicy, error) {
	document, err := s.GetDocument(context.Background(), "route-policy/"+name)
	if err != nil {
		return model.RoutePolicy{}, err
	}
	var policy model.RoutePolicy
	if err := json.Unmarshal(document.Data, &policy); err != nil {
		return model.RoutePolicy{}, ErrCorrupt
	}
	return policy, nil
}

func (s *SQLite) ListPolicies() ([]model.RoutePolicy, error) {
	documents, err := s.ListDocuments(context.Background(), "route-policy/")
	if err != nil {
		return nil, err
	}
	policies := make([]model.RoutePolicy, 0, len(documents))
	for _, document := range documents {
		var policy model.RoutePolicy
		if err := json.Unmarshal(document.Data, &policy); err != nil {
			return nil, ErrCorrupt
		}
		policies = append(policies, policy)
	}
	sort.Slice(policies, func(i, j int) bool { return policies[i].Name < policies[j].Name })
	return policies, nil
}

func (s *SQLite) SetRuntimeProfile(profile model.RuntimeProfile) error {
	ctx := context.Background()
	expected := uint64(0)
	if current, err := s.GetDocument(ctx, "runtime-profile"); err == nil {
		expected = current.Revision
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	raw, err := json.Marshal(profile)
	if err != nil {
		return err
	}
	_, err = s.PutDocument(ctx, PutDocumentInput{
		Domain:           "runtime-profile",
		SchemaVersion:    1,
		ExpectedRevision: expected,
		Data:             raw,
		UpdatedBy:        "legacy-runtime-profile-api",
		ChangeKind:       "runtime_profile_put",
	})
	return err
}

func (s *SQLite) GetRuntimeProfile() (model.RuntimeProfile, bool, error) {
	document, err := s.GetDocument(context.Background(), "runtime-profile")
	if errors.Is(err, ErrNotFound) {
		return model.RuntimeProfile{}, false, nil
	}
	if err != nil {
		return model.RuntimeProfile{}, false, err
	}
	var profile model.RuntimeProfile
	if err := json.Unmarshal(document.Data, &profile); err != nil {
		return model.RuntimeProfile{}, false, ErrCorrupt
	}
	return profile, true, nil
}
