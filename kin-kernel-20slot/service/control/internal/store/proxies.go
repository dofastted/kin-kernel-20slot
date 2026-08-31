package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const proxyAuthKind = "proxy-auth"

type ProxyRecord struct {
	ID                  string    `json:"id"`
	Revision            uint64    `json:"revision"`
	Scheme              string    `json:"scheme"`
	Host                string    `json:"host"`
	Port                int       `json:"port"`
	Enabled             bool      `json:"enabled"`
	Status              string    `json:"status"`
	BoundVMIDs          []string  `json:"bound_vm_ids"`
	HasAuth             bool      `json:"has_auth"`
	SecretRef           string    `json:"secret_ref,omitempty"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	LatencyMS           *int      `json:"latency_ms,omitempty"`
	LastProbeAt         string    `json:"last_probe_at,omitempty"`
	LastError           string    `json:"last_error,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type PutProxyInput struct {
	ID               string
	ExpectedRevision uint64
	Scheme           string
	Host             string
	Port             int
	Enabled          bool
	Status           string
	BoundVMIDs       []string
	Username         *string
	Password         *string
	ClearAuth        bool
	UpdatedBy        string
	Import           bool
}

func (s *SQLite) ListProxies(ctx context.Context) ([]ProxyRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, revision, scheme, host, port, enabled, status,
		bound_vm_ids_json, consecutive_failures, latency_ms, last_probe_at, last_error, secret_id,
		created_at, updated_at FROM control_proxies ORDER BY created_at, id`)
	if err != nil {
		return nil, fmt.Errorf("list proxies: %w", err)
	}
	defer rows.Close()
	var items []ProxyRecord
	for rows.Next() {
		item, scanErr := scanProxy(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *SQLite) GetProxy(ctx context.Context, id string) (ProxyRecord, error) {
	return scanProxy(s.db.QueryRowContext(ctx, `SELECT id, revision, scheme, host, port, enabled, status,
		bound_vm_ids_json, consecutive_failures, latency_ms, last_probe_at, last_error, secret_id,
		created_at, updated_at FROM control_proxies WHERE id = ?`, id))
}

func (s *SQLite) PutProxy(ctx context.Context, input PutProxyInput) (ProxyRecord, error) {
	if err := validateProxyInput(input); err != nil {
		return ProxyRecord{}, err
	}
	if (input.Username != nil || input.Password != nil || input.ClearAuth) && !s.SecretReady() {
		return ProxyRecord{}, ErrSecretUnavailable
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProxyRecord{}, fmt.Errorf("begin proxy write: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, err := getProxyTx(ctx, tx, input.ID)
	exists := err == nil
	if err != nil && !errors.Is(err, ErrNotFound) {
		return ProxyRecord{}, err
	}
	if exists && current.Revision != input.ExpectedRevision {
		return ProxyRecord{}, ErrConflict
	}
	if !exists && input.ExpectedRevision != 0 {
		return ProxyRecord{}, ErrConflict
	}
	_, next, err := nextRevision(ctx, tx)
	if err != nil {
		return ProxyRecord{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	createdAt := now
	status := input.Status
	failures := 0
	var latency any
	var lastProbe any
	lastError := ""
	if exists {
		createdAt = current.CreatedAt.Format(time.RFC3339Nano)
		if status == "" {
			status = current.Status
		}
		failures = current.ConsecutiveFailures
		if current.LatencyMS != nil {
			latency = *current.LatencyMS
		}
		if current.LastProbeAt != "" {
			lastProbe = current.LastProbeAt
		}
		lastError = current.LastError
	}
	if status == "" {
		status = "unknown"
	}
	secretID, hasAuth, err := applyProxyAuthTx(ctx, tx, s.secretKey, input, current)
	if err != nil {
		return ProxyRecord{}, err
	}
	bound, err := json.Marshal(normalizeVMIDs(input.BoundVMIDs))
	if err != nil {
		return ProxyRecord{}, err
	}
	if exists {
		_, err = tx.ExecContext(ctx, `UPDATE control_proxies SET revision=?, scheme=?, host=?, port=?,
			enabled=?, status=?, bound_vm_ids_json=?, secret_id=?, updated_at=? WHERE id=? AND revision=?`,
			next, input.Scheme, input.Host, input.Port, boolToInt(input.Enabled), status, string(bound),
			nullableString(secretID), now, input.ID, current.Revision)
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO control_proxies
			(id, revision, scheme, host, port, enabled, status, bound_vm_ids_json, consecutive_failures,
			 latency_ms, last_probe_at, last_error, secret_id, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			input.ID, next, input.Scheme, input.Host, input.Port, boolToInt(input.Enabled), status,
			string(bound), failures, latency, lastProbe, nullableString(lastError), nullableString(secretID),
			createdAt, now)
	}
	if err != nil {
		return ProxyRecord{}, fmt.Errorf("store proxy: %w", err)
	}
	if !hasAuth {
		if err := deleteSecretTx(ctx, tx, proxyAuthKind, input.ID); err != nil {
			return ProxyRecord{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return ProxyRecord{}, fmt.Errorf("commit proxy write: %w", err)
	}
	return s.GetProxy(ctx, input.ID)
}

func (s *SQLite) DeleteProxy(ctx context.Context, id string, expectedRevision uint64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin proxy delete: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, err := getProxyTx(ctx, tx, id)
	if err != nil {
		return err
	}
	if current.Revision != expectedRevision {
		return ErrConflict
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM control_proxies WHERE id = ? AND revision = ?`, id, expectedRevision); err != nil {
		return fmt.Errorf("delete proxy: %w", err)
	}
	if err := deleteSecretTx(ctx, tx, proxyAuthKind, id); err != nil {
		return err
	}
	if _, _, err := nextRevision(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLite) RevealProxyURI(ctx context.Context, id string) (string, error) {
	if !s.SecretReady() {
		return "", ErrSecretUnavailable
	}
	record, err := s.GetProxy(ctx, id)
	if err != nil {
		return "", err
	}
	username := ""
	password := ""
	if record.HasAuth {
		_, plain, secretErr := s.GetSecret(ctx, proxyAuthKind, id)
		if secretErr != nil {
			return "", secretErr
		}
		var auth struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.Unmarshal([]byte(plain), &auth); err != nil {
			return "", ErrCorrupt
		}
		username = auth.Username
		password = auth.Password
	}
	return socks5URI(record.Host, record.Port, username, password), nil
}

func (s *SQLite) UpdateProxyProbe(ctx context.Context, id, status string, latencyMS int, lastError string, enabled *bool) (ProxyRecord, error) {
	if status != "ok" && status != "fail" && status != "dead" {
		return ProxyRecord{}, fmt.Errorf("%w: invalid probe status", ErrInvalid)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProxyRecord{}, fmt.Errorf("begin proxy probe: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, err := getProxyTx(ctx, tx, id)
	if err != nil {
		return ProxyRecord{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	failures := 0
	nextEnabled := current.Enabled
	if status != "ok" {
		failures = current.ConsecutiveFailures + 1
	}
	if enabled != nil {
		nextEnabled = *enabled
	}
	_, err = tx.ExecContext(ctx, `UPDATE control_proxies SET status=?, consecutive_failures=?,
		latency_ms=?, last_probe_at=?, last_error=?, enabled=?, updated_at=? WHERE id=?`,
		status, failures, latencyMS, now, nullableString(lastError), boolToInt(nextEnabled), now, id)
	if err != nil {
		return ProxyRecord{}, fmt.Errorf("update proxy probe: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ProxyRecord{}, err
	}
	return s.GetProxy(ctx, id)
}

func applyProxyAuthTx(ctx context.Context, tx *sql.Tx, key []byte, input PutProxyInput, current ProxyRecord) (string, bool, error) {
	if input.ClearAuth {
		return "", false, nil
	}
	if input.Username == nil && input.Password == nil {
		return current.SecretRef, current.HasAuth, nil
	}
	username := ""
	password := ""
	if current.HasAuth {
		var existing struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		plain, err := getSecretPlainTx(ctx, tx, key, proxyAuthKind, input.ID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return "", false, err
		}
		if err == nil {
			if unmarshalErr := json.Unmarshal([]byte(plain), &existing); unmarshalErr != nil {
				return "", false, ErrCorrupt
			}
			username = existing.Username
			password = existing.Password
		}
	}
	if input.Username != nil {
		username = *input.Username
	}
	if input.Password != nil {
		password = *input.Password
	}
	if username == "" && password == "" {
		if err := deleteSecretTx(ctx, tx, proxyAuthKind, input.ID); err != nil {
			return "", false, err
		}
		return "", false, nil
	}
	raw, err := json.Marshal(map[string]string{"username": username, "password": password})
	if err != nil {
		return "", false, err
	}
	metadata, err := putSecretTx(ctx, tx, key, proxyAuthKind, input.ID, string(raw))
	if err != nil {
		return "", false, err
	}
	return metadata.ID, true, nil
}

func getSecretPlainTx(ctx context.Context, tx *sql.Tx, key []byte, kind, ownerID string) (string, error) {
	if len(key) != 32 {
		return "", ErrSecretUnavailable
	}
	var ciphertext string
	err := tx.QueryRowContext(ctx, `SELECT ciphertext FROM control_secrets WHERE kind=? AND owner_id=?`, kind, ownerID).Scan(&ciphertext)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return decryptSecret(key, ciphertext)
}

func getProxyTx(ctx context.Context, tx *sql.Tx, id string) (ProxyRecord, error) {
	return scanProxy(tx.QueryRowContext(ctx, `SELECT id, revision, scheme, host, port, enabled, status,
		bound_vm_ids_json, consecutive_failures, latency_ms, last_probe_at, last_error, secret_id,
		created_at, updated_at FROM control_proxies WHERE id = ?`, id))
}

type proxyScanner interface {
	Scan(dest ...any) error
}

func scanProxy(row proxyScanner) (ProxyRecord, error) {
	var item ProxyRecord
	var enabled int
	var boundJSON string
	var latency sql.NullInt64
	var lastProbe sql.NullString
	var lastError sql.NullString
	var secretID sql.NullString
	var createdAt string
	var updatedAt string
	err := row.Scan(&item.ID, &item.Revision, &item.Scheme, &item.Host, &item.Port, &enabled, &item.Status,
		&boundJSON, &item.ConsecutiveFailures, &latency, &lastProbe, &lastError, &secretID, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ProxyRecord{}, ErrNotFound
	}
	if err != nil {
		return ProxyRecord{}, fmt.Errorf("read proxy: %w", err)
	}
	item.Enabled = enabled == 1
	item.HasAuth = secretID.Valid && secretID.String != ""
	if item.HasAuth {
		item.SecretRef = secretID.String
	}
	if latency.Valid {
		value := int(latency.Int64)
		item.LatencyMS = &value
	}
	if lastProbe.Valid {
		item.LastProbeAt = lastProbe.String
	}
	if lastError.Valid {
		item.LastError = lastError.String
	}
	item.BoundVMIDs = []string{}
	if boundJSON != "" && boundJSON != "null" {
		if err := json.Unmarshal([]byte(boundJSON), &item.BoundVMIDs); err != nil {
			return ProxyRecord{}, ErrCorrupt
		}
	}
	item.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return ProxyRecord{}, ErrCorrupt
	}
	item.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return ProxyRecord{}, ErrCorrupt
	}
	return item, nil
}

func validateProxyInput(input PutProxyInput) error {
	if !documentDomainPattern.MatchString(input.ID) {
		return fmt.Errorf("%w: invalid proxy id", ErrInvalid)
	}
	if input.Scheme != "socks5" {
		return fmt.Errorf("%w: proxy scheme must be socks5", ErrInvalid)
	}
	host := strings.TrimSpace(input.Host)
	if !looksLikeHost(host) {
		return fmt.Errorf("%w: invalid proxy host", ErrInvalid)
	}
	if input.Port < 1 || input.Port > 65535 {
		return fmt.Errorf("%w: invalid proxy port", ErrInvalid)
	}
	if input.Status != "" && input.Status != "unknown" && input.Status != "ok" && input.Status != "fail" && input.Status != "dead" {
		return fmt.Errorf("%w: invalid proxy status", ErrInvalid)
	}
	if len(input.BoundVMIDs) > 32 {
		return fmt.Errorf("%w: too many bound slots", ErrInvalid)
	}
	if strings.TrimSpace(input.UpdatedBy) == "" || len(input.UpdatedBy) > 128 {
		return fmt.Errorf("%w: invalid proxy operator", ErrInvalid)
	}
	return nil
}

func looksLikeHost(value string) bool {
	host := strings.TrimSpace(value)
	if host == "" || isAllDigits(host) {
		return false
	}
	return len(host) <= 256
}

func isAllDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func normalizeVMIDs(ids []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		value := strings.TrimSpace(id)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func socks5URI(host string, port int, username, password string) string {
	parsed := &url.URL{Scheme: "socks5", Host: host + ":" + strconv.Itoa(port)}
	if username != "" || password != "" {
		parsed.User = url.UserPassword(username, password)
	}
	return parsed.String()
}
