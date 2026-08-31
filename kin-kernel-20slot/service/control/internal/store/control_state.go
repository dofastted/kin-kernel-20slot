package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type DomainOwner struct {
	Domain     string    `json:"domain"`
	Owner      string    `json:"owner"`
	State      string    `json:"state"`
	SourceHash string    `json:"source_hash,omitempty"`
	Revision   uint64    `json:"revision"`
	UpdatedAt  time.Time `json:"updated_at"`
	UpdatedBy  string    `json:"updated_by"`
}

type PutDomainOwnerInput struct {
	Domain           string
	Owner            string
	State            string
	SourceHash       string
	ExpectedRevision uint64
	UpdatedBy        string
}

var validOwnerStates = map[string]bool{
	"node":            true,
	"shadow_import":   true,
	"shadow_match":    true,
	"frozen":          true,
	"go":              true,
	"verified":        true,
	"rollback_export": true,
	"rejected":        true,
}

func validOwnerTransition(currentOwner, currentState, nextOwner, nextState string) bool {
	if currentOwner == nextOwner && currentState == nextState {
		return true
	}
	transition := currentOwner + "/" + currentState + "->" + nextOwner + "/" + nextState
	switch transition {
	case "node/node->node/shadow_import",
		"node/shadow_import->node/shadow_match",
		"node/shadow_import->node/node",
		"node/shadow_match->node/frozen",
		"node/shadow_match->node/rejected",
		"node/shadow_match->node/node",
		"node/frozen->go/go",
		"node/frozen->node/node",
		"go/go->go/verified",
		"go/verified->go/rollback_export",
		"go/rollback_export->node/node",
		"node/rejected->node/node",
		"node/rejected->node/shadow_import":
		return true
	default:
		return false
	}
}

func (s *SQLite) GetDomainOwner(ctx context.Context, domain string) (DomainOwner, error) {
	var owner DomainOwner
	var updatedAt string
	err := s.db.QueryRowContext(ctx, `SELECT domain, owner, state, COALESCE(source_hash, ''),
		revision, updated_at, updated_by FROM control_domain_owners WHERE domain = ?`, domain).Scan(
		&owner.Domain,
		&owner.Owner,
		&owner.State,
		&owner.SourceHash,
		&owner.Revision,
		&updatedAt,
		&owner.UpdatedBy,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return DomainOwner{Domain: domain, Owner: "node", State: "node"}, nil
	}
	if err != nil {
		return DomainOwner{}, fmt.Errorf("read domain owner: %w", err)
	}
	owner.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return DomainOwner{}, ErrCorrupt
	}
	return owner, nil
}

func (s *SQLite) PutDomainOwner(ctx context.Context, input PutDomainOwnerInput) (DomainOwner, error) {
	if !documentDomainPattern.MatchString(input.Domain) {
		return DomainOwner{}, fmt.Errorf("%w: invalid owner domain", ErrInvalid)
	}
	if input.Owner != "node" && input.Owner != "go" {
		return DomainOwner{}, fmt.Errorf("%w: owner must be node or go", ErrInvalid)
	}
	if !validOwnerStates[input.State] {
		return DomainOwner{}, fmt.Errorf("%w: invalid owner state", ErrInvalid)
	}
	if strings.TrimSpace(input.UpdatedBy) == "" || len(input.UpdatedBy) > 128 || len(input.SourceHash) > 256 {
		return DomainOwner{}, fmt.Errorf("%w: owner metadata exceeds its size limit", ErrInvalid)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return DomainOwner{}, fmt.Errorf("begin owner update: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	currentOwner := DomainOwner{Domain: input.Domain, Owner: "node", State: "node"}
	err = tx.QueryRowContext(ctx, "SELECT owner, state, revision FROM control_domain_owners WHERE domain = ?", input.Domain).Scan(&currentOwner.Owner, &currentOwner.State, &currentOwner.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		currentOwner.Revision = 0
	} else if err != nil {
		return DomainOwner{}, fmt.Errorf("read owner revision: %w", err)
	}
	if currentOwner.Revision != input.ExpectedRevision {
		return DomainOwner{}, fmt.Errorf("%w: expected %d, current %d", ErrConflict, input.ExpectedRevision, currentOwner.Revision)
	}
	if !validOwnerTransition(currentOwner.Owner, currentOwner.State, input.Owner, input.State) {
		return DomainOwner{}, fmt.Errorf("%w: invalid owner transition %s/%s -> %s/%s", ErrConflict, currentOwner.Owner, currentOwner.State, input.Owner, input.State)
	}
	parent, revision, err := nextRevision(ctx, tx)
	if err != nil {
		return DomainOwner{}, err
	}
	now := time.Now().UTC()
	nowText := now.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO control_domain_owners
		(domain, owner, state, source_hash, revision, updated_at, updated_by)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(domain) DO UPDATE SET owner=excluded.owner, state=excluded.state,
			source_hash=excluded.source_hash, revision=excluded.revision,
			updated_at=excluded.updated_at, updated_by=excluded.updated_by`,
		input.Domain, input.Owner, input.State, nullableString(input.SourceHash), revision, nowText, input.UpdatedBy,
	); err != nil {
		return DomainOwner{}, fmt.Errorf("upsert domain owner: %w", err)
	}
	summary, _ := json.Marshal(map[string]string{"owner": input.Owner, "state": input.State})
	if _, err := tx.ExecContext(ctx, `INSERT INTO control_revisions
		(revision, parent_revision, domain, change_kind, summary_json, created_at, created_by)
		VALUES (?, ?, ?, 'domain_owner_put', ?, ?, ?)`,
		revision, parent, input.Domain, string(summary), nowText, input.UpdatedBy,
	); err != nil {
		return DomainOwner{}, fmt.Errorf("insert owner revision: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return DomainOwner{}, fmt.Errorf("commit owner update: %w", err)
	}
	return DomainOwner{
		Domain:     input.Domain,
		Owner:      input.Owner,
		State:      input.State,
		SourceHash: input.SourceHash,
		Revision:   revision,
		UpdatedAt:  now,
		UpdatedBy:  input.UpdatedBy,
	}, nil
}

type SlotState struct {
	SlotID            string          `json:"slot_id"`
	DesiredRevision   uint64          `json:"desired_revision"`
	EffectiveRevision uint64          `json:"effective_revision"`
	ApplyState        string          `json:"apply_state"`
	Desired           json.RawMessage `json:"desired"`
	Observed          json.RawMessage `json:"observed"`
	LastError         string          `json:"last_error,omitempty"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

type PutSlotDesiredInput struct {
	SlotID                  string
	ExpectedDesiredRevision uint64
	ApplyState              string
	Desired                 json.RawMessage
	UpdatedBy               string
	Operation               *EnqueueOperationInput
}

func (s *SQLite) PutSlotDesired(ctx context.Context, input PutSlotDesiredInput) (SlotState, error) {
	if !documentDomainPattern.MatchString(input.SlotID) {
		return SlotState{}, fmt.Errorf("%w: invalid slot_id", ErrInvalid)
	}
	if input.ApplyState != "effective" && input.ApplyState != "pending_restart" && input.ApplyState != "rejected" {
		return SlotState{}, fmt.Errorf("%w: invalid apply_state", ErrInvalid)
	}
	if strings.TrimSpace(input.UpdatedBy) == "" || len(input.UpdatedBy) > 128 || len(input.Desired) > 1<<20 {
		return SlotState{}, fmt.Errorf("%w: slot metadata exceeds its size limit", ErrInvalid)
	}
	desired, _, err := canonicalDocument(input.Desired)
	if err != nil {
		return SlotState{}, err
	}
	var operationPayload json.RawMessage
	if input.Operation != nil {
		operationPayload, err = prepareOperationInput(*input.Operation)
		if err != nil {
			return SlotState{}, err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SlotState{}, fmt.Errorf("begin slot update: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var current uint64
	var effective uint64
	var observed string
	err = tx.QueryRowContext(ctx, `SELECT desired_revision, effective_revision, observed_json
		FROM control_slot_state WHERE slot_id = ?`, input.SlotID).Scan(&current, &effective, &observed)
	if errors.Is(err, sql.ErrNoRows) {
		current, effective, observed = 0, 0, "{}"
	} else if err != nil {
		return SlotState{}, fmt.Errorf("read slot revision: %w", err)
	}
	if current != input.ExpectedDesiredRevision {
		return SlotState{}, fmt.Errorf("%w: expected %d, current %d", ErrConflict, input.ExpectedDesiredRevision, current)
	}
	parent, revision, err := nextRevision(ctx, tx)
	if err != nil {
		return SlotState{}, err
	}
	if input.ApplyState == "effective" {
		effective = revision
	}
	now := time.Now().UTC()
	nowText := now.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO control_slot_state
		(slot_id, desired_revision, effective_revision, apply_state, desired_json, observed_json, last_error, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
		ON CONFLICT(slot_id) DO UPDATE SET desired_revision=excluded.desired_revision,
			effective_revision=excluded.effective_revision, apply_state=excluded.apply_state,
			desired_json=excluded.desired_json, last_error=NULL, updated_at=excluded.updated_at`,
		input.SlotID, revision, effective, input.ApplyState, string(desired), observed, nowText,
	); err != nil {
		return SlotState{}, fmt.Errorf("upsert slot desired state: %w", err)
	}
	summary, _ := json.Marshal(map[string]string{"slot_id": input.SlotID, "apply_state": input.ApplyState})
	if _, err := tx.ExecContext(ctx, `INSERT INTO control_revisions
		(revision, parent_revision, domain, change_kind, summary_json, created_at, created_by)
		VALUES (?, ?, ?, 'slot_desired_put', ?, ?, ?)`,
		revision, parent, "slot/"+input.SlotID, string(summary), nowText, input.UpdatedBy,
	); err != nil {
		return SlotState{}, fmt.Errorf("insert slot revision: %w", err)
	}
	if input.Operation != nil {
		var value map[string]any
		if err := json.Unmarshal(operationPayload, &value); err != nil {
			return SlotState{}, fmt.Errorf("decode slot operation payload: %w", err)
		}
		value["slot_id"] = input.SlotID
		value["slot_revision"] = revision
		raw, err := json.Marshal(value)
		if err != nil {
			return SlotState{}, fmt.Errorf("encode slot operation payload: %w", err)
		}
		operationPayload, _, err = canonicalDocument(raw)
		if err != nil {
			return SlotState{}, err
		}
	}
	if input.Operation != nil {
		id, err := randomID("op")
		if err != nil {
			return SlotState{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO control_operations
			(id, kind, target, status, payload_json, attempts, max_attempts, idempotency_key, created_at, updated_at)
			VALUES (?, ?, ?, 'pending', ?, 0, ?, ?, ?, ?)`,
			id, input.Operation.Kind, input.Operation.Target, string(operationPayload), input.Operation.MaxAttempts,
			input.Operation.IdempotencyKey, nowText, nowText,
		); err != nil {
			return SlotState{}, fmt.Errorf("enqueue slot operation: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return SlotState{}, fmt.Errorf("commit slot update: %w", err)
	}
	return s.GetSlotState(ctx, input.SlotID)
}

func (s *SQLite) GetSlotState(ctx context.Context, slotID string) (SlotState, error) {
	var state SlotState
	var desired string
	var observed string
	var lastError sql.NullString
	var updatedAt string
	err := s.db.QueryRowContext(ctx, `SELECT slot_id, desired_revision, effective_revision,
		apply_state, desired_json, observed_json, last_error, updated_at
		FROM control_slot_state WHERE slot_id = ?`, slotID).Scan(
		&state.SlotID,
		&state.DesiredRevision,
		&state.EffectiveRevision,
		&state.ApplyState,
		&desired,
		&observed,
		&lastError,
		&updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SlotState{}, ErrNotFound
	}
	if err != nil {
		return SlotState{}, fmt.Errorf("read slot state: %w", err)
	}
	state.Desired, _, err = canonicalDocument(json.RawMessage(desired))
	if err != nil {
		return SlotState{}, ErrCorrupt
	}
	state.Observed, _, err = canonicalDocument(json.RawMessage(observed))
	if err != nil {
		return SlotState{}, ErrCorrupt
	}
	state.LastError = lastError.String
	state.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return SlotState{}, ErrCorrupt
	}
	return state, nil
}

func (s *SQLite) AcknowledgeSlot(ctx context.Context, slotID string, effectiveRevision uint64, observed json.RawMessage, applyState, lastError string) (SlotState, error) {
	if applyState != "effective" && applyState != "pending_restart" && applyState != "rejected" {
		return SlotState{}, fmt.Errorf("%w: invalid apply_state", ErrInvalid)
	}
	if len(observed) > 1<<20 || len(lastError) > 4096 {
		return SlotState{}, fmt.Errorf("%w: observed state exceeds its size limit", ErrInvalid)
	}
	canonical, _, err := canonicalDocument(observed)
	if err != nil {
		return SlotState{}, err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE control_slot_state SET
		effective_revision = CASE WHEN ? = 'effective' THEN ? ELSE effective_revision END,
		apply_state = ?, observed_json = ?, last_error = ?, updated_at = ?
		WHERE slot_id = ? AND desired_revision = ?`,
		applyState, effectiveRevision, applyState, string(canonical), nullableString(lastError),
		time.Now().UTC().Format(time.RFC3339Nano), slotID, effectiveRevision,
	)
	if err != nil {
		return SlotState{}, fmt.Errorf("acknowledge slot state: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return SlotState{}, err
	}
	if changed != 1 {
		return SlotState{}, ErrConflict
	}
	return s.GetSlotState(ctx, slotID)
}

type Operation struct {
	ID             string          `json:"id"`
	Kind           string          `json:"kind"`
	Target         string          `json:"target"`
	Status         string          `json:"status"`
	Payload        json.RawMessage `json:"payload"`
	Result         json.RawMessage `json:"result,omitempty"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	LeaseOwner     string          `json:"lease_owner,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	IdempotencyKey string          `json:"idempotency_key"`
	LastError      string          `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type EnqueueOperationInput struct {
	Kind           string
	Target         string
	Payload        json.RawMessage
	MaxAttempts    int
	IdempotencyKey string
}

func prepareOperationInput(input EnqueueOperationInput) (json.RawMessage, error) {
	if !documentDomainPattern.MatchString(input.Kind) || strings.TrimSpace(input.Target) == "" {
		return nil, fmt.Errorf("%w: invalid operation kind or target", ErrInvalid)
	}
	if len(input.Target) > 512 {
		return nil, fmt.Errorf("%w: operation target exceeds 512 characters", ErrInvalid)
	}
	if input.MaxAttempts < 1 || input.MaxAttempts > 20 {
		return nil, fmt.Errorf("%w: max_attempts must be between 1 and 20", ErrInvalid)
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" || len(input.IdempotencyKey) > 256 {
		return nil, fmt.Errorf("%w: idempotency_key is required and must be at most 256 characters", ErrInvalid)
	}
	payload, _, err := canonicalDocument(input.Payload)
	if err != nil {
		return nil, err
	}
	if len(payload) > 1<<20 {
		return nil, fmt.Errorf("%w: operation payload exceeds 1 MiB", ErrInvalid)
	}
	return payload, nil
}

func (s *SQLite) EnqueueOperation(ctx context.Context, input EnqueueOperationInput) (Operation, error) {
	payload, err := prepareOperationInput(input)
	if err != nil {
		return Operation{}, err
	}
	if existing, err := s.getOperationByIdempotencyKey(ctx, input.IdempotencyKey); err == nil {
		return existing, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Operation{}, err
	}
	id, err := randomID("op")
	if err != nil {
		return Operation{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.db.ExecContext(ctx, `INSERT INTO control_operations
		(id, kind, target, status, payload_json, attempts, max_attempts, idempotency_key, created_at, updated_at)
		VALUES (?, ?, ?, 'pending', ?, 0, ?, ?, ?, ?)`,
		id, input.Kind, input.Target, string(payload), input.MaxAttempts, input.IdempotencyKey, now, now,
	)
	if err != nil {
		if existing, getErr := s.getOperationByIdempotencyKey(ctx, input.IdempotencyKey); getErr == nil {
			return existing, nil
		}
		return Operation{}, fmt.Errorf("enqueue operation: %w", err)
	}
	return s.GetOperation(ctx, id)
}

func (s *SQLite) ClaimOperation(ctx context.Context, worker string, lease time.Duration) (Operation, error) {
	if strings.TrimSpace(worker) == "" || len(worker) > 128 {
		return Operation{}, fmt.Errorf("%w: worker is required and must be at most 128 characters", ErrInvalid)
	}
	if lease <= 0 || lease > time.Hour {
		return Operation{}, fmt.Errorf("%w: lease must be between 1ns and 1h", ErrInvalid)
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, fmt.Errorf("begin operation claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var id string
	err = tx.QueryRowContext(ctx, `SELECT id FROM control_operations
		WHERE attempts < max_attempts AND (
			status = 'pending' OR
			(status IN ('claimed', 'running') AND julianday(lease_expires_at) < julianday(?))
		) ORDER BY created_at, id LIMIT 1`, now.Format(time.RFC3339Nano)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, ErrNotFound
	}
	if err != nil {
		return Operation{}, fmt.Errorf("select operation to claim: %w", err)
	}
	leaseEnd := now.Add(lease).Format(time.RFC3339Nano)
	result, err := tx.ExecContext(ctx, `UPDATE control_operations SET status = 'claimed',
		attempts = attempts + 1, lease_owner = ?, lease_expires_at = ?, updated_at = ?
		WHERE id = ? AND attempts < max_attempts AND (
			status = 'pending' OR
			(status IN ('claimed', 'running') AND julianday(lease_expires_at) < julianday(?))
		)`, worker, leaseEnd, now.Format(time.RFC3339Nano), id, now.Format(time.RFC3339Nano))
	if err != nil {
		return Operation{}, fmt.Errorf("claim operation: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return Operation{}, ErrConflict
	}
	if err := tx.Commit(); err != nil {
		return Operation{}, fmt.Errorf("commit operation claim: %w", err)
	}
	return s.GetOperation(ctx, id)
}

func (s *SQLite) CompleteOperation(ctx context.Context, id, worker, status string, result json.RawMessage, lastError string) (Operation, error) {
	if status != "succeeded" && status != "failed" && status != "cancelled" {
		return Operation{}, fmt.Errorf("%w: terminal operation status required", ErrInvalid)
	}
	if strings.TrimSpace(worker) == "" || len(worker) > 128 || len(lastError) > 4096 {
		return Operation{}, fmt.Errorf("%w: completion metadata exceeds its size limit", ErrInvalid)
	}
	canonical := json.RawMessage("{}")
	var err error
	if len(result) > 0 {
		canonical, _, err = canonicalDocument(result)
		if err != nil {
			return Operation{}, err
		}
	}
	if len(canonical) > 1<<20 {
		return Operation{}, fmt.Errorf("%w: operation result exceeds 1 MiB", ErrInvalid)
	}
	operation, err := s.GetOperation(ctx, id)
	if err != nil {
		return Operation{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, fmt.Errorf("begin operation completion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	update, err := tx.ExecContext(ctx, `UPDATE control_operations SET status = ?, result_json = ?,
		last_error = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
		WHERE id = ? AND lease_owner = ? AND status IN ('claimed', 'running')`,
		status, string(canonical), nullableString(lastError), now, id, worker,
	)
	if err != nil {
		return Operation{}, fmt.Errorf("complete operation: %w", err)
	}
	changed, err := update.RowsAffected()
	if err != nil || changed != 1 {
		return Operation{}, ErrConflict
	}
	if err := acknowledgeCompletedOperationTx(ctx, tx, operation, canonical, status, lastError, now); err != nil {
		return Operation{}, fmt.Errorf("acknowledge completed operation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Operation{}, fmt.Errorf("commit operation completion: %w", err)
	}
	return s.GetOperation(ctx, id)
}

func acknowledgeCompletedOperationTx(ctx context.Context, tx *sql.Tx, operation Operation, result json.RawMessage, status, lastError, now string) error {
	var metadata struct {
		ApplyDomain   string `json:"apply_domain"`
		ApplyRevision uint64 `json:"apply_revision"`
		SlotID        string `json:"slot_id"`
		SlotRevision  uint64 `json:"slot_revision"`
	}
	if err := json.Unmarshal(operation.Payload, &metadata); err != nil {
		return ErrCorrupt
	}
	state := "rejected"
	if status == "succeeded" {
		state = "effective"
	}
	if metadata.ApplyDomain != "" && metadata.ApplyRevision > 0 {
		_, err := tx.ExecContext(ctx, `UPDATE control_apply_state SET
			effective_revision = CASE WHEN ? = 'effective' THEN ? ELSE effective_revision END,
			apply_state = ?, last_error = ?, updated_at = ?
			WHERE domain = ? AND desired_revision = ?`,
			state, metadata.ApplyRevision, state, nullableString(lastError), now,
			metadata.ApplyDomain, metadata.ApplyRevision,
		)
		return err
	}
	if metadata.SlotID != "" && metadata.SlotRevision > 0 {
		_, err := tx.ExecContext(ctx, `UPDATE control_slot_state SET
			effective_revision = CASE WHEN ? = 'effective' THEN ? ELSE effective_revision END,
			apply_state = ?, observed_json = ?, last_error = ?, updated_at = ?
			WHERE slot_id = ? AND desired_revision = ?`,
			state, metadata.SlotRevision, state, string(result), nullableString(lastError), now,
			metadata.SlotID, metadata.SlotRevision,
		)
		return err
	}
	return nil
}

func (s *SQLite) GetOperation(ctx context.Context, id string) (Operation, error) {
	return scanOperation(s.db.QueryRowContext(ctx, `SELECT id, kind, target, status, payload_json,
		result_json, attempts, max_attempts, lease_owner, lease_expires_at, idempotency_key,
		last_error, created_at, updated_at FROM control_operations WHERE id = ?`, id))
}

func (s *SQLite) getOperationByIdempotencyKey(ctx context.Context, key string) (Operation, error) {
	return scanOperation(s.db.QueryRowContext(ctx, `SELECT id, kind, target, status, payload_json,
		result_json, attempts, max_attempts, lease_owner, lease_expires_at, idempotency_key,
		last_error, created_at, updated_at FROM control_operations WHERE idempotency_key = ?`, key))
}

func scanOperation(scanner interface{ Scan(...any) error }) (Operation, error) {
	var operation Operation
	var payload string
	var result sql.NullString
	var leaseOwner sql.NullString
	var leaseExpires sql.NullString
	var lastError sql.NullString
	var createdAt string
	var updatedAt string
	err := scanner.Scan(
		&operation.ID,
		&operation.Kind,
		&operation.Target,
		&operation.Status,
		&payload,
		&result,
		&operation.Attempts,
		&operation.MaxAttempts,
		&leaseOwner,
		&leaseExpires,
		&operation.IdempotencyKey,
		&lastError,
		&createdAt,
		&updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, ErrNotFound
	}
	if err != nil {
		return Operation{}, fmt.Errorf("read operation: %w", err)
	}
	operation.Payload = json.RawMessage(payload)
	if result.Valid {
		operation.Result = json.RawMessage(result.String)
	}
	operation.LeaseOwner = leaseOwner.String
	operation.LastError = lastError.String
	operation.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return Operation{}, ErrCorrupt
	}
	operation.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return Operation{}, ErrCorrupt
	}
	if leaseExpires.Valid {
		parsed, parseErr := time.Parse(time.RFC3339Nano, leaseExpires.String)
		if parseErr != nil {
			return Operation{}, ErrCorrupt
		}
		operation.LeaseExpiresAt = &parsed
	}
	return operation, nil
}

func randomID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(raw[:]), nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
