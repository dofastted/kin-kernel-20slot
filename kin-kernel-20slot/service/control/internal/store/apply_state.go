package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type ApplyState struct {
	Domain            string    `json:"domain"`
	DesiredRevision   uint64    `json:"desired_revision"`
	EffectiveRevision uint64    `json:"effective_revision"`
	State             string    `json:"state"`
	RuntimeHash       string    `json:"runtime_hash"`
	KernelHash        string    `json:"kernel_hash"`
	LastError         string    `json:"last_error,omitempty"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type CommitApplyInput struct {
	State       string
	RuntimeHash string
	KernelHash  string
	Operation   *EnqueueOperationInput
}

func (s *SQLite) CommitDocument(ctx context.Context, input PutDocumentInput, apply CommitApplyInput) (Document, ApplyState, *Operation, error) {
	if apply.State != "effective" && apply.State != "pending_restart" && apply.State != "rejected" {
		return Document{}, ApplyState{}, nil, fmt.Errorf("%w: invalid apply state", ErrInvalid)
	}
	if len(apply.RuntimeHash) != 64 || len(apply.KernelHash) != 64 {
		return Document{}, ApplyState{}, nil, fmt.Errorf("%w: runtime_hash and kernel_hash must be sha256 hex", ErrInvalid)
	}
	var payload json.RawMessage
	var operation *Operation
	if apply.Operation != nil {
		var err error
		payload, err = prepareOperationInput(*apply.Operation)
		if err != nil {
			return Document{}, ApplyState{}, nil, err
		}
	}
	var committed ApplyState
	document, err := s.putDocumentWithHook(ctx, input, func(tx *sql.Tx, revision uint64, now time.Time) error {
		current, getErr := getApplyStateTx(ctx, tx, input.Domain)
		if getErr != nil && !errors.Is(getErr, ErrNotFound) {
			return getErr
		}
		effectiveRevision := current.EffectiveRevision
		if apply.State == "effective" {
			effectiveRevision = revision
		}
		committed = ApplyState{
			Domain:            input.Domain,
			DesiredRevision:   revision,
			EffectiveRevision: effectiveRevision,
			State:             apply.State,
			RuntimeHash:       apply.RuntimeHash,
			KernelHash:        apply.KernelHash,
			UpdatedAt:         now,
		}
		if err := upsertApplyStateTx(ctx, tx, committed); err != nil {
			return err
		}
		if apply.Operation == nil {
			return nil
		}
		enrichedPayload, enrichErr := operationPayloadWithApply(payload, input.Domain, revision)
		if enrichErr != nil {
			return enrichErr
		}
		payload = enrichedPayload
		id, err := randomID("op")
		if err != nil {
			return err
		}
		nowText := now.Format(time.RFC3339Nano)
		_, err = tx.ExecContext(ctx, `INSERT INTO control_operations
			(id, kind, target, status, payload_json, attempts, max_attempts, idempotency_key, created_at, updated_at)
			VALUES (?, ?, ?, 'pending', ?, 0, ?, ?, ?, ?)`,
			id, apply.Operation.Kind, apply.Operation.Target, string(payload), apply.Operation.MaxAttempts,
			apply.Operation.IdempotencyKey, nowText, nowText,
		)
		if err != nil {
			return fmt.Errorf("enqueue config operation: %w", err)
		}
		operation = &Operation{
			ID:             id,
			Kind:           apply.Operation.Kind,
			Target:         apply.Operation.Target,
			Status:         "pending",
			Payload:        payload,
			MaxAttempts:    apply.Operation.MaxAttempts,
			IdempotencyKey: apply.Operation.IdempotencyKey,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		return nil
	})
	if err != nil {
		return Document{}, ApplyState{}, nil, err
	}
	return document, committed, operation, nil
}

func (s *SQLite) GetApplyState(ctx context.Context, domain string) (ApplyState, error) {
	return scanApplyState(s.db.QueryRowContext(ctx, `SELECT domain, desired_revision, effective_revision,
		apply_state, runtime_hash, kernel_hash, last_error, updated_at
		FROM control_apply_state WHERE domain = ?`, domain))
}
func operationPayloadWithApply(payload json.RawMessage, domain string, revision uint64) (json.RawMessage, error) {
	var value map[string]any
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, fmt.Errorf("decode operation payload: %w", err)
	}
	value["apply_domain"] = domain
	value["apply_revision"] = revision
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode operation payload: %w", err)
	}
	canonical, _, err := canonicalDocument(raw)
	return canonical, err
}

func (s *SQLite) AcknowledgeApply(ctx context.Context, domain string, revision uint64, state, lastError string) (ApplyState, error) {
	if state != "effective" && state != "rejected" {
		return ApplyState{}, fmt.Errorf("%w: acknowledge state must be effective or rejected", ErrInvalid)
	}
	if len(lastError) > 4096 {
		return ApplyState{}, fmt.Errorf("%w: last_error exceeds 4096 characters", ErrInvalid)
	}
	effective := uint64(0)
	if state == "effective" {
		effective = revision
	}
	result, err := s.db.ExecContext(ctx, `UPDATE control_apply_state SET
		effective_revision = CASE WHEN ? = 'effective' THEN ? ELSE effective_revision END,
		apply_state = ?, last_error = ?, updated_at = ?
		WHERE domain = ? AND desired_revision = ?`,
		state, effective, state, nullableString(lastError), time.Now().UTC().Format(time.RFC3339Nano), domain, revision,
	)
	if err != nil {
		return ApplyState{}, fmt.Errorf("acknowledge apply state: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return ApplyState{}, err
	}
	if changed != 1 {
		return ApplyState{}, ErrConflict
	}
	return s.GetApplyState(ctx, domain)
}

func getApplyStateTx(ctx context.Context, tx *sql.Tx, domain string) (ApplyState, error) {
	return scanApplyState(tx.QueryRowContext(ctx, `SELECT domain, desired_revision, effective_revision,
		apply_state, runtime_hash, kernel_hash, last_error, updated_at
		FROM control_apply_state WHERE domain = ?`, domain))
}

func upsertApplyStateTx(ctx context.Context, tx *sql.Tx, state ApplyState) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO control_apply_state
		(domain, desired_revision, effective_revision, apply_state, runtime_hash, kernel_hash, last_error, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(domain) DO UPDATE SET desired_revision=excluded.desired_revision,
			effective_revision=excluded.effective_revision, apply_state=excluded.apply_state,
			runtime_hash=excluded.runtime_hash, kernel_hash=excluded.kernel_hash,
			last_error=excluded.last_error, updated_at=excluded.updated_at`,
		state.Domain, state.DesiredRevision, state.EffectiveRevision, state.State,
		state.RuntimeHash, state.KernelHash, nullableString(state.LastError), state.UpdatedAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("upsert apply state: %w", err)
	}
	return nil
}

func scanApplyState(scanner interface{ Scan(...any) error }) (ApplyState, error) {
	var state ApplyState
	var lastError sql.NullString
	var updatedAt string
	err := scanner.Scan(
		&state.Domain,
		&state.DesiredRevision,
		&state.EffectiveRevision,
		&state.State,
		&state.RuntimeHash,
		&state.KernelHash,
		&lastError,
		&updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ApplyState{}, ErrNotFound
	}
	if err != nil {
		return ApplyState{}, fmt.Errorf("read apply state: %w", err)
	}
	state.LastError = lastError.String
	state.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return ApplyState{}, ErrCorrupt
	}
	return state, nil
}
