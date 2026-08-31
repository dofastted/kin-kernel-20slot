package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"kin.local/kin-control/internal/store"
)

const (
	routingDomain = "routing"
	modelDomain   = "model-policy"
	slotDomain    = "slot"
)

type Service struct {
	store *store.SQLite
}

func NewService(controlStore *store.SQLite) *Service {
	return &Service{store: controlStore}
}

type PutInput struct {
	ExpectedRevision uint64
	Data             json.RawMessage
	UpdatedBy        string
	Import           bool
}

type SlotEnvelope struct {
	SlotID        string              `json:"slot_id"`
	SchemaVersion int                 `json:"schema_version"`
	Revision      uint64              `json:"revision"`
	ConfigHash    string              `json:"config_hash"`
	RuntimeHash   string              `json:"runtime_hash"`
	KernelHash    string              `json:"kernel_hash"`
	Data          SlotPolicy          `json:"data"`
	Effective     EffectiveSlotPolicy `json:"effective"`
	Apply         ApplyStatus         `json:"apply"`
}

type BatchSlotFailure struct {
	SlotID  string `json:"slot_id"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type BatchSlotResult struct {
	Updated int                `json:"updated"`
	Missing []string           `json:"missing"`
	Failed  []BatchSlotFailure `json:"failed"`
	Items   []SlotEnvelope     `json:"items"`
}

func (s *Service) GetRouting(ctx context.Context) (DocumentEnvelope[RoutingConfig], error) {
	document, err := s.store.GetDocument(ctx, routingDomain)
	if errors.Is(err, store.ErrNotFound) {
		value := DefaultRouting()
		configHash, runtimeHash, kernelHash, hashErr := RoutingHashes(value)
		if hashErr != nil {
			return DocumentEnvelope[RoutingConfig]{}, hashErr
		}
		return DocumentEnvelope[RoutingConfig]{
			SchemaVersion: SchemaVersion,
			ConfigHash:    configHash,
			RuntimeHash:   runtimeHash,
			KernelHash:    kernelHash,
			Data:          value,
			Apply:         ApplyStatus{State: "effective"},
		}, nil
	}
	if err != nil {
		return DocumentEnvelope[RoutingConfig]{}, err
	}
	value, err := DecodeStrict[RoutingConfig](document.Data)
	if err != nil || ValidateRouting(value) != nil {
		return DocumentEnvelope[RoutingConfig]{}, store.ErrCorrupt
	}
	configHash, runtimeHash, kernelHash, err := RoutingHashes(value)
	if err != nil {
		return DocumentEnvelope[RoutingConfig]{}, err
	}
	apply := s.documentApply(ctx, routingDomain, document.Revision)
	return DocumentEnvelope[RoutingConfig]{
		SchemaVersion: document.SchemaVersion,
		Revision:      document.Revision,
		ConfigHash:    configHash,
		RuntimeHash:   runtimeHash,
		KernelHash:    kernelHash,
		Data:          value,
		Apply:         apply,
		Degraded:      document.Degraded,
	}, nil
}

func (s *Service) PutRouting(ctx context.Context, input PutInput) (DocumentEnvelope[RoutingConfig], error) {
	if err := s.ensureWritable(ctx, routingDomain, input.Import); err != nil {
		return DocumentEnvelope[RoutingConfig]{}, err
	}
	current, err := s.GetRouting(ctx)
	if err != nil {
		return DocumentEnvelope[RoutingConfig]{}, err
	}
	value, err := MergeRouting(current.Data, input.Data)
	if err != nil {
		return DocumentEnvelope[RoutingConfig]{}, fmt.Errorf("%w: %v", store.ErrInvalid, err)
	}
	configHash, runtimeHash, kernelHash, err := RoutingHashes(value)
	if err != nil {
		return DocumentEnvelope[RoutingConfig]{}, err
	}
	state := "effective"
	var operation *store.EnqueueOperationInput
	if current.Revision > 0 && runtimeHash != current.RuntimeHash {
		state = "pending_restart"
		payload, _ := json.Marshal(map[string]any{"reason": "routing_runtime_changed", "runtime_hash": runtimeHash})
		operation = &store.EnqueueOperationInput{
			Kind: "fleet.reload", Target: "all", Payload: payload, MaxAttempts: 3,
			IdempotencyKey: fmt.Sprintf("routing-reload-%s", runtimeHash),
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return DocumentEnvelope[RoutingConfig]{}, err
	}
	document, apply, _, err := s.store.CommitDocument(ctx, store.PutDocumentInput{
		Domain: routingDomain, SchemaVersion: SchemaVersion, ExpectedRevision: input.ExpectedRevision,
		Data: raw, UpdatedBy: input.UpdatedBy, ChangeKind: "routing_put",
	}, store.CommitApplyInput{State: state, RuntimeHash: runtimeHash, KernelHash: kernelHash, Operation: operation})
	if err != nil {
		return DocumentEnvelope[RoutingConfig]{}, err
	}
	return DocumentEnvelope[RoutingConfig]{
		SchemaVersion: SchemaVersion,
		Revision:      document.Revision,
		ConfigHash:    configHash,
		RuntimeHash:   runtimeHash,
		KernelHash:    kernelHash,
		Data:          value,
		Apply:         applyFromStore(apply),
	}, nil
}

func (s *Service) GetModelPolicy(ctx context.Context) (DocumentEnvelope[ModelPolicy], error) {
	document, err := s.store.GetDocument(ctx, modelDomain)
	if err != nil {
		return DocumentEnvelope[ModelPolicy]{}, err
	}
	value, err := DecodeStrict[ModelPolicy](document.Data)
	if err != nil || ValidateModelPolicy(value) != nil {
		return DocumentEnvelope[ModelPolicy]{}, store.ErrCorrupt
	}
	configHash, runtimeHash, kernelHash, err := ModelPolicyHashes(value)
	if err != nil {
		return DocumentEnvelope[ModelPolicy]{}, err
	}
	return DocumentEnvelope[ModelPolicy]{
		SchemaVersion: document.SchemaVersion,
		Revision:      document.Revision,
		ConfigHash:    configHash,
		RuntimeHash:   runtimeHash,
		KernelHash:    kernelHash,
		Data:          value,
		Apply:         s.documentApply(ctx, modelDomain, document.Revision),
		Degraded:      document.Degraded,
	}, nil
}

func (s *Service) PutModelPolicy(ctx context.Context, input PutInput) (DocumentEnvelope[ModelPolicy], error) {
	if err := s.ensureWritable(ctx, modelDomain, input.Import); err != nil {
		return DocumentEnvelope[ModelPolicy]{}, err
	}
	value, err := DecodeStrict[ModelPolicy](input.Data)
	if err != nil {
		return DocumentEnvelope[ModelPolicy]{}, fmt.Errorf("%w: %v", store.ErrInvalid, err)
	}
	if err := ValidateModelPolicy(value); err != nil {
		return DocumentEnvelope[ModelPolicy]{}, fmt.Errorf("%w: %v", store.ErrInvalid, err)
	}
	configHash, runtimeHash, kernelHash, err := ModelPolicyHashes(value)
	if err != nil {
		return DocumentEnvelope[ModelPolicy]{}, err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return DocumentEnvelope[ModelPolicy]{}, err
	}
	document, apply, _, err := s.store.CommitDocument(ctx, store.PutDocumentInput{
		Domain: modelDomain, SchemaVersion: SchemaVersion, ExpectedRevision: input.ExpectedRevision,
		Data: raw, UpdatedBy: input.UpdatedBy, ChangeKind: "model_policy_put",
	}, store.CommitApplyInput{State: "effective", RuntimeHash: runtimeHash, KernelHash: kernelHash})
	if err != nil {
		return DocumentEnvelope[ModelPolicy]{}, err
	}
	return DocumentEnvelope[ModelPolicy]{
		SchemaVersion: SchemaVersion,
		Revision:      document.Revision,
		ConfigHash:    configHash,
		RuntimeHash:   runtimeHash,
		KernelHash:    kernelHash,
		Data:          value,
		Apply:         applyFromStore(apply),
	}, nil
}

type storedSlotPolicy struct {
	Policy      SlotPolicy `json:"policy"`
	ConfigHash  string     `json:"config_hash"`
	RuntimeHash string     `json:"runtime_hash"`
	KernelHash  string     `json:"kernel_hash"`
}

func (s *Service) GetSlot(ctx context.Context, slotID string) (SlotEnvelope, error) {
	routing, err := s.GetRouting(ctx)
	if err != nil {
		return SlotEnvelope{}, err
	}
	state, err := s.store.GetSlotState(ctx, slotID)
	if errors.Is(err, store.ErrNotFound) {
		value := SlotPolicy{}
		effective := ResolveSlotPolicy(routing.Data, value)
		configHash, runtimeHash, kernelHash, hashErr := SlotPolicyHashes(value, effective)
		if hashErr != nil {
			return SlotEnvelope{}, hashErr
		}
		return SlotEnvelope{
			SlotID:        slotID,
			SchemaVersion: SchemaVersion,
			ConfigHash:    configHash, RuntimeHash: runtimeHash, KernelHash: kernelHash,
			Data: value, Effective: effective, Apply: ApplyStatus{State: "effective"},
		}, nil
	}
	if err != nil {
		return SlotEnvelope{}, err
	}
	value, err := DecodeStrict[storedSlotPolicy](state.Desired)
	if err != nil || ValidateSlotPolicy(value.Policy) != nil {
		return SlotEnvelope{}, store.ErrCorrupt
	}
	return SlotEnvelope{
		SlotID:        slotID,
		SchemaVersion: SchemaVersion,
		Revision:      state.DesiredRevision,
		ConfigHash:    value.ConfigHash,
		RuntimeHash:   value.RuntimeHash,
		KernelHash:    value.KernelHash,
		Data:          value.Policy,
		Effective:     ResolveSlotPolicy(routing.Data, value.Policy),
		Apply: ApplyStatus{State: state.ApplyState, DesiredRevision: state.DesiredRevision,
			EffectiveRevision: state.EffectiveRevision, LastError: state.LastError},
	}, nil
}

func (s *Service) PutSlot(ctx context.Context, slotID string, input PutInput) (SlotEnvelope, error) {
	if !namePattern.MatchString(slotID) {
		return SlotEnvelope{}, fmt.Errorf("%w: invalid slot id", store.ErrInvalid)
	}
	if err := s.ensureWritable(ctx, slotDomain, input.Import); err != nil {
		return SlotEnvelope{}, err
	}
	current, err := s.GetSlot(ctx, slotID)
	if err != nil {
		return SlotEnvelope{}, err
	}
	value, err := MergeSlotPolicy(current.Data, input.Data)
	if err != nil {
		return SlotEnvelope{}, fmt.Errorf("%w: %v", store.ErrInvalid, err)
	}
	routing, err := s.GetRouting(ctx)
	if err != nil {
		return SlotEnvelope{}, err
	}
	effective := ResolveSlotPolicy(routing.Data, value)
	configHash, runtimeHash, kernelHash, err := SlotPolicyHashes(value, effective)
	if err != nil {
		return SlotEnvelope{}, err
	}
	applyState := "effective"
	var operation *store.EnqueueOperationInput
	if current.Revision > 0 && runtimeHash != current.RuntimeHash {
		applyState = "pending_restart"
		payload, _ := json.Marshal(map[string]any{"reason": "slot_runtime_changed", "runtime_hash": runtimeHash})
		operation = &store.EnqueueOperationInput{
			Kind: "slot.reload", Target: slotID, Payload: payload, MaxAttempts: 3,
			IdempotencyKey: fmt.Sprintf("slot-reload-%s-%s", slotID, runtimeHash),
		}
	}
	stored := storedSlotPolicy{Policy: value, ConfigHash: configHash, RuntimeHash: runtimeHash, KernelHash: kernelHash}
	raw, err := json.Marshal(stored)
	if err != nil {
		return SlotEnvelope{}, err
	}
	state, err := s.store.PutSlotDesired(ctx, store.PutSlotDesiredInput{
		SlotID: slotID, ExpectedDesiredRevision: input.ExpectedRevision, ApplyState: applyState,
		Desired: raw, UpdatedBy: input.UpdatedBy, Operation: operation,
	})
	if err != nil {
		return SlotEnvelope{}, err
	}
	return SlotEnvelope{
		SlotID:        slotID,
		SchemaVersion: SchemaVersion,
		Revision:      state.DesiredRevision,
		ConfigHash:    configHash,
		RuntimeHash:   runtimeHash,
		KernelHash:    kernelHash,
		Data:          value,
		Effective:     effective,
		Apply: ApplyStatus{State: state.ApplyState, DesiredRevision: state.DesiredRevision,
			EffectiveRevision: state.EffectiveRevision, LastError: state.LastError},
	}, nil
}

func (s *Service) PutSlots(ctx context.Context, slotIDs []string, input PutInput) (BatchSlotResult, error) {
	if len(slotIDs) == 0 || len(slotIDs) > 1000 {
		return BatchSlotResult{}, fmt.Errorf("%w: slot ids must contain between 1 and 1000 entries", store.ErrInvalid)
	}
	seen := map[string]bool{}
	result := BatchSlotResult{Items: make([]SlotEnvelope, 0, len(slotIDs)), Missing: []string{}, Failed: []BatchSlotFailure{}}
	for _, slotID := range slotIDs {
		if seen[slotID] {
			continue
		}
		seen[slotID] = true
		current, err := s.GetSlot(ctx, slotID)
		if err != nil {
			return BatchSlotResult{}, err
		}
		perSlot := input
		perSlot.ExpectedRevision = current.Revision
		updated, err := s.PutSlot(ctx, slotID, perSlot)
		if errors.Is(err, store.ErrNotFound) {
			result.Missing = append(result.Missing, slotID)
			continue
		}
		if err != nil {
			code := "internal_error"
			if errors.Is(err, store.ErrConflict) {
				code = "conflict"
			} else if errors.Is(err, store.ErrInvalid) {
				code = "invalid_request"
			}
			result.Failed = append(result.Failed, BatchSlotFailure{SlotID: slotID, Code: code, Message: err.Error()})
			continue
		}
		result.Items = append(result.Items, updated)
	}
	result.Updated = len(result.Items)
	return result, nil
}

func (s *Service) ensureWritable(ctx context.Context, domain string, importing bool) error {
	owner, err := s.store.GetDomainOwner(ctx, domain)
	if err != nil {
		return err
	}
	if importing && owner.State == "shadow_import" {
		return nil
	}
	if !importing && owner.Owner == "go" && (owner.State == "go" || owner.State == "verified") {
		return nil
	}
	return fmt.Errorf("%w: domain %s is owned by %s in state %s", store.ErrConflict, domain, owner.Owner, owner.State)
}

func (s *Service) documentApply(ctx context.Context, domain string, revision uint64) ApplyStatus {
	state, err := s.store.GetApplyState(ctx, domain)
	if err != nil {
		return ApplyStatus{State: "effective", DesiredRevision: revision, EffectiveRevision: revision}
	}
	return applyFromStore(state)
}

func applyFromStore(state store.ApplyState) ApplyStatus {
	return ApplyStatus{State: state.State, DesiredRevision: state.DesiredRevision, EffectiveRevision: state.EffectiveRevision, LastError: state.LastError}
}

func NowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
