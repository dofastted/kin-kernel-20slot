package config

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"kin.local/kin-control/internal/store"
)

var proxyProbing atomic.Bool

func (s *Service) GetProxyPool(ctx context.Context) (DocumentEnvelope[ProxyPoolConfig], error) {
	document, err := s.store.GetDocument(ctx, proxyDomain)
	if errors.Is(err, store.ErrNotFound) {
		value := DefaultProxyPool()
		configHash, runtimeHash, kernelHash, hashErr := ProxyPoolHashes(value)
		if hashErr != nil {
			return DocumentEnvelope[ProxyPoolConfig]{}, hashErr
		}
		return DocumentEnvelope[ProxyPoolConfig]{
			SchemaVersion: SchemaVersion, ConfigHash: configHash, RuntimeHash: runtimeHash, KernelHash: kernelHash,
			Data: value, Apply: ApplyStatus{State: "effective"},
		}, nil
	}
	if err != nil {
		return DocumentEnvelope[ProxyPoolConfig]{}, err
	}
	value, err := DecodeStrict[ProxyPoolConfig](document.Data)
	if err != nil || ValidateProxyPool(value) != nil {
		return DocumentEnvelope[ProxyPoolConfig]{}, store.ErrCorrupt
	}
	configHash, runtimeHash, kernelHash, err := ProxyPoolHashes(value)
	if err != nil {
		return DocumentEnvelope[ProxyPoolConfig]{}, err
	}
	return DocumentEnvelope[ProxyPoolConfig]{
		SchemaVersion: document.SchemaVersion, Revision: document.Revision,
		ConfigHash: configHash, RuntimeHash: runtimeHash, KernelHash: kernelHash,
		Data: value, Apply: s.documentApply(ctx, proxyDomain, document.Revision), Degraded: document.Degraded,
	}, nil
}

func (s *Service) PutProxyPool(ctx context.Context, input PutInput) (DocumentEnvelope[ProxyPoolConfig], error) {
	if err := s.ensureWritable(ctx, proxyDomain, input.Import); err != nil {
		return DocumentEnvelope[ProxyPoolConfig]{}, err
	}
	current, err := s.GetProxyPool(ctx)
	if err != nil {
		return DocumentEnvelope[ProxyPoolConfig]{}, err
	}
	value, err := MergeProxyPool(current.Data, input.Data)
	if err != nil {
		return DocumentEnvelope[ProxyPoolConfig]{}, fmt.Errorf("%w: %v", store.ErrInvalid, err)
	}
	configHash, runtimeHash, kernelHash, err := ProxyPoolHashes(value)
	if err != nil {
		return DocumentEnvelope[ProxyPoolConfig]{}, err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return DocumentEnvelope[ProxyPoolConfig]{}, err
	}
	document, apply, _, err := s.store.CommitDocument(ctx, store.PutDocumentInput{
		Domain: proxyDomain, SchemaVersion: SchemaVersion, ExpectedRevision: input.ExpectedRevision,
		Data: raw, UpdatedBy: input.UpdatedBy, ChangeKind: "proxy_pool_put",
	}, store.CommitApplyInput{State: "effective", RuntimeHash: runtimeHash, KernelHash: kernelHash})
	if err != nil {
		return DocumentEnvelope[ProxyPoolConfig]{}, err
	}
	return DocumentEnvelope[ProxyPoolConfig]{
		SchemaVersion: SchemaVersion, Revision: document.Revision,
		ConfigHash: configHash, RuntimeHash: runtimeHash, KernelHash: kernelHash,
		Data: value, Apply: applyFromStore(apply),
	}, nil
}

func (s *Service) ListProxies(ctx context.Context) (ProxyPoolSnapshot, error) {
	pool, err := s.GetProxyPool(ctx)
	if err != nil {
		return ProxyPoolSnapshot{}, err
	}
	records, err := s.store.ListProxies(ctx)
	if err != nil {
		return ProxyPoolSnapshot{}, err
	}
	items := make([]PublicProxy, 0, len(records))
	for _, record := range records {
		items = append(items, PublicProxyFromStore(record, pool.Data.BindLimit))
	}
	return ProxyPoolSnapshot{Config: pool, Totals: ProxyTotals(items, pool.Data.BindLimit, proxyProbing.Load()), Proxies: items}, nil
}

func (s *Service) GetProxyRecord(ctx context.Context, id string) (store.ProxyRecord, error) {
	return s.store.GetProxy(ctx, id)
}

func (s *Service) ProbeAllProxies(ctx context.Context) (map[string]any, error) {
	if err := s.ensureWritable(ctx, proxyDomain, false); err != nil {
		return nil, err
	}
	if !proxyProbing.CompareAndSwap(false, true) {
		return map[string]any{"ok": false, "error": "probe_in_progress"}, nil
	}
	defer proxyProbing.Store(false)
	records, err := s.store.ListProxies(ctx)
	if err != nil {
		return nil, err
	}
	results := []map[string]any{}
	for _, record := range records {
		if !record.Enabled {
			continue
		}
		_, probe, probeErr := s.ProbeProxy(ctx, record.ID)
		if probeErr != nil {
			results = append(results, map[string]any{"id": record.ID, "ok": false, "error": probeErr.Error()})
			continue
		}
		probe["id"] = record.ID
		results = append(results, probe)
	}
	return map[string]any{"ok": true, "total": len(results), "results": results}, nil
}

func (s *Service) PutProxy(ctx context.Context, input store.PutProxyInput) (PublicProxy, []map[string]any, error) {
	if err := s.ensureWritable(ctx, proxyDomain, input.Import); err != nil {
		return PublicProxy{}, nil, err
	}
	pool, err := s.GetProxyPool(ctx)
	if err != nil {
		return PublicProxy{}, nil, err
	}
	if len(input.BoundVMIDs) > pool.Data.BindLimit {
		return PublicProxy{}, nil, fmt.Errorf("%w: bind_limit exceeded", store.ErrInvalid)
	}
	if input.Scheme == "" {
		input.Scheme = "socks5"
	}
	record, err := s.store.PutProxy(ctx, input)
	if err != nil {
		return PublicProxy{}, nil, err
	}
	workers := s.enqueueBoundReloads(ctx, record, "proxy_edit")
	return PublicProxyFromStore(record, pool.Data.BindLimit), workers, nil
}
func (s *Service) AllocateProxy(ctx context.Context, vmID, updatedBy string) (PublicProxy, []map[string]any, error) {
	if err := s.ensureWritable(ctx, proxyDomain, false); err != nil {
		return PublicProxy{}, nil, err
	}
	pool, err := s.GetProxyPool(ctx)
	if err != nil {
		return PublicProxy{}, nil, err
	}
	records, err := s.store.ListProxies(ctx)
	if err != nil {
		return PublicProxy{}, nil, err
	}
	var chosen *store.ProxyRecord
	for i := range records {
		record := records[i]
		if !record.Enabled || record.Status == "dead" {
			continue
		}
		if containsString(record.BoundVMIDs, vmID) {
			return PublicProxyFromStore(record, pool.Data.BindLimit), nil, nil
		}
		if len(record.BoundVMIDs) >= pool.Data.BindLimit {
			continue
		}
		if chosen == nil || len(record.BoundVMIDs) < len(chosen.BoundVMIDs) {
			chosen = &records[i]
		}
	}
	if chosen == nil {
		return PublicProxy{}, nil, fmt.Errorf("%w: no free proxy", store.ErrConflict)
	}
	return s.BindProxy(ctx, chosen.ID, vmID, chosen.Revision, updatedBy)
}

func (s *Service) ImportProxies(ctx context.Context, records []store.PutProxyInput, updatedBy string, importing bool) (added []PublicProxy, skipped []map[string]string, err error) {
	if err := s.ensureWritable(ctx, proxyDomain, importing); err != nil {
		return nil, nil, err
	}
	pool, err := s.GetProxyPool(ctx)
	if err != nil {
		return nil, nil, err
	}
	existing, err := s.store.ListProxies(ctx)
	if err != nil {
		return nil, nil, err
	}
	seen := map[string]bool{}
	for _, item := range existing {
		seen[proxyIdentity(item.Host, item.Port, "")] = true
	}
	for _, input := range records {
		if input.Host == "" || input.Port == 0 {
			skipped = append(skipped, map[string]string{"reason": "parse_failed"})
			continue
		}
		username := ""
		if input.Username != nil {
			username = *input.Username
		}
		key := proxyIdentity(input.Host, input.Port, username)
		if seen[key] {
			skipped = append(skipped, map[string]string{"reason": "duplicate", "host": input.Host})
			continue
		}
		if input.ID == "" {
			id, idErr := newProxyID()
			if idErr != nil {
				return added, skipped, idErr
			}
			input.ID = id
		}
		input.Scheme = "socks5"
		input.Enabled = true
		input.UpdatedBy = updatedBy
		input.Import = importing
		record, putErr := s.store.PutProxy(ctx, input)
		if putErr != nil {
			skipped = append(skipped, map[string]string{"reason": putErr.Error(), "host": input.Host})
			continue
		}
		seen[key] = true
		added = append(added, PublicProxyFromStore(record, pool.Data.BindLimit))
	}
	return added, skipped, nil
}

func (s *Service) BindProxy(ctx context.Context, proxyID, vmID string, expectedRevision uint64, updatedBy string) (PublicProxy, []map[string]any, error) {
	if err := s.ensureWritable(ctx, proxyDomain, false); err != nil {
		return PublicProxy{}, nil, err
	}
	pool, err := s.GetProxyPool(ctx)
	if err != nil {
		return PublicProxy{}, nil, err
	}
	current, err := s.store.GetProxy(ctx, proxyID)
	if err != nil {
		return PublicProxy{}, nil, err
	}
	if expectedRevision != current.Revision {
		return PublicProxy{}, nil, store.ErrConflict
	}
	ids := append([]string{}, current.BoundVMIDs...)
	if !containsString(ids, vmID) {
		if len(ids) >= pool.Data.BindLimit {
			return PublicProxy{}, nil, fmt.Errorf("%w: bind_limit exceeded", store.ErrInvalid)
		}
		ids = append(ids, vmID)
	}
	record, err := s.store.PutProxy(ctx, store.PutProxyInput{
		ID: current.ID, ExpectedRevision: current.Revision, Scheme: current.Scheme, Host: current.Host,
		Port: current.Port, Enabled: current.Enabled, Status: current.Status, BoundVMIDs: ids, UpdatedBy: updatedBy,
	})
	if err != nil {
		return PublicProxy{}, nil, err
	}
	workers := s.enqueueBoundReloads(ctx, record, "proxy_bind")
	return PublicProxyFromStore(record, pool.Data.BindLimit), workers, nil
}

func (s *Service) UnbindProxy(ctx context.Context, proxyID, vmID string, expectedRevision uint64, updatedBy string) (PublicProxy, []map[string]any, error) {
	if err := s.ensureWritable(ctx, proxyDomain, false); err != nil {
		return PublicProxy{}, nil, err
	}
	pool, err := s.GetProxyPool(ctx)
	if err != nil {
		return PublicProxy{}, nil, err
	}
	current, err := s.store.GetProxy(ctx, proxyID)
	if err != nil {
		return PublicProxy{}, nil, err
	}
	if expectedRevision != current.Revision {
		return PublicProxy{}, nil, store.ErrConflict
	}
	removed := current.BoundVMIDs
	ids := current.BoundVMIDs
	if vmID != "" {
		ids = filterString(ids, vmID)
		removed = []string{vmID}
	} else {
		ids = nil
	}
	record, err := s.store.PutProxy(ctx, store.PutProxyInput{
		ID: current.ID, ExpectedRevision: current.Revision, Scheme: current.Scheme, Host: current.Host,
		Port: current.Port, Enabled: current.Enabled, Status: current.Status, BoundVMIDs: ids, UpdatedBy: updatedBy,
	})
	if err != nil {
		return PublicProxy{}, nil, err
	}
	workers := s.enqueueSlotReloads(ctx, removed, record.ID, "proxy_unbind")
	return PublicProxyFromStore(record, pool.Data.BindLimit), workers, nil
}

func (s *Service) DeleteProxy(ctx context.Context, id string, expectedRevision uint64) error {
	if err := s.ensureWritable(ctx, proxyDomain, false); err != nil {
		return err
	}
	return s.store.DeleteProxy(ctx, id, expectedRevision)
}

func (s *Service) RevealProxy(ctx context.Context, id string) (string, error) {
	if err := s.ensureReadable(ctx, proxyDomain); err != nil {
		return "", err
	}
	return s.store.RevealProxyURI(ctx, id)
}

func (s *Service) ProbeProxy(ctx context.Context, id string) (PublicProxy, map[string]any, error) {
	if err := s.ensureWritable(ctx, proxyDomain, false); err != nil {
		return PublicProxy{}, nil, err
	}
	pool, err := s.GetProxyPool(ctx)
	if err != nil {
		return PublicProxy{}, nil, err
	}
	current, err := s.store.GetProxy(ctx, id)
	if err != nil {
		return PublicProxy{}, nil, err
	}
	ok, latency, errText := ProbeSOCKS5(current.Host, current.Port, pool.Data.ProbeTimeoutMS, current.HasAuth)
	status := "ok"
	enabled := current.Enabled
	if !ok {
		status = "fail"
		if current.ConsecutiveFailures+1 >= pool.Data.MaxFailures {
			status = "dead"
			enabled = false
		}
	}
	record, err := s.store.UpdateProxyProbe(ctx, id, status, latency, errText, &enabled)
	if err != nil {
		return PublicProxy{}, nil, err
	}
	return PublicProxyFromStore(record, pool.Data.BindLimit), map[string]any{"ok": ok, "latency_ms": latency, "error": errText}, nil
}

func (s *Service) ensureReadable(ctx context.Context, domain string) error {
	owner, err := s.store.GetDomainOwner(ctx, domain)
	if err != nil {
		return err
	}
	if owner.State == "shadow_import" || owner.Owner == "go" || owner.State == "shadow_match" || owner.State == "frozen" {
		return nil
	}
	return fmt.Errorf("%w: domain %s is owned by %s in state %s", store.ErrConflict, domain, owner.Owner, owner.State)
}

func (s *Service) enqueueBoundReloads(ctx context.Context, record store.ProxyRecord, reason string) []map[string]any {
	return s.enqueueSlotReloads(ctx, record.BoundVMIDs, record.ID, reason)
}

func (s *Service) enqueueSlotReloads(ctx context.Context, slotIDs []string, proxyID, reason string) []map[string]any {
	workers := make([]map[string]any, 0, len(slotIDs))
	for _, slotID := range slotIDs {
		payload, _ := json.Marshal(map[string]any{"slot_id": slotID, "proxy_id": proxyID, "reason": reason, "require_running": true})
		_, err := s.store.EnqueueOperation(ctx, store.EnqueueOperationInput{
			Kind: "slot.reload", Target: slotID, Payload: payload, MaxAttempts: 3,
			IdempotencyKey: fmt.Sprintf("proxy-reload-%s-%s-%s", proxyID, slotID, reason),
		})
		workers = append(workers, map[string]any{"vm_id": slotID, "ok": err == nil, "error": errorString(err)})
	}
	return workers
}

func proxyIdentity(host string, port int, username string) string {
	return strings.ToLower(strings.TrimSpace(host)) + ":" + fmt.Sprintf("%d", port) + ":" + username
}

func newProxyID() (string, error) {
	var raw [4]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return "px-" + hex.EncodeToString(raw[:]), nil
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func filterString(items []string, drop string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item != drop {
			out = append(out, item)
		}
	}
	return out
}

func errorString(err error) any {
	if err == nil {
		return nil
	}
	return err.Error()
}
