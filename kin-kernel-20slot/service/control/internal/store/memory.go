package store

import (
	"errors"
	"sort"
	"sync"
	"time"

	"kin.local/kin-control/internal/model"
)

var ErrNotFound = errors.New("not found")

type Memory struct {
	mu       sync.RWMutex
	kernels  map[string]model.Kernel
	policies map[string]model.RoutePolicy
	profile  *model.RuntimeProfile
	revision uint64
}

func NewMemory() *Memory {
	return &Memory{
		kernels:  make(map[string]model.Kernel),
		policies: make(map[string]model.RoutePolicy),
	}
}

func (m *Memory) UpsertKernel(reg model.KernelRegistration, now time.Time) model.Kernel {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.kernels[reg.ID]
	if !ok {
		existing = model.Kernel{ID: reg.ID}
	}
	existing.Address = reg.Address
	existing.Capacity = reg.Capacity
	existing.Provider = reg.Provider
	existing.Revision = reg.Revision
	existing.Status = model.KernelHealthy
	existing.LastHeartbeat = now.UTC()
	m.kernels[reg.ID] = existing
	m.revision++
	return existing
}

func (m *Memory) ListKernels() []model.Kernel {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]model.Kernel, 0, len(m.kernels))
	for _, kernel := range m.kernels {
		result = append(result, kernel)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (m *Memory) Heartbeat(id string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	kernel, ok := m.kernels[id]
	if !ok {
		return ErrNotFound
	}
	kernel.LastHeartbeat = now.UTC()
	kernel.Status = model.KernelHealthy
	m.kernels[id] = kernel
	return nil
}

func (m *Memory) SetDraining(id string, draining bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	kernel, ok := m.kernels[id]
	if !ok {
		return ErrNotFound
	}
	kernel.Draining = draining
	m.kernels[id] = kernel
	m.revision++
	return nil
}

func (m *Memory) MarkStale(now time.Time, timeout time.Duration) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var changed []string
	for id, kernel := range m.kernels {
		if now.Sub(kernel.LastHeartbeat) <= timeout || kernel.Status == model.KernelUnhealthy {
			continue
		}
		kernel.Status = model.KernelUnhealthy
		m.kernels[id] = kernel
		changed = append(changed, id)
		m.revision++
	}
	sort.Strings(changed)
	return changed
}

func (m *Memory) PutPolicy(policy model.RoutePolicy) model.RoutePolicy {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.policies[policy.Name] = policy
	m.revision++
	return policy
}

func (m *Memory) GetPolicy(name string) (model.RoutePolicy, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	policy, ok := m.policies[name]
	if !ok {
		return model.RoutePolicy{}, ErrNotFound
	}
	return policy, nil
}

func (m *Memory) ListPolicies() []model.RoutePolicy {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]model.RoutePolicy, 0, len(m.policies))
	for _, policy := range m.policies {
		result = append(result, policy)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func (m *Memory) Revision() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.revision
}

func (m *Memory) SetRuntimeProfile(profile model.RuntimeProfile) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.profile = &profile
	m.revision++
}

func (m *Memory) GetRuntimeProfile() (model.RuntimeProfile, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.profile == nil {
		return model.RuntimeProfile{}, false
	}
	return *m.profile, true
}

