package model

import "time"

type KernelStatus string

const (
	KernelHealthy   KernelStatus = "healthy"
	KernelUnhealthy KernelStatus = "unhealthy"
)

type Kernel struct {
	ID            string       `json:"id"`
	Address       string       `json:"address"`
	Capacity      int          `json:"capacity"`
	Provider      string       `json:"provider"`
	Revision      uint64       `json:"revision"`
	Status        KernelStatus `json:"status"`
	Draining      bool         `json:"draining"`
	LastHeartbeat time.Time    `json:"last_heartbeat"`
}

type KernelRegistration struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Capacity int    `json:"capacity"`
	Provider string `json:"provider"`
	Revision uint64 `json:"revision"`
}

type RoutePolicy struct {
	Name           string `json:"name"`
	Tenant         string `json:"tenant"`
	ModelPattern   string `json:"model_pattern"`
	Provider       string `json:"provider"`
	SlotGroup      string `json:"slot_group"`
	MaxInflight    int    `json:"max_inflight"`
	MaxWaitingTool int    `json:"max_waiting_tool"`
}

type Snapshot struct {
	Revision  uint64        `json:"revision"`
	IssuedAt  time.Time     `json:"issued_at"`
	ExpiresAt time.Time     `json:"expires_at"`
	Kernels   []Kernel      `json:"kernels"`
	Policies  []RoutePolicy `json:"policies"`
	Demo      bool          `json:"demo_unsigned"`
}
