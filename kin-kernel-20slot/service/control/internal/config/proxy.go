package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"kin.local/kin-control/internal/store"
)

const proxyDomain = "proxy"

type ProxyPoolConfig struct {
	ProbeIntervalMin  int  `json:"probe_interval_min"`
	ProbeTimeoutMS    int  `json:"probe_timeout_ms"`
	MaxFailures       int  `json:"max_failures"`
	Enabled           bool `json:"enabled"`
	DisconnectOnError bool `json:"disconnect_on_error"`
	BindLimit         int  `json:"bind_limit"`
}

type PublicProxy struct {
	ID                  string    `json:"id"`
	Revision            uint64    `json:"revision"`
	Host                string    `json:"host"`
	Port                int       `json:"port"`
	HasAuth             bool      `json:"has_auth"`
	Status              string    `json:"status"`
	Enabled             bool      `json:"enabled"`
	BoundVMID           *string   `json:"bound_vm_id"`
	BoundVMIDs          []string  `json:"bound_vm_ids"`
	BoundCount          int       `json:"bound_count"`
	BindLimit           int       `json:"bind_limit"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	LatencyMS           *int      `json:"latency_ms"`
	LastProbeAt         string    `json:"last_probe_at,omitempty"`
	LastError           string    `json:"last_error,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
}

type ProxyPoolSnapshot struct {
	Config  DocumentEnvelope[ProxyPoolConfig] `json:"config"`
	Totals  map[string]int                    `json:"totals"`
	Proxies []PublicProxy                     `json:"proxies"`
}

func DefaultProxyPool() ProxyPoolConfig {
	return ProxyPoolConfig{
		ProbeIntervalMin:  10,
		ProbeTimeoutMS:    8000,
		MaxFailures:       2,
		Enabled:           true,
		DisconnectOnError: false,
		BindLimit:         5,
	}
}

func ValidateProxyPool(value ProxyPoolConfig) error {
	switch value.ProbeIntervalMin {
	case 5, 10, 30, 60:
	default:
		return errors.New("probe_interval_min must be 5, 10, 30 or 60")
	}
	if value.ProbeTimeoutMS < 1000 || value.ProbeTimeoutMS > 60000 {
		return errors.New("probe_timeout_ms must be between 1000 and 60000")
	}
	if value.MaxFailures < 1 || value.MaxFailures > 10 {
		return errors.New("max_failures must be between 1 and 10")
	}
	if value.BindLimit < 1 || value.BindLimit > 32 {
		return errors.New("bind_limit must be between 1 and 32")
	}
	return nil
}

func MergeProxyPool(current ProxyPoolConfig, patch json.RawMessage) (ProxyPoolConfig, error) {
	base, err := json.Marshal(current)
	if err != nil {
		return ProxyPoolConfig{}, err
	}
	var baseMap map[string]any
	var patchMap map[string]any
	if err := decodeMap(base, &baseMap); err != nil {
		return ProxyPoolConfig{}, err
	}
	if err := decodeMap(patch, &patchMap); err != nil {
		return ProxyPoolConfig{}, fmt.Errorf("invalid proxy pool patch: %w", err)
	}
	mergeMap(baseMap, patchMap)
	merged, err := json.Marshal(baseMap)
	if err != nil {
		return ProxyPoolConfig{}, err
	}
	result, err := DecodeStrict[ProxyPoolConfig](merged)
	if err != nil {
		return ProxyPoolConfig{}, err
	}
	return result, ValidateProxyPool(result)
}

func ProxyPoolHashes(value ProxyPoolConfig) (configHash, runtimeHash, kernelHash string, err error) {
	configHash, err = Hash(value)
	if err != nil {
		return "", "", "", err
	}
	runtimeHash = configHash
	kernelHash, err = Hash(struct {
		BindLimit         int  `json:"bind_limit"`
		DisconnectOnError bool `json:"disconnect_on_error"`
	}{value.BindLimit, value.DisconnectOnError})
	return configHash, runtimeHash, kernelHash, err
}

func PublicProxyFromStore(record store.ProxyRecord, bindLimit int) PublicProxy {
	ids := append([]string{}, record.BoundVMIDs...)
	var bound *string
	if len(ids) > 0 {
		bound = &ids[0]
	}
	return PublicProxy{
		ID: record.ID, Revision: record.Revision, Host: record.Host, Port: record.Port, HasAuth: record.HasAuth,
		Status: record.Status, Enabled: record.Enabled, BoundVMID: bound, BoundVMIDs: ids,
		BoundCount: len(ids), BindLimit: bindLimit, ConsecutiveFailures: record.ConsecutiveFailures,
		LatencyMS: record.LatencyMS, LastProbeAt: record.LastProbeAt, LastError: record.LastError,
		CreatedAt: record.CreatedAt,
	}
}

func ProxyTotals(items []PublicProxy, bindLimit int, probing bool) map[string]int {
	totals := map[string]int{
		"total": len(items), "free": 0, "open": 0, "bound": 0, "ok": 0, "dead": 0,
		"probing": boolToInt(probing), "slots_used": 0, "slots_cap": len(items) * bindLimit, "bind_limit": bindLimit,
	}
	for _, item := range items {
		if item.Enabled && item.BoundCount == 0 && item.Status != "dead" {
			totals["free"]++
		}
		if item.Enabled && item.Status != "dead" && item.BoundCount < bindLimit {
			totals["open"]++
		}
		if item.BoundCount > 0 {
			totals["bound"]++
		}
		if item.Enabled && item.Status == "ok" {
			totals["ok"]++
		}
		if !item.Enabled || item.Status == "dead" {
			totals["dead"]++
		}
		totals["slots_used"] += item.BoundCount
	}
	return totals
}

func ProbeSOCKS5(host string, port, timeoutMS int, hasAuth bool) (ok bool, latencyMS int, errText string) {
	started := time.Now()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)), time.Duration(timeoutMS)*time.Millisecond)
	if err != nil {
		return false, int(time.Since(started).Milliseconds()), err.Error()
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Duration(timeoutMS) * time.Millisecond))
	method := byte(0x00)
	if hasAuth {
		method = 0x02
	}
	if _, err := conn.Write([]byte{0x05, 0x01, method}); err != nil {
		return false, int(time.Since(started).Milliseconds()), err.Error()
	}
	buf := make([]byte, 2)
	n, err := conn.Read(buf)
	latencyMS = int(time.Since(started).Milliseconds())
	if err != nil {
		return false, latencyMS, err.Error()
	}
	if n >= 2 && buf[0] == 0x05 {
		if buf[1] == 0xff {
			return false, latencyMS, "no_acceptable_auth_method"
		}
		if buf[1] == 0x00 || buf[1] == 0x02 {
			return true, latencyMS, ""
		}
	}
	return n > 0, latencyMS, ""
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
