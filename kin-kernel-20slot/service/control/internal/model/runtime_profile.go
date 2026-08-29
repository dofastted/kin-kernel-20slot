package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

type RuntimeProfile struct {
	ExecutionMode      string   `json:"execution_mode"`
	SystemLayout       string   `json:"system_layout"`
	Timezone           string   `json:"timezone"`
	SlotCount          int      `json:"slot_count"`
	Socks5             string   `json:"socks5"`
	AllowedModels      []string `json:"allowed_models"`
	AllowedServerTools []string `json:"allowed_server_tools"`
	AllowedBetas       []string `json:"allowed_betas"`
	MaxBodyBytes       int      `json:"max_body_bytes"`
	MaxOutputTokens    int      `json:"max_output_tokens"`
}

// ConfigHash normalizes the profile through a map[string]any round-trip
// (encoding/json sorts map keys alphabetically on Marshal) so the hash is
// stable regardless of struct field construction order, then hashes the
// resulting canonical, whitespace-free JSON with SHA-256.
func (p RuntimeProfile) ConfigHash() (string, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	var asMap map[string]any
	if err := json.Unmarshal(raw, &asMap); err != nil {
		return "", err
	}
	normalized, err := json.Marshal(asMap)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(normalized)
	return hex.EncodeToString(sum[:]), nil
}
