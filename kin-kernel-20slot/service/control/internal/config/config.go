package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

const SchemaVersion = 1

var (
	namePattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]{0,127}$`)
	modelIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
)

type StickyConfig struct {
	Enabled    bool     `json:"enabled"`
	Mode       string   `json:"mode"`
	TTLSeconds int      `json:"ttl_seconds"`
	HeaderKeys []string `json:"header_keys"`
	BodyKeys   []string `json:"body_keys"`
}

type ConcurrencyConfig struct {
	DefaultMaxPerAccount  int `json:"default_max_per_account"`
	DefaultKeyConcurrency int `json:"default_key_concurrency"`
	DefaultMaxRPM         int `json:"default_max_rpm,omitempty"`
	FableMaxPerAccount    int `json:"fable_max_per_account"`
}

type WeeklySplitConfig struct {
	Enabled    bool    `json:"enabled"`
	FableShare float64 `json:"fable_share"`
}

type QuotaConfig struct {
	SafetyRatio       float64           `json:"safety_ratio"`
	WeeklySafetyRatio float64           `json:"weekly_safety_ratio"`
	WarnRatio         float64           `json:"warn_ratio"`
	BlockOn5h         bool              `json:"block_on_5h"`
	BlockOn7d         bool              `json:"block_on_7d"`
	WeeklySplit       WeeklySplitConfig `json:"weekly_split"`
}

type TierPolicy struct {
	MaxConcurrency    int     `json:"max_concurrency"`
	MaxRPM            int     `json:"max_rpm"`
	Limit5h           float64 `json:"limit_5h"`
	Limit7d           float64 `json:"limit_7d"`
	MaxSessions       int     `json:"max_sessions"`
	SessionIdleMin    int     `json:"session_idle_min"`
	SafetyRatio       float64 `json:"safety_ratio"`
	WeeklySafetyRatio float64 `json:"weekly_safety_ratio"`
	WarnRatio         float64 `json:"warn_ratio"`
}

type PoolConfig struct {
	Strategy              string `json:"strategy"`
	ManualScheduleWins    bool   `json:"manual_schedule_wins"`
	MaxWaitersPerAccount  int    `json:"max_waiters_per_account"`
	FallbackWaitTimeoutMS int    `json:"fallback_wait_timeout_ms"`
	StickyWaitTimeoutMS   int    `json:"sticky_wait_timeout_ms"`
	WorkerHealthTTLMS     int    `json:"worker_health_ttl_ms"`
	HeartbeatStaleMS      int    `json:"heartbeat_stale_ms"`
}

type FailoverConfig struct {
	MaxAccountSwitches   int    `json:"max_account_switches"`
	MaxTotalAttempts     int    `json:"max_total_attempts"`
	TotalRetryDeadlineMS int    `json:"total_retry_deadline_ms"`
	DeliveryMode         string `json:"delivery_mode"`
	Oauth401CooldownMS   int    `json:"oauth_401_cooldown_ms"`
	StreamKeepaliveMS    int    `json:"stream_keepalive_ms"`
	SignatureRepair      bool   `json:"signature_repair"`
}

type InferenceConfig struct {
	Engine       string `json:"engine"`
	FallbackToGo bool   `json:"fallback_to_go"`
}

type TemplateCacheControl struct {
	Type string `json:"type"`
}

type PersonaTemplateBlock struct {
	ID           string                `json:"id,omitempty"`
	Note         string                `json:"note,omitempty"`
	DropIfEmpty  bool                  `json:"drop_if_empty,omitempty"`
	Hide         bool                  `json:"hide,omitempty"`
	Type         string                `json:"type,omitempty"`
	Text         string                `json:"text"`
	CacheControl *TemplateCacheControl `json:"cache_control,omitempty"`
}

type PersonaRule struct {
	ID          string   `json:"id"`
	Enabled     bool     `json:"enabled"`
	NoToolsOnly bool     `json:"no_tools_only"`
	Match       []string `json:"match"`
	Append      string   `json:"append"`
}

type CompatibilityConfig struct {
	ToolNameRewrite   bool                              `json:"tool_name_rewrite"`
	CacheControlLimit int                               `json:"cache_control_limit"`
	PersonaInject     string                            `json:"persona_inject,omitempty"`
	PersonaAgent      string                            `json:"persona_agent,omitempty"`
	PersonaPark       bool                              `json:"persona_park,omitempty"`
	PersonaParkStyle  string                            `json:"persona_park_style,omitempty"`
	PersonaExpansion  string                            `json:"persona_expansion,omitempty"`
	PersonaPreset     string                            `json:"persona_preset,omitempty"`
	PersonaTemplates  map[string][]PersonaTemplateBlock `json:"persona_templates"`
	OverlayPreset     string                            `json:"overlay_preset"`
	OverlayTemplates  map[string][]PersonaTemplateBlock `json:"overlay_templates"`
	PersonaStanding   string                            `json:"persona_standing"`
	PersonaLeakAppend string                            `json:"persona_leak_append"`
	PersonaRules      []PersonaRule                     `json:"persona_rules"`
	CacheTTL          string                            `json:"cache_ttl"`
}

type RoutingConfig struct {
	Sticky        StickyConfig          `json:"sticky"`
	Concurrency   ConcurrencyConfig     `json:"concurrency"`
	Quota         QuotaConfig           `json:"quota"`
	Tiers         map[string]TierPolicy `json:"tiers"`
	Pool          PoolConfig            `json:"pool"`
	Failover      FailoverConfig        `json:"failover"`
	Inference     InferenceConfig       `json:"inference"`
	Compatibility CompatibilityConfig   `json:"compatibility"`
}

func DefaultRouting() RoutingConfig {
	defaultTier := TierPolicy{MaxConcurrency: 2, MaxRPM: 0, Limit5h: .85, Limit7d: .8, MaxSessions: 0, SessionIdleMin: 5, SafetyRatio: .85, WeeklySafetyRatio: .8, WarnRatio: .75}
	maxTier := TierPolicy{MaxConcurrency: 4, MaxRPM: 0, Limit5h: .95, Limit7d: .95, MaxSessions: 0, SessionIdleMin: 5, SafetyRatio: .95, WeeklySafetyRatio: .95, WarnRatio: .85}
	return RoutingConfig{
		Sticky:        StickyConfig{Enabled: true, Mode: "conversation", TTLSeconds: 86400, HeaderKeys: []string{"x-session-id", "x-conversation-id", "x-claude-code-session-id", "session-id", "thread-id"}, BodyKeys: []string{"conversation_id", "session_id", "thread_id", "prompt_cache_key"}},
		Concurrency:   ConcurrencyConfig{DefaultMaxPerAccount: 2, DefaultKeyConcurrency: 2, FableMaxPerAccount: 4},
		Quota:         QuotaConfig{SafetyRatio: .85, WeeklySafetyRatio: .8, WarnRatio: .75, BlockOn5h: true, BlockOn7d: true, WeeklySplit: WeeklySplitConfig{FableShare: .5}},
		Tiers:         map[string]TierPolicy{"default": defaultTier, "pro": defaultTier, "max": maxTier},
		Pool:          PoolConfig{Strategy: "weighted-round-robin", ManualScheduleWins: true, MaxWaitersPerAccount: 32, FallbackWaitTimeoutMS: 30000, StickyWaitTimeoutMS: 45000, WorkerHealthTTLMS: 5000, HeartbeatStaleMS: 15000},
		Failover:      FailoverConfig{MaxAccountSwitches: 10, MaxTotalAttempts: 12, TotalRetryDeadlineMS: 120000, DeliveryMode: "realtime", Oauth401CooldownMS: 120000, StreamKeepaliveMS: 10000},
		Inference:     InferenceConfig{Engine: "go", FallbackToGo: true},
		Compatibility: CompatibilityConfig{ToolNameRewrite: true, CacheControlLimit: 4, PersonaInject: "rewrite", PersonaAgent: "default", PersonaParkStyle: "user", PersonaExpansion: "official_tools", PersonaTemplates: map[string][]PersonaTemplateBlock{"official": {}, "zero": {}, "custom": {}}, OverlayPreset: "off", OverlayTemplates: map[string][]PersonaTemplateBlock{"official": {}, "minimal": {}, "custom": {}}, CacheTTL: "5m"},
	}
}

type ModelCapabilities struct {
	ContextWindow             int    `json:"context_window"`
	Supports1M                bool   `json:"supports_1m"`
	ThinkingMode              string `json:"thinking_mode"`
	SupportsAdaptive          bool   `json:"supports_adaptive"`
	RequiresAdaptive          bool   `json:"requires_adaptive"`
	SupportsInterleaved       bool   `json:"supports_interleaved"`
	SupportsEffort            bool   `json:"supports_effort"`
	SupportsContextManagement bool   `json:"supports_context_management"`
}

type ModelBetas struct {
	Required      []string `json:"required"`
	Drop          []string `json:"drop"`
	AllowClient   bool     `json:"allow_client"`
	PassContext1M bool     `json:"pass_context_1m"`
}

type ModelParams struct {
	MaxTokensDefault       int    `json:"max_tokens_default"`
	MaxTokensCap           int    `json:"max_tokens_cap"`
	ThinkingFallbackBudget int    `json:"thinking_fallback_budget"`
	OnAdaptive             string `json:"on_adaptive"`
	OnEnabled              string `json:"on_enabled"`
}

type ModelDefinition struct {
	Enabled      bool              `json:"enabled"`
	DisplayName  string            `json:"display_name"`
	Family       string            `json:"family"`
	Sort         int               `json:"sort"`
	Capabilities ModelCapabilities `json:"capabilities"`
	Betas        ModelBetas        `json:"betas"`
	Params       ModelParams       `json:"params"`
	Aliases      []string          `json:"aliases"`
}

type ModelDefaults struct {
	Enabled                bool     `json:"enabled"`
	MaxTokens              int      `json:"max_tokens"`
	ThinkingFallbackBudget int      `json:"thinking_fallback_budget"`
	StripContext1M         bool     `json:"strip_context_1m"`
	Context1MWhitelist     []string `json:"context_1m_whitelist"`
	NormalizeThinking      bool     `json:"normalize_thinking"`
}

type ModelPolicy struct {
	Version     int                        `json:"version"`
	UpdatedAt   string                     `json:"updated_at"`
	Source      string                     `json:"source"`
	Defaults    ModelDefaults              `json:"defaults"`
	Models      map[string]ModelDefinition `json:"models"`
	Aliases     map[string]string          `json:"aliases"`
	CatalogMode string                     `json:"catalog_mode"`
}

type SlotPolicy struct {
	Tier            string   `json:"tier,omitempty"`
	MaxConcurrency  *int     `json:"max_concurrency,omitempty"`
	MaxRPM          *int     `json:"max_rpm,omitempty"`
	AllowedModels   []string `json:"allowed_models,omitempty"`
	Weight          *int     `json:"weight,omitempty"`
	Schedulable     *bool    `json:"schedulable,omitempty"`
	InferenceEngine *string  `json:"inference_engine,omitempty"`
	PersonaPreset   *string  `json:"persona_preset,omitempty"`
}

type EffectiveSlotPolicy struct {
	Tier            string   `json:"tier"`
	MaxConcurrency  int      `json:"max_concurrency"`
	MaxRPM          int      `json:"max_rpm"`
	AllowedModels   []string `json:"allowed_models,omitempty"`
	Weight          int      `json:"weight"`
	Schedulable     bool     `json:"schedulable"`
	InferenceEngine string   `json:"inference_engine"`
	PersonaPreset   string   `json:"persona_preset"`
}

type ApplyStatus struct {
	State             string `json:"state"`
	DesiredRevision   uint64 `json:"desired_revision"`
	EffectiveRevision uint64 `json:"effective_revision"`
	LastError         string `json:"last_error,omitempty"`
}

type DocumentEnvelope[T any] struct {
	SchemaVersion int         `json:"schema_version"`
	Revision      uint64      `json:"revision"`
	ConfigHash    string      `json:"config_hash"`
	RuntimeHash   string      `json:"runtime_hash"`
	KernelHash    string      `json:"kernel_hash"`
	Data          T           `json:"data"`
	Apply         ApplyStatus `json:"apply"`
	Degraded      bool        `json:"degraded"`
}

func MergeRouting(current RoutingConfig, patch json.RawMessage) (RoutingConfig, error) {
	base, err := json.Marshal(current)
	if err != nil {
		return RoutingConfig{}, err
	}
	var baseMap map[string]any
	var patchMap map[string]any
	if err := decodeMap(base, &baseMap); err != nil {
		return RoutingConfig{}, err
	}
	if err := decodeMap(patch, &patchMap); err != nil {
		return RoutingConfig{}, fmt.Errorf("invalid routing patch: %w", err)
	}
	mergeMap(baseMap, patchMap)
	merged, err := json.Marshal(baseMap)
	if err != nil {
		return RoutingConfig{}, err
	}
	result, err := DecodeStrict[RoutingConfig](merged)
	if err != nil {
		return RoutingConfig{}, err
	}
	result = NormalizeRouting(result)
	return result, ValidateRouting(result)
}
func DecodeStrict[T any](raw json.RawMessage) (T, error) {
	var value T
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("decode typed config: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return value, errors.New("typed config must contain exactly one value")
	}
	return value, nil
}

func MergeSlotPolicy(current SlotPolicy, patch json.RawMessage) (SlotPolicy, error) {
	base, err := json.Marshal(current)
	if err != nil {
		return SlotPolicy{}, err
	}
	var baseMap map[string]any
	var patchMap map[string]any
	if err := decodeMap(base, &baseMap); err != nil {
		return SlotPolicy{}, err
	}
	if err := decodeMap(patch, &patchMap); err != nil {
		return SlotPolicy{}, fmt.Errorf("invalid slot patch: %w", err)
	}
	for key, value := range patchMap {
		if value == nil {
			delete(baseMap, key)
			continue
		}
		baseMap[key] = value
	}
	merged, err := json.Marshal(baseMap)
	if err != nil {
		return SlotPolicy{}, err
	}
	result, err := DecodeStrict[SlotPolicy](merged)
	if err != nil {
		return SlotPolicy{}, err
	}
	if len(result.AllowedModels) == 0 {
		result.AllowedModels = nil
	}
	return result, ValidateSlotPolicy(result)
}

func decodeMap(raw []byte, target *map[string]any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if *target == nil {
		return errors.New("object required")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("object must contain exactly one value")
	}
	return nil
}

func NormalizeRouting(value RoutingConfig) RoutingConfig {
	if tier, ok := value.Tiers["default"]; ok {
		value.Concurrency.DefaultMaxPerAccount = tier.MaxConcurrency
		value.Concurrency.DefaultKeyConcurrency = tier.MaxConcurrency
		value.Concurrency.DefaultMaxRPM = tier.MaxRPM
		value.Quota.SafetyRatio = tier.Limit5h
		value.Quota.WeeklySafetyRatio = tier.Limit7d
		value.Quota.WarnRatio = tier.WarnRatio
	}
	return value
}

func mergeMap(dst, patch map[string]any) {
	for key, value := range patch {
		patchMap, patchOK := value.(map[string]any)
		dstMap, dstOK := dst[key].(map[string]any)
		if patchOK && dstOK {
			mergeMap(dstMap, patchMap)
			continue
		}
		dst[key] = value
	}
}

func ValidateRouting(value RoutingConfig) error {
	if value.Sticky.Mode != "conversation" && value.Sticky.Mode != "none" {
		return errors.New("sticky.mode must be conversation or none")
	}
	if value.Sticky.TTLSeconds < 1 || value.Sticky.TTLSeconds > 30*24*60*60 {
		return errors.New("sticky.ttl_seconds must be between 1 and 2592000")
	}
	if err := validateStringList("sticky.header_keys", value.Sticky.HeaderKeys, 32, 128); err != nil {
		return err
	}
	if err := validateStringList("sticky.body_keys", value.Sticky.BodyKeys, 32, 128); err != nil {
		return err
	}
	if value.Concurrency.DefaultMaxPerAccount < 1 || value.Concurrency.DefaultMaxPerAccount > 1000 || value.Concurrency.DefaultKeyConcurrency < 1 || value.Concurrency.DefaultKeyConcurrency > 1000 || value.Concurrency.DefaultMaxRPM < 0 || value.Concurrency.FableMaxPerAccount < 1 || value.Concurrency.FableMaxPerAccount > 1000 {
		return errors.New("concurrency values are outside allowed bounds")
	}
	if err := ratioOrder(value.Quota.WarnRatio, value.Quota.SafetyRatio, value.Quota.WeeklySafetyRatio); err != nil {
		return fmt.Errorf("quota: %w", err)
	}
	if value.Quota.WeeklySplit.FableShare < 0 || value.Quota.WeeklySplit.FableShare > 1 {
		return errors.New("quota.weekly_split.fable_share must be between 0 and 1")
	}
	for _, tier := range []string{"default", "pro", "max"} {
		policy, ok := value.Tiers[tier]
		if !ok {
			return fmt.Errorf("tiers.%s is required", tier)
		}
		if err := validateTier(tier, policy); err != nil {
			return err
		}
	}
	for tier := range value.Tiers {
		if tier != "default" && tier != "pro" && tier != "max" {
			return fmt.Errorf("unknown tier %q", tier)
		}
	}
	if value.Pool.Strategy != "weighted-round-robin" {
		return errors.New("pool.strategy must be weighted-round-robin")
	}
	if value.Pool.MaxWaitersPerAccount < 0 || value.Pool.MaxWaitersPerAccount > 10000 || value.Pool.FallbackWaitTimeoutMS < 0 || value.Pool.StickyWaitTimeoutMS < 0 || value.Pool.WorkerHealthTTLMS < 100 || value.Pool.HeartbeatStaleMS < value.Pool.WorkerHealthTTLMS {
		return errors.New("pool values are outside allowed bounds")
	}
	if value.Failover.MaxAccountSwitches < 0 || value.Failover.MaxTotalAttempts < 1 || value.Failover.MaxTotalAttempts > 100 || value.Failover.MaxAccountSwitches > value.Failover.MaxTotalAttempts || value.Failover.TotalRetryDeadlineMS < 1000 || value.Failover.TotalRetryDeadlineMS > 15*60*1000 || value.Failover.Oauth401CooldownMS < 0 || value.Failover.StreamKeepaliveMS < 1000 {
		return errors.New("failover values are outside allowed bounds")
	}
	if value.Failover.DeliveryMode != "realtime" && value.Failover.DeliveryMode != "verified" {
		return errors.New("failover.delivery_mode must be realtime or verified")
	}
	if value.Inference.Engine != "go" && value.Inference.Engine != "rust" {
		return errors.New("inference.engine must be go or rust")
	}
	return validateCompatibility(value.Compatibility)
}

func validateTier(name string, value TierPolicy) error {
	if value.MaxConcurrency < 1 || value.MaxConcurrency > 1000 || value.MaxRPM < 0 || value.MaxSessions < 0 || value.SessionIdleMin < 0 || value.SessionIdleMin > 24*60 {
		return fmt.Errorf("tiers.%s values are outside allowed bounds", name)
	}
	if value.Limit5h < 0 || value.Limit5h > 1 || value.Limit7d < 0 || value.Limit7d > 1 {
		return fmt.Errorf("tiers.%s limits must be between 0 and 1", name)
	}
	if err := ratioOrder(value.WarnRatio, value.SafetyRatio, value.WeeklySafetyRatio); err != nil {
		return fmt.Errorf("tiers.%s: %w", name, err)
	}
	return nil
}

func ratioOrder(warn, five, seven float64) error {
	for _, value := range []float64{warn, five, seven} {
		if value < 0 || value > 1 {
			return errors.New("ratios must be between 0 and 1")
		}
	}
	if warn > five || warn > seven {
		return errors.New("warn_ratio must not exceed safety ratios")
	}
	return nil
}

func validateCompatibility(value CompatibilityConfig) error {
	if value.CacheControlLimit < 0 || value.CacheControlLimit > 32 {
		return errors.New("compatibility.cache_control_limit must be between 0 and 32")
	}
	if value.PersonaPreset != "" && value.PersonaPreset != "official" && value.PersonaPreset != "zero" && value.PersonaPreset != "custom" {
		return errors.New("compatibility.persona_preset is invalid")
	}
	if value.OverlayPreset != "off" && value.OverlayPreset != "official" && value.OverlayPreset != "minimal" && value.OverlayPreset != "custom" {
		return errors.New("compatibility.overlay_preset is invalid")
	}
	if len(value.PersonaStanding) > 40000 || len(value.PersonaLeakAppend) > 40000 {
		return errors.New("compatibility persona text exceeds 40000 characters")
	}
	if err := validateTemplateMap(value.PersonaTemplates, []string{"official", "zero", "custom"}, false); err != nil {
		return fmt.Errorf("compatibility.persona_templates: %w", err)
	}
	if err := validateTemplateMap(value.OverlayTemplates, []string{"official", "minimal", "custom"}, true); err != nil {
		return fmt.Errorf("compatibility.overlay_templates: %w", err)
	}
	ids := map[string]bool{}
	for _, rule := range value.PersonaRules {
		if !namePattern.MatchString(rule.ID) || ids[rule.ID] {
			return fmt.Errorf("invalid or duplicate persona rule id %q", rule.ID)
		}
		ids[rule.ID] = true
		if len(rule.Match) > 64 || len(rule.Append) > 40000 {
			return fmt.Errorf("persona rule %q exceeds size limits", rule.ID)
		}
		for _, pattern := range rule.Match {
			if len(pattern) > 1024 {
				return fmt.Errorf("persona rule %q regex exceeds 1024 characters", rule.ID)
			}
			if _, err := regexp.Compile(pattern); err != nil {
				return fmt.Errorf("persona rule %q has invalid regex: %w", rule.ID, err)
			}
		}
	}
	if value.CacheTTL != "5m" && value.CacheTTL != "1h" {
		return errors.New("compatibility.cache_ttl must be 5m or 1h")
	}
	return nil
}

func validateTemplateMap(templates map[string][]PersonaTemplateBlock, allowed []string, overlay bool) error {
	allowedSet := map[string]bool{}
	for _, name := range allowed {
		allowedSet[name] = true
	}
	for name, blocks := range templates {
		if !allowedSet[name] {
			return fmt.Errorf("unknown preset %q", name)
		}
		if len(blocks) > 24 {
			return fmt.Errorf("preset %q exceeds 24 blocks", name)
		}
		total := 0
		wrappers := 0
		for index, block := range blocks {
			if len(block.Text) > 40000 {
				return fmt.Errorf("preset %q block %d exceeds 40000 characters", name, index)
			}
			total += len(block.Text)
			if overlay {
				if block.Type != "wrapper" && block.Type != "body" {
					return fmt.Errorf("preset %q block %d type must be wrapper or body", name, index)
				}
				if block.Type == "wrapper" {
					wrappers++
					if !strings.Contains(block.Text, "{{overlay_body}}") {
						return fmt.Errorf("preset %q wrapper must contain {{overlay_body}}", name)
					}
				}
			} else if block.Type != "" && block.Type != "text" {
				return fmt.Errorf("preset %q block %d type must be text", name, index)
			}
			if block.CacheControl != nil && block.CacheControl.Type != "ephemeral" {
				return fmt.Errorf("preset %q block %d cache_control.type must be ephemeral", name, index)
			}
		}
		if total > 200000 {
			return fmt.Errorf("preset %q exceeds 200000 characters", name)
		}
		if overlay && len(blocks) > 0 && wrappers != 1 {
			return fmt.Errorf("preset %q must contain exactly one wrapper", name)
		}
	}
	return nil
}

func ValidateModelPolicy(value ModelPolicy) error {
	if value.Version < 1 {
		return errors.New("model policy version must be positive")
	}
	if value.CatalogMode != "policy_only" && value.CatalogMode != "worker_intersect_policy" && value.CatalogMode != "worker_only" {
		return errors.New("model policy catalog_mode is invalid")
	}
	if len(value.Models) == 0 || len(value.Models) > 1000 {
		return errors.New("model policy models must contain between 1 and 1000 entries")
	}
	aliases := map[string]string{}
	for id, model := range value.Models {
		if !modelIDPattern.MatchString(id) {
			return fmt.Errorf("invalid model id %q", id)
		}
		if err := validateModel(id, model); err != nil {
			return err
		}
		for _, alias := range model.Aliases {
			key := strings.ToLower(strings.TrimSpace(alias))
			if !modelIDPattern.MatchString(key) {
				return fmt.Errorf("model %q has invalid alias %q", id, alias)
			}
			if prior, ok := aliases[key]; ok && prior != id {
				return fmt.Errorf("alias %q targets both %q and %q", alias, prior, id)
			}
			aliases[key] = id
		}
	}
	for alias, target := range value.Aliases {
		key := strings.ToLower(strings.TrimSpace(alias))
		if !modelIDPattern.MatchString(key) || !modelIDPattern.MatchString(target) {
			return fmt.Errorf("invalid global alias %q", alias)
		}
		aliases[key] = target
	}
	for alias := range aliases {
		if err := validateAliasChain(alias, aliases, value.Models); err != nil {
			return err
		}
	}
	if value.Defaults.MaxTokens < 1 || value.Defaults.MaxTokens > 10_000_000 || value.Defaults.ThinkingFallbackBudget < 0 || value.Defaults.ThinkingFallbackBudget > value.Defaults.MaxTokens {
		return errors.New("model policy default token limits are invalid")
	}
	return validateStringList("defaults.context_1m_whitelist", value.Defaults.Context1MWhitelist, 1000, 128)
}

func validateModel(id string, value ModelDefinition) error {
	if len(value.DisplayName) > 256 || len(value.Family) > 64 || value.Sort < 0 || value.Sort > 100000 {
		return fmt.Errorf("model %q metadata exceeds bounds", id)
	}
	if value.Capabilities.ContextWindow < 1 || value.Capabilities.ContextWindow > 10_000_000 {
		return fmt.Errorf("model %q context_window is outside bounds", id)
	}
	if value.Capabilities.ThinkingMode != "enabled_only" && value.Capabilities.ThinkingMode != "adaptive_or_enabled" && value.Capabilities.ThinkingMode != "adaptive_only" {
		return fmt.Errorf("model %q thinking_mode is invalid", id)
	}
	if value.Params.MaxTokensDefault < 1 || value.Params.MaxTokensCap < value.Params.MaxTokensDefault || value.Params.MaxTokensCap > 10_000_000 || value.Params.ThinkingFallbackBudget < 0 || value.Params.ThinkingFallbackBudget > value.Params.MaxTokensCap {
		return fmt.Errorf("model %q token limits are invalid", id)
	}
	if !validThinkingAction(value.Params.OnAdaptive) || !validThinkingAction(value.Params.OnEnabled) {
		return fmt.Errorf("model %q thinking action is invalid", id)
	}
	if err := validateStringList("model betas.required", value.Betas.Required, 128, 256); err != nil {
		return fmt.Errorf("model %q: %w", id, err)
	}
	if err := validateStringList("model betas.drop", value.Betas.Drop, 128, 256); err != nil {
		return fmt.Errorf("model %q: %w", id, err)
	}
	return validateStringList("model aliases", value.Aliases, 128, 128)
}

func validThinkingAction(value string) bool {
	return value == "passthrough" || value == "convert_to_enabled" || value == "convert_to_adaptive" || value == "strip" || value == "reject"
}

func validateAliasChain(start string, aliases map[string]string, models map[string]ModelDefinition) error {
	seen := map[string]bool{}
	current := start
	for {
		if seen[current] {
			return fmt.Errorf("model alias cycle contains %q", current)
		}
		seen[current] = true
		target, ok := aliases[current]
		if !ok {
			if _, exists := models[current]; !exists {
				return fmt.Errorf("model alias %q has missing target %q", start, current)
			}
			return nil
		}
		if _, exists := models[target]; exists {
			return nil
		}
		current = strings.ToLower(target)
	}
}

func ValidateSlotPolicy(value SlotPolicy) error {
	if value.Tier != "" && value.Tier != "default" && value.Tier != "pro" && value.Tier != "max" {
		return errors.New("slot tier must be default, pro, max, or empty")
	}
	if value.MaxConcurrency != nil && (*value.MaxConcurrency < 1 || *value.MaxConcurrency > 1000) {
		return errors.New("slot max_concurrency must be between 1 and 1000")
	}
	if value.MaxRPM != nil && (*value.MaxRPM < 0 || *value.MaxRPM > 1_000_000) {
		return errors.New("slot max_rpm must be between 0 and 1000000")
	}
	if value.Weight != nil && (*value.Weight < 1 || *value.Weight > 1000) {
		return errors.New("slot weight must be between 1 and 1000")
	}
	if value.InferenceEngine != nil && *value.InferenceEngine != "go" && *value.InferenceEngine != "rust" {
		return errors.New("slot inference_engine must be go, rust, or inherit")
	}
	if value.PersonaPreset != nil && *value.PersonaPreset != "official" && *value.PersonaPreset != "zero" {
		return errors.New("slot persona_preset must be official, zero, or inherit")
	}
	return validateStringList("slot allowed_models", value.AllowedModels, 1000, 128)
}

func ResolveSlotPolicy(routing RoutingConfig, value SlotPolicy) EffectiveSlotPolicy {
	tier := value.Tier
	if tier == "" {
		tier = "default"
	}
	tierPolicy, ok := routing.Tiers[tier]
	if !ok {
		tierPolicy = routing.Tiers["default"]
	}
	result := EffectiveSlotPolicy{Tier: tier, MaxConcurrency: tierPolicy.MaxConcurrency, MaxRPM: tierPolicy.MaxRPM, Weight: 1, Schedulable: true, InferenceEngine: routing.Inference.Engine, PersonaPreset: routing.Compatibility.PersonaPreset}
	if result.PersonaPreset == "" {
		result.PersonaPreset = "official"
	}
	if value.MaxConcurrency != nil {
		result.MaxConcurrency = *value.MaxConcurrency
	}
	if value.MaxRPM != nil {
		result.MaxRPM = *value.MaxRPM
	}
	if len(value.AllowedModels) > 0 {
		result.AllowedModels = append([]string(nil), value.AllowedModels...)
	}
	if value.Weight != nil {
		result.Weight = *value.Weight
	}
	if value.Schedulable != nil {
		result.Schedulable = *value.Schedulable
	}
	if value.InferenceEngine != nil {
		result.InferenceEngine = *value.InferenceEngine
	}
	if value.PersonaPreset != nil {
		result.PersonaPreset = *value.PersonaPreset
	}
	return result
}

func RoutingHashes(value RoutingConfig) (configHash, runtimeHash, kernelHash string, err error) {
	configHash, err = Hash(value)
	if err != nil {
		return "", "", "", err
	}
	runtimeHash, err = Hash(struct {
		Engine string `json:"engine"`
	}{value.Inference.Engine})
	if err != nil {
		return "", "", "", err
	}
	kernelHash, err = Hash(struct {
		Sticky        StickyConfig          `json:"sticky"`
		Concurrency   ConcurrencyConfig     `json:"concurrency"`
		Quota         QuotaConfig           `json:"quota"`
		Tiers         map[string]TierPolicy `json:"tiers"`
		Pool          PoolConfig            `json:"pool"`
		Failover      FailoverConfig        `json:"failover"`
		Inference     InferenceConfig       `json:"inference"`
		Compatibility CompatibilityConfig   `json:"compatibility"`
	}{value.Sticky, value.Concurrency, value.Quota, value.Tiers, value.Pool, value.Failover, value.Inference, value.Compatibility})
	return configHash, runtimeHash, kernelHash, err
}

func ModelPolicyHashes(value ModelPolicy) (configHash, runtimeHash, kernelHash string, err error) {
	configHash, err = Hash(value)
	if err != nil {
		return "", "", "", err
	}
	projection := value
	projection.UpdatedAt = ""
	projection.Source = ""
	kernelHash, err = Hash(projection)
	return configHash, configHash, kernelHash, err
}

func SlotPolicyHashes(value SlotPolicy, effective EffectiveSlotPolicy) (configHash, runtimeHash, kernelHash string, err error) {
	configHash, err = Hash(value)
	if err != nil {
		return "", "", "", err
	}
	runtimeHash, err = Hash(struct {
		InferenceEngine string `json:"inference_engine"`
		PersonaPreset   string `json:"persona_preset"`
	}{effective.InferenceEngine, effective.PersonaPreset})
	if err != nil {
		return "", "", "", err
	}
	kernelHash, err = Hash(effective)
	return configHash, runtimeHash, kernelHash, err
}

func Hash(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func validateStringList(field string, values []string, maxItems, maxLength int) error {
	if len(values) > maxItems {
		return fmt.Errorf("%s exceeds %d items", field, maxItems)
	}
	seen := map[string]bool{}
	for _, value := range values {
		if strings.TrimSpace(value) == "" || len(value) > maxLength {
			return fmt.Errorf("%s contains an empty or oversized value", field)
		}
		if seen[value] {
			return fmt.Errorf("%s contains duplicate %q", field, value)
		}
		seen[value] = true
	}
	return nil
}

func SortedModelIDs(value ModelPolicy) []string {
	ids := make([]string, 0, len(value.Models))
	for id := range value.Models {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
