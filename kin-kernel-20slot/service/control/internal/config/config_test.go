package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDefaultRoutingAndMergeValidation(t *testing.T) {
	base := DefaultRouting()
	if err := ValidateRouting(base); err != nil {
		t.Fatalf("default routing is invalid: %v", err)
	}
	if base.Inference.Engine != "go" || !base.Inference.FallbackToGo {
		t.Fatalf("default inference must stay on go worker: %#v", base.Inference)
	}

	merged, err := MergeRouting(base, json.RawMessage(`{"inference":{"engine":"rust"}}`))
	if err != nil {
		t.Fatalf("merge inference engine: %v", err)
	}
	if merged.Inference.Engine != "rust" || merged.Sticky.Mode != base.Sticky.Mode {
		t.Fatalf("merge lost defaults: %#v", merged)
	}
	if _, err := MergeRouting(base, json.RawMessage(`{"unknown":true}`)); err == nil {
		t.Fatal("unknown routing field was accepted")
	}
	if _, err := MergeRouting(base, json.RawMessage(`{"compatibility":{"tools":{"name_pattern":"["}}}`)); err == nil {
		t.Fatal("invalid RE2 expression was accepted")
	}
}

func TestPersonaTemplateBounds(t *testing.T) {
	value := DefaultRouting()
	value.Compatibility.OverlayTemplates["custom"] = []PersonaTemplateBlock{{Type: "body", Text: "x"}}
	if err := ValidateRouting(value); err == nil || !strings.Contains(err.Error(), "wrapper") {
		t.Fatalf("expected wrapper error, got %v", err)
	}
	value.Compatibility.OverlayTemplates["custom"] = []PersonaTemplateBlock{{Type: "wrapper", Text: "before {{overlay_body}} after"}}
	if err := ValidateRouting(value); err != nil {
		t.Fatalf("valid wrapper rejected: %v", err)
	}
	value.Compatibility.PersonaTemplates["custom"] = []PersonaTemplateBlock{{Type: "text", Text: strings.Repeat("x", 40001)}}
	if err := ValidateRouting(value); err == nil {
		t.Fatal("oversized persona block was accepted")
	}
}

func TestModelPolicyAliasValidation(t *testing.T) {
	policy := minimalModelPolicy()
	policy.Aliases = map[string]string{"friendly": "missing"}
	if err := ValidateModelPolicy(policy); err == nil || !strings.Contains(err.Error(), "missing target") {
		t.Fatalf("expected missing alias target, got %v", err)
	}
	policy.Aliases = map[string]string{"one": "two", "two": "one"}
	if err := ValidateModelPolicy(policy); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected alias cycle, got %v", err)
	}
	policy = minimalModelPolicy()
	model := policy.Models["claude-test"]
	model.Params.MaxTokensCap = model.Params.MaxTokensDefault - 1
	policy.Models["claude-test"] = model
	if err := ValidateModelPolicy(policy); err == nil || !strings.Contains(err.Error(), "token limits") {
		t.Fatalf("expected token bound error, got %v", err)
	}
}

func TestSlotInheritanceMergeAndPrecedence(t *testing.T) {
	routing := DefaultRouting()
	routing.Tiers["pro"] = TierPolicy{MaxConcurrency: 9, MaxRPM: 90, Limit5h: .85, Limit7d: .8, SessionIdleMin: 5, SafetyRatio: .85, WeeklySafetyRatio: .8, WarnRatio: .75}
	maxConcurrency := 17
	engine := "rust"
	policy := SlotPolicy{Tier: "pro", MaxConcurrency: &maxConcurrency, InferenceEngine: &engine}
	effective := ResolveSlotPolicy(routing, policy)
	if effective.MaxConcurrency != 17 || effective.MaxRPM != 90 || effective.InferenceEngine != "rust" {
		t.Fatalf("slot precedence mismatch: %#v", effective)
	}

	merged, err := MergeSlotPolicy(policy, json.RawMessage(`{"max_concurrency":null,"allowed_models":[]}`))
	if err != nil {
		t.Fatalf("clear slot override: %v", err)
	}
	if merged.MaxConcurrency != nil || merged.AllowedModels != nil {
		t.Fatalf("null/empty did not restore inheritance: %#v", merged)
	}
	if ResolveSlotPolicy(routing, merged).MaxConcurrency != 9 {
		t.Fatal("tier value did not apply after clearing slot override")
	}
}

func TestProjectionHashesIsolateRestartFields(t *testing.T) {
	base := DefaultRouting()
	_, runtimeBase, kernelBase, err := RoutingHashes(base)
	if err != nil {
		t.Fatal(err)
	}
	hot := base
	hot.Compatibility.PersonaTemplates = map[string][]PersonaTemplateBlock{"custom": {{Type: "text", Text: "hello"}}}
	_, runtimeHot, kernelHot, err := RoutingHashes(hot)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeHot != runtimeBase {
		t.Fatal("hot persona template changed restart hash")
	}
	if kernelHot == kernelBase {
		t.Fatal("kernel projection ignored persona template")
	}
	restart := base
	restart.Inference.Engine = "rust"
	_, runtimeRestart, _, err := RoutingHashes(restart)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeRestart == runtimeBase {
		t.Fatal("inference engine did not change restart hash")
	}
}

func minimalModelPolicy() ModelPolicy {
	return ModelPolicy{
		Version:  1,
		Defaults: ModelDefaults{Enabled: true, MaxTokens: 1024, ThinkingFallbackBudget: 128},
		Models: map[string]ModelDefinition{
			"claude-test": {
				Enabled: true, DisplayName: "Test", Family: "sonnet", Sort: 1,
				Capabilities: ModelCapabilities{ContextWindow: 200000, ThinkingMode: "enabled_only"},
				Betas:        ModelBetas{},
				Params:       ModelParams{MaxTokensDefault: 1024, MaxTokensCap: 4096, ThinkingFallbackBudget: 128, OnAdaptive: "passthrough", OnEnabled: "passthrough"},
			},
		},
		Aliases:     map[string]string{},
		CatalogMode: "policy_only",
	}
}
