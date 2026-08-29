package model

import "testing"

func TestRuntimeProfileConfigHashStableAcrossConstructionOrder(t *testing.T) {
	t.Parallel()

	a := RuntimeProfile{
		ExecutionMode:      "native_slot",
		SystemLayout:       "zero",
		Timezone:           "America/New_York",
		SlotCount:          20,
		Socks5:             "socks5h://127.0.0.1:1080",
		AllowedModels:      []string{"claude-opus-5", "claude-sonnet-5"},
		AllowedServerTools: []string{"web_search"},
		AllowedBetas:       []string{"beta-1"},
		MaxBodyBytes:       1 << 20,
		MaxOutputTokens:    8192,
	}

	b := RuntimeProfile{
		MaxOutputTokens:    8192,
		MaxBodyBytes:       1 << 20,
		AllowedBetas:       []string{"beta-1"},
		AllowedServerTools: []string{"web_search"},
		AllowedModels:      []string{"claude-opus-5", "claude-sonnet-5"},
		Socks5:             "socks5h://127.0.0.1:1080",
		SlotCount:          20,
		Timezone:           "America/New_York",
		SystemLayout:       "zero",
		ExecutionMode:      "native_slot",
	}

	hashA, err := a.ConfigHash()
	if err != nil {
		t.Fatalf("a.ConfigHash() error = %v", err)
	}
	hashB, err := b.ConfigHash()
	if err != nil {
		t.Fatalf("b.ConfigHash() error = %v", err)
	}
	if hashA != hashB {
		t.Fatalf("hashes differ despite identical field values: %s != %s", hashA, hashB)
	}
}

func TestRuntimeProfileConfigHashChangesWithField(t *testing.T) {
	t.Parallel()

	base := RuntimeProfile{
		ExecutionMode:      "native_slot",
		SystemLayout:       "zero",
		Timezone:           "America/New_York",
		SlotCount:          20,
		Socks5:             "socks5h://127.0.0.1:1080",
		AllowedModels:      []string{"claude-opus-5"},
		AllowedServerTools: []string{"web_search"},
		AllowedBetas:       []string{"beta-1"},
		MaxBodyBytes:       1 << 20,
		MaxOutputTokens:    8192,
	}
	baseHash, err := base.ConfigHash()
	if err != nil {
		t.Fatalf("base.ConfigHash() error = %v", err)
	}

	variants := []func(p RuntimeProfile) RuntimeProfile{
		func(p RuntimeProfile) RuntimeProfile { p.ExecutionMode = "native_messages"; return p },
		func(p RuntimeProfile) RuntimeProfile { p.SystemLayout = "identity"; return p },
		func(p RuntimeProfile) RuntimeProfile { p.Timezone = "UTC"; return p },
		func(p RuntimeProfile) RuntimeProfile { p.SlotCount = 1; return p },
		func(p RuntimeProfile) RuntimeProfile { p.Socks5 = ""; return p },
		func(p RuntimeProfile) RuntimeProfile {
			p.AllowedModels = []string{"claude-haiku-4-5"}
			return p
		},
		func(p RuntimeProfile) RuntimeProfile { p.AllowedServerTools = nil; return p },
		func(p RuntimeProfile) RuntimeProfile { p.AllowedBetas = nil; return p },
		func(p RuntimeProfile) RuntimeProfile { p.MaxBodyBytes = 1; return p },
		func(p RuntimeProfile) RuntimeProfile { p.MaxOutputTokens = 1; return p },
	}

	for i, mutate := range variants {
		mutated := mutate(base)
		hash, err := mutated.ConfigHash()
		if err != nil {
			t.Fatalf("variant %d ConfigHash() error = %v", i, err)
		}
		if hash == baseHash {
			t.Fatalf("variant %d produced identical hash to base: %s", i, hash)
		}
	}
}
