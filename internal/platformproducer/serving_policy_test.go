package platformproducer

import (
	"encoding/json"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestServingPolicyHasExplicitBoundsAndPinnedSources(t *testing.T) {
	for name, change := range map[string]func(*Policy){
		"valid":                            func(*Policy) {},
		"missing settings":                 func(p *Policy) { p.Serving = nil },
		"legacy input":                     func(p *Policy) { p.InputSource = "business-migration" },
		"implicit domains":                 func(p *Policy) { p.RequireApplicationDomains = false },
		"implicit route policy":            func(p *Policy) { p.RequireRouteDefaults = false },
		"implicit query":                   func(p *Policy) { p.RequireDNSQueryPolicy = false },
		"no gray observation":              func(p *Policy) { p.Serving.GrayMinSeconds = 0 },
		"no full observation":              func(p *Policy) { p.Serving.FullMinSeconds = 0 },
		"unbounded timeout":                func(p *Policy) { p.Serving.RolloutTimeoutSeconds = 3601 },
		"timeout shorter than observation": func(p *Policy) { p.Serving.RolloutTimeoutSeconds = 60 },
		"undeclared arbitrary selector":    func(p *Policy) { p.Serving.CanaryRuleRef = "all=true" },
	} {
		t.Run(name, func(t *testing.T) {
			p := Policy{SchemaVersion: Schema, Generation: "policy", Mode: "serving", InputSource: "business-static-intent", TargetScope: "global", IntervalSeconds: 30, RefreshSeconds: 300, StaticIntentArtifactID: "intent", StaticIntentDigest: "sha256:" + strings.Repeat("a", 64), DNSPolicyArtifactID: "input", DNSPolicyDigest: "sha256:" + strings.Repeat("b", 64), RequireApplicationDomains: true, RequireRouteDefaults: true, RequireDNSQueryPolicy: true, Serving: &ServingPolicy{CanaryRuleRef: "cohort=first", GrayMinSeconds: 60, FullMinSeconds: 60, RolloutTimeoutSeconds: 300}}
			change(&p)
			raw, _ := json.Marshal(p)
			var content map[string]any
			json.Unmarshal(raw, &content)
			_, err := Decode(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: Scope, Generation: "policy", Content: content})
			if (name == "valid") != (err == nil) {
				t.Fatal("policy boundary differs", err)
			}
			if name == "valid" {
				content["serving"].(map[string]any)["shell"] = "arbitrary"
				if _, err = Decode(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: Scope, Generation: "policy", Content: content}); err == nil {
					t.Fatal("arbitrary action accepted")
				}
			}
		})
	}
}
