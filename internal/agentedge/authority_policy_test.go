package agentedge

import (
	"encoding/json"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func authorityPolicyFixture(t *testing.T) AuthorityPolicy {
	g, _, _, _ := grantFixture(t)
	return AuthorityPolicy{SchemaVersion: AuthorityPolicySchema, Generation: "policy-one", Scope: PolicyScope, Mode: "shadow", Origin: g.Origin, SigningKeyID: "key-one",
		TopologyIntentArtifactID: "intent-one", TopologyIntentDigest: g.Candidates[0].Publication.TopologyDigest,
		Constraint: platformconfig.EdgeSelectionConstraint{OwnerKind: "platform", Hostname: "api.example.test", AllowedPoolIDs: []string{"pool-public"}, RequiredCapabilities: []string{"http", "tls"}, MinCandidates: 1, MinDistinctCells: 1, MinDistinctDomains: map[string]int{"host": 1}, FactMaxAgeSeconds: g.Policy.FactMaxAgeSeconds},
		Selection:  g.Policy, Capacity: CapacityPolicy{MaxNodeCPUPercent: 85, MaxNodeMemoryPercent: 85, FactMaxAgeSeconds: 120}, GrantTTLSeconds: 60, MinimumLeaseSeconds: 20}
}

func TestIndependentAuthorityPolicyBindsTypedArtifactAndAllBounds(t *testing.T) {
	p := authorityPolicyFixture(t)
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"scope", "foreign host", "tenant route", "missing trust key", "probe budget", "freshness mismatch", "capacity missing", "digest", "unknown field"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(p)
			var content map[string]any
			json.Unmarshal(raw, &content)
			switch scenario {
			case "scope":
				content["scope"] = "global"
			case "foreign host":
				content["origin"] = "https://foreign.example.test"
			case "tenant route":
				content["constraint"].(map[string]any)["owner_kind"] = "tenant"
			case "missing trust key":
				content["signing_key_id"] = ""
			case "probe budget":
				content["minimum_lease_seconds"] = 1
			case "freshness mismatch":
				content["constraint"].(map[string]any)["fact_max_age_seconds"] = 60
			case "capacity missing":
				delete(content, "capacity")
			case "unknown field":
				content["script"] = "ignored input"
			}
			digest, _ := platformconfig.Digest(content)
			if scenario == "digest" {
				digest = "sha256:wrong"
			}
			a := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: PolicyScope, Generation: p.Generation, Content: content, ContentHash: digest}
			if _, err := DecodeAuthorityPolicy(a); err == nil {
				t.Fatal("invalid policy artifact accepted")
			}
		})
	}
	raw, _ := json.Marshal(p)
	var content map[string]any
	json.Unmarshal(raw, &content)
	digest, _ := platformconfig.Digest(content)
	if _, err := DecodeAuthorityPolicy(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: PolicyScope, Generation: p.Generation, Content: content, ContentHash: digest}); err != nil {
		t.Fatal("valid independent policy rejected", err)
	}
}
