package platformproducer

import (
	"encoding/json"
	"strings"
	"testing"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestCellProducerPolicyCannotCrossAuthorityOrUseImplicitInputs(t *testing.T) {
	for _, scenario := range []string{"valid", "global owner", "other owner", "other target", "missing authority", "global target", "missing policy", "implicit domains", "implicit defaults", "implicit query", "bad cell"} {
		t.Run(scenario, func(t *testing.T) {
			p := Policy{SchemaVersion: Schema, Generation: "policy", AuthorityCellID: "cell-a", TargetScope: "authority-cell:cell-a", Mode: "shadow", InputSource: "business-static-intent", StaticIntentArtifactID: "intent", StaticIntentDigest: "sha256:" + strings.Repeat("a", 64), DNSPolicyArtifactID: "dns", DNSPolicyDigest: "sha256:" + strings.Repeat("b", 64), IntervalSeconds: 30, RefreshSeconds: 120, RequireApplicationDomains: true, RequireRouteDefaults: true, RequireDNSQueryPolicy: true}
			scope := "platform-config-producer:cell-a"
			switch scenario {
			case "global owner":
				scope = Scope
			case "other owner":
				scope = "platform-config-producer:cell-b"
			case "other target":
				p.TargetScope = "authority-cell:cell-b"
			case "missing authority":
				p.AuthorityCellID = ""
			case "global target":
				p.TargetScope = "global"
			case "missing policy":
				p.DNSPolicyArtifactID = ""
			case "implicit domains":
				p.RequireApplicationDomains = false
			case "implicit defaults":
				p.RequireRouteDefaults = false
			case "implicit query":
				p.RequireDNSQueryPolicy = false
			case "bad cell":
				p.AuthorityCellID = "cell-a--b"
				p.TargetScope = "authority-cell:" + p.AuthorityCellID
				scope = Scope + ":" + p.AuthorityCellID
			}
			raw, _ := json.Marshal(p)
			a := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: scope, Generation: p.Generation}
			json.Unmarshal(raw, &a.Content)
			_, err := Decode(a)
			if (err == nil) != (scenario == "valid") {
				t.Fatal(scenario, err)
			}
		})
	}
}

func TestPinnedCellInputsRequireSameAuthorityAndExactMembership(t *testing.T) {
	for _, scenario := range []string{"valid", "static scope", "static authority", "dns scope", "dns authority", "membership digest", "missing member", "legacy alias", "ambient placement", "missing query"} {
		t.Run(scenario, func(t *testing.T) {
			intent := platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Generation: "intent", Scope: "authority-cell:cell-a", AuthorityCellID: "cell-a", EdgeTopology: &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: "cell-a"}}, Pools: []edgetopology.ServingPool{{ID: "pool-public"}}, Edges: []edgetopology.Edge{{ID: "node-a", AuthorityCellID: "cell-a", ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "node-a"}}}}, DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "cell-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}}
			topology, err := platformconfig.TrafficConsumerTopologyFromIntent(intent)
			if err != nil {
				t.Fatal(err)
			}
			digest, _ := platformconfig.Digest(topology)
			raw, _ := json.Marshal(intent)
			a := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPlatformIntent, ScopeKey: intent.Scope, Generation: intent.Generation}
			json.Unmarshal(raw, &a.Content)
			static, err := DecodeStaticIntent(a)
			if err != nil {
				t.Fatal(err)
			}
			policy := Policy{AuthorityCellID: "cell-a", TargetScope: intent.Scope}
			dns := ProjectionPolicyInput{Scope: intent.Scope, AuthorityCellID: "cell-a", ConsumerTopologyDigest: digest, DNSPlacementMode: platformconfig.DNSPlacementConsumerReadiness, DNSQueryPolicy: &platformconfig.DNSQueryPolicy{RankingMode: "disabled", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 60, MaximumTTLSeconds: 120}}
			switch scenario {
			case "static scope":
				static.Scope = "global"
			case "static authority":
				static.AuthorityCellID = "cell-b"
			case "dns scope":
				dns.Scope = "global"
			case "dns authority":
				dns.AuthorityCellID = "cell-b"
			case "membership digest":
				dns.ConsumerTopologyDigest = "sha256:" + strings.Repeat("b", 64)
			case "missing member":
				static.Consumers = nil
			case "legacy alias":
				static.EdgeTopology.Cells[0].LegacyGroupID = "edge-group-old"
			case "ambient placement":
				dns.DNSPlacementMode = ""
			case "missing query":
				dns.DNSQueryPolicy = nil
			}
			if err := ValidatePinnedSources(policy, static, &dns); (err == nil) != (scenario == "valid") {
				t.Fatal(scenario, err)
			}
		})
	}
}
