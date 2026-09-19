package platformproducer

import (
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"testing"
)

func TestDNSInputsRequireCompleteOwnedConfiguration(t *testing.T) {
	for _, scenario := range []string{"valid", "extra field", "no authority", "no clients", "wrong owner", "no probe policy", "wrong scope", "wrong generation", "unknown template", "partial template"} {
		t.Run(scenario, func(t *testing.T) {
			consumers := []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 45}}
			probe := &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 90, MaxConcurrency: 4, MaxProbes: 1024}
			p := DNSPolicyInput{SchemaVersion: platformconfig.SchemaVersion, Generation: "policy", Scope: "global", Authorities: []platformconfig.DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 45, RefreshSeconds: 600, RetrySeconds: 90, ExpireSeconds: 7200}}, Clients: []platformconfig.DNSClientPolicy{{NodeID: "dns-a", Rules: []platformconfig.DNSClientRule{}}}, DNSReadiness: probe, TLSReadiness: probe, Cohorts: []platformconfig.TrafficRolloutCohort{{ID: "first", EdgeGroupIDs: []string{"edge-group-a"}}}}
			templates := []HostedZoneTemplate{{NodeID: "dns-a", TemplateZone: "example.test"}}
			switch scenario {
			case "no authority":
				p.Authorities = nil
			case "no clients":
				p.Clients = nil
			case "wrong owner":
				p.Authorities[0].NodeID = "other"
			case "no probe policy":
				p.DNSReadiness = nil
			case "wrong scope":
				p.Scope = "other"
			case "wrong generation":
				p.Generation = "other"
			case "unknown template":
				templates[0].TemplateZone = "unknown.test"
			case "partial template":
				templates = append(templates, templates[0])
			}
			raw, _ := json.Marshal(p)
			a := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Generation: "policy", ScopeKey: "global"}
			json.Unmarshal(raw, &a.Content)
			if scenario == "extra field" {
				a.Content["route_constraints"] = []any{}
			}
			_, err := DecodeDNSInputs(a, consumers, templates)
			if (scenario == "valid") != (err == nil) {
				t.Fatal("invalid DNS policy boundary", err)
			}
		})
	}
}
