package platformproducer

import (
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"testing"
)

func TestDNSInputsRequireCompleteOwnedConfiguration(t *testing.T) {
	for _, scenario := range []string{"valid", "null minimum", "null staleness", "null rules", "null states", "extra field", "no authority", "no clients", "wrong owner", "no probe policy", "wrong scope", "wrong generation", "unknown template", "partial template"} {
		t.Run(scenario, func(t *testing.T) {
			consumers := []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 45}}
			probe := &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 90, MaxConcurrency: 4, MaxProbes: 1024}
			p := ProjectionPolicyInput{SchemaVersion: platformconfig.SchemaVersion, Generation: "policy", Scope: "global", Authorities: []platformconfig.DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 45, RefreshSeconds: 600, RetrySeconds: 90, ExpireSeconds: 7200}}, Clients: []platformconfig.DNSClientPolicy{{NodeID: "dns-a", Rules: []platformconfig.DNSClientRule{}}}, DNSReadiness: probe, TLSReadiness: probe, Cohorts: []platformconfig.TrafficRolloutCohort{{ID: "first", EdgeGroupIDs: []string{"edge-group-a"}}}}
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
			switch scenario {
			case "null minimum":
				a.Content["minimum_healthy_edges"] = nil
			case "null staleness":
				a.Content["max_stale_seconds"] = nil
			case "null rules":
				a.Content["route_constraints"] = nil
			case "null states":
				a.Content["dns_route_state_constraints"] = nil
			}
			_, err := DecodeProjectionPolicy(a, consumers, templates)
			if (scenario == "valid") != (err == nil) {
				t.Fatal("invalid DNS policy boundary", err)
			}
		})
	}
}

func TestProjectionRouteDefaultsRejectPartialNullAndInvalidInputs(t *testing.T) {
	// Route group validation is independent of the DNS topology already covered
	// above. Decode must additionally distinguish omitted from explicit null.
	for _, scenario := range []string{"complete", "empty", "minimum missing", "staleness missing", "rules missing", "dns states missing", "zero minimum", "large minimum", "zero staleness", "large staleness", "duplicate rule", "bad dns state"} {
		t.Run(scenario, func(t *testing.T) {
			minimum, stale := 2, 90
			rules := []platformconfig.RoutePolicyConstraint{{ID: "default", Hostname: "app.example.test", RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true, MinHealthyEdgeNodes: 3}}
			states := []platformconfig.DNSRouteStateConstraint{{RecordKind: model.EdgeDNSRecordKindCustomDomainTarget, InactiveBehavior: "omit"}}
			p := ProjectionPolicyInput{Generation: "policy", MinimumHealthyEdges: &minimum, MaxStaleSeconds: &stale, RouteConstraints: &rules, DNSRouteStateConstraints: &states}
			switch scenario {
			case "empty":
				rules = []platformconfig.RoutePolicyConstraint{}
				states = []platformconfig.DNSRouteStateConstraint{}
			case "minimum missing":
				p.MinimumHealthyEdges = nil
			case "staleness missing":
				p.MaxStaleSeconds = nil
			case "rules missing":
				p.RouteConstraints = nil
			case "dns states missing":
				p.DNSRouteStateConstraints = nil
			case "zero minimum":
				minimum = 0
			case "large minimum":
				minimum = 10001
			case "zero staleness":
				stale = 0
			case "large staleness":
				stale = 604801
			case "duplicate rule":
				rules = append(rules, rules[0])
			case "bad dns state":
				states[0].InactiveBehavior = "ignore_proof"
			}
			base, present, err := p.RouteDefaults()
			valid := scenario == "complete" || scenario == "empty"
			if valid != (err == nil && present) {
				t.Fatal("invalid route default boundary", present, err)
			}
			if valid && (base.MinimumHealthyEdges != minimum || base.MaxStaleSeconds != stale) {
				t.Fatal("policy values not retained")
			}
		})
	}
}
