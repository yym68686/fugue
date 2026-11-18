package platformconfig_test

import (
	"encoding/json"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func TestAvailabilityHandoffPermitsOnlyDeclaredPolicyAddition(t *testing.T) {
	route := platformconfig.DNSAvailabilityRoute{Hostname: "app.example.test", Policy: model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 20}}
	for _, scenario := range []string{"valid", "undeclared", "changed TTL", "changed order", "unrelated host", "unrelated query policy"} {
		t.Run(scenario, func(t *testing.T) {
			r := celldns.AvailabilityRequest(t)
			celldns.RetainGlobalOnly(&r)
			r.Policy.DNSQueryPolicy.AvailabilityRoutes = []platformconfig.DNSAvailabilityRoute{route}
			declared := route
			switch scenario {
			case "undeclared":
				declared.Hostname = "other.example.test"
			case "changed TTL":
				r.Policy.DNSAnswerRules[0].TTLSeconds++
			case "changed order":
				declared.Policy.OrderedEdgeIDs = []string{"edge-b", "edge-a"}
			case "unrelated host":
				r.Intent.DNS = append(r.Intent.DNS, platformconfig.DNSIntent{Hostname: "other.example.test", Type: "TXT", TTL: 60, Values: []string{"new"}})
			case "unrelated query policy":
				r.Policy.DNSQueryPolicy.MinimumTTLSeconds++
			}
			next := celldns.Compile(t, r).DNSArtifact
			before, _ := json.Marshal(next)
			err := platformconfig.ValidateDNSAvailabilityHandoff(r.PreviousTrafficPublication.DNS, next, "dns-a", declared)
			if (err == nil) != (scenario == "valid") {
				t.Fatal("unexpected scoped availability handoff", err)
			}
			if platformconfig.ValidateDNSAuthorityHandoff(r.PreviousTrafficPublication.DNS, next, "dns-a") == nil {
				t.Fatal("ordinary handoff silently changed availability")
			}
			after, _ := json.Marshal(next)
			if string(before) != string(after) {
				t.Fatal("handoff validation mutated signed artifact")
			}
		})
	}
}

func TestDNSAuthorityHandoffPreservesWholeNodeAnswerBehavior(t *testing.T) {
	req := celldns.TransitionRequest(t)
	compiled := celldns.Compile(t, req)
	if err := platformconfig.ValidateDNSAuthorityHandoff(req.PreviousTrafficPublication.DNS, compiled.DNSArtifact, "dns-a"); err != nil {
		t.Fatal(err)
	}
	if err := platformconfig.ValidateDNSAuthorityHandoff(req.PreviousTrafficPublication.DNS, compiled.DNSArtifact, "dns-other"); err == nil {
		t.Fatal("undeclared physical DNS node accepted")
	}
	for name, change := range map[string]func(*platformconfig.CompileRequest){
		"ttl": func(r *platformconfig.CompileRequest) { r.Policy.DNSAnswerRules[0].TTLSeconds++ },
		"nameserver": func(r *platformconfig.CompileRequest) {
			r.Policy.DNSAuthorities[0].Nameservers = []string{"other.example.test"}
		},
		"ecs": func(r *platformconfig.CompileRequest) {
			r.Policy.DNSAnswerRules[0].PhysicalOrder = nil
			r.Policy.DNSAnswerRules[0].SelectionMode = "global"
			r.Policy.DNSAnswerRules[0].ECSEnabled = true
		},
		"extra record": func(r *platformconfig.CompileRequest) {
			r.Intent.DNS = append(r.Intent.DNS, platformconfig.DNSIntent{Hostname: "extra.example.test", Type: "TXT", Values: []string{"new"}, TTL: 60})
		},
		"selection": func(r *platformconfig.CompileRequest) {
			r.Policy.DNSAnswerRules[0].PhysicalOrder = nil
			r.Policy.DNSAnswerRules[0].SelectionMode = "weighted"
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := celldns.TransitionRequest(t)
			change(&r)
			c := celldns.Compile(t, r)
			if err := platformconfig.ValidateDNSAuthorityHandoff(r.PreviousTrafficPublication.DNS, c.DNSArtifact, "dns-a"); err == nil {
				t.Fatal("changed node DNS behavior authorized handoff")
			}
		})
	}
}

func TestDNSAuthorityHandoffPreservesUnknownBehavior(t *testing.T) {
	for _, field := range []string{"query policy", "authority", "consumer view", "query view", "query record"} {
		t.Run(field, func(t *testing.T) {
			req := celldns.TransitionRequest(t)
			next := celldns.Compile(t, req).DNSArtifact
			policy := next.Content["policy"].(map[string]any)
			var row map[string]any
			switch field {
			case "query policy":
				row = policy["dns_query_policy"].(map[string]any)
			case "authority":
				row = policy["dns_authorities"].([]any)[0].(map[string]any)
			case "consumer view":
				row = next.Content["consumer_views"].([]any)[0].(map[string]any)
			case "query view":
				row = next.Content["query_views"].([]any)[0].(map[string]any)
			case "query record":
				row = next.Content["query_views"].([]any)[0].(map[string]any)["records"].([]any)[0].(map[string]any)
			}
			row["future_behavior"] = map[string]any{"enabled": true}
			before, err := json.Marshal(next)
			if err != nil {
				t.Fatal(err)
			}
			err = platformconfig.ValidateDNSAuthorityHandoff(req.PreviousTrafficPublication.DNS, next, "dns-a")
			prefix := "DNS handoff changes "
			if field == "query policy" {
				// This policy has an existing strict decoder, so unknown fields
				// must be rejected before the generic behavior comparison.
				prefix = "DNS artifact policy cannot be decoded: "
			}
			if err == nil || !strings.HasPrefix(err.Error(), prefix) {
				t.Fatal("unknown configured behavior was not compared", err)
			}
			after, _ := json.Marshal(next)
			if string(before) != string(after) {
				t.Fatal("handoff changed signed input")
			}
		})
	}
}
