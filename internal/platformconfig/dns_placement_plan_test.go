package platformconfig

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func consumerPlacementFixture() CompileRequest {
	r := dnsQueryFixture()
	r.Policy.DNSPlacementMode = DNSPlacementConsumerReadiness
	r.Policy.DNSQueryPolicy = &DNSQueryPolicy{RankingMode: "disabled", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 60, MaximumTTLSeconds: 120}
	r.Policy.TLSReadiness = r.Policy.DNSReadiness
	r.Policy.DNSAuthorities = []DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}}
	r.Policy.DNSClientPolicies = []DNSClientPolicy{{NodeID: "dns-a"}}
	r.Policy.TrafficRolloutCohorts = []TrafficRolloutCohort{{ID: "first", EdgeGroupIDs: []string{"edge-group-a"}}}
	r.RuntimeSnapshot.DNSPlacements = nil
	return r
}

func TestDNSConsumerPlacementCompilesNewRouteWithoutServingAndReplays(t *testing.T) {
	r := consumerPlacementFixture()
	r.Intent.Routes[0].UpstreamURL = "http://new-origin:8080"
	first, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(r)
	if len(flattenedRecords(t, first)) != 1 || len(compiledQueryViews(t, first)) != 1 {
		t.Fatal("candidate not complete")
	}
	for _, record := range compiledQueryViews(t, first)[0].Records {
		for _, c := range record.Candidates {
			if c.Healthy || c.RouteReady || c.TLSReady || c.ServingGeneration != "" {
				t.Fatal("planning fabricated readiness")
			}
		}
	}
	if !DNSArtifactRequiresTrafficRelease(first.DNSArtifact) {
		t.Fatal("unleased plan escaped traffic-only admission")
	}
	r.CreatedAt = time.Now().Add(time.Hour)
	second, err := Compile(r)
	if err != nil || !reflect.DeepEqual(first.DNSArtifact.Content, second.DNSArtifact.Content) {
		t.Fatal("replay depended on live state", err)
	}
	r.CreatedAt = time.Time{}
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("planning mutated input")
	}
}

func TestDNSConsumerPlacementRequiresCompleteExecutionBindings(t *testing.T) {
	for name, mutate := range map[string]func(*CompileRequest){
		"unknown mode":   func(r *CompileRequest) { r.Policy.DNSPlacementMode = "bypass" },
		"query strategy": func(r *CompileRequest) { r.Policy.DNSQueryPolicy = nil },
		"DNS readiness":  func(r *CompileRequest) { r.Policy.DNSReadiness = nil },
		"TLS readiness":  func(r *CompileRequest) { r.Policy.TLSReadiness = nil },
		"authorities":    func(r *CompileRequest) { r.Policy.DNSAuthorities = nil },
		"clients":        func(r *CompileRequest) { r.Policy.DNSClientPolicies = nil },
		"cohorts":        func(r *CompileRequest) { r.Policy.TrafficRolloutCohorts = nil },
		"consumers":      func(r *CompileRequest) { r.Intent.DNSConsumers = nil },
		"mixed lease facts": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements = placementFixture().RuntimeSnapshot.DNSPlacements
		},
		"missing rule":          func(r *CompileRequest) { r.Policy.DNSAnswerRules = nil },
		"wrong candidate owner": func(r *CompileRequest) { r.RuntimeSnapshot.DNSSelections[0].Candidates[0].EdgeID = "foreign" },
		"duplicate address": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSEdgeEndpoints[1].A = r.RuntimeSnapshot.DNSEdgeEndpoints[0].A
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := consumerPlacementFixture()
			mutate(&r)
			if _, err := Compile(r); err == nil {
				t.Fatal("unbound candidate compiled")
			}
		})
	}
}
