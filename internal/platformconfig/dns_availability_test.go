package platformconfig

import (
	"encoding/json"
	"reflect"
	"testing"

	"fugue/internal/model"
)

func availabilityCompileFixture() CompileRequest {
	r := dnsQueryFixture()
	p := model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-b", "edge-a"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
	r.Policy.DNSQueryPolicy = &DNSQueryPolicy{RankingMode: "active", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 60, MaximumTTLSeconds: 120, AvailabilityRoutes: []DNSAvailabilityRoute{{Hostname: "app.example.test", Policy: p}}}
	r.Policy.DNSAnswerRules = []DNSAnswerRule{{NodeID: "dns-a", Hostname: "app.example.test", Type: "A", SelectionMode: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}, TTLSeconds: 60}}
	r.RuntimeSnapshot.DNSSelections[0].SelectedEdgeGroupID = ""
	r.RuntimeSnapshot.DNSSelections[0].ScopedCandidates = nil
	rebindPlacement(&r)
	return r
}

func TestAvailabilityCompilationBindsExactPolicyWithoutGrantingReadiness(t *testing.T) {
	input := availabilityCompileFixture()
	before, _ := json.Marshal(input)
	result, err := Compile(input)
	if err != nil {
		t.Fatal(err)
	}
	views := compiledQueryViews(t, result)
	policy := views[0].Records[0].AnswerPolicy.Availability
	if !reflect.DeepEqual(policy, &input.Policy.DNSQueryPolicy.AvailabilityRoutes[0].Policy) {
		t.Fatal("availability declaration lost")
	}
	for _, candidate := range views[0].Records[0].Candidates {
		if candidate.Healthy || candidate.RouteReady || candidate.TLSReady {
			t.Fatal("compiler invented readiness")
		}
	}
	after, _ := json.Marshal(input)
	if string(before) != string(after) {
		t.Fatal("compile changed signed input")
	}
	raw, _ := json.Marshal(result.DNSArtifact.Content["consumer_views"])
	var consumers []DNSConsumerView
	_ = json.Unmarshal(raw, &consumers)
	raw, _ = json.Marshal(result.DNSArtifact.Content["readiness_plan"])
	var plan DNSReadinessPlan
	_ = json.Unmarshal(raw, &plan)
	for _, name := range []string{"remove", "membership", "recovery", "version"} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(views)
			var changed []DNSQueryView
			_ = json.Unmarshal(raw, &changed)
			p := changed[0].Records[0].AnswerPolicy.Availability
			switch name {
			case "remove":
				changed[0].Records[0].AnswerPolicy.Availability = nil
			case "membership":
				p.OrderedEdgeIDs = append(p.OrderedEdgeIDs, "foreign")
			case "recovery":
				p.RecoverySuccesses = 2
			case "version":
				p.Version = "unknown"
			}
			if ValidateDNSQueryViews(changed, flattenedRecords(t, result), consumers, &plan, input.Policy) == nil {
				t.Fatal("modified availability authority accepted")
			}
		})
	}
	normalized := NormalizePolicySnapshot(input.Policy)
	normalized.DNSQueryPolicy.AvailabilityRoutes[0].Policy.OrderedEdgeIDs[0] = "foreign"
	if input.Policy.DNSQueryPolicy.AvailabilityRoutes[0].Policy.OrderedEdgeIDs[0] != "edge-b" {
		t.Fatal("normalization aliases intent")
	}
}

func TestAvailabilityCannotAuthorizeLegacyOrUnboundedRecovery(t *testing.T) {
	for _, name := range []string{"legacy", "too_slow", "duplicate_host", "scoped", "null_routes"} {
		t.Run(name, func(t *testing.T) {
			r := availabilityCompileFixture()
			switch name {
			case "legacy":
				r.Policy.DNSAnswerRules[0].SelectionMode = "pinned"
				r.Policy.DNSAnswerRules[0].PhysicalOrder = nil
			case "too_slow":
				r.Policy.DNSQueryPolicy.AvailabilityRoutes[0].Policy.RecoveryMinSeconds = 300
				r.Policy.DNSReadiness.ProbeIntervalSeconds = 5
			case "duplicate_host":
				r.Policy.DNSQueryPolicy.AvailabilityRoutes = append(r.Policy.DNSQueryPolicy.AvailabilityRoutes, r.Policy.DNSQueryPolicy.AvailabilityRoutes[0])
			case "scoped":
				r.Policy.DNSQueryPolicy.ECSEnabled = true
			case "null_routes":
				raw, _ := json.Marshal(r.Policy.DNSQueryPolicy)
				var f map[string]any
				_ = json.Unmarshal(raw, &f)
				f["availability_routes"] = nil
				raw, _ = json.Marshal(f)
				if json.Unmarshal(raw, new(DNSQueryPolicy)) == nil {
					t.Fatal("null authority accepted")
				}
				return
			}
			rebindPlacement(&r)
			if _, err := Compile(r); err == nil {
				t.Fatal("unsafe availability declaration compiled")
			}
		})
	}
}

func TestRedundantAvailabilityCompilationBindsExactAnswerCount(t *testing.T) {
	input := availabilityCompileFixture()
	policy := &input.Policy.DNSQueryPolicy.AvailabilityRoutes[0].Policy
	policy.Version, policy.MaxAnswers = model.DNSAvailabilityRedundantVersion, 2
	rebindPlacement(&input)
	compiled, err := Compile(input)
	if err != nil {
		t.Fatal(err)
	}
	record := compiledQueryViews(t, compiled)[0].Records[0]
	if !reflect.DeepEqual(record.AnswerPolicy.Availability, policy) {
		t.Fatal("compiler lost explicit redundant authority")
	}
	record.AnswerPolicy.Availability.MaxAnswers = 3
	if validateDNSAvailabilityIntent(record, input.Policy) == nil {
		t.Fatal("unsigned answer fanout accepted")
	}
	for _, candidate := range record.Candidates {
		if candidate.Healthy || candidate.TLSReady || candidate.RouteReady {
			t.Fatal("answer count invented readiness")
		}
	}
}
