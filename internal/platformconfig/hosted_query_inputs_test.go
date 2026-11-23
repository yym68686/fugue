package platformconfig

import (
	"reflect"
	"slices"
	"testing"
	"time"
)

func TestHostedQueryInputsRetainSelectedForeignTimeAndRejectAmbiguousSources(t *testing.T) {
	fixture := dnsQueryFixture()
	rule, fact := fixture.Policy.DNSAnswerRules[0], fixture.RuntimeSnapshot.DNSSelections[0]
	foreign, selected := rule, fact
	foreign.Hostname, selected.Hostname = "peer.example.test", "peer.example.test"
	selected.ObservedAt = fact.ObservedAt.Add(-time.Hour)
	source := HostedAddressSource{PriorDNSSelections: []DNSSelectionObservation{fact, selected}}
	source.Prior.Policy.DNSAnswerRules = []DNSAnswerRule{rule, foreign}
	owned := map[string]bool{rule.Hostname: true}
	input := func() CompileRequest {
		r := fixture
		r.Policy.DNSAnswerRules = slices.Clone(source.Prior.Policy.DNSAnswerRules)
		r.RuntimeSnapshot.DNSSelections = slices.Clone(source.PriorDNSSelections)
		r.Policy.DNSAnswerRules[1].TTLSeconds++
		r.RuntimeSnapshot.DNSSelections[1].ObservedAt = fact.ObservedAt
		return r
	}
	r := input()
	if err := bindHostedQueryInputs(&r, source, owned); err != nil ||
		!reflect.DeepEqual(r.RuntimeSnapshot.DNSSelections[1], selected) ||
		!reflect.DeepEqual(r.Policy.DNSAnswerRules, NormalizePolicySnapshot(source.Prior.Policy).DNSAnswerRules) {
		t.Fatal("foreign selection, TTL or original timestamp was not preserved", err)
	}
	for name, change := range map[string]func(*CompileRequest){
		"new owned family": func(r *CompileRequest) {
			a, b := rule, fact
			a.Type, b.Type = "AAAA", "AAAA"
			r.Policy.DNSAnswerRules = append(r.Policy.DNSAnswerRules, a)
			r.RuntimeSnapshot.DNSSelections = append(r.RuntimeSnapshot.DNSSelections, b)
		},
		"missing owned member": func(r *CompileRequest) {
			r.Policy.DNSAnswerRules = r.Policy.DNSAnswerRules[1:]
			r.RuntimeSnapshot.DNSSelections = r.RuntimeSnapshot.DNSSelections[1:]
		},
		"duplicate fact":           func(r *CompileRequest) { r.RuntimeSnapshot.DNSSelections[1] = fact },
		"orphan fact":              func(r *CompileRequest) { r.RuntimeSnapshot.DNSSelections[1].NodeID = "unknown" },
		"wrong capture generation": func(r *CompileRequest) { r.RuntimeSnapshot.PolicyGeneration = "different" },
	} {
		t.Run(name, func(t *testing.T) {
			req := input()
			change(&req)
			if err := bindHostedQueryInputs(&req, source, owned); err == nil {
				t.Fatal("invalid scoped selection accepted")
			}
		})
	}
}
