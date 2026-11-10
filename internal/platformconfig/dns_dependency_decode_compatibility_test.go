package platformconfig_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

// Preserve the independent typed views as an oracle for the shared canonical
// decoder, including rejection order and an empty result on every failure.
func independentDNSDependencyEvidence(a model.PlatformArtifact) (platformconfig.DNSRouteDependencyEvidence, error) {
	var evidence platformconfig.DNSRouteDependencyEvidence
	raw, err := json.Marshal(a.Content)
	if err != nil {
		return evidence, fmt.Errorf("DNS artifact content cannot be encoded: %w", err)
	}
	pubs, err := platformconfig.DecodeCellRoutePublications(a)
	if err != nil {
		return evidence, err
	}
	previous, err := platformconfig.DecodePreviousTrafficPublication(a)
	if err != nil {
		return evidence, err
	}
	deps := make([]platformconfig.DNSRouteDependency, 0, len(pubs)+2)
	for _, p := range pubs {
		r := p.Reference
		deps = append(deps, platformconfig.DNSRouteDependency{GroupID: r.AuthorityCellID, Parent: p.Parent, ReleaseID: r.ReleaseID, ReleaseChannel: r.ReleaseChannel, CanaryRuleRef: r.CanaryRuleRef, FencingToken: r.FencingToken, Artifacts: []model.PlatformArtifact{p.Parent, p.Route, p.TLS}})
	}
	if previous != nil {
		var payload struct {
			Source *platformconfig.CellDNSPlanSource `json:"cell_dns_source"`
		}
		if json.Unmarshal(raw, &payload) != nil || payload.Source == nil {
			return evidence, fmt.Errorf("DNS transition source missing")
		}
		aliases, err := platformconfig.RouteAuthorityAliases(payload.Source.Intent)
		if err != nil {
			return evidence, err
		}
		groups := make([]string, 0, len(aliases))
		for group := range aliases {
			groups = append(groups, group)
		}
		slices.Sort(groups)
		for _, group := range groups {
			deps = append(deps, platformconfig.DNSRouteDependency{GroupID: group, Parent: previous.Parent, ReleaseID: previous.Reference.ReleaseID, ReleaseChannel: "full", FencingToken: previous.Reference.FencingToken, Artifacts: []model.PlatformArtifact{previous.Parent, previous.Route, previous.TLS, previous.DNS}, RequireVerified: true})
		}
	}
	evidence.Dependencies, evidence.Publications, evidence.Previous = deps, pubs, previous
	return evidence, nil
}

// Dependency evidence outlives the decoder's intermediate envelope. Mutating
// the caller's source or a returned view must not alter the other, including
// large fields that take the bounded validation-cache path.
func TestDNSDependencyEvidenceOwnsDecodedViews(t *testing.T) {
	req, _, _ := celldns.SourceAuthorizedRequest(t, true)
	a := celldns.Compile(t, req).DNSArtifact
	a.Content["policy"].(map[string]any)["unknown_padding"] = strings.Repeat("x", 256<<10)
	evidence, err := platformconfig.DecodeDNSRouteDependencyEvidence(a)
	if err != nil || evidence.Previous == nil || len(evidence.Publications) == 0 {
		t.Fatal("missing complete publication evidence", err)
	}
	before, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	previous := a.Content["previous_traffic_publication"].(map[string]any)
	previous["route"].(map[string]any)["content"].(map[string]any)["caller_mutation"] = true
	a.Content["cell_route_publications"].([]any)[0].(map[string]any)["parent"].(map[string]any)["content"].(map[string]any)["caller_mutation"] = true
	after, err := json.Marshal(evidence)
	if err != nil || string(before) != string(after) {
		t.Fatal("source mutation changed decoded evidence", err)
	}
	source, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	evidence.Previous.Route.Content["returned_mutation"] = true
	evidence.Publications[0].Parent.Content["returned_mutation"] = true
	unchanged, err := json.Marshal(a)
	if err != nil || string(source) != string(unchanged) {
		t.Fatal("decoded evidence mutation changed source", err)
	}
}

func TestDNSDependencyDecodePreservesTypedViewsAndErrors(t *testing.T) {
	for _, transition := range []bool{false, true} {
		req, _, _ := celldns.SourceAuthorizedRequest(t, transition)
		original := celldns.Compile(t, req).DNSArtifact
		encoded, _ := json.Marshal(original)
		check := func(t *testing.T, a model.PlatformArtifact) {
			t.Helper()
			before, _ := json.Marshal(a)
			want, oldErr := independentDNSDependencyEvidence(a)
			got, newErr := platformconfig.DecodeDNSRouteDependencyEvidence(a)
			if fmt.Sprint(oldErr) != fmt.Sprint(newErr) || !reflect.DeepEqual(want, got) {
				t.Fatalf("typed views changed: old error=%v new error=%v results equal=%t", oldErr, newErr, reflect.DeepEqual(want, got))
			}
			var cost platformconfig.DNSDependencyDecodeCost
			observed, observedErr := platformconfig.DecodeDNSRouteDependencyEvidenceObserved(a, &cost)
			if fmt.Sprint(oldErr) != fmt.Sprint(observedErr) || !reflect.DeepEqual(want, observed) || cost.Attempts != 1 || (cost.Rejected == 1) != (oldErr != nil) {
				t.Fatalf("observed decoder changed results or rejection: cost=%+v old=%v observed=%v", cost, oldErr, observedErr)
			}
			for _, us := range []int64{cost.EnvelopeEncodeUS, cost.EnvelopeDecodeUS, cost.CellPublicationsUS, cost.PreviousDecodeUS, cost.PreviousValidationUS, cost.SourceDependenciesUS, cost.AliasFallbackUS} {
				if us < 0 {
					t.Fatal("negative stage duration", cost)
				}
			}
			// Alias compatibility keeps its entire original path. Its time
			// must not be misreported as canonical typed-stage work.
			if cost.AliasFallbacks != 0 && (cost.AliasFallbacks != 1 || cost.EnvelopeDecodeUS != 0 || cost.CellPublicationsUS != 0 || cost.PreviousDecodeUS != 0 || cost.PreviousValidationUS != 0 || cost.SourceDependenciesUS != 0) {
				t.Fatal("alias work mixed with canonical stages", cost)
			}
			after, _ := json.Marshal(a)
			if string(before) != string(after) {
				t.Fatal("immutable input was mutated")
			}
		}
		clone := func(t *testing.T) model.PlatformArtifact {
			t.Helper()
			var a model.PlatformArtifact
			if err := json.Unmarshal(encoded, &a); err != nil {
				t.Fatal(err)
			}
			return a
		}
		t.Run(fmt.Sprintf("transition-%t/valid", transition), func(t *testing.T) { check(t, clone(t)) })
		for _, field := range []string{"policy", "cell_route_publications", "cell_dns_source", "previous_traffic_publication"} {
			for i, value := range []any{nil, false, 1, "invalid", []any{}, []any{nil}, map[string]any{}, map[string]any{"intent": false}, map[string]any{"dns_query_policy": map[string]any{"ranking_mode": "active"}}} {
				t.Run(fmt.Sprintf("transition-%t/%s/%d", transition, field, i), func(t *testing.T) {
					a := clone(t)
					if i == 0 {
						delete(a.Content, field)
					} else {
						a.Content[field] = value
					}
					check(t, a)
				})
			}
		}
		for _, change := range []func(map[string]any){
			func(c map[string]any) { c["policy"] = nil },
			func(c map[string]any) {
				c["policy"].(map[string]any)["dns_query_policy"] = map[string]any{"ranking_mode": "active"}
			},
			func(c map[string]any) { c["policy"], c["previous_traffic_publication"] = false, false },
			func(c map[string]any) { c["cell_dns_source"], c["policy"] = false, false },
			func(c map[string]any) { c["Policy"] = c["policy"]; c["policy"] = nil },
			func(c map[string]any) { c["CELL_DNS_SOURCE"] = c["cell_dns_source"]; c["cell_dns_source"] = nil },
			func(c map[string]any) {
				c["CELL_ROUTE_PUBLICATIONS"] = c["cell_route_publications"]
				delete(c, "cell_route_publications")
			},
			func(c map[string]any) {
				c["PREVIOUS_TRAFFIC_PUBLICATION"] = c["previous_traffic_publication"]
				c["previous_traffic_publication"] = nil
			},
			func(c map[string]any) { c["unrelated"] = func() {} },
		} {
			a := clone(t)
			change(a.Content)
			check(t, a)
		}
	}
	for _, content := range []map[string]any{nil, {}, {"policy": nil}, {"cell_dns_source": nil}, {"previous_traffic_publication": nil}} {
		a := model.PlatformArtifact{Content: content}
		want, oldErr := independentDNSDependencyEvidence(a)
		got, newErr := platformconfig.DecodeDNSRouteDependencyEvidence(a)
		if fmt.Sprint(oldErr) != fmt.Sprint(newErr) || !reflect.DeepEqual(want, got) {
			t.Fatalf("ordinary envelope changed: old=%v new=%v", oldErr, newErr)
		}
	}
}
