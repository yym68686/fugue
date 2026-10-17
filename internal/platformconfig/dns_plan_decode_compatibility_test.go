package platformconfig

import (
	"encoding/json"
	"fmt"
	"testing"

	"fugue/internal/model"
)

// Prior validation order is a compatibility oracle for synthetic malformed inputs.
func previousDNSCellPlanValidation(artifact model.PlatformArtifact) error {
	pubs, err := DecodeCellRoutePublications(artifact)
	if err != nil {
		return err
	}
	var payload struct {
		Plan   *DNSReadinessPlan  `json:"readiness_plan"`
		Source *CellDNSPlanSource `json:"cell_dns_source"`
		Policy PolicySnapshot     `json:"policy"`
	}
	raw, _ := json.Marshal(artifact.Content)
	if json.Unmarshal(raw, &payload) != nil {
		return fmt.Errorf("invalid DNS readiness content")
	}
	if len(pubs) == 0 {
		if payload.Source != nil || artifact.Content["previous_traffic_publication"] != nil {
			return fmt.Errorf("DNS Cell source in ordinary publication")
		}
		if payload.Plan != nil {
			for _, p := range payload.Plan.Probes {
				if p.CellPublicationDigest != "" || p.PreviousAuthority != nil {
					return fmt.Errorf("foreign Cell proof in ordinary DNS")
				}
			}
		}
		return nil
	}
	if payload.Plan == nil || payload.Source == nil || payload.Source.Intent.PublicationRole != PublicationRoleCellDNS || ValidatePlatformIntent(payload.Source.Intent) != nil || ValidatePolicySnapshot(payload.Policy) != nil {
		return fmt.Errorf("DNS Cell requirements or source missing")
	}
	intentDigest, _ := Digest(NormalizePlatformIntent(payload.Source.Intent))
	policyDigest, _ := Digest(payload.Policy)
	if intentDigest != artifact.Metadata["intent_digest"] || policyDigest != artifact.Metadata["policy_digest"] {
		return fmt.Errorf("DNS Cell source differs from signed lineage")
	}
	if _, err := CompileTrafficConsumerTopology(payload.Source.Intent, payload.Policy); err != nil {
		return err
	}
	previous, err := DecodePreviousTrafficPublication(artifact)
	if err != nil {
		return err
	}
	want, err := compileCellDNSReadiness(payload.Source.Intent, pubs, RuntimeSnapshot{DNSEdgeEndpoints: payload.Source.Endpoints, CapturedAt: payload.Source.CapturedAt}, payload.Policy, previous)
	if err != nil {
		return err
	}
	wantDigest, _ := Digest(want)
	actualDigest, _ := Digest(payload.Plan)
	if wantDigest != actualDigest {
		return fmt.Errorf("DNS Cell requirements differ from referenced route/TLS publications")
	}
	return nil
}

func TestDNSCellPlanDecodeReusePreservesOrdinaryValidation(t *testing.T) {
	compiled, err := Compile(dnsReadinessFixture())
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"policy", "readiness_plan", "cell_dns_source", "cell_route_publications", "previous_traffic_publication", "unrelated"} {
		for index, value := range []any{nil, false, 1, "invalid", []any{}, []any{nil}, map[string]any{}, map[string]any{"publication_role": PublicationRoleCellDNS}, map[string]any{"dns_query_policy": map[string]any{"ranking_mode": "active"}}} {
			t.Run(fmt.Sprintf("%s/%d", field, index), func(t *testing.T) {
				raw, _ := json.Marshal(compiled.DNSArtifact)
				var a model.PlatformArtifact
				if err := json.Unmarshal(raw, &a); err != nil {
					t.Fatal(err)
				}
				a.Content[field] = value
				want, got := previousDNSCellPlanValidation(a), ValidateDNSCellPlan(a)
				if fmt.Sprint(want) != fmt.Sprint(got) {
					t.Fatalf("validation changed: old=%v new=%v", want, got)
				}
			})
		}
	}
	for _, content := range []map[string]any{nil, {}, {"policy": nil},
		{"Policy": map[string]any{"publication_role": PublicationRoleCellDNS}, "policy": map[string]any{}},
		{"Policy": map[string]any{}, "policy": nil}} {
		a := model.PlatformArtifact{Content: content}
		if want, got := previousDNSCellPlanValidation(a), ValidateDNSCellPlan(a); fmt.Sprint(want) != fmt.Sprint(got) {
			t.Fatalf("envelope validation changed: old=%v new=%v", want, got)
		}
	}
}
func TestDNSCellPlanDecodeReuseDoesNotCacheAcrossCalls(t *testing.T) {
	compiled, err := Compile(dnsReadinessFixture())
	if err != nil {
		t.Fatal(err)
	}
	a := compiled.DNSArtifact
	if err := ValidateDNSCellPlan(a); err != nil {
		t.Fatal(err)
	}
	plan := a.Content["readiness_plan"].(map[string]any)
	probes := plan["probes"].([]any)
	probes[0].(map[string]any)["cell_publication_digest"] = "foreign"
	if err := ValidateDNSCellPlan(a); err == nil {
		t.Fatal("same-artifact mutation reused validation")
	}
}
