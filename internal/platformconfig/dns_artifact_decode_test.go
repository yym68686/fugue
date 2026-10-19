package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"math"
	"reflect"
	"testing"
)

func previousDNSArtifactDecode(artifact model.PlatformArtifact) (DNSArtifactPayload, error) {
	var payload DNSArtifactPayload
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return payload, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil || payload.Schema != SchemaVersion || payload.Generation != artifact.Metadata["intent_generation"] || ValidatePolicySnapshot(payload.Policy) != nil {
		return payload, fmt.Errorf("invalid DNS candidate schema")
	}
	digest, err := Digest(payload.Policy)
	if err != nil || payload.Policy.Scope != artifact.ScopeKey || digest != payload.Lineage.PolicyDigest || payload.Lineage.IntentGeneration != payload.Generation || payload.Lineage.PolicyGeneration != payload.Policy.Generation {
		return payload, fmt.Errorf("invalid DNS policy binding")
	}
	for key, value := range LineageMetadata(payload.Lineage) {
		if value == "" || artifact.Metadata[key] != value {
			return payload, fmt.Errorf("invalid DNS lineage")
		}
	}
	if err := ValidateDNSCellPlan(artifact); err != nil {
		return payload, err
	}
	if err := ValidateDNSIntents(payload.Records); err != nil {
		return payload, err
	}
	if err := ValidateDNSReadinessPlan(payload.ReadinessPlan, payload.Policy.DNSReadiness); err != nil {
		return payload, err
	}
	if err := ValidateDNSQueryViews(payload.QueryViews, payload.Records, payload.ConsumerViews, payload.ReadinessPlan, payload.Policy); err != nil {
		return payload, err
	}
	return payload, nil
}

func TestDNSArtifactDecodeMatchesPriorValidation(t *testing.T) {
	compiled, err := Compile(dnsReadinessFixture())
	if err != nil {
		t.Fatal(err)
	}
	baseline := compiled.DNSArtifact
	// Persistence materializes the compiler's scope into the stored artifact.
	baseline.ScopeKey = baseline.Scope.Key
	if _, err := previousDNSArtifactDecode(baseline); err != nil {
		t.Fatal("valid fixture rejected", err)
	}
	clone := func() model.PlatformArtifact {
		raw, err := json.Marshal(baseline)
		if err != nil {
			t.Fatal(err)
		}
		var a model.PlatformArtifact
		if err = json.Unmarshal(raw, &a); err != nil {
			t.Fatal(err)
		}
		return a
	}
	check := func(t *testing.T, a model.PlatformArtifact) {
		t.Helper()
		want, oldErr := previousDNSArtifactDecode(a)
		got, newErr := DecodeDNSArtifact(a)
		if fmt.Sprint(oldErr) != fmt.Sprint(newErr) || !reflect.DeepEqual(want, got) {
			t.Fatalf("decode behavior changed: old=%v new=%v", oldErr, newErr)
		}
	}
	check(t, baseline)
	for _, field := range []string{"schema_version", "generation", "records", "consumer_views", "readiness_plan", "query_views", "policy", "lineage", "cell_route_publications", "cell_dns_source", "previous_traffic_publication", "unrelated"} {
		for i, value := range []any{nil, false, 17, "invalid", []any{}, []any{nil}, map[string]any{}, map[string]any{"publication_role": PublicationRoleCellDNS}, math.NaN()} {
			t.Run(fmt.Sprintf("%s/%d", field, i), func(t *testing.T) { a := clone(); a.Content[field] = value; check(t, a) })
		}
	}
	for _, field := range []string{"Policy", "Readiness_Plan", "Cell_Dns_Source", "Cell_Route_Publications", "Previous_Traffic_Publication"} {
		for i, value := range []any{nil, false, []any{}, map[string]any{}, map[string]any{"publication_role": PublicationRoleCellDNS}} {
			t.Run(fmt.Sprintf("alias/%s/%d", field, i), func(t *testing.T) { a := clone(); a.Content[field] = value; check(t, a) })
		}
	}
	for name, mutate := range map[string]func(*model.PlatformArtifact){
		"scope":   func(a *model.PlatformArtifact) { a.ScopeKey = "other" },
		"lineage": func(a *model.PlatformArtifact) { a.Metadata["intent_digest"] = "other" },
		"cell_binding": func(a *model.PlatformArtifact) {
			p := a.Content["readiness_plan"].(map[string]any)
			p["probes"].([]any)[0].(map[string]any)["cell_publication_digest"] = "foreign"
		},
		"previous_nil_pointer": func(a *model.PlatformArtifact) {
			a.Content["previous_traffic_publication"] = (*PreviousTrafficPublicationInput)(nil)
		},
		"nil_content": func(a *model.PlatformArtifact) { a.Content = nil },
	} {
		t.Run(name, func(t *testing.T) { a := clone(); mutate(&a); check(t, a) })
	}
}
func TestDNSArtifactDecodeRereadsInputAndIsolatesResults(t *testing.T) {
	compiled, err := Compile(dnsReadinessFixture())
	if err != nil {
		t.Fatal(err)
	}
	a := compiled.DNSArtifact
	a.ScopeKey = a.Scope.Key
	before, _ := json.Marshal(a)
	first, err := DecodeDNSArtifact(a)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.ReadinessPlan.Probes) == 0 {
		t.Fatal("fixture has no proofs")
	}
	first.ReadinessPlan.Probes[0].RouteDigest = "changed"
	next, err := DecodeDNSArtifact(a)
	if err != nil || next.ReadinessPlan.Probes[0].RouteDigest == "changed" {
		t.Fatal("decoded result shared caller mutation", err)
	}
	after, _ := json.Marshal(a)
	if !bytes.Equal(before, after) {
		t.Fatal("decode mutated artifact")
	}
	a.Content["foreign"] = true
	if _, err = DecodeDNSArtifact(a); err == nil {
		t.Fatal("subsequent read reused prior validation")
	}
	delete(a.Content, "foreign")
	if _, err = DecodeDNSArtifact(a); err != nil {
		t.Fatal("restored valid input rejected", err)
	}
}
