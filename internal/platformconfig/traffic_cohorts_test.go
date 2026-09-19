package platformconfig

import (
	"encoding/json"
	"fugue/internal/model"
	"reflect"
	"strings"
	"testing"
)

func TestTrafficCohortsAreCanonicalPolicyProjection(t *testing.T) {
	r := dnsQueryFixture()
	r.Policy.TrafficRolloutCohorts = []TrafficRolloutCohort{{ID: "second", EdgeGroupIDs: []string{"edge-group-b", "edge-group-a"}}, {ID: "first", EdgeGroupIDs: []string{"edge-group-a"}}}
	rebindPlacement(&r)
	before, _ := json.Marshal(r)
	out, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := Compile(r)
	if err != nil || !reflect.DeepEqual(out.ReleaseArtifact.Content, replay.ReleaseArtifact.Content) {
		t.Fatal("cohort compilation not deterministic", err)
	}
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("compiler mutated caller")
	}
	groups, err := ResolveTrafficCanary(out.ReleaseArtifact, "cohort=first")
	if err != nil || !reflect.DeepEqual(groups, []string{"edge-group-a"}) {
		t.Fatal(groups, err)
	}
	for _, child := range []model.PlatformArtifact{out.RouteArtifact, out.DNSArtifact, out.TLSArtifact} {
		if err := ValidateTrafficCohortProjection(out.ReleaseArtifact, child); err != nil {
			t.Fatal(err)
		}
	}
	r.Policy.TrafficRolloutCohorts[1].EdgeGroupIDs = []string{"edge-group-b"}
	rebindPlacement(&r)
	changed, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	if out.Lineage.PolicyDigest == changed.Lineage.PolicyDigest || out.ReleaseArtifact.Generation == changed.ReleaseArtifact.Generation || out.Lineage.IntentDigest != changed.Lineage.IntentDigest {
		t.Fatal("target change did not change policy and ReleaseSet")
	}
	if ValidateTrafficCohortProjection(out.ReleaseArtifact, changed.RouteArtifact) == nil {
		t.Fatal("mismatched child policy accepted")
	}
}

func TestTrafficCohortSchemaAndReferenceFailClosed(t *testing.T) {
	for _, cohorts := range [][]TrafficRolloutCohort{
		{{ID: "a", EdgeGroupIDs: nil}}, {{ID: "A", EdgeGroupIDs: []string{"edge-group-a"}}}, {{ID: "a", EdgeGroupIDs: []string{"edge-group-a", "edge-group-a"}}},
		{{ID: "a", EdgeGroupIDs: []string{"*"}}}, {{ID: strings.Repeat("a", 129), EdgeGroupIDs: []string{"edge-group-a"}}},
		{{ID: "a", EdgeGroupIDs: []string{"edge-group-a"}}, {ID: "a", EdgeGroupIDs: []string{"edge-group-b"}}},
	} {
		if ValidateTrafficRolloutCohorts(cohorts) == nil {
			t.Fatal("invalid cohort accepted", cohorts)
		}
	}
	r := dnsQueryFixture()
	r.Policy.TrafficRolloutCohorts = []TrafficRolloutCohort{{ID: "selected", EdgeGroupIDs: []string{"edge-group-a"}}}
	rebindPlacement(&r)
	out, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range []string{"edge=edge-a", "edge_group=edge-group-a", "cohort=missing", "cohort=SELECTED", " cohort=selected", "cohort:selected", "cohort=selected,other", "cohort=*"} {
		if _, err := ResolveTrafficCanary(out.ReleaseArtifact, ref); err == nil {
			t.Fatal("unbound reference accepted", ref)
		}
	}
	out.ReleaseArtifact.Content["traffic_rollout_cohorts"] = []any{map[string]any{"id": "selected", "edge_group_ids": []string{"edge-group-b"}}}
	if ValidateTrafficCohortProjection(out.ReleaseArtifact, out.RouteArtifact) == nil {
		t.Fatal("parent widened child policy")
	}
}
