package platformconfig

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
)

func declaredCellCompileFixture(t testing.TB) CompileRequest {
	t.Helper()
	intent := PlatformIntent{AuthorityCellID: "cell-a", SchemaVersion: SchemaVersion, Scope: AuthorityCellScope("cell-a"), Generation: "intent-a", EdgeTopology: &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: "cell-a"}}, Pools: []edgetopology.ServingPool{{ID: "pool-public"}}, Edges: []edgetopology.Edge{{ID: "node-a", AuthorityCellID: "cell-a", ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "node-a"}}}}, DNSConsumers: []DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "cell-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}, Routes: []RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: false}}}
	topology, err := TrafficConsumerTopologyFromIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := Digest(topology)
	now := time.Now().UTC()
	return CompileRequest{Intent: intent, Policy: PolicySnapshot{AuthorityCellID: "cell-a", ConsumerTopologyDigest: digest, Scope: intent.Scope, Generation: "policy-a", TrafficRolloutCohorts: []TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: []string{"cell-a"}}}}, RuntimeSnapshot: RuntimeSnapshot{CapturedAt: &now, DNSConsumers: []DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: "cell-a", ObservedAt: now, A: []string{"8.8.8.8"}}}}}
}

func TestCellConsumerTopologyBindsIntentPolicyAndEveryArtifact(t *testing.T) {
	req := declaredCellCompileFixture(t)
	before, _ := json.Marshal(req)
	compiled, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	parent := compiled.ReleaseArtifact
	parent.ScopeKey = parent.Scope.Key
	declared, err := TrafficConsumersFromRelease(parent)
	if err != nil || declared == nil || !reflect.DeepEqual(declared.EdgeNodeIDs, []string{"node-a"}) || !reflect.DeepEqual(declared.DNSNodeIDs, []string{"dns-a"}) {
		t.Fatal(declared, err)
	}
	for _, child := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.DNSArtifact, compiled.TLSArtifact} {
		child.ScopeKey = child.Scope.Key
		if err := ValidateTrafficCohortProjection(parent, child); err != nil {
			t.Fatal(err)
		}
		clone := child
		clone.Metadata = map[string]string{}
		for k, v := range child.Metadata {
			clone.Metadata[k] = v
		}
		delete(clone.Metadata, "consumer_topology_digest")
		if ValidateTrafficConsumerTopologyProjection(parent, clone) == nil {
			t.Fatal("child lost membership binding")
		}
	}
	rebuilt := BuildReleaseSetArtifact(compiled.ReleaseSet, []string{"route", "dns", "tls"}, time.Now())
	rebuilt.ScopeKey = rebuilt.Scope.Key
	if _, err := TrafficConsumersFromRelease(rebuilt); err != nil {
		t.Fatal("materialization lost topology binding", err)
	}
	after, _ := json.Marshal(req)
	if string(before) != string(after) {
		t.Fatal("compiler changed original intent/policy")
	}
	// Runtime observations are not execution enrollment or membership removal.
	req.RuntimeSnapshot.Facts = map[string]any{"healthy_nodes": []string{"foreign-node"}}
	other, err := Compile(req)
	if err != nil || !reflect.DeepEqual(compiled.ReleaseSet.ConsumerTopology, other.ReleaseSet.ConsumerTopology) {
		t.Fatal("runtime facts altered membership", err)
	}
}

func TestCellConsumerTopologyRejectsImplicitOrUnapprovedMembership(t *testing.T) {
	for _, scenario := range []string{"global scope", "missing intent authority", "missing policy authority", "wrong policy digest", "missing edge topology", "legacy alias", "foreign DNS", "invalid physical node", "missing DNS", "foreign cohort", "extra edge", "empty projection", "projection substitution", "child foreign scope"} {
		t.Run(scenario, func(t *testing.T) {
			r := declaredCellCompileFixture(t)
			switch scenario {
			case "global scope":
				r.Intent.Scope, r.Policy.Scope = "global", "global"
			case "missing intent authority":
				r.Intent.AuthorityCellID = ""
			case "missing policy authority":
				r.Policy.AuthorityCellID = ""
			case "wrong policy digest":
				r.Policy.ConsumerTopologyDigest = "sha256:" + strings.Repeat("a", 64)
			case "missing edge topology":
				r.Intent.EdgeTopology = nil
			case "legacy alias":
				r.Intent.EdgeTopology.Cells[0].LegacyGroupID = "edge-group-old"
			case "foreign DNS":
				r.Intent.DNSConsumers[0].EdgeGroupID = "cell-b"
			case "invalid physical node":
				r.Intent.DNSConsumers[0].NodeID = "zone:alias"
			case "missing DNS":
				r.Intent.DNSConsumers = nil
			case "foreign cohort":
				r.Policy.TrafficRolloutCohorts[0].EdgeGroupIDs = []string{"cell-b"}
			case "extra edge":
				e := r.Intent.EdgeTopology.Edges[0]
				e.ID = "node-b"
				r.Intent.EdgeTopology.Edges = append(r.Intent.EdgeTopology.Edges, e)
			}
			compiled, err := Compile(r)
			if scenario == "empty projection" || scenario == "projection substitution" || scenario == "child foreign scope" {
				if err != nil {
					t.Fatal(err)
				}
				parent, child := compiled.ReleaseArtifact, compiled.RouteArtifact
				parent.ScopeKey = parent.Scope.Key
				child.ScopeKey = child.Scope.Key
				if scenario == "empty projection" {
					delete(parent.Content, "consumer_topology")
				}
				if scenario == "projection substitution" {
					parent.Content["consumer_topology"].(map[string]any)["dns_node_ids"] = []string{"foreign-node"}
				}
				if scenario == "child foreign scope" {
					child.ScopeKey = AuthorityCellScope("cell-b")
				}
				if ValidateTrafficConsumerTopologyProjection(parent, child) == nil {
					t.Fatal("unbound projection accepted")
				}
			} else if err == nil {
				t.Fatal("invalid declared topology compiled")
			}
		})
	}
}
