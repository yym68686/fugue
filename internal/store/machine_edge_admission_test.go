package store

import (
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/model/edgecohort"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEdgeAdmissionRequiresExactLiveIntentAndSurvivesRediscovery(t *testing.T) {
	now := time.Now().UTC()
	intent := model.EdgeNodeEnrollment{ID: "fixture-enrollment", NodeID: "node-new", EdgeGroupID: "edge-group-fixture", AuthorityRecordDigest: "sha256:" + strings.Repeat("a", 64), Slot: "a", FenceSequence: 5, BundleVersion: "fixture.p1.r0", WorkerSourceSHA: strings.Repeat("b", 40), WorkerImageDigest: "sha256:" + strings.Repeat("c", 64), IssuedAt: now, ExpiresAt: now.Add(time.Minute)}
	raw, _ := json.Marshal(intent)
	m := model.Machine{ClusterNodeName: intent.NodeID, Policy: model.MachinePolicy{AllowEdge: true, AllowAppRuntime: true}, Labels: map[string]string{model.MachineEdgeEnrollmentLabel: string(raw), "fugue.io/edge-group-id": intent.EdgeGroupID, "fugue.io/edge-workload": "dynamic", edgecohort.PhaseLabel: edgecohort.Preparing}}
	before := m
	if applyEdgeAdmission(&m, "foreign") == nil || !reflect.DeepEqual(before, m) {
		t.Fatal("foreign intent changed membership")
	}
	if err := applyEdgeAdmission(&m, intent.ID); err != nil {
		t.Fatal(err)
	}
	if m.Labels[edgecohort.PhaseLabel] != edgecohort.Admitted || !m.Policy.AllowAppRuntime || m.Policy.AllowSharedPool {
		t.Fatal("admission changed roles")
	}
	next := model.Machine{}
	preserveMachineEdgeEnrollment(&next, m)
	if next.Labels[edgecohort.PhaseLabel] != edgecohort.Admitted {
		t.Fatal("runtime discovery forgot admitted membership")
	}
	if err := applyMachineEdgeEnrollment(&m, intent.EdgeGroupID); err != nil || m.Labels[edgecohort.PhaseLabel] != edgecohort.Admitted {
		t.Fatal("retry demoted admitted node")
	}
}
