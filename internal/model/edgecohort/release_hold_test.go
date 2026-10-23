package edgecohort

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func releaseHoldNodes() []corev1.Node {
	nodes := []corev1.Node{}
	for _, name := range []string{"fixed-a", "fixed-b", "elastic-c"} {
		n := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name), ResourceVersion: "20", Labels: map[string]string{"fugue.io/edge-group-id": "edge-pool-a", "fugue.io/role.edge": "true", "fugue.io/edge-workload": "dynamic", "fugue.io/schedulable": "true"}}}
		if name == "elastic-c" {
			n.Labels[PhaseLabel] = Admitted
		}
		nodes = append(nodes, n)
	}
	return nodes
}

func attachReleaseHold(n *corev1.Node) {
	raw, _ := json.Marshal(map[string]any{"schema": "fugue.edge-release-hold/v1", "node_uid": string(n.UID), "edge_group_id": n.Labels["fugue.io/edge-group-id"], "reason": "quota_exhausted"})
	n.Annotations = map[string]string{"fugue.io/edge-release-hold": string(raw)}
}

func TestReleaseHoldIsIndependentOfNodeReadyAndPreservesUnheldMembers(t *testing.T) {
	for _, index := range []int{0, 2} {
		nodes := releaseHoldNodes()
		attachReleaseHold(&nodes[index])
		pods := []corev1.Pod{}
		for _, n := range nodes {
			pods = append(pods, corev1.Pod{Spec: corev1.PodSpec{NodeName: n.Name}})
		}
		cohort, err := FromNodes(nodes, "edge-pool-a", 2)
		if err != nil || cohort.Expected != 2 || len(cohort.FilterPods(pods)) != 2 || !cohort.Suspended[nodes[index].Name] {
			t.Fatalf("explicit hold did not isolate a reachable throttled member: %#v %v", cohort, err)
		}
		// Removing intent affects only a subsequent snapshot, never a frozen
		// release; removal itself is not a runtime proof of recovery.
		nodes[index].Annotations = nil
		later, err := FromNodes(nodes, "edge-pool-a", 2)
		if err != nil || later.Expected != 3 || cohort.Expected != 2 {
			t.Fatal("hold removal changed the frozen cohort", later, err)
		}
	}
}

func TestReleaseHoldCannotHideMissingBaselineOrForeignIdentity(t *testing.T) {
	for _, scenario := range []string{"uid", "group", "unknown-field", "reason", "null", "deleted", "missing-baseline", "duplicate", "all-held"} {
		t.Run(scenario, func(t *testing.T) {
			nodes := releaseHoldNodes()
			attachReleaseHold(&nodes[0])
			raw := map[string]any{}
			_ = json.Unmarshal([]byte(nodes[0].Annotations["fugue.io/edge-release-hold"]), &raw)
			switch scenario {
			case "uid":
				raw["node_uid"] = "replaced"
			case "group":
				raw["edge_group_id"] = "edge-pool-b"
			case "unknown-field":
				raw["allow_unready"] = true
			case "reason":
				raw["reason"] = "ready"
			case "null":
				raw = nil
			case "deleted":
				now := metav1.Now()
				nodes[0].DeletionTimestamp = &now
			case "missing-baseline":
				nodes = append(nodes[:1], nodes[2:]...)
			case "duplicate":
				nodes = append(nodes, nodes[0])
			case "all-held":
				attachReleaseHold(&nodes[1])
				attachReleaseHold(&nodes[2])
			}
			encoded, _ := json.Marshal(raw)
			nodes[0].Annotations["fugue.io/edge-release-hold"] = string(encoded)
			if _, err := FromNodes(nodes, "edge-pool-a", 2); err == nil {
				t.Fatal("invalid hold reduced the required release cohort")
			}
		})
	}
}
