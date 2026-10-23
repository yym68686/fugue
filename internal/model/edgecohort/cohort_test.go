package edgecohort

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestEnrollmentIntentChangesMembershipWithoutWeakeningBaseline(t *testing.T) {
	n := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-new", UID: "uid-new", Labels: map[string]string{"fugue.io/edge-group-id": "edge-group-fixture", "fugue.io/role.edge": "true", "fugue.io/edge-workload": "dynamic", PhaseLabel: Preparing}}}
	pods := []corev1.Pod{{Spec: corev1.PodSpec{NodeName: "node-existing"}}, {Spec: corev1.PodSpec{NodeName: n.Name}}}
	s, err := FromNodes([]corev1.Node{n}, "edge-group-fixture", 2)
	if err != nil || s.Expected != 2 || len(s.FilterPods(pods)) != 1 {
		t.Fatal("preparing node entered live cohort", s, err)
	}
	n.Labels[PhaseLabel] = Admitted
	s, err = FromNodes([]corev1.Node{n}, "edge-group-fixture", 2)
	if err != nil || s.Expected != 3 || len(s.FilterPods(pods)) != 2 {
		t.Fatal("admitted declaration not retained", s, err)
	}
	n.Labels[PhaseLabel] = "unknown"
	if _, err = FromNodes([]corev1.Node{n}, "edge-group-fixture", 2); err == nil {
		t.Fatal("unknown phase accepted")
	}
	n.Labels["fugue.io/edge-group-id"] = "edge-group-other"
	s, err = FromNodes([]corev1.Node{n}, "edge-group-fixture", 2)
	if err != nil || s.Expected != 2 {
		t.Fatal("foreign authority changed cohort")
	}
}

func TestExplicitSuspensionFiltersPlacementButNeverLowersDeclaredBaseline(t *testing.T) {
	node := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "suspended-node", UID: "node-uid", ResourceVersion: "20", Labels: map[string]string{"fugue.io/edge-group-id": "edge-pool-a", "fugue.io/role.edge": "true", "fugue.io/schedulable": "false"}}}
	pods := []corev1.Pod{{Spec: corev1.PodSpec{NodeName: "active-node"}}, {Spec: corev1.PodSpec{NodeName: node.Name}}}
	cohort, err := FromNodes([]corev1.Node{node}, "edge-pool-a", 2)
	if err != nil || cohort.Expected != 2 || len(cohort.FilterPods(pods)) != 1 {
		t.Fatal("explicit suspension must not fabricate the missing baseline member", cohort, err)
	}
	for _, label := range []string{"", "true"} {
		node.Labels["fugue.io/schedulable"] = label
		cohort, err = FromNodes([]corev1.Node{node}, "edge-pool-a", 2)
		if err != nil || len(cohort.FilterPods(pods)) != 2 {
			t.Fatal("missing health or scheduling evidence removed a member", cohort, err)
		}
	}
	node.Labels["fugue.io/schedulable"] = "false"
	node.UID = ""
	if _, err := FromNodes([]corev1.Node{node}, "edge-pool-a", 2); err == nil {
		t.Fatal("unidentified suspension accepted")
	}
}

func TestAdmittedSuspensionUsesTheSameMembershipForCountAndPods(t *testing.T) {
	node := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "elastic-edge", UID: "elastic-uid", ResourceVersion: "12", Labels: map[string]string{"fugue.io/edge-group-id": "edge-pool-a", "fugue.io/role.edge": "true", "fugue.io/edge-workload": "dynamic", "fugue.io/schedulable": "false", PhaseLabel: Admitted}}}
	pods := []corev1.Pod{{Spec: corev1.PodSpec{NodeName: "fixed-a"}}, {Spec: corev1.PodSpec{NodeName: "fixed-b"}}, {Spec: corev1.PodSpec{NodeName: node.Name}}}
	suspended, err := FromNodes([]corev1.Node{node}, "edge-pool-a", 2)
	if err != nil || suspended.Expected != len(suspended.FilterPods(pods)) || suspended.Expected != 2 {
		t.Fatal("suspended extension leaves an impossible release cohort", suspended, err)
	}
	// A later enrollment snapshot may include the node again, but it cannot
	// mutate the cohort frozen for the current release or fabricate health.
	node.Labels["fugue.io/schedulable"] = "true"
	recovered, err := FromNodes([]corev1.Node{node}, "edge-pool-a", 2)
	if err != nil || recovered.Expected != 3 || len(recovered.FilterPods(pods)) != 3 || suspended.Expected != 2 || len(suspended.FilterPods(pods)) != 2 {
		t.Fatal("recovery changed a frozen release or bypassed member accounting", suspended, recovered, err)
	}
}
