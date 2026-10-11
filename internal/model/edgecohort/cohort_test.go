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
