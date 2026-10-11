package main

import (
	"context"
	"fugue/internal/model/edgecohort"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestManagedCohortSnapshotDoesNotMutateRunningActivator(t *testing.T) {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-new", UID: "uid-new", Labels: map[string]string{"fugue.io/edge-group-id": "edge-group-fixture", "fugue.io/role.edge": "true", "fugue.io/edge-workload": "dynamic", edgecohort.PhaseLabel: edgecohort.Preparing}}}
	client := fake.NewSimpleClientset(n)
	a := &frontAuthorityActivator{client: client, config: frontAuthorityConfig{GroupID: "edge-group-fixture", ExpectedNodes: 2}}
	snapshot, err := a.withManagedCohort(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	n.Labels[edgecohort.PhaseLabel] = edgecohort.Admitted
	if _, err = client.CoreV1().Nodes().Update(context.Background(), n, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	next, err := a.withManagedCohort(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if a.config.ExpectedNodes != 2 || a.cohort != nil || snapshot.config.ExpectedNodes != 2 || next.config.ExpectedNodes != 3 || !snapshot.cohort.Preparing[n.Name] || !next.cohort.Admitted[n.Name] {
		t.Fatal("in-flight authority snapshot changed with membership")
	}
}
