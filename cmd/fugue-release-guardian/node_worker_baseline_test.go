package main

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNodeWorkerBaselineRefusesUnenrolledGroupIngress(t *testing.T) {
	const group = "edge-group-fixture"
	member := nodeWorkerBaselineMember{Node: "node-a", ServiceUID: "service-a"}
	service := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "public-b", UID: "service-b", Labels: map[string]string{"app.kubernetes.io/managed-by": "fugue-front-serving-transport"}},
		Spec:       corev1.ServiceSpec{Selector: map[string]string{"front": "b"}},
	}
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "front-b", Labels: map[string]string{"front": "b", "fugue.io/edge-group-id": group}},
	}
	if err := checkNodeWorkerBaselineCoverage(group, []nodeWorkerBaselineMember{member}, []corev1.Service{service}, []corev1.Pod{pod}); err == nil || !strings.Contains(err.Error(), "outside the enrolled group") {
		t.Fatalf("omitted public group Service was accepted: %v", err)
	}
	service.UID = "service-a"
	if err := checkNodeWorkerBaselineCoverage(group, []nodeWorkerBaselineMember{member}, []corev1.Service{service}, []corev1.Pod{pod}); err != nil {
		t.Fatal(err)
	}
	pod.Spec.Containers = []corev1.Container{{Name: "edge-front", Ports: []corev1.ContainerPort{{HostPort: 443, ContainerPort: 443}}}}
	if err := checkNodeWorkerBaselineCoverage(group, []nodeWorkerBaselineMember{member}, nil, []corev1.Pod{pod}); err == nil || !strings.Contains(err.Error(), "legacy direct ingress") {
		t.Fatalf("legacy listener without a Service was accepted: %v", err)
	}
}

func TestNodeWorkerBaselineSeparatelyBindsServingAndRevocationEvidence(t *testing.T) {
	scope := productioncontrol.Scope{ClusterID: "cluster-a", ResourceID: productioncontrol.FrontWorkerResource("fixture", "edge-group-fixture")}
	now := time.Unix(1760000000, 0)
	boundary := releaseguardian.FrontWorkerBaselineBoundary{Policy: shaDigest([]byte("policy"))}
	settings := nodeWorkerBaselineSettings{Evidence: nodeWorkerBaselineEvidence{LegacyRevocation: shaDigest([]byte("old grant removal"))}}
	serving := []nodeWorkerBaselineObservation{{Member: nodeWorkerBaselineMember{Node: "node-a"}, Serving: nodeWorkerTerminalServing{WorkerUID: "worker-a"}}}
	first, err := makeNodeWorkerBaselineCheckpoint(scope, settings, boundary, serving, now)
	if err != nil {
		t.Fatal(err)
	}
	settings.Evidence.LegacyRevocation = shaDigest([]byte("updated grant removal"))
	after, err := makeNodeWorkerBaselineCheckpoint(scope, settings, boundary, serving, now)
	if err != nil || after.Verified != first.Verified || after.WitnessDraft.AuthorityDigest == first.WitnessDraft.AuthorityDigest {
		t.Fatalf("changed provider evidence did not invalidate the authority proof on its own: %v", err)
	}
	serving[0].Serving.WorkerUID = "worker-b"
	updated, err := makeNodeWorkerBaselineCheckpoint(scope, settings, boundary, serving, now)
	if err != nil || updated.Verified == after.Verified || updated.WitnessDraft.ObservationDigest == after.WitnessDraft.ObservationDigest ||
		updated.WitnessDraft.AuthorityDigest != after.WitnessDraft.AuthorityDigest {
		t.Fatalf("changed Worker did not invalidate the serving proof: %v", err)
	}
}
