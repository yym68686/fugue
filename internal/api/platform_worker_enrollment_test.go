package api

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func testWorkerEnrollment(at time.Time) *platformproducer.OfflineWorkerEnrollment {
	return &platformproducer.OfflineWorkerEnrollment{
		ID: strings.Repeat("a", 32), NodeUID: "11111111-1111-1111-1111-111111111111",
		ServiceAccount: "worker-account", DaemonSetUIDs: []string{"22222222-2222-2222-2222-222222222222"},
		PodCreatedAfter: at.UTC().Format(time.RFC3339),
	}
}

func TestReplacementWorkerBindsPhysicalNodeAndFreshWorkload(t *testing.T) {
	at := time.Now().Truncate(time.Second)
	hold := platformproducer.OfflineEdge{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", WorkerEnrollment: testWorkerEnrollment(at)}
	yes := true
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Labels: map[string]string{"fugue.io/edge-group-id": hold.EdgeGroupID}, CreationTimestamp: metav1.NewTime(at.Add(time.Second)),
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", UID: "22222222-2222-2222-2222-222222222222", Controller: &yes}},
	}, Spec: corev1.PodSpec{NodeName: hold.EdgeID, ServiceAccountName: hold.WorkerEnrollment.ServiceAccount}}
	node := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: hold.EdgeID, UID: "11111111-1111-1111-1111-111111111111",
		Labels: map[string]string{"fugue.io/edge-group-id": hold.EdgeGroupID, "fugue.io/role.edge": "true"}}}
	if !matchesReplacementWorker(hold, pod, node) {
		t.Fatal("exact replacement workload rejected")
	}
	for _, mutate := range []func(*corev1.Pod, *corev1.Node){
		func(p *corev1.Pod, _ *corev1.Node) { p.CreationTimestamp = metav1.NewTime(at) },
		func(p *corev1.Pod, _ *corev1.Node) { p.OwnerReferences[0].UID = "foreign" },
		func(p *corev1.Pod, _ *corev1.Node) { p.Spec.ServiceAccountName = "foreign" },
		func(p *corev1.Pod, _ *corev1.Node) { p.Spec.NodeName = "foreign" },
		func(_ *corev1.Pod, n *corev1.Node) { n.UID = "replacement-node" },
		func(_ *corev1.Pod, n *corev1.Node) { n.Labels["fugue.io/edge-group-id"] = "edge-group-foreign" },
	} {
		p, n := pod.DeepCopy(), node.DeepCopy()
		mutate(p, n)
		if matchesReplacementWorker(hold, *p, *n) {
			t.Fatal("foreign or pre-enrollment workload authorized")
		}
	}
}

func TestReplacementWorkerRequiresFreshRecoveryForDNSCandidate(t *testing.T) {
	s, _, legacy, input := offlineRecoveryFixture(t)
	at := time.Now().UTC()
	nodes := []model.EdgeNode{{ID: legacy.EdgeID, EdgeGroupID: legacy.EdgeGroupID, PublicIPv4: legacy.PublicIPv4}, {ID: "edge-b", EdgeGroupID: legacy.EdgeGroupID, PublicIPv4: "9.9.9.9"}}
	replacement := legacy
	replacement.WorkerEnrollment = testWorkerEnrollment(at.Add(-time.Hour))
	input.OfflineEdges = []platformproducer.OfflineEdge{replacement}
	old, _ := offlineEdgeRouteEnrollments(legacy)
	newTargets, _ := offlineEdgeRouteEnrollments(replacement)
	for i := range old {
		if old[i].ConsumerID == newTargets[i].ConsumerID || old[i].ScopeKey != newTargets[i].ScopeKey || old[i].ArtifactKind != newTargets[i].ArtifactKind {
			t.Fatal("replacement reused retirement slot or changed scope", old, newTargets)
		}
	}
	for _, edge := range []platformproducer.OfflineEdge{legacy, replacement} {
		o := platformproducer.OfflineRecoveryObservation{EdgeID: edge.EdgeID, DeclarationDigest: platformproducer.OfflineRecoveryDigest(edge), LastAttempt: at, LastSuccess: at, ProofValidUntil: at.Add(time.Hour), Successes: 3, Admitted: true}
		if _, err := s.store.UpdateObservationCheckpoint(t.Context(), platformproducer.OfflineRecoveryKey(edge), func([]byte) ([]byte, error) { return json.Marshal(o) }); err != nil {
			t.Fatal(err)
		}
		projection := platformIntentProjectionResponse{Policy: platformconfig.PolicySnapshot{Generation: "old", Scope: "global"}}
		if err := s.projectDNSReadinessWithRecovery(t.Context(), &projection, nodes, at, &input); err != nil {
			t.Fatal(err)
		}
		want := 1
		if edge.WorkerEnrollment != nil {
			want = 2
		}
		if len(projection.RuntimeSnapshot.DNSEdgeEndpoints) != want {
			t.Fatal("candidate membership did not require the exact fresh enrollment proof", edge, projection.RuntimeSnapshot.DNSEdgeEndpoints)
		}
	}
}
