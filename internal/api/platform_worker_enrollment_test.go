package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
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

func TestIndependentWorkerPreparationRequiresExactLiveSingletonOwners(t *testing.T) {
	at := time.Now().Truncate(time.Second)
	h := platformproducer.OfflineEdge{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", WorkerEnrollment: testWorkerEnrollment(at)}
	grant := &platformproducer.OfflineWorkerDeployment{
		Name: "local-worker", UID: "33333333-3333-3333-3333-333333333333",
		EdgeImage:  "registry.example/system/edge@sha256:" + strings.Repeat("a", 64),
		CaddyImage: "registry.example/system/caddy@sha256:" + strings.Repeat("b", 64),
	}
	h.WorkerEnrollment.IndependentDeployment = grant
	node := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: h.EdgeID, UID: "11111111-1111-1111-1111-111111111111",
		Labels: map[string]string{"fugue.io/edge-group-id": h.EdgeGroupID, "fugue.io/role.edge": "true"}}}
	yes, one := true, int32(1)
	const ns, rsName = "test-system", "local-worker-live"
	template := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"fugue.io/edge-group-id": h.EdgeGroupID, "fugue.io/edge-node": h.EdgeID, "component": "worker"}},
		Spec: corev1.PodSpec{ServiceAccountName: h.WorkerEnrollment.ServiceAccount, NodeSelector: map[string]string{"kubernetes.io/hostname": h.EdgeID},
			Containers: []corev1.Container{{Name: "edge", Image: grant.EdgeImage}, {Name: "caddy", Image: grant.CaddyImage}}},
	}
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: grant.Name, Namespace: ns, UID: "33333333-3333-3333-3333-333333333333", ResourceVersion: "20"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one, Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"component": "worker"}}, Template: template,
		},
	}
	rsTemplate := template.DeepCopy()
	rsTemplate.Labels[appsv1.DefaultDeploymentUniqueLabelKey] = "new-template"
	rs := appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: rsName, Namespace: ns, UID: "44444444-4444-4444-4444-444444444444", ResourceVersion: "30",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: grant.Name, UID: deployment.UID, Controller: &yes}}},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"component": "worker", appsv1.DefaultDeploymentUniqueLabelKey: "new-template"}}, Template: *rsTemplate,
		},
	}
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "local-worker-pod", Namespace: ns, UID: "55555555-5555-5555-5555-555555555555", ResourceVersion: "33",
			Labels: rsTemplate.Labels, CreationTimestamp: metav1.NewTime(at.Add(time.Second)),
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rsName, UID: rs.UID, Controller: &yes}}},
		Spec: *rsTemplate.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodPending}, // init, not selected or Ready
	}
	pod.Spec.NodeName = node.Name
	for _, test := range []struct {
		name string
		edit func(*corev1.Pod, *corev1.Node, *appsv1.ReplicaSet, *appsv1.Deployment)
	}{
		{"matching pending candidate", nil},
		{"foreign node uid", func(_ *corev1.Pod, n *corev1.Node, _ *appsv1.ReplicaSet, _ *appsv1.Deployment) { n.UID = "wrong" }},
		{"old pod", func(p *corev1.Pod, _ *corev1.Node, _ *appsv1.ReplicaSet, _ *appsv1.Deployment) {
			p.CreationTimestamp = metav1.NewTime(at)
		}},
		{"mutable image", func(p *corev1.Pod, _ *corev1.Node, _ *appsv1.ReplicaSet, _ *appsv1.Deployment) {
			p.Spec.Containers[0].Image = "edge:latest"
		}},
		{"foreign Pod owner UID", func(p *corev1.Pod, _ *corev1.Node, _ *appsv1.ReplicaSet, _ *appsv1.Deployment) {
			p.OwnerReferences[0].UID = "wrong"
		}},
		{"foreign ReplicaSet parent", func(_ *corev1.Pod, _ *corev1.Node, r *appsv1.ReplicaSet, _ *appsv1.Deployment) {
			r.OwnerReferences[0].UID = "wrong"
		}},
		{"recreated Deployment", func(_ *corev1.Pod, _ *corev1.Node, _ *appsv1.ReplicaSet, d *appsv1.Deployment) { d.UID = "wrong" }},
		{"stale ReplicaSet template", func(_ *corev1.Pod, _ *corev1.Node, r *appsv1.ReplicaSet, _ *appsv1.Deployment) {
			r.Spec.Template.Spec.Containers[1].Image = "different-caddy"
		}},
		{"deployment rolls with surge", func(_ *corev1.Pod, _ *corev1.Node, _ *appsv1.ReplicaSet, d *appsv1.Deployment) {
			d.Spec.Strategy.Type = appsv1.RollingUpdateDeploymentStrategyType
		}},
		{"selector no longer pins", func(_ *corev1.Pod, _ *corev1.Node, _ *appsv1.ReplicaSet, d *appsv1.Deployment) {
			d.Spec.Template.Spec.NodeSelector = nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, n, r, d := pod.DeepCopy(), node.DeepCopy(), rs.DeepCopy(), deployment.DeepCopy()
			if test.edit != nil {
				test.edit(p, n, r, d)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet || req.Header.Get("Authorization") != "Bearer api-reader" {
					t.Error("owner verification used a write or the wrong credential")
					w.WriteHeader(http.StatusForbidden)
					return
				}
				switch req.URL.Path {
				case "/apis/apps/v1/namespaces/" + ns + "/replicasets/" + rsName:
					_ = json.NewEncoder(w).Encode(r)
				case "/apis/apps/v1/namespaces/" + ns + "/deployments/" + grant.Name:
					_ = json.NewEncoder(w).Encode(d)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client := &clusterNodeClient{client: server.Client(), baseURL: server.URL, bearerToken: "api-reader"}
			err := verifyIndependentWorkerDeployment(t.Context(), client, h, *p, *n)
			if (err == nil) != (test.edit == nil) {
				t.Fatalf("unexpected independent Workload verification: %v", err)
			}
		})
	}
}

func TestIndependentWorkerCannotReportAfterItsPinnedFrontIsRemoved(t *testing.T) {
	const pin = "11111111-1111-1111-1111-111111111111"
	who := platformcontrol.PlatformComponentIdentityClaims{NodeID: "edge-a", ScopeKey: "global", Component: model.PlatformConsumerComponentEdgeWorker,
		WorkerFrontServiceUID: pin}
	for _, pins := range []WorkerFrontBindings{nil, {"edge-a": {uid: "22222222-2222-2222-2222-222222222222"}}} {
		s := &Server{workerFrontBindings: pins}
		w := httptest.NewRecorder()
		if _, ok := s.admitPhysicalWorker(w, httptest.NewRequest(http.MethodPost, "/", nil), who, true); ok || w.Code != http.StatusServiceUnavailable {
			t.Fatalf("independent Worker escaped its exact Front fence: %d", w.Code)
		}
	}
}
