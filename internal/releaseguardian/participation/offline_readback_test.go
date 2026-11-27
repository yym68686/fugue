package participation

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func draftFixtureOfflineHold(t *testing.T, topology productioncontrol.VerifiedParticipationTopology, now time.Time) (productioncontrol.Submission, *corev1.Node, map[string]string) {
	t.Helper()
	member := topology.Policy.Members[0]
	node, uid, group, ns := member.Node, member.NodeUID, member.Group, topology.Policy.Namespace
	snapshot := participationFixtureSnapshot(t, topology.Policy, now, "", 0)
	shadow, err := evaluateKubernetesShadow(topology.Policy, node, memberCapture{report: MemberObservation{
		Node: describeObject(&snapshot.Node), Readiness: snapshot.Node.Status.Conditions[0].Status,
	}, snapshot: snapshot}, &ReaderObservation{Namespace: ns, Group: group, Baseline: 2}, nil, now)
	if err != nil || shadow.Preview == nil || shadow.Preview.HoldPlan == nil {
		t.Fatalf("fixture hold preview: %v", err)
	}
	previous := productioncontrol.Version{Generation: 1, Digest: HoldStateDigest(node, uid, group, snapshot.Node.Annotations)}
	head := productioncontrol.ResourceState{LastVerified: previous, Baseline: productioncontrol.Baseline{
		Scope: HoldResource("fixture", node), Verified: previous, EvidenceDigest: digest([]byte("evidence")),
	}}
	submission, err := DraftHold("fixture", "request-a", node, topology, head, shadow)
	if err != nil {
		t.Fatal(err)
	}
	return submission, &snapshot.Node, shadow.Preview.HoldPlan.After
}

func TestOfflineHoldReadbackUsesOnlyBoundLiveReads(t *testing.T) {
	const node, uid, group, ns = "node-a", "uid-a", "cell-a", "fixture"
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	adopt := false
	for _, tc := range []struct {
		mode string
		want OfflineHoldReadbackState
	}{
		{"predecessor", OfflineHoldPredecessorVisible},
		{"target", OfflineHoldTargetVisible},
		{"old digest at new version", OfflineHoldConditionChanged},
		{"drift", OfflineHoldConditionChanged},
		{"contradiction", ""},
		{"recreated node", ""},
		{"changed role", ""},
		{"changed group", ""},
		{"moving version", ""},
		{"recreated namespace", ""},
		{"missing", ""},
		{"redirect", ""},
		{"changed approval", ""},
		{"changed endpoint", ""},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			var original, target corev1.Node
			var nodeReads, nsReads atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.RawQuery != "timeout=2s" ||
					r.Header.Get("Authorization") != "Bearer fixture-reader" {
					t.Errorf("unexpected readback request: %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v1/namespaces/" + ns:
					n := corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, UID: "ns-a", ResourceVersion: "1"}}
					if nsReads.Add(1) == 2 && tc.mode == "recreated namespace" {
						n.UID = "ns-b"
					}
					_ = json.NewEncoder(w).Encode(n)
				case "/api/v1/nodes/" + node:
					count := nodeReads.Add(1)
					n := target.DeepCopy()
					switch tc.mode {
					case "predecessor":
						n = original.DeepCopy()
					case "old digest at new version":
						n = original.DeepCopy()
						n.ResourceVersion = "11"
					case "drift":
						n.Annotations[participationAnnotation] = "changed"
					case "contradiction":
						n.ResourceVersion = original.ResourceVersion
					case "recreated node":
						n.UID = "new-uid"
					case "changed role":
						delete(n.Labels, participationRole)
					case "changed group":
						n.Labels[participationGroup] = "cell-b"
					case "moving version":
						if count == 2 {
							n.ResourceVersion = "12"
						}
					case "missing":
						http.NotFound(w, r)
						return
					case "redirect":
						http.Redirect(w, r, "/must-not-follow", http.StatusTemporaryRedirect)
						return
					}
					_ = json.NewEncoder(w).Encode(n)
				default:
					t.Errorf("unexpected endpoint %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			policy := productioncontrol.ParticipationPolicy{
				Schema: productioncontrol.BoundParticipationPolicySchema, Generation: 1, Namespace: ns,
				Kubernetes: &productioncontrol.ParticipationKubernetesBinding{
					Endpoint: server.URL, CADigest: digest(server.Certificate().Raw), NamespaceUID: "ns-a",
				},
				Members: []productioncontrol.ParticipationMember{{
					Node: node, NodeUID: uid, Group: group, Component: "component-a", Front: "front-a",
					Workers: productioncontrol.ParticipationWorkers{A: "worker-a", B: "worker-b"},
					Reason:  "network_unavailable", AdoptHold: &adopt, StableSeconds: 300,
				}},
			}
			topology := productioncontrol.VerifiedParticipationTopology{
				Policy: policy, Version: productioncontrol.Version{Generation: 1, Digest: digest([]byte("topology"))},
				ApprovalDigest: digest([]byte("approval")), RecordedAt: now.Add(-time.Minute),
			}
			submission, before, after := draftFixtureOfflineHold(t, topology, now)
			original = *before
			target = *original.DeepCopy()
			target.ResourceVersion = "11"
			target.Annotations = after
			cfg := &rest.Config{Host: server.URL, BearerToken: "fixture-reader", Timeout: 2 * time.Second,
				TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
			observer, err := NewBoundKubernetesObserver(cfg, policy)
			if err != nil {
				t.Fatal(err)
			}
			switch tc.mode {
			case "changed approval":
				topology.ApprovalDigest = digest([]byte("new approval"))
			case "changed endpoint":
				binding := *policy.Kubernetes
				binding.Endpoint = "https://new.invalid"
				topology.Policy.Kubernetes = &binding
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			r, err := observer.ObserveOfflineHoldReadback(ctx, topology, submission)
			if tc.want == "" {
				if r != (OfflineHoldReadback{}) || err == nil {
					t.Fatalf("unproven readback returned %+v, %v", r, err)
				}
				if (tc.mode == "changed approval" || tc.mode == "changed endpoint") && (nodeReads.Load() != 0 || nsReads.Load() != 0) {
					t.Fatal("unapproved topology issued provider reads")
				}
				return
			}
			if err != nil || r.State != tc.want || r.Node.Name != node || r.Node.UID != uid ||
				r.ExpectedResourceVersion != original.ResourceVersion || r.PredecessorDigest != submission.Intent.CurrentDigest ||
				r.TargetDigest != submission.Intent.TargetDigest || !canonicalDigest(r.ObservedDigest) ||
				nodeReads.Load() != 2 || nsReads.Load() != 2 {
				t.Fatalf("wrong readback: %+v, %v (reads: node %d namespace %d)", r, err, nodeReads.Load(), nsReads.Load())
			}
		})
	}
}
