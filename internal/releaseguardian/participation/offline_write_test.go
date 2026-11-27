package participation

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model/edgecohort"
	"fugue/internal/productioncontrol"
	"fugue/internal/testfixture/kubeadmission"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestOfflineHoldWriteAgainstIsolatedProvider(t *testing.T) {
	const node, group, ns = "node-a", "cell-a", "fixture"
	const writer = "system:serviceaccount:fixture:hold-writer"
	cfg, identities := kubeadmission.Start(t, writer)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	nodes := admin.CoreV1().Nodes()
	n, err := nodes.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: node, Annotations: map[string]string{"fixture.example/preserve": "original"},
		Labels: map[string]string{participationRole: "true", participationGroup: group},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
	n, err = nodes.UpdateStatus(ctx, n, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	adopt := false
	topology := productioncontrol.VerifiedParticipationTopology{
		Policy: productioncontrol.ParticipationPolicy{Schema: productioncontrol.BoundParticipationPolicySchema,
			Generation: 1, Namespace: ns, Kubernetes: &productioncontrol.ParticipationKubernetesBinding{
				Endpoint: cfg.Host, CADigest: digest([]byte("fixture-ca")), NamespaceUID: "fixture-ns",
			}, Members: []productioncontrol.ParticipationMember{{
				Node: node, NodeUID: string(n.UID), Group: group, Component: "fixture-a", AdoptHold: &adopt, Reason: "network_unavailable",
			}}},
		Version:        productioncontrol.Version{Generation: 1, Digest: digest([]byte("topology"))},
		ApprovalDigest: digest([]byte("approval")), RecordedAt: now.Add(-time.Minute),
	}
	peer := n.DeepCopy()
	peer.Name, peer.UID = "node-b", "peer-uid"
	preview, err := PreviewParticipation(topology.Policy, node, ParticipationSnapshot{
		Node: *n, Inventory: &corev1.NodeList{Items: []corev1.Node{*n, *peer}}, ValidatedReaderBaseline: 2,
		Transport: &ParticipationTransport{Node: node, Group: group, ExternalIPs: []string{"192.0.2.4"}},
	}, now)
	if err != nil || preview.HoldPlan == nil {
		t.Fatalf("fixture preview failed: %v", err)
	}
	base := productioncontrol.Version{Generation: 1, Digest: HoldStateDigest(node, string(n.UID), group, n.Annotations)}
	head := productioncontrol.ResourceState{LastVerified: base, Baseline: productioncontrol.Baseline{
		Scope: HoldResource("fixture", node), Verified: base, EvidenceDigest: digest([]byte("fixture-evidence")),
	}}
	submission, err := DraftHold("fixture", "offline-a", node, topology, head, KubernetesShadow{
		Member:  MemberObservation{Node: describeObject(n), Readiness: corev1.ConditionFalse},
		Preview: &preview,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy, binding, err := RenderOfflineHoldGuard(submission, writer)
	if err != nil {
		t.Fatal(err)
	}
	admission := admin.AdmissionregistrationV1()
	if _, err := admission.ValidatingAdmissionPolicies().Create(ctx, policy, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := admission.ValidatingAdmissionPolicyBindings().Create(ctx, binding, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	illegal := n.DeepCopy()
	illegal.Annotations = preview.HoldPlan.After
	dryRun := metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := nodes.Update(ctx, illegal, dryRun)
		return apierrors.IsForbidden(err), nil
	}); err != nil {
		t.Fatalf("admission did not fence the wrong identity: %v", err)
	}
	if got, err := writeOfflineHoldOnce(ctx, cfg, submission, n); got != nil || !apierrors.IsForbidden(err) {
		t.Fatalf("write from wrong identity was not fenced: %v", err)
	}
	got, err := writeOfflineHoldOnce(ctx, identities[writer], submission, n)
	if err != nil || got == nil || got.ResourceVersion == n.ResourceVersion || !reflect.DeepEqual(got.Annotations, preview.HoldPlan.After) {
		t.Fatalf("designated writer failed to apply the exact approved delta: %v", err)
	}
	if replay, err := writeOfflineHoldOnce(ctx, identities[writer], submission, n); err == nil || replay != nil {
		t.Fatal("provider accepted stale request replay")
	}
	stored, err := nodes.Get(ctx, node, metav1.GetOptions{})
	if err != nil || stored.ResourceVersion != got.ResourceVersion || !reflect.DeepEqual(stored.Annotations, got.Annotations) {
		t.Fatalf("readback changed after a denied replay: %v", err)
	}
}

func TestOfflineHoldWriteSendsOnlyOneExactPatch(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	adopt := false
	const node, uid, group = "node-a", "uid-a", "cell-a"
	policy := productioncontrol.ParticipationPolicy{
		Schema: productioncontrol.BoundParticipationPolicySchema, Generation: 1, Namespace: "fixture",
		Kubernetes: &productioncontrol.ParticipationKubernetesBinding{
			Endpoint: "https://cluster.example.test", CADigest: digest([]byte("ca")), NamespaceUID: "ns-a",
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
	for _, withAnnotations := range []bool{false, true} {
		s := participationFixtureSnapshot(t, policy, now, "", 0)
		if !withAnnotations {
			s.Node.Annotations = nil
			s.Inventory.Items[0].Annotations = nil
		}
		shadow, err := evaluateKubernetesShadow(policy, node, memberCapture{report: MemberObservation{
			Node: describeObject(&s.Node), Readiness: s.Node.Status.Conditions[0].Status,
		}, snapshot: s}, &ReaderObservation{Namespace: policy.Namespace, Group: group, Baseline: 2}, nil, now)
		if err != nil || shadow.Preview == nil || shadow.Preview.HoldPlan == nil {
			t.Fatalf("fixture hold preview: %v", err)
		}
		previous := productioncontrol.Version{Generation: 1, Digest: HoldStateDigest(node, uid, group, s.Node.Annotations)}
		head := productioncontrol.ResourceState{LastVerified: previous, Baseline: productioncontrol.Baseline{
			Scope: HoldResource("fixture", node), Verified: previous, EvidenceDigest: digest([]byte("evidence")),
		}}
		submission, err := DraftHold("fixture", "request-a", node, topology, head, shadow)
		if err != nil {
			t.Fatal(err)
		}
		for _, mode := range []string{"ok", "limit", "redirect", "drifted response", "stale input", "untrusted CA"} {
			t.Run(mode, func(t *testing.T) {
				var requests atomic.Int32
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					if r.Method != http.MethodPatch || r.URL.Path != "/api/v1/nodes/"+node || r.URL.RawQuery != "timeout=2s" ||
						r.Header.Get("Authorization") != "Bearer fixture-only" ||
						r.Header.Get("Content-Type") != "application/json-patch+json" || r.ProtoMajor != 1 {
						t.Errorf("unexpected provider request: %s %s (query %q, protocol %s, type %q)", r.Method, r.URL.Path, r.URL.RawQuery, r.Proto, r.Header.Get("Content-Type"))
						http.Error(w, "unexpected request", http.StatusBadRequest)
						return
					}
					want := []nodePatchOperation{
						{Op: "test", Path: "/metadata/uid", Value: uid},
						{Op: "test", Path: "/metadata/resourceVersion", Value: s.Node.ResourceVersion},
					}
					if withAnnotations {
						want = append(want, nodePatchOperation{Op: "add", Path: "/metadata/annotations/fugue.io~1automatic-edge-participation",
							Value: shadow.Preview.HoldPlan.After[participationAnnotation]},
							nodePatchOperation{Op: "add", Path: "/metadata/annotations/fugue.io~1edge-release-hold",
								Value: shadow.Preview.HoldPlan.After[edgecohort.ReleaseHoldAnnotation]})
					} else {
						want = append(want, nodePatchOperation{Op: "add", Path: "/metadata/annotations", Value: map[string]any{
							participationAnnotation:          shadow.Preview.HoldPlan.After[participationAnnotation],
							edgecohort.ReleaseHoldAnnotation: shadow.Preview.HoldPlan.After[edgecohort.ReleaseHoldAnnotation],
						}})
					}
					var ops []nodePatchOperation
					if err := json.NewDecoder(r.Body).Decode(&ops); err != nil || !reflect.DeepEqual(ops, want) {
						t.Errorf("unexpected patch: %+v (%v)", ops, err)
					}
					w.Header().Set("Content-Type", "application/json")
					switch mode {
					case "limit":
						w.Header().Set("Retry-After", "1")
						w.WriteHeader(http.StatusTooManyRequests)
						_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"TooManyRequests","code":429}`))
					case "redirect":
						http.Redirect(w, r, "/must-not-follow", http.StatusTemporaryRedirect)
					default:
						observed := s.Node.DeepCopy()
						observed.Kind, observed.APIVersion, observed.ResourceVersion = "Node", "v1", "11"
						observed.Annotations = shadow.Preview.HoldPlan.After
						if mode == "drifted response" {
							observed.Annotations = map[string]string{"unrelated": "drift"}
						}
						_ = json.NewEncoder(w).Encode(observed)
					}
				}))
				defer server.Close()
				ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
				cfg := &rest.Config{Host: server.URL, BearerToken: "fixture-only", Timeout: 2 * time.Second,
					TLSClientConfig: rest.TLSClientConfig{CAData: ca}}
				live := s.Node.DeepCopy()
				wantRequests := int32(1)
				switch mode {
				case "stale input":
					live.ResourceVersion = "12"
					wantRequests = 0
				case "untrusted CA":
					cfg.CAData = nil
					wantRequests = 0
				}
				got, err := writeOfflineHoldOnce(context.Background(), cfg, submission, live)
				if requests.Load() != wantRequests || (err == nil) != (mode == "ok") ||
					(mode == "ok" && (got == nil || got.ResourceVersion != "11")) || (err != nil && got != nil) {
					t.Fatalf("result version, error or attempt count incorrect: requests=%d err=%v", requests.Load(), err)
				}
			})
		}
	}
}
