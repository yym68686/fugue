package releaseguardian

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
	"testing"
	"time"

	"fugue/internal/model/edgecohort"
	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian/participation"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
)

func TestOfflineHoldGuardAtProvider(t *testing.T) {
	cfg := startAdmissionTestServer(t)
	admin := admissionClient(t, cfg, "")
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	const node, group, ns = "member-a", "cell-a", "fixture"
	const writer = "system:serviceaccount:fixture:hold-writer"
	n, err := admin.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: node, Labels: map[string]string{"fugue.io/role.edge": "true", "fugue.io/edge-group-id": group},
		Annotations: map[string]string{"fixture.example/preserve": "original"},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	adopt := false
	now := time.Now().UTC()
	topology := productioncontrol.VerifiedParticipationTopology{
		Version:        productioncontrol.Version{Generation: 1, Digest: digest([]byte("topology"))},
		ApprovalDigest: digest([]byte("approval")), RecordedAt: now.Add(-time.Hour),
		Policy: productioncontrol.ParticipationPolicy{Schema: productioncontrol.BoundParticipationPolicySchema,
			Generation: 1, Namespace: ns, Kubernetes: &productioncontrol.ParticipationKubernetesBinding{
				Endpoint: cfg.Host, CADigest: digest([]byte("fixture")), NamespaceUID: "ns-fixture",
			}, Members: []productioncontrol.ParticipationMember{{
				Node: node, NodeUID: string(n.UID), Group: group, Component: "fixture-a", AdoptHold: &adopt, Reason: "network_unavailable",
			}}},
	}
	snapshot := n.DeepCopy()
	snapshot.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
	peer := n.DeepCopy()
	peer.Name, peer.UID = "member-b", "peer-uid"
	preview, err := participation.PreviewParticipation(topology.Policy, node, participation.ParticipationSnapshot{
		Node: *snapshot, Inventory: &corev1.NodeList{Items: []corev1.Node{*snapshot, *peer}}, ValidatedReaderBaseline: 2,
		Transport: &participation.ParticipationTransport{Node: node, Group: group, ExternalIPs: []string{"192.0.2.4"}},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	base := productioncontrol.Version{Generation: 1, Digest: participation.HoldStateDigest(node, string(n.UID), group, n.Annotations)}
	head := productioncontrol.ResourceState{LastVerified: base, Baseline: productioncontrol.Baseline{
		Scope: participation.HoldResource("fixture", node), Verified: base, EvidenceDigest: digest([]byte("evidence")),
	}}
	s, err := participation.DraftHold("fixture", "hold-a", node, topology, head, participation.KubernetesShadow{
		Member:  participation.MemberObservation{Node: participation.ObjectVersion{Name: node, UID: string(n.UID), ResourceVersion: n.ResourceVersion}},
		Preview: &preview,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := participation.RenderOfflineHoldGuard(s, "system:serviceaccount:fixture:default"); err == nil {
		t.Fatal("accepted the default ServiceAccount")
	}
	policy, binding, err := participation.RenderOfflineHoldGuard(s, writer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.AdmissionregistrationV1().ValidatingAdmissionPolicies().Create(ctx, policy, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Create(ctx, binding, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	observer, err := participation.NewKubernetesObserver(cfg)
	if err != nil {
		t.Fatal(err)
	}
	read, err := observer.ObserveOfflineHoldGuard(ctx, topology, s, writer)
	if err != nil || read.Node != node || read.Policy.Name != policy.Name || read.Binding.Name != binding.Name ||
		read.Policy.UID == "" || read.Binding.UID == "" {
		t.Fatalf("exact stored guard was not observed: %+v, %v", read, err)
	}
	if _, err := observer.ObserveOfflineHoldGuard(ctx, topology, s, "system:serviceaccount:fixture:another-writer"); err == nil {
		t.Fatal("accepted guard for a different writer")
	}
	changedTopology := topology
	changedTopology.ApprovalDigest = digest([]byte("new approval"))
	if _, err := observer.ObserveOfflineHoldGuard(ctx, changedTopology, s, writer); err == nil {
		t.Fatal("accepted a guard referring to a superseded topology approval")
	}
	if _, err := observer.ObserveNodeFreeze(ctx, topology.Policy, node); err == nil {
		t.Fatal("mistook an armed guard for a strict freeze")
	}
	native := admissionClient(t, cfg, writer).CoreV1().Nodes()
	legacy := admissionClient(t, cfg, "system:serviceaccount:fixture:legacy").CoreV1().Nodes()
	proposal := func() *corev1.Node {
		next := n.DeepCopy()
		next.Annotations = maps.Clone(preview.HoldPlan.After)
		return next
	}
	dry := metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}}
	isDenied := func(err error) bool {
		return apierrors.IsForbidden(err) && strings.Contains(err.Error(), "Fugue participation freeze denies mutation")
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := legacy.Update(ctx, proposal(), dry)
		return isDenied(err), nil
	}); err != nil {
		t.Fatalf("legacy identity was not fenced: %v", err)
	}
	for name, edit := range map[string]func(*corev1.Node){
		"other annotation":   func(n *corev1.Node) { n.Annotations["fixture.example/preserve"] = "changed" },
		"removed annotation": func(n *corev1.Node) { delete(n.Annotations, "fixture.example/preserve") },
		"other label":        func(n *corev1.Node) { n.Labels["fixture.example/extra"] = "changed" },
		"node spec":          func(n *corev1.Node) { n.Spec.Unschedulable = true },
		"other hold":         func(n *corev1.Node) { n.Annotations[edgecohort.ReleaseHoldAnnotation] = "wrong" },
		"unrelated-only update": func(n *corev1.Node) {
			n.Annotations = map[string]string{"fixture.example/preserve": "changed"}
		},
	} {
		next := proposal()
		edit(next)
		if _, err := native.Update(ctx, next, dry); !isDenied(err) {
			t.Fatalf("%s: expected exact guard denial, got %v", name, err)
		}
	}
	applied, err := native.Update(ctx, proposal(), metav1.UpdateOptions{})
	if err != nil || !maps.Equal(applied.Annotations, preview.HoldPlan.After) || applied.ResourceVersion == n.ResourceVersion {
		t.Fatalf("exact hold did not apply at the approved version: %v", err)
	}
	retargeted := proposal()
	retargeted.ResourceVersion = applied.ResourceVersion
	if _, err := native.Update(ctx, retargeted, dry); !isDenied(err) {
		t.Fatalf("replay using the new resourceVersion was not denied: %v", err)
	}
	if err := native.Delete(ctx, node, metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}}); !isDenied(err) {
		t.Fatalf("native writer was allowed to delete node: %v", err)
	}
}

func TestParticipationFreezeAdmissionAtProvider(t *testing.T) {
	cfg := startAdmissionTestServer(t)
	admin := admissionClient(t, cfg, "")
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	const protected = "member-a"
	const phase = "fugue.io/automatic-edge-participation"
	const hold = edgecohort.ReleaseHoldAnnotation
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: protected,
		Labels:      map[string]string{"test-fence": "protected", "fugue.io/edge-group-id": "cell-a", "fugue.io/role.edge": "true"},
		Annotations: map[string]string{hold: "prior hold"}}}
	if _, err := admin.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	policy, binding, err := participation.FreezeNodePolicy(protected)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.AdmissionregistrationV1().ValidatingAdmissionPolicies().Create(ctx, policy, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Create(ctx, binding, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	oldCI := admissionClient(t, cfg, "system:serviceaccount:fixture:legacy-runner").CoreV1().Nodes()
	oldAPI := admissionClient(t, cfg, "system:serviceaccount:fixture:legacy-api").CoreV1().Nodes()
	kubelet := admissionClient(t, cfg, "system:node:"+protected).CoreV1().Nodes()
	isDenied := func(err error) bool {
		return apierrors.IsForbidden(err) && strings.Contains(err.Error(), "Fugue participation freeze denies mutation")
	}
	requireDenied := func(err error) {
		t.Helper()
		if !isDenied(err) {
			t.Fatalf("expected participation freeze denial, got %v", err)
		}
	}
	patchAnnotation := func(key string, value any) []byte {
		raw, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]any{key: value}}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 12*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := oldCI.Patch(ctx, protected, types.MergePatchType, patchAnnotation(phase, "proposal"),
			metav1.PatchOptions{DryRun: []string{metav1.DryRunAll}})
		return isDenied(err), nil
	}); err != nil {
		t.Fatalf("the provider did not activate the freeze: %v", err)
	}
	for _, value := range []any{"different hold", nil, ""} {
		_, err := oldCI.Patch(ctx, protected, types.MergePatchType, patchAnnotation(hold, value), metav1.PatchOptions{})
		requireDenied(err)
	}
	_, err = oldAPI.Patch(ctx, protected, types.MergePatchType, patchAnnotation(phase, "new phase"), metav1.PatchOptions{})
	requireDenied(err)
	for _, patch := range []string{
		`{"metadata":{"labels":{"fugue.io/edge-group-id":"other"}}}`,
		`{"metadata":{"labels":{"fugue.io/role.edge":null}}}`,
		`{"metadata":{"labels":{"fugue.io/edge-enrollment-phase":"preparing"}}}`,
		`{"metadata":{"labels":{"fugue.io/schedulable":"false"}}}`,
		`{"metadata":{"labels":{"fugue.io/edge-workload":"dynamic"}}}`,
	} {
		_, err := oldAPI.Patch(ctx, protected, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
		requireDenied(err)
	}
	requireDenied(oldAPI.Delete(ctx, protected, metav1.DeleteOptions{}))
	requireDenied(oldCI.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: "test-fence=protected"}))

	live, err := kubelet.Get(ctx, protected, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	live.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	live, err = kubelet.UpdateStatus(ctx, live, metav1.UpdateOptions{})
	if err != nil || len(live.Status.Conditions) != 1 || live.Status.Conditions[0].Status != corev1.ConditionTrue {
		t.Fatalf("ordinary kubelet status failed under freeze: %v", err)
	}
	_, err = oldAPI.Patch(ctx, protected, types.MergePatchType, patchAnnotation(hold, "via status"), metav1.PatchOptions{}, "status")
	if err != nil {
		requireDenied(err)
	}
	live, err = admin.CoreV1().Nodes().Get(ctx, protected, metav1.GetOptions{})
	if err != nil || live.Annotations[hold] != "prior hold" || live.Annotations[phase] != "" {
		t.Fatalf("frozen participation state changed: %v", err)
	}
	if _, err := kubelet.Patch(ctx, protected, types.MergePatchType,
		[]byte(`{"metadata":{"labels":{"fixture.example/other":"allowed"}}}`), metav1.PatchOptions{}); err != nil {
		t.Fatalf("freeze interfered with unrelated node metadata: %v", err)
	}

	other := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "neighbor", Annotations: map[string]string{hold: "unrelated"}}}
	if _, err := oldCI.Create(ctx, other, metav1.CreateOptions{}); err != nil {
		t.Fatalf("freeze leaked to an unrelated Node: %v", err)
	}
	admitted := "replacement-a"
	replacementPolicy, replacementBinding, err := participation.FreezeNodePolicy(admitted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.AdmissionregistrationV1().ValidatingAdmissionPolicies().Create(ctx, replacementPolicy, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Create(ctx, replacementBinding, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 12*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := oldCI.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: admitted}},
			metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
		return isDenied(err), nil
	}); err != nil {
		t.Fatalf("the provider did not deny creation under the frozen name: %v", err)
	}
}
