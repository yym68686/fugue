package releaseguardian

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/model/edgecohort"
	"fugue/internal/releaseguardian/participation"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
)

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
