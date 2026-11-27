package releaseguardian

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func TestFrontWorkerTerminalClosureRequiresFinalizedNativeReceipt(t *testing.T) {
	ctx := t.Context()
	j := nodeWorkerFenceFixture(t)
	config := frontWorkerCutoverFixture()
	config.Group, config.Phase = j.Recipient.Group, KubeCutoverNative
	start := j.CreatedAt.Add(time.Hour)
	claim := productioncontrol.Claim{
		Scope: productioncontrol.Scope{ClusterID: "production_1", ResourceID: productioncontrol.FrontWorkerResource(config.Namespace, config.Group)},
		Fence: 3, IntentDigest: digest([]byte("intent")), ApprovalDigest: digest([]byte("approval")),
	}
	pairs, err := FrontWorkerCutoverPolicies(config)
	if err != nil {
		t.Fatal(err)
	}
	var objects []runtime.Object
	for _, pair := range pairs {
		pair.Policy.UID, pair.Binding.UID = types.UID(pair.Policy.Name), types.UID(pair.Binding.Name)
		pair.Policy.ResourceVersion, pair.Binding.ResourceVersion = "4", "5"
		pair.Policy.Generation, pair.Policy.Status.ObservedGeneration = 1, 1
		pair.Policy.Status.TypeChecking = &admissionv1.TypeChecking{}
		objects = append(objects, pair.Policy, pair.Binding)
	}
	client := fake.NewSimpleClientset(objects...)
	client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
		cm.UID, cm.ResourceVersion, cm.CreationTimestamp = types.UID(cm.Name), "23", metav1.NewTime(start)
		return false, nil, nil
	})
	store, err := NewAuthorityStore(client, config.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.ClaimNativeNodeWorker(ctx, j, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordNodeWorkerResult(ctx, fence, j.Expected, "expired_without_cutover", nil); err != nil {
		t.Fatal(err)
	}
	after := start.Add(time.Minute)
	refuse := func(at time.Time, c productioncontrol.Claim, reason string) {
		t.Helper()
		if _, _, err := store.ObserveFrontWorkerTerminalClosure(ctx, config, j, c, at); err == nil || !strings.Contains(err.Error(), reason) {
			t.Fatalf("expected refusal matching %q, got %v", reason, err)
		}
	}
	refuse(after, claim, "fence")
	if err := store.FinalizeNodeWorker(ctx, fence); err != nil {
		t.Fatal(err)
	}
	refuse(start.Add(10*time.Second), claim, "mature finalization")
	other := claim
	other.Fence++
	refuse(after, other, "immutable terminal receipt")
	configMaps := client.CoreV1().ConfigMaps(config.Namespace)
	activatedName := transitionActivatedJournalName(config.Group)
	if _, err := configMaps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: activatedName}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	refuse(after, claim, "ordinary Front activation")
	if err := configMaps.Delete(ctx, activatedName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	groupLease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: FrontWorkerGroupLeaseName(config.Group), Namespace: config.Namespace, UID: "group-lease", ResourceVersion: "30"},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: ptr.To("other-guardian"), RenewTime: &metav1.MicroTime{Time: j.CreatedAt}, LeaseDurationSeconds: ptr.To(int32(30))},
	}
	leases := client.CoordinationV1().Leases(config.Namespace)
	if _, err := leases.Create(ctx, groupLease, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	refuse(after, claim, "still has a holder")
	groupLease.Spec.HolderIdentity = ptr.To("")
	if _, err := leases.Update(ctx, groupLease, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	got, receipt, err := store.ObserveFrontWorkerTerminalClosure(ctx, config, j, claim, after)
	if err != nil || got.Claim != claim || got.Journal != j.Digest || got.Archive.UID == "" ||
		len(got.Leases) != 3 || got.Leases[0].UID != "group-lease" || receipt.Outcome != "expired_without_cutover" {
		t.Fatalf("closed transaction not observed: %+v, %+v, %v", got, receipt, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" {
			t.Fatalf("closure readback issued %s", action.GetVerb())
		}
	}
}
