package releaseguardian

import (
	"crypto/ed25519"
	"database/sql"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	authentication "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestFrontWorkerProviderGateBindsApprovalWriterAndLivePolicy(t *testing.T) {
	ctx := t.Context()
	now := time.Now().UTC()
	public, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	config := frontWorkerCutoverFixture()
	config.Group = nodeWorkerFenceFixture(t).Recipient.Group
	config.Phase = KubeCutoverNative
	policies, err := FrontWorkerCutoverPolicies(config)
	if err != nil {
		t.Fatal(err)
	}
	var objects []runtime.Object
	for _, pair := range policies {
		pair.Policy.UID, pair.Policy.ResourceVersion, pair.Policy.Generation = types.UID(pair.Policy.Name), "31", 4
		pair.Policy.Status.ObservedGeneration = pair.Policy.Generation
		pair.Policy.Status.TypeChecking = &admissionv1.TypeChecking{}
		pair.Binding.UID, pair.Binding.ResourceVersion = types.UID(pair.Binding.Name), "32"
		objects = append(objects, pair.Policy, pair.Binding)
	}
	client := fake.NewSimpleClientset(objects...)
	identity := config.Guardian
	client.PrependReactor("create", "selfsubjectreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &authentication.SelfSubjectReview{
			Status: authentication.SelfSubjectReviewStatus{UserInfo: authentication.UserInfo{Username: identity}},
		}, nil
	})
	observed, err := ObserveFrontWorkerCutover(ctx, client, config)
	if err != nil {
		t.Fatal(err)
	}
	policyDigest, err := FrontWorkerPolicyDigest(config, observed)
	if err != nil {
		t.Fatal(err)
	}
	scope := productioncontrol.Scope{ClusterID: "cluster-1", ResourceID: productioncontrol.FrontWorkerResource(config.Namespace, config.Group)}
	plan := productioncontrol.FrontWorkerShadow{
		Namespace: config.Namespace, GroupFence: FrontRepairFenceName(config.Group), LeaseName: config.ComponentLeases[0],
		Journal: nodeWorkerFenceFixture(t),
	}
	a := FrontWorkerCutoverAttestation{
		Scope: scope, IntentDigest: digest([]byte("intent")), ApprovalDigest: digest([]byte("approval")),
		JournalDigest: plan.Journal.Digest, PolicyDigest: policyDigest, EndpointAdmission: digest([]byte("admission")),
		LegacyRevocation: digest([]byte("revocation")), LegacyRequestDrain: digest([]byte("drain")),
		ShadowEvidence: digest([]byte("shadow")), NativeRuntime: digest([]byte("runtime")),
		ObservedAt: now.Add(-time.Second).Unix(), ExpiresAt: now.Add(time.Minute).Unix(),
	}
	message, err := a.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	a.Signature = ed25519.Sign(key, message)
	wire, err := declarativerelease.CanonicalJSON(a)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := productioncontrol.NewJournal(&sql.DB{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := NewFrontWorkerProviderGate(client, config, journal, wire, public)
	if err != nil {
		t.Fatal(err)
	}
	gate.now = func() time.Time { return now }
	verify := func() error { return gate.Verify(ctx, scope, a.IntentDigest, a.ApprovalDigest, plan) }
	if err := verify(); err != nil {
		t.Fatal(err)
	}
	if err := gate.Verify(ctx, scope, a.IntentDigest, digest([]byte("other approval")), plan); err == nil {
		t.Fatal("attestation authorized a different exact approval")
	}
	ownerJournal, err := productioncontrol.NewJournal(&sql.DB{}, productioncontrol.Approvers{
		"owner": {ClusterID: scope.ClusterID, PublicKey: public},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewFrontWorkerProviderGate(client, config, ownerJournal, wire, public); err == nil {
		t.Fatal("owner approval key supplied technical cutover evidence")
	}
	identity = "system:serviceaccount:front-test:old-runner"
	if err := verify(); err == nil {
		t.Fatal("cutover attestation authorized the legacy writer")
	}
	identity = config.Guardian
	gate.now = func() time.Time { return now.Add(2 * time.Minute) }
	if err := verify(); err == nil {
		t.Fatal("expired attestation authorized a new write")
	}
	claim := productioncontrol.Claim{Scope: scope, Fence: 1, IntentDigest: a.IntentDigest, ApprovalDigest: a.ApprovalDigest}
	if err := gate.VerifyRecovery(ctx, claim, plan); err != nil {
		t.Fatalf("fenced recovery lost the original authority after its forward window: %v", err)
	}
	bindings := client.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings()
	binding := policies[0].Binding.DeepCopy()
	binding.ResourceVersion = "33"
	if _, err := bindings.Update(ctx, binding, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := gate.VerifyRecovery(ctx, claim, plan); err == nil {
		t.Fatal("changed admission binding was accepted for native recovery")
	}
}
