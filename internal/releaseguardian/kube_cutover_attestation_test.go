package releaseguardian

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
)

func TestSignedDeploymentCutoverEvidenceBindsDraftAndProvider(t *testing.T) {
	ctx := context.Background()
	f := healthyPublicationFixture(t)
	draft, err := f.store.DraftNativeRelease(ctx, f.key, "isolated-cluster", "request-a", f.candidate)
	if err != nil {
		t.Fatal(err)
	}
	b := draft.Boundary
	config := cutoverFixture()
	config.Phase, config.Key, config.Namespace, config.Deployment = KubeCutoverNative, f.key, b.Namespace, b.Workload.Name
	config.MonitorComponent, config.ComponentLease = b.MonitorComponent, b.ComponentLease
	scope, err := DeploymentCutoverScope(config, b.Scope.ClusterID)
	if err != nil || scope != b.Scope {
		t.Fatalf("enrolled scope diverges from the journal: %v", err)
	}
	policy, release, err := DeploymentCutoverDigests(config, b)
	if err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	a := DeploymentCutoverAttestation{PolicyScopeDigest: policy, ReleaseBindingDigest: release,
		EndpointAdmission: digest([]byte("fixture endpoint admission")), LegacyRevocation: digest([]byte("fixture revocation")),
		LegacyRequestDrain: digest([]byte("fixture drainage")), NativeGuardian: digest([]byte("fixture Guardian")),
		NodeArtifactPulls: digest([]byte("fixture disconnected pull")), RegistryRetention: digest([]byte("fixture retention")),
		ObservedAt: f.now.Unix(), ExpiresAt: f.now.Add(time.Minute).Unix()}
	write := func(a DeploymentCutoverAttestation) []byte {
		t.Helper()
		raw, err := declarativerelease.CanonicalJSON(a)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	signing, err := a.SigningInput()
	if err != nil {
		t.Fatal(err)
	}
	a.Signature = ed25519.Sign(key, signing)
	v, err := NewSignedDeploymentCutoverEvidence(write(a), pub)
	if err != nil {
		t.Fatal(err)
	}
	now := f.now
	v.(*signedDeploymentCutoverEvidence).now = func() time.Time { return now }
	if err := v.VerifyDeploymentCutoverEvidence(ctx, config, b); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*KubeDeploymentCutover, *NativePublicationBoundary){
		"cluster": func(_ *KubeDeploymentCutover, b *NativePublicationBoundary) { b.Scope.ClusterID = "other-cluster" },
		"principal": func(c *KubeDeploymentCutover, _ *NativePublicationBoundary) {
			c.Publisher = "system:serviceaccount:fixture:other"
		},
		"uid":      func(_ *KubeDeploymentCutover, b *NativePublicationBoundary) { b.WorkloadUID += "-other" },
		"version":  func(_ *KubeDeploymentCutover, b *NativePublicationBoundary) { b.WorkloadVersion += "0" },
		"pointer":  func(_ *KubeDeploymentCutover, b *NativePublicationBoundary) { b.DesiredResourceVersion = "700" },
		"rollback": func(_ *KubeDeploymentCutover, b *NativePublicationBoundary) { b.RollbackOCI = b.ForwardOCI },
		"receipt": func(_ *KubeDeploymentCutover, b *NativePublicationBoundary) {
			b.ForwardReceipt.TopDigest = b.RollbackReceipt.TopDigest
		},
		"freeze": func(c *KubeDeploymentCutover, _ *NativePublicationBoundary) { c.Phase = KubeCutoverFrozen },
	} {
		t.Run(name, func(t *testing.T) {
			replayConfig, replay := config, b
			mutate(&replayConfig, &replay)
			if err := v.VerifyDeploymentCutoverEvidence(ctx, replayConfig, replay); err == nil {
				t.Fatal("accepted an attestation under a different live boundary")
			}
		})
	}
	now = f.now.Add(time.Minute)
	if err := v.VerifyDeploymentCutoverEvidence(ctx, config, b); err == nil {
		t.Fatal("accepted an expired provider attestation")
	}
	forged := a
	forged.LegacyRequestDrain = digest([]byte("different drainage record"))
	if _, err := NewSignedDeploymentCutoverEvidence(write(forged), pub); err == nil {
		t.Fatal("accepted changed drainage with the original signature")
	}
	for _, invalid := range []string{"", a.LegacyRevocation} {
		a.LegacyRequestDrain = invalid
		if _, err := a.SigningInput(); err == nil {
			t.Fatal("accepted missing or reused drainage evidence")
		}
	}
}
