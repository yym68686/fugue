package productioncontrol

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
	"time"
)

func approvalFixture(t *testing.T) (Intent, []byte, ed25519.PrivateKey, Approvers, time.Time) {
	t.Helper()
	payload := []byte(`{"artifact":"immutable"}`)
	intent, err := NewIntent("cluster-a", "namespace/api", "request-1", ComponentRelease, 9,
		"sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64), payload)
	if err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return intent, payload, private, Approvers{"owner-a": {ClusterID: intent.ClusterID, PublicKey: public}},
		time.Unix(1760000000, 0)
}

func TestApprovalBindsExactIntentAndTrustScope(t *testing.T) {
	intent, _, private, trusted, now := approvalFixture(t)
	approval, err := SignApproval(intent, "owner-a", private, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := approval.Verify(intent, trusted, now); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Intent){
		"cluster":    func(i *Intent) { i.ClusterID = "cluster-b" },
		"resource":   func(i *Intent) { i.ResourceID = "namespace/other" },
		"generation": func(i *Intent) { i.Generation++ },
		"current":    func(i *Intent) { i.CurrentDigest = i.PayloadDigest },
		"rollback":   func(i *Intent) { i.RollbackDigest = i.CurrentDigest },
		"payload":    func(i *Intent) { i.PayloadDigest = i.CurrentDigest },
		"request":    func(i *Intent) { i.RequestID = "request-2" },
		"operation":  func(i *Intent) { i.Operation = Maintenance },
	} {
		t.Run(name, func(t *testing.T) {
			changed := intent
			mutate(&changed)
			if err := approval.Verify(changed, trusted, now); err == nil {
				t.Fatal("approval was replayed against a different intent")
			}
		})
	}
	for _, at := range []time.Time{now.Add(-time.Second), now.Add(time.Hour)} {
		if err := approval.Verify(intent, trusted, at); err == nil {
			t.Fatal("accepted future or expired approval")
		}
	}
	if err := approval.Verify(intent, nil, now); err == nil {
		t.Fatal("accepted an approval with no configured trust")
	}
	owner := trusted["owner-a"]
	owner.ClusterID = "cluster-b"
	if err := approval.Verify(intent, Approvers{"owner-a": owner}, now); err == nil {
		t.Fatal("accepted a key trusted for a different cluster")
	}
	approval.Signature[0] ^= 1
	if err := approval.Verify(intent, trusted, now); err == nil {
		t.Fatal("accepted a forged signature")
	}
}

func TestIntentRequiresKnownOperationAndUnchangedPayload(t *testing.T) {
	intent, payload, _, _, _ := approvalFixture(t)
	if err := intent.VerifyPayload(payload); err != nil {
		t.Fatal(err)
	}
	if err := intent.VerifyPayload(append(payload, '\n')); err == nil {
		t.Fatal("accepted a changed payload")
	}
	intent.Operation = "untyped_command"
	if _, err := intent.Digest(); err == nil {
		t.Fatal("accepted unsupported operation")
	}
}
