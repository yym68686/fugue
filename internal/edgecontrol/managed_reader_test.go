package edgecontrol

import (
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestManagedReaderIdentityIsNodeGroupPurposeAndTimeBound(t *testing.T) {
	now := time.Now().UTC()
	group, node := "edge-group-fixture", "node-new"
	dir, inventoryToken := writeInventoryPlatformIdentityGroupFixture(t, now, group, node)
	keyring := platformcontrol.DerivePlatformComponentIdentityKeyring("inventory-platform-identity-key-0123456789abcdef", "inventory-platform-current", "", "", nil)
	claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "edge-route-reader", Component: model.PlatformConsumerComponentEdgeWorker, NodeID: node, ScopeKey: group, ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle}}
	token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, claims, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	h := &groupBundleHandler{managedIdentityKeyringDir: dir, now: func() time.Time { return now }}
	if !h.authenticateManagedReader(group, node, token) {
		t.Fatal("valid node-bound reader rejected")
	}
	for _, test := range []struct{ group, node, token string }{{group, "node-other", token}, {"edge-group-other", node, token}, {group, node, inventoryToken}, {group, node, token + "x"}} {
		if h.authenticateManagedReader(test.group, test.node, test.token) {
			t.Fatal("reader authorization crossed identity or purpose")
		}
	}
	h.now = func() time.Time { return now.Add(2 * time.Minute) }
	if h.authenticateManagedReader(group, node, token) {
		t.Fatal("expired read identity accepted")
	}
}
