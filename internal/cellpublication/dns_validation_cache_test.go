package cellpublication

import (
	"testing"

	"fugue/internal/testfixture/celldns"
)

func TestDNSPureValidationReuseDoesNotCacheTrust(t *testing.T) {
	a := celldns.Compile(t, celldns.TransitionRequest(t)).DNSArtifact
	keys := celldns.Keys()
	for i := 0; i < 2; i++ {
		if _, err := VerifyDNSArtifact(a, keys); err != nil {
			t.Fatal(err)
		}
	}
	revoked := celldns.Keys()
	revoked.RevokedKeyIDs = map[string]struct{}{keys.PrimaryKeyID: {}}
	if _, err := VerifyDNSArtifact(a, revoked); err == nil {
		t.Fatal("warm configuration validation bypassed revoked signing key")
	}
	previous := a.Content["previous_traffic_publication"].(map[string]any)
	previous["route"].(map[string]any)["provenance"].(map[string]any)["signature"] = "forged"
	if _, err := VerifyDNSArtifact(a, keys); err == nil {
		t.Fatal("warm configuration validation bypassed predecessor signature")
	}
}
