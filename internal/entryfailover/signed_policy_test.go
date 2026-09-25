package entryfailover

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"
)

func TestSignedPolicyRejectsScopeAndModeTampering(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Sign(testPolicy(), "operator", priv)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]ed25519.PublicKey{"operator": pub}
	if err := s.Verify(keys); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ParseSignedPolicy(raw, keys); err != nil {
		t.Fatal(err)
	}
	s.Policy.Mode = "automatic"
	if err = s.Verify(keys); err == nil {
		t.Fatal("unsigned mode escalation accepted")
	}
	s.Policy.Mode = "shadow"
	s.Policy.Hostnames[1] = "other.example.test"
	if err = s.Verify(keys); err == nil {
		t.Fatal("unsigned scope expansion accepted")
	}
}
