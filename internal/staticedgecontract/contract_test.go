package staticedgecontract

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"
)

func TestBundleDigestAndSignatureRoundTrip(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	b := Bundle{Schema: SchemaV1, EdgeID: "edge-test", Role: "edge", Generation: 7, Mode: "serving", CaddyConfig: json.RawMessage(`{"apps":{}}`), HealthChecks: []string{"health"}, SigningKeyID: "key-1"}
	if e := SignBundle(&b, priv, "key-1"); e != nil {
		t.Fatal(e)
	}
	if e := VerifyBundle(b, pub); e != nil {
		t.Fatal(e)
	}
	b.SigningKeyID = "other"
	if VerifyBundle(b, pub) == nil {
		t.Fatal("key id was not signed")
	}
	b.SigningKeyID = "key-1"
	b.Generation++
	if VerifyBundle(b, pub) == nil {
		t.Fatal("tamper accepted")
	}
}
func TestStrictJSON(t *testing.T) {
	for _, raw := range []string{`{"edge_id":"a","edge_id":"b"}`, `{} {}`, `{"unknown":1}`} {
		var b Bundle
		if StrictJSON([]byte(raw), &b) == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	a, _ := CanonicalJSON([]byte(`{ "n":9007199254740993,"x":1 }`))
	b, _ := CanonicalJSON([]byte(`{"x":1,"n":9007199254740993}`))
	if string(a) != string(b) {
		t.Fatal("canonicalization differs")
	}
}
