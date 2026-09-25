package entryfailover

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"strings"
	"testing"
)

func TestProbeRPCRequiresSignedScopedRequest(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	verificationKey := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	policy := testPolicy()
	policy.Vantages = append(policy.Vantages, Vantage{ID: "remote", Transport: "ssh", SSHHost: "separate", BinaryPath: "/usr/local/bin/fugue-entry-failover", PublicKeyPath: "/etc/fugue-entry-failover/signing.pub"})
	signed, err := Sign(policy, "signer", private)
	if err != nil {
		t.Fatal(err)
	}
	request := ProbeRPCRequest{Schema: PolicySchema, SignedPolicy: signed, TargetID: "west", VantageID: "remote", Nonce: strings.Repeat("a", 32)}
	raw, _ := json.Marshal(request)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ProbeRPC(ctx, raw, verificationKey); err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("cancelled but signed request did not reach bounded probe: %v", err)
	}
	request.TargetID = "outside"
	raw, _ = json.Marshal(request)
	if _, err := ProbeRPC(context.Background(), raw, verificationKey); err == nil {
		t.Fatal("outside target accepted")
	}
	request.TargetID = "west"
	request.SignedPolicy.Policy.Targets[0].Address = "192.0.2.99"
	raw, _ = json.Marshal(request)
	if _, err := ProbeRPC(context.Background(), raw, verificationKey); err == nil {
		t.Fatal("tampered policy accepted")
	}
	request = ProbeRPCRequest{Schema: PolicySchema, SignedPolicy: signed, TargetID: "west", VantageID: "remote", Nonce: strings.Repeat("a", 32)}
	raw, _ = json.Marshal(request)
	if _, err := ProbeRPC(context.Background(), append(raw, []byte(" {}")...), verificationKey); err == nil {
		t.Fatal("trailing data accepted")
	}
}
