package trafficoverride

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func testSigningKey(t *testing.T) (string, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawStdEncoding.EncodeToString(privateKey), base64.RawStdEncoding.EncodeToString(publicKey)
}

func TestTrafficOverrideSignatureRejectsTamperingAndAcceptsPreviousKey(t *testing.T) {
	now := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	override := model.TrafficOverride{
		Hostname:           "app.example.com",
		Generation:         1,
		State:              model.TrafficOverrideStateStaged,
		Answers:            []string{"192.0.2.10"},
		RequiredHostRoutes: []string{"app.example.com"},
		RouteGeneration:    "route-generation-1",
		RouteDigest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ExpiresAt:          now.Add(time.Hour),
		Reason:             "test emergency path",
		Operator:           "api-key/admin",
		SignedAt:           now,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	privateKey1, publicKey1 := testSigningKey(t)
	privateKey2, publicKey2 := testSigningKey(t)
	signed, err := Sign(override, privateKey1, "traffic-override-key-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(signed, publicKey1, "traffic-override-key-1"); err != nil {
		t.Fatal(err)
	}
	tampered := signed
	tampered.Answers = []string{"192.0.2.11"}
	if err := Verify(tampered, publicKey1, "traffic-override-key-1"); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("tampered override must fail verification: %v", err)
	}
	keyring := model.TrafficOverrideSigningKeyring{
		CurrentKeyID:       "traffic-override-key-2",
		CurrentPrivateKey:  privateKey2,
		CurrentPublicKey:   publicKey2,
		PreviousKeyID:      "traffic-override-key-1",
		PreviousPrivateKey: privateKey1,
		PreviousPublicKey:  publicKey1,
	}
	if err := VerifyWithKeyring(signed, keyring); err != nil {
		t.Fatalf("previous key should verify during rotation: %v", err)
	}
}
