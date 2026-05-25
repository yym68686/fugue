package meshrecovery

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"testing"
	"time"
)

func TestPeerDirectorySignatureRoundTrip(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	directory, err := SignPeerDirectory(PeerDirectory{
		Generation: "meshgen-1",
		Nodes: []MeshNode{
			{NodeID: "node-a", MeshIP: "100.64.0.10"},
		},
	}, "signing-key", "key-1", time.Minute, now)
	if err != nil {
		t.Fatalf("sign directory: %v", err)
	}
	if directory.Signature == "" {
		t.Fatalf("expected signature")
	}
	if err := VerifyPeerDirectory(directory, "signing-key", "key-1", now.Add(30*time.Second)); err != nil {
		t.Fatalf("verify directory: %v", err)
	}

	directory.Nodes[0].MeshIP = "100.64.0.11"
	if err := VerifyPeerDirectory(directory, "signing-key", "key-1", now.Add(30*time.Second)); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected invalid signature after tamper, got %v", err)
	}
}

func TestGenerationManifestSignatureExpiry(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	manifest, err := SignGenerationManifest(GenerationManifest{
		Generation:     "meshgen-reset",
		Mode:           GenerationModeReset,
		LoginServer:    "https://mesh.example.test",
		RejoinRequired: true,
	}, "signing-key", "key-1", time.Minute, now)
	if err != nil {
		t.Fatalf("sign generation: %v", err)
	}
	if err := VerifyGenerationManifest(manifest, "signing-key", "key-1", now.Add(2*time.Minute)); !errors.Is(err, ErrExpiredManifest) {
		t.Fatalf("expected expired manifest, got %v", err)
	}
	if err := VerifyGenerationManifest(manifest, "signing-key", "other-key", now.Add(30*time.Second)); !errors.Is(err, ErrKeyIDMismatch) {
		t.Fatalf("expected key id mismatch, got %v", err)
	}
}

func TestEd25519PeerDirectorySignatureUsesPublicKeyVerification(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	privateValue := "ed25519-private:" + base64.RawURLEncoding.EncodeToString(privateKey)
	publicValue := "ed25519-public:" + base64.RawURLEncoding.EncodeToString(publicKey)

	directory, err := SignPeerDirectory(PeerDirectory{
		Generation: "meshgen-ed25519",
		Nodes:      []MeshNode{{NodeID: "node-a"}},
	}, privateValue, "key-ed25519", time.Minute, now)
	if err != nil {
		t.Fatalf("sign directory: %v", err)
	}
	if err := VerifyPeerDirectory(directory, publicValue, "key-ed25519", now); err != nil {
		t.Fatalf("verify directory with public key: %v", err)
	}
	if _, err := SignPeerDirectory(directory, publicValue, "key-ed25519", time.Minute, now); !errors.Is(err, ErrUnsupportedSigningKey) {
		t.Fatalf("expected public key signing to fail, got %v", err)
	}
}
