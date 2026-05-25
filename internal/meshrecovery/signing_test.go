package meshrecovery

import (
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
