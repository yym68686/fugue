package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/releaseguardian"
)

func TestMaterializeAuthorityPublicKeyWritesOnlyDerivedPublicIdentity(t *testing.T) {
	material := []byte("candidate-canary-signing-material-that-remains-secret")
	input := filepath.Join(t.TempDir(), "token")
	output := "/tmp/fugue-authority-public-key-test"
	defer os.Remove(output)
	if err := os.WriteFile(input, material, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := materializeAuthorityPublicKey(input + "," + output); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(output)
	want, _ := releaseguardian.CandidateCanaryPublicKey(material)
	if err != nil || !bytes.Equal(raw, want) || bytes.Contains(raw, material) {
		t.Fatalf("materialized verifier is invalid: bytes=%d err=%v", len(raw), err)
	}
	info, _ := os.Stat(output)
	if info == nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("public verifier mode=%v", info)
	}
}

func TestMaterializeAuthorityPublicKeyRejectsPersistentOrMissingInputs(t *testing.T) {
	for _, value := range []string{"relative,/tmp/key", "/missing,/tmp/key", "/tmp/input,/var/run/key", "/tmp/input"} {
		if err := materializeAuthorityPublicKey(value); err == nil {
			t.Fatalf("invalid source accepted: %q", value)
		}
	}
}
