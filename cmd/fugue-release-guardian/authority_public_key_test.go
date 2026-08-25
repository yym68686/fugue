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

func TestMaterializeAuthorityPublicKeys(t *testing.T) {
	firstInput := filepath.Join(t.TempDir(), "first-token")
	secondInput := filepath.Join(t.TempDir(), "second-token")
	firstOutput := filepath.Join("/tmp", "fugue-authority-public-key-first-"+filepath.Base(t.TempDir()))
	secondOutput := filepath.Join("/tmp", "fugue-authority-public-key-second-"+filepath.Base(t.TempDir()))
	t.Cleanup(func() { _ = os.Remove(firstOutput); _ = os.Remove(secondOutput) })
	if err := os.WriteFile(firstInput, []byte("first-candidate-canary-signing-material-that-remains-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondInput, []byte("second-candidate-canary-signing-material-that-remains-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := materializeAuthorityPublicKeys(firstInput + "," + firstOutput + ";" + secondInput + "," + secondOutput); err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{firstOutput, secondOutput} {
		if raw, err := os.ReadFile(output); err != nil || len(raw) != 32 {
			t.Fatalf("authority public key %s len=%d err=%v", output, len(raw), err)
		}
	}
	if err := materializeAuthorityPublicKeys(firstInput + "," + firstOutput + ";" + secondInput + "," + firstOutput); err == nil {
		t.Fatal("duplicate authority public key output was accepted")
	}
}

func TestMaterializeAuthorityPublicKeyRejectsPersistentOrMissingInputs(t *testing.T) {
	for _, value := range []string{"relative,/tmp/key", "/missing,/tmp/key", "/tmp/input,/var/run/key", "/tmp/input"} {
		if err := materializeAuthorityPublicKey(value); err == nil {
			t.Fatalf("invalid source accepted: %q", value)
		}
	}
}
