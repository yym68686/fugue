package sshidentity

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"fugue/internal/model"

	"golang.org/x/crypto/ssh"
)

func testKeyring(test *testing.T) Keyring {
	test.Helper()
	keyring, err := NewKeyring("storage-v1", map[string][]byte{"storage-v1": bytes.Repeat([]byte{7}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	return keyring
}

func TestIdentityEncryptedRoundTrip(test *testing.T) {
	keyring := testKeyring(test)
	record, err := keyring.Create("tenant_sample", "app_sample", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	privatePEM, err := keyring.PrivateKeyPEM(record)
	if err != nil {
		test.Fatal(err)
	}
	signer, err := ssh.ParsePrivateKey(privatePEM)
	if err != nil || ssh.FingerprintSHA256(signer.PublicKey()) != record.Identity.Fingerprint {
		test.Fatal("stored identity differs from its private key", err)
	}
	encoded, err := json.Marshal(record)
	if err != nil || bytes.Contains(encoded, []byte("PRIVATE KEY")) || bytes.Contains(encoded, privatePEM) {
		test.Fatal("private key is present in persisted metadata", err)
	}
	var recovered model.AppSSHIdentityRecord
	if err := json.Unmarshal(encoded, &recovered); err != nil {
		test.Fatal(err)
	}
	recoveredSigner, err := keyring.Signer(recovered)
	if err != nil {
		test.Fatal(err)
	}
	if !bytes.Equal(recoveredSigner.PublicKey().Marshal(), signer.PublicKey().Marshal()) {
		test.Fatal("identity changed after serialization")
	}
}

func TestIdentityRejectsMetadataAndCiphertextReplacement(test *testing.T) {
	keyring := testKeyring(test)
	record, err := keyring.Create("tenant_sample", "app_sample", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	other, err := keyring.Create("tenant_other", "app_other", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	mutations := map[string]func(*model.AppSSHIdentityRecord){
		"tenant":      func(candidate *model.AppSSHIdentityRecord) { candidate.Identity.TenantID = "tenant_other" },
		"application": func(candidate *model.AppSSHIdentityRecord) { candidate.Identity.AppID = "app_other" },
		"identity":    func(candidate *model.AppSSHIdentityRecord) { candidate.Identity.ID = other.Identity.ID },
		"generation":  func(candidate *model.AppSSHIdentityRecord) { candidate.Identity.Generation++ },
		"created": func(candidate *model.AppSSHIdentityRecord) {
			candidate.Identity.CreatedAt = candidate.Identity.CreatedAt.Add(time.Second)
		},
		"public key": func(candidate *model.AppSSHIdentityRecord) { candidate.Identity.PublicKey = other.Identity.PublicKey },
		"matching public metadata": func(candidate *model.AppSSHIdentityRecord) {
			candidate.Identity.PublicKey = other.Identity.PublicKey
			candidate.Identity.Fingerprint = other.Identity.Fingerprint
		},
		"ciphertext swap":      func(candidate *model.AppSSHIdentityRecord) { candidate.Ciphertext = other.Ciphertext },
		"missing key material": func(candidate *model.AppSSHIdentityRecord) { candidate.Ciphertext = "" },
	}
	for name, mutate := range mutations {
		test.Run(name, func(test *testing.T) {
			candidate := record
			mutate(&candidate)
			if _, err := keyring.Signer(candidate); !errors.Is(err, ErrIntegrity) {
				test.Fatalf("replacement was not rejected: %v", err)
			}
		})
	}
}

func TestIdentityRequiresExplicitEncryptionKeys(test *testing.T) {
	if _, err := (Keyring{}).Create("tenant_sample", "app_sample", time.Now()); !errors.Is(err, ErrKeyUnavailable) {
		test.Fatalf("unconfigured keyring created identity: %v", err)
	}
	if _, err := NewKeyring("storage-v1", map[string][]byte{"storage-v1": []byte("short")}); !errors.Is(err, ErrInvalidKeyring) {
		test.Fatalf("weak encryption key accepted: %v", err)
	}
	keyring := testKeyring(test)
	record, err := keyring.Create("tenant_sample", "app_sample", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	if _, err := (Keyring{}).Signer(record); !errors.Is(err, ErrKeyUnavailable) {
		test.Fatalf("missing key did not fail closed: %v", err)
	}
}

func TestEncryptionRotationPreservesHostIdentity(test *testing.T) {
	keyring := testKeyring(test)
	record, err := keyring.Create("tenant_sample", "app_sample", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	rotated, err := NewKeyring("storage-v2", map[string][]byte{
		"storage-v1": bytes.Repeat([]byte{7}, 32),
		"storage-v2": bytes.Repeat([]byte{9}, 32),
	})
	if err != nil {
		test.Fatal(err)
	}
	rewrapped, err := rotated.Rewrap(record)
	if err != nil {
		test.Fatal(err)
	}
	if rewrapped.Identity != record.Identity || rewrapped.EncryptionKeyID != "storage-v2" || rewrapped.Ciphertext == record.Ciphertext {
		test.Fatal("storage key rotation changed SSH identity or reused ciphertext")
	}
	newOnly, err := NewKeyring("storage-v2", map[string][]byte{"storage-v2": bytes.Repeat([]byte{9}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	if _, err := newOnly.Signer(rewrapped); err != nil {
		test.Fatal(err)
	}
	if _, err := newOnly.Signer(record); !errors.Is(err, ErrKeyUnavailable) {
		test.Fatalf("retired storage key was accepted: %v", err)
	}
}
