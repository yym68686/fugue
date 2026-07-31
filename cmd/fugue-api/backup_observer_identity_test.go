package main

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/backupidentity"
)

func TestBackupObserverIdentityKeyringFromEnvIsDedicatedRotationAwareAndDefaultOff(t *testing.T) {
	for _, name := range []string{
		"FUGUE_BACKUP_OBSERVER_IDENTITY_SIGNING_KEY",
		"FUGUE_BACKUP_OBSERVER_IDENTITY_SIGNING_KEY_ID",
		"FUGUE_BACKUP_OBSERVER_IDENTITY_PREVIOUS_SIGNING_KEY",
		"FUGUE_BACKUP_OBSERVER_IDENTITY_PREVIOUS_SIGNING_KEY_ID",
		"FUGUE_BACKUP_OBSERVER_IDENTITY_REVOKED_KEY_IDS",
	} {
		t.Setenv(name, "")
	}
	if keyring := backupObserverIdentityKeyringFromEnv(); keyring.ActiveKeyID != "" || len(keyring.Keys) != 0 {
		t.Fatalf("backup observer identity must be disabled without dedicated keys: %+v", keyring)
	}

	activeSource := strings.Repeat("a", 32)
	previousSource := strings.Repeat("p", 32)
	t.Setenv("FUGUE_BACKUP_OBSERVER_IDENTITY_SIGNING_KEY", activeSource)
	t.Setenv("FUGUE_BACKUP_OBSERVER_IDENTITY_SIGNING_KEY_ID", "backup-key-2")
	t.Setenv("FUGUE_BACKUP_OBSERVER_IDENTITY_PREVIOUS_SIGNING_KEY", previousSource)
	t.Setenv("FUGUE_BACKUP_OBSERVER_IDENTITY_PREVIOUS_SIGNING_KEY_ID", "backup-key-1")
	t.Setenv("FUGUE_BACKUP_OBSERVER_IDENTITY_REVOKED_KEY_IDS", " revoked-key, backup-key-0;revoked-key ")
	keyring := backupObserverIdentityKeyringFromEnv()
	if keyring.ActiveKeyID != "backup-key-2" || len(keyring.Keys) != 2 ||
		keyring.Keys["backup-key-2"] == activeSource || keyring.Keys["backup-key-1"] == previousSource {
		t.Fatalf("backup observer keyring drifted or reused raw keys: %+v", keyring)
	}
	if _, ok := keyring.RevokedKeyIDs["revoked-key"]; !ok {
		t.Fatalf("revoked key IDs were not parsed: %+v", keyring.RevokedKeyIDs)
	}

	cell := "backup/registry/0123456789abcdef"
	token, err := backupidentity.Issue(keyring, backupidentity.Claims{
		CredentialID: backupidentity.CredentialIDForCell(cell),
		RunID:        "run-1", CellKey: cell,
		SpecDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("issue from configured keyring: %v", err)
	}
	if _, err := backupidentity.Parse(keyring, token, time.Now().UTC()); err != nil {
		t.Fatalf("verify from configured keyring: %v", err)
	}
}

func TestBackupObserverIdentityKeyringRejectsPartialOrWeakConfiguration(t *testing.T) {
	t.Setenv("FUGUE_BACKUP_OBSERVER_IDENTITY_SIGNING_KEY", "weak")
	t.Setenv("FUGUE_BACKUP_OBSERVER_IDENTITY_SIGNING_KEY_ID", "backup-key-1")
	t.Setenv("FUGUE_BACKUP_OBSERVER_IDENTITY_PREVIOUS_SIGNING_KEY", "")
	t.Setenv("FUGUE_BACKUP_OBSERVER_IDENTITY_PREVIOUS_SIGNING_KEY_ID", "")
	t.Setenv("FUGUE_BACKUP_OBSERVER_IDENTITY_REVOKED_KEY_IDS", "")
	if keyring := backupObserverIdentityKeyringFromEnv(); keyring.ActiveKeyID != "" || len(keyring.Keys) != 0 {
		t.Fatalf("weak key unexpectedly enabled backup observer identity: %+v", keyring)
	}
	t.Setenv("FUGUE_BACKUP_OBSERVER_IDENTITY_SIGNING_KEY", strings.Repeat("a", 32))
	t.Setenv("FUGUE_BACKUP_OBSERVER_IDENTITY_SIGNING_KEY_ID", "")
	if keyring := backupObserverIdentityKeyringFromEnv(); keyring.ActiveKeyID != "" || len(keyring.Keys) != 0 {
		t.Fatalf("key without ID unexpectedly enabled backup observer identity: %+v", keyring)
	}
}
