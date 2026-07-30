package main

import (
	"os"
	"strings"

	"fugue/internal/backupidentity"
)

func backupObserverIdentityKeyringFromEnv() backupidentity.Keyring {
	return backupidentity.DeriveKeyring(
		strings.TrimSpace(os.Getenv("FUGUE_BACKUP_OBSERVER_IDENTITY_SIGNING_KEY")),
		strings.TrimSpace(os.Getenv("FUGUE_BACKUP_OBSERVER_IDENTITY_SIGNING_KEY_ID")),
		strings.TrimSpace(os.Getenv("FUGUE_BACKUP_OBSERVER_IDENTITY_PREVIOUS_SIGNING_KEY")),
		strings.TrimSpace(os.Getenv("FUGUE_BACKUP_OBSERVER_IDENTITY_PREVIOUS_SIGNING_KEY_ID")),
		backupObserverIdentityRevokedKeyIDs(os.Getenv("FUGUE_BACKUP_OBSERVER_IDENTITY_REVOKED_KEY_IDS")),
	)
}

func backupObserverIdentityRevokedKeyIDs(raw string) []string {
	values := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n'
	})
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			out = append(out, value)
		}
	}
	return out
}
