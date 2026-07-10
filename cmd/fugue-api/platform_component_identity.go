package main

import (
	"os"
	"strings"

	"fugue/internal/platformcontrol"
)

func platformComponentIdentityKeyringFromEnv() platformcontrol.PlatformComponentIdentityKeyring {
	return platformcontrol.DerivePlatformComponentIdentityKeyring(
		strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY")),
		strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID")),
		strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY")),
		strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID")),
		platformComponentIdentityRevokedKeyIDs(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS")),
	)
}

func platformComponentIdentityRevokedKeyIDs(raw string) []string {
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
