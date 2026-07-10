package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"os"
	"strings"

	"fugue/internal/bundleauth"
	"fugue/internal/platformcontrol"
)

const platformConsumerHeartbeatAuditKeyContext = "fugue/platform-consumer-heartbeat-audit/v1"

func platformComponentIdentityKeyringFromEnv() platformcontrol.PlatformComponentIdentityKeyring {
	return platformcontrol.DerivePlatformComponentIdentityKeyring(
		strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY")),
		strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID")),
		strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY")),
		strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID")),
		platformComponentIdentityRevokedKeyIDs(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS")),
	)
}

func platformConsumerHeartbeatAuditKeyringFromEnv() bundleauth.Keyring {
	activeKey := strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY"))
	activeKeyID := strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID"))
	previousKey := strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY"))
	previousKeyID := strings.TrimSpace(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID"))
	revokedIdentityKeyIDs := platformComponentIdentityRevokedKeyIDs(os.Getenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS"))
	revokedAuditKeyIDs := make([]string, 0, len(revokedIdentityKeyIDs))
	for _, keyID := range revokedIdentityKeyIDs {
		if auditKeyID := platformConsumerHeartbeatAuditKeyID(keyID); auditKeyID != "" {
			revokedAuditKeyIDs = append(revokedAuditKeyIDs, auditKeyID)
		}
	}
	return bundleauth.NewKeyring(
		derivePlatformConsumerHeartbeatAuditKey(activeKey),
		platformConsumerHeartbeatAuditKeyID(activeKeyID),
		derivePlatformConsumerHeartbeatAuditKey(previousKey),
		platformConsumerHeartbeatAuditKeyID(previousKeyID),
		revokedAuditKeyIDs,
	)
}

func derivePlatformConsumerHeartbeatAuditKey(identityKey string) string {
	identityKey = strings.TrimSpace(identityKey)
	if identityKey == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(identityKey))
	_, _ = mac.Write([]byte(platformConsumerHeartbeatAuditKeyContext))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func platformConsumerHeartbeatAuditKeyID(identityKeyID string) string {
	identityKeyID = strings.TrimSpace(identityKeyID)
	if identityKeyID == "" {
		return ""
	}
	return "heartbeat-audit:" + identityKeyID
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
