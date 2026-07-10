package api

import (
	"testing"

	"fugue/internal/bundleauth"
)

func TestNewServerCopiesPlatformConsumerHeartbeatAuditKeyring(t *testing.T) {
	configured := bundleauth.NewKeyring(
		"audit-current-key",
		"heartbeat-audit:current",
		"audit-previous-key",
		"heartbeat-audit:previous",
		[]string{"heartbeat-audit:revoked"},
	)

	server := NewServer(nil, nil, nil, ServerConfig{
		HeartbeatAuditKeyring: configured,
	})
	configured.PrimaryKey = "mutated-current-key"
	configured.RevokedKeyIDs["heartbeat-audit:mutated"] = struct{}{}
	delete(configured.RevokedKeyIDs, "heartbeat-audit:revoked")

	got := server.heartbeatAuditKeyring
	if got.PrimaryKey != "audit-current-key" || got.PrimaryKeyID != "heartbeat-audit:current" {
		t.Fatalf("current audit key was not copied: %+v", got)
	}
	if got.PreviousKey != "audit-previous-key" || got.PreviousKeyID != "heartbeat-audit:previous" {
		t.Fatalf("previous audit key was not copied: %+v", got)
	}
	if _, ok := got.RevokedKeyIDs["heartbeat-audit:revoked"]; !ok {
		t.Fatalf("copied audit keyring lost revoked key: %+v", got.RevokedKeyIDs)
	}
	if _, ok := got.RevokedKeyIDs["heartbeat-audit:mutated"]; ok {
		t.Fatalf("server audit keyring aliases caller-owned map: %+v", got.RevokedKeyIDs)
	}
}
