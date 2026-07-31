package model

import "time"

const (
	PlatformComponentCredentialAPIVersionV1 = "platform-component-identity.fugue.dev/v1"
	PlatformComponentCredentialKind         = "PlatformComponentCredential"
)

// PlatformComponentCredential is the versioned, short-lived handoff from the
// control plane to an independently running platform component. Token is
// intentionally present only in this response type; it must never be persisted
// or included in audit metadata.
type PlatformComponentCredential struct {
	APIVersion    string    `json:"api_version"`
	Kind          string    `json:"kind"`
	CredentialID  string    `json:"credential_id"`
	Token         string    `json:"token"`
	TokenID       string    `json:"token_id"`
	Component     string    `json:"component"`
	NodeID        string    `json:"node_id"`
	ScopeKey      string    `json:"scope_key"`
	ArtifactKinds []string  `json:"artifact_kinds"`
	IssuedAt      time.Time `json:"issued_at"`
	ExpiresAt     time.Time `json:"expires_at"`
	RenewAfter    time.Time `json:"renew_after"`
}

type PlatformComponentCredentialResponse struct {
	Credential PlatformComponentCredential `json:"credential"`
}
