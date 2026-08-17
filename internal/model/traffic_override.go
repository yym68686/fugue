package model

import "time"

const (
	TrafficOverrideSchemaV1          = "traffic-override.fugue.dev/v1"
	TrafficOverrideSigningSchemaV1   = "traffic-override-signing.fugue.dev/v1"
	TrafficOverrideStateStaged       = "staged"
	TrafficOverrideStateRevoked      = "revoked"
	TrafficOverrideMaxAnswers        = 16
	TrafficOverrideMaxRequiredRoutes = 16
)

// TrafficOverride is an independently signed emergency traffic artifact. DNS
// consumers do not read this object until the separate overlay path is enabled.
type TrafficOverride struct {
	Schema             string    `json:"schema"`
	Hostname           string    `json:"hostname"`
	Generation         uint64    `json:"generation"`
	State              string    `json:"state"`
	Answers            []string  `json:"answers"`
	RequiredHostRoutes []string  `json:"required_host_routes"`
	RouteGeneration    string    `json:"route_generation"`
	RouteDigest        string    `json:"route_digest"`
	PreparedDigest     string    `json:"prepared_digest"`
	ActivateAt         time.Time `json:"activate_at"`
	ExpiresAt          time.Time `json:"expires_at"`
	Reason             string    `json:"reason"`
	Operator           string    `json:"operator"`
	ArtifactDigest     string    `json:"artifact_digest"`
	KeyID              string    `json:"key_id"`
	Signature          string    `json:"signature"`
	SignedAt           time.Time `json:"signed_at"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// TrafficOverrideSigningKeyring is private store state. Key material must never
// be serialized into an API response or log.
type TrafficOverrideSigningKeyring struct {
	Schema             string    `json:"schema"`
	Generation         uint64    `json:"generation"`
	CurrentKeyID       string    `json:"current_key_id"`
	CurrentPrivateKey  string    `json:"current_private_key"`
	CurrentPublicKey   string    `json:"current_public_key"`
	PreviousKeyID      string    `json:"previous_key_id,omitempty"`
	PreviousPrivateKey string    `json:"previous_private_key,omitempty"`
	PreviousPublicKey  string    `json:"previous_public_key,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	RotatedAt          time.Time `json:"rotated_at,omitempty"`
	UpdatedAt          time.Time `json:"updated_at"`
}

type TrafficOverrideSigningKeyStatus struct {
	Schema            string    `json:"schema"`
	Generation        uint64    `json:"generation"`
	CurrentKeyID      string    `json:"current_key_id"`
	CurrentPublicKey  string    `json:"current_public_key"`
	PreviousKeyID     string    `json:"previous_key_id,omitempty"`
	PreviousPublicKey string    `json:"previous_public_key,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	RotatedAt         time.Time `json:"rotated_at,omitempty"`
	UpdatedAt         time.Time `json:"updated_at"`
}

func (keyring TrafficOverrideSigningKeyring) Status() TrafficOverrideSigningKeyStatus {
	return TrafficOverrideSigningKeyStatus{
		Schema:            keyring.Schema,
		Generation:        keyring.Generation,
		CurrentKeyID:      keyring.CurrentKeyID,
		CurrentPublicKey:  keyring.CurrentPublicKey,
		PreviousKeyID:     keyring.PreviousKeyID,
		PreviousPublicKey: keyring.PreviousPublicKey,
		CreatedAt:         keyring.CreatedAt,
		RotatedAt:         keyring.RotatedAt,
		UpdatedAt:         keyring.UpdatedAt,
	}
}
