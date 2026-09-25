package model

import "time"

const (
	StaticEdgeTransportMTLS = "mtls"
	StaticEdgeTransportSSH  = "ssh"

	StaticEdgeStatusPending = "pending"
	StaticEdgeStatusReady   = "ready"
	StaticEdgeStatusRevoked = "revoked"
)

// StaticEdgeRegistration is declarative account/project metadata only. The
// independent executor remains the authority for DNS mutations and runtime
// health; this record lets Fugue and its web clients observe ownership and the
// latest possession proof without putting the executor on the API request path.
type StaticEdgeRegistration struct {
	ID                     string     `json:"id"`
	TenantID               string     `json:"tenant_id"`
	ProjectID              string     `json:"project_id"`
	Name                   string     `json:"name"`
	EdgeID                 string     `json:"edge_id"`
	Transport              string     `json:"transport"`
	ManagerURL             string     `json:"manager_url,omitempty"`
	CertificateFingerprint string     `json:"certificate_fingerprint,omitempty"`
	SigningKeyID           string     `json:"signing_key_id,omitempty"`
	PossessionProofDigest  string     `json:"possession_proof_digest,omitempty"`
	Status                 string     `json:"status"`
	LastProofAt            *time.Time `json:"last_proof_at,omitempty"`
	CreatedAt              time.Time  `json:"created_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
}

type StaticEdgeRegistrationListResponse struct {
	Registrations []StaticEdgeRegistration `json:"registrations"`
	GeneratedAt   time.Time                `json:"generated_at"`
}

type StaticEdgeRegistrationResponse struct {
	Registration StaticEdgeRegistration `json:"registration"`
}
