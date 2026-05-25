package meshrecovery

import "time"

const (
	SchemaVersionV1 = "1.0"
	DefaultIssuer   = "fugue-mesh-recovery"

	GenerationModeNormal = "normal"
	GenerationModeReset  = "reset"

	NodeStatusHealthy = "healthy"
	NodeStatusStale   = "stale"
)

type MeshNode struct {
	NodeID            string    `json:"node_id"`
	Hostname          string    `json:"hostname,omitempty"`
	Roles             []string  `json:"roles,omitempty"`
	Region            string    `json:"region,omitempty"`
	Country           string    `json:"country,omitempty"`
	PublicIPv4        string    `json:"public_ipv4,omitempty"`
	PublicIPv6        string    `json:"public_ipv6,omitempty"`
	PrivateIPv4       string    `json:"private_ipv4,omitempty"`
	MeshIP            string    `json:"mesh_ip,omitempty"`
	APIEndpoints      []string  `json:"api_endpoints,omitempty"`
	RecoveryEndpoints []string  `json:"recovery_endpoints,omitempty"`
	EdgeEndpoints     []string  `json:"edge_endpoints,omitempty"`
	LastSeen          time.Time `json:"last_seen,omitempty"`
	TTLSeconds        int       `json:"ttl_seconds,omitempty"`
	Status            string    `json:"status,omitempty"`
	Source            string    `json:"source,omitempty"`
}

type PeerDirectory struct {
	SchemaVersion string     `json:"schema_version,omitempty"`
	Generation    string     `json:"generation"`
	GeneratedAt   time.Time  `json:"generated_at"`
	ValidUntil    time.Time  `json:"valid_until,omitempty"`
	Issuer        string     `json:"issuer,omitempty"`
	KeyID         string     `json:"key_id,omitempty"`
	Signature     string     `json:"signature,omitempty"`
	LoginServer   string     `json:"login_server,omitempty"`
	Nodes         []MeshNode `json:"nodes"`
}

type GenerationManifest struct {
	SchemaVersion      string    `json:"schema_version,omitempty"`
	Generation         string    `json:"generation"`
	PreviousGeneration string    `json:"previous_generation,omitempty"`
	Mode               string    `json:"mode,omitempty"`
	LoginServer        string    `json:"login_server,omitempty"`
	RejoinRequired     bool      `json:"rejoin_required,omitempty"`
	Message            string    `json:"message,omitempty"`
	IssuedAt           time.Time `json:"issued_at"`
	ValidUntil         time.Time `json:"valid_until,omitempty"`
	Issuer             string    `json:"issuer,omitempty"`
	KeyID              string    `json:"key_id,omitempty"`
	Signature          string    `json:"signature,omitempty"`
}

type HeartbeatRequest struct {
	Node MeshNode `json:"node"`
}

type HeartbeatResponse struct {
	Directory  PeerDirectory      `json:"directory"`
	Generation GenerationManifest `json:"generation"`
}

type RejoinResponse struct {
	Generation GenerationManifest `json:"generation"`
	AuthKey    string             `json:"auth_key,omitempty"`
}
