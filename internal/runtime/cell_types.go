package runtime

import "time"

const (
	CellRuntimeLabelPrefix          = "fugue.io/cell-"
	CellRuntimeLabelMeshProvider    = CellRuntimeLabelPrefix + "mesh-provider"
	CellRuntimeLabelMeshIP          = CellRuntimeLabelPrefix + "mesh-ip"
	CellRuntimeLabelMeshHostname    = CellRuntimeLabelPrefix + "mesh-hostname"
	CellRuntimeLabelDiscoverySource = CellRuntimeLabelPrefix + "discovery-source"
	CellRuntimeLabelPeerCount       = CellRuntimeLabelPrefix + "peer-count"
	CellRuntimeLabelObservedPeers   = CellRuntimeLabelPrefix + "observed-peer-count"
	CellRuntimeLabelRouteCount      = CellRuntimeLabelPrefix + "route-count"
	CellRuntimeLabelOutboxPending   = CellRuntimeLabelPrefix + "outbox-pending"
	CellRuntimeLabelObservedAt      = CellRuntimeLabelPrefix + "observed-at"
)

type CellSnapshot struct {
	RuntimeID          string     `json:"runtime_id,omitempty"`
	RuntimeName        string     `json:"runtime_name,omitempty"`
	MachineName        string     `json:"machine_name,omitempty"`
	MachineFingerprint string     `json:"machine_fingerprint,omitempty"`
	Endpoint           string     `json:"endpoint,omitempty"`
	Mesh               CellMesh   `json:"mesh,omitempty"`
	Peers              []CellPeer `json:"peers,omitempty"`
	ObservedPeerCount  int        `json:"observed_peer_count,omitempty"`
	RouteCount         int        `json:"route_count,omitempty"`
	OutboxPending      int        `json:"outbox_pending,omitempty"`
	ObservedAt         time.Time  `json:"observed_at"`
}

type CellMesh struct {
	Provider        string `json:"provider,omitempty"`
	IP              string `json:"ip,omitempty"`
	Hostname        string `json:"hostname,omitempty"`
	DiscoverySource string `json:"discovery_source,omitempty"`
}

type CellPeer struct {
	Hostname string     `json:"hostname,omitempty"`
	IP       string     `json:"ip,omitempty"`
	Online   bool       `json:"online,omitempty"`
	LastSeen *time.Time `json:"last_seen,omitempty"`
}

type CellRoute struct {
	Hostname    string    `json:"hostname"`
	AppID       string    `json:"app_id,omitempty"`
	TenantID    string    `json:"tenant_id,omitempty"`
	RuntimeID   string    `json:"runtime_id,omitempty"`
	ServicePort int       `json:"service_port,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type CellPeerObservation struct {
	Key           string        `json:"key"`
	Peer          CellPeer      `json:"peer"`
	Snapshot      *CellSnapshot `json:"snapshot,omitempty"`
	Source        string        `json:"source,omitempty"`
	LastAttemptAt time.Time     `json:"last_attempt_at"`
	LastSeenAt    *time.Time    `json:"last_seen_at,omitempty"`
	LastError     string        `json:"last_error,omitempty"`
}
