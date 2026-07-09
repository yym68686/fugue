package model

import "time"

const (
	AutonomySchemaVersionV1 = "1.0"

	AutonomyArtifactKindCaddyConfig = "caddy_config_lkg"
	AutonomyArtifactKindTLSMaterial = "tls_material_lkg"
	AutonomyArtifactKindEndpointLKG = "endpoint_lkg"

	EndpointFallbackPolicyStatelessHTTPDefault = "stateless_http_default"
	EndpointFallbackPolicyStatefulExplicit     = "stateful_explicit"
	EndpointFallbackPolicyDisabled             = "disabled"

	EndpointFallbackStatusAllowed = "allowed"
	EndpointFallbackStatusExpired = "expired"
	EndpointFallbackStatusBlocked = "blocked"

	EdgeRepairSafetyL0ObserveOnly       = "L0_observe_only"
	EdgeRepairSafetyL1TemporaryFilter   = "L1_temporary_filter"
	EdgeRepairSafetyL2LocalReload       = "L2_local_reload"
	EdgeRepairSafetyL3StatelessRestart  = "L3_stateless_restart"
	EdgeRepairSafetyL4GuardedNodeRepair = "L4_guarded_node_repair"
	EdgeRepairSafetyL5HumanOnly         = "L5_human_only"

	PeerSignalStatusPass           = "pass"
	PeerSignalStatusSuspect        = "suspect"
	PeerSignalStatusUnhealthy      = "unhealthy"
	PeerSignalStatusSelfQuarantine = "self_quarantine"

	PeerHealthDecisionClear           = "clear"
	PeerHealthDecisionSuspect         = "suspect"
	PeerHealthDecisionTemporaryFilter = "temporary_filter"

	ProviderPowerEventClassProviderPlanned    = "provider_planned"
	ProviderPowerEventClassProviderUnplanned  = "provider_unplanned"
	ProviderPowerEventClassGuestInitiated     = "guest_initiated"
	ProviderPowerEventClassUnknownPowerLoss   = "unknown_power_loss"
	ProviderPowerEventClassNoProviderEvidence = "no_provider_evidence"
)

type CaddyConfigLKG struct {
	SchemaVersion      string                 `json:"schema_version"`
	Kind               string                 `json:"kind"`
	Generation         string                 `json:"generation"`
	RouteGeneration    string                 `json:"route_generation"`
	PreviousGeneration string                 `json:"previous_generation,omitempty"`
	GeneratedAt        time.Time              `json:"generated_at"`
	AppliedAt          time.Time              `json:"applied_at,omitempty"`
	ValidUntil         time.Time              `json:"valid_until,omitempty"`
	ConfigPath         string                 `json:"config_path,omitempty"`
	ConfigSHA256       string                 `json:"config_sha256"`
	RouteCount         int                    `json:"route_count,omitempty"`
	TLSMaterialRefs    []TLSMaterialReference `json:"tls_material_refs,omitempty"`
	Issuer             string                 `json:"issuer,omitempty"`
	KeyID              string                 `json:"key_id,omitempty"`
	Signature          string                 `json:"signature,omitempty"`
}

type TLSMaterialReference struct {
	SchemaVersion      string    `json:"schema_version,omitempty"`
	Kind               string    `json:"kind,omitempty"`
	Hostname           string    `json:"hostname"`
	Namespace          string    `json:"namespace,omitempty"`
	SecretName         string    `json:"secret_name,omitempty"`
	CertificatePath    string    `json:"certificate_path,omitempty"`
	PrivateKeyRef      string    `json:"private_key_ref,omitempty"`
	CertificateSHA256  string    `json:"certificate_sha256,omitempty"`
	SerialNumber       string    `json:"serial_number,omitempty"`
	NotBefore          time.Time `json:"not_before,omitempty"`
	NotAfter           time.Time `json:"not_after,omitempty"`
	Generation         string    `json:"generation,omitempty"`
	PreviousGeneration string    `json:"previous_generation,omitempty"`
}

type OriginHealthRecord struct {
	Hostname         string                  `json:"hostname"`
	PathPrefix       string                  `json:"path_prefix,omitempty"`
	RouteGeneration  string                  `json:"route_generation,omitempty"`
	ServiceIdentity  string                  `json:"service_identity,omitempty"`
	EndpointLKGID    string                  `json:"endpoint_lkg_id,omitempty"`
	Status           string                  `json:"status"`
	LastFailureClass string                  `json:"last_failure_class,omitempty"`
	ServiceDNSProbe  *OriginProbeObservation `json:"service_dns_probe,omitempty"`
	ClusterIPProbe   *OriginProbeObservation `json:"cluster_ip_probe,omitempty"`
	EndpointIPProbe  *OriginProbeObservation `json:"endpoint_ip_probe,omitempty"`
	HTTPProbe        *OriginProbeObservation `json:"http_probe,omitempty"`
	CheckedAt        time.Time               `json:"checked_at"`
}

type OriginProbeObservation struct {
	Status    string            `json:"status"`
	Target    string            `json:"target,omitempty"`
	LatencyMS int64             `json:"latency_ms,omitempty"`
	Error     string            `json:"error,omitempty"`
	Evidence  map[string]string `json:"evidence,omitempty"`
	CheckedAt time.Time         `json:"checked_at,omitempty"`
}

type EndpointLKG struct {
	SchemaVersion     string                `json:"schema_version"`
	Kind              string                `json:"kind"`
	ID                string                `json:"id,omitempty"`
	Hostname          string                `json:"hostname"`
	PathPrefix        string                `json:"path_prefix,omitempty"`
	RouteGeneration   string                `json:"route_generation"`
	ServiceIdentity   string                `json:"service_identity"`
	AppID             string                `json:"app_id,omitempty"`
	RuntimeID         string                `json:"runtime_id,omitempty"`
	Namespace         string                `json:"namespace,omitempty"`
	ServiceName       string                `json:"service_name,omitempty"`
	ServicePort       int                   `json:"service_port,omitempty"`
	StatelessHTTP     bool                  `json:"stateless_http"`
	FallbackPolicy    string                `json:"fallback_policy"`
	PodCIDR           string                `json:"pod_cidr,omitempty"`
	NodeName          string                `json:"node_name,omitempty"`
	Endpoints         []EndpointLKGEndpoint `json:"endpoints"`
	GeneratedAt       time.Time             `json:"generated_at"`
	ValidUntil        time.Time             `json:"valid_until"`
	LastSuccessAt     time.Time             `json:"last_success_at,omitempty"`
	LastFailureAt     time.Time             `json:"last_failure_at,omitempty"`
	LastFailureReason string                `json:"last_failure_reason,omitempty"`
	SuccessCount      int                   `json:"success_count,omitempty"`
	FailureCount      int                   `json:"failure_count,omitempty"`
	Issuer            string                `json:"issuer,omitempty"`
	KeyID             string                `json:"key_id,omitempty"`
	Signature         string                `json:"signature,omitempty"`
}

type EndpointLKGEndpoint struct {
	IP          string                  `json:"ip"`
	Port        int                     `json:"port"`
	PodName     string                  `json:"pod_name,omitempty"`
	NodeName    string                  `json:"node_name,omitempty"`
	PodCIDR     string                  `json:"pod_cidr,omitempty"`
	Ready       bool                    `json:"ready"`
	HTTPPath    string                  `json:"http_path,omitempty"`
	LastProbe   *OriginProbeObservation `json:"last_probe,omitempty"`
	Annotations map[string]string       `json:"annotations,omitempty"`
}

type EndpointFallbackDecision struct {
	Status        string            `json:"status"`
	Reason        string            `json:"reason,omitempty"`
	Hostname      string            `json:"hostname,omitempty"`
	PathPrefix    string            `json:"path_prefix,omitempty"`
	EndpointCount int               `json:"endpoint_count,omitempty"`
	TTLSeconds    int64             `json:"ttl_seconds,omitempty"`
	Evidence      map[string]string `json:"evidence,omitempty"`
}

type EdgeRepairPolicy struct {
	Action           string        `json:"action"`
	SafetyClass      string        `json:"safety_class"`
	FeatureFlag      string        `json:"feature_flag,omitempty"`
	Cooldown         time.Duration `json:"cooldown"`
	MaxAttempts      int           `json:"max_attempts"`
	HumanBoundary    string        `json:"human_boundary,omitempty"`
	QuarantineOnFail bool          `json:"quarantine_on_fail,omitempty"`
}

type EdgeRepairAttempt struct {
	Action      string            `json:"action"`
	SafetyClass string            `json:"safety_class"`
	Subject     string            `json:"subject,omitempty"`
	Attempt     int               `json:"attempt"`
	Status      string            `json:"status"`
	Message     string            `json:"message,omitempty"`
	Evidence    map[string]string `json:"evidence,omitempty"`
	StartedAt   time.Time         `json:"started_at"`
	FinishedAt  time.Time         `json:"finished_at,omitempty"`
}

type PeerHealthSignal struct {
	SchemaVersion string            `json:"schema_version"`
	SignalID      string            `json:"signal_id"`
	IssuerNodeID  string            `json:"issuer_node_id"`
	IssuerEdgeID  string            `json:"issuer_edge_id,omitempty"`
	SubjectNodeID string            `json:"subject_node_id,omitempty"`
	SubjectEdgeID string            `json:"subject_edge_id,omitempty"`
	Status        string            `json:"status"`
	Scope         string            `json:"scope,omitempty"`
	FailureDomain string            `json:"failure_domain,omitempty"`
	Evidence      map[string]string `json:"evidence,omitempty"`
	EvidenceHash  string            `json:"evidence_hash"`
	ObservedAt    time.Time         `json:"observed_at"`
	ExpiresAt     time.Time         `json:"expires_at"`
	KeyID         string            `json:"key_id,omitempty"`
	Signature     string            `json:"signature,omitempty"`
}

type PeerHealthDecision struct {
	SubjectEdgeID  string             `json:"subject_edge_id,omitempty"`
	Decision       string             `json:"decision"`
	Reason         string             `json:"reason,omitempty"`
	ExpiresAt      time.Time          `json:"expires_at,omitempty"`
	SignalCount    int                `json:"signal_count,omitempty"`
	FailureDomains []string           `json:"failure_domains,omitempty"`
	Signals        []PeerHealthSignal `json:"signals,omitempty"`
}

type LocalWALReplaySummary struct {
	Component          string            `json:"component,omitempty"`
	NodeID             string            `json:"node_id,omitempty"`
	RecordsRead        int               `json:"records_read"`
	RecordsAccepted    int               `json:"records_accepted"`
	RecordsExpired     int               `json:"records_expired"`
	RecordsRejected    int               `json:"records_rejected"`
	IncidentsMerged    int               `json:"incidents_merged"`
	TemporaryActionsGC int               `json:"temporary_actions_gc"`
	Evidence           map[string]string `json:"evidence,omitempty"`
	ReplayedAt         time.Time         `json:"replayed_at"`
}

type ProviderPowerEvent struct {
	ID         string            `json:"id,omitempty"`
	Provider   string            `json:"provider"`
	Region     string            `json:"region,omitempty"`
	InstanceID string            `json:"instance_id"`
	EventType  string            `json:"event_type,omitempty"`
	ActionID   string            `json:"action_id,omitempty"`
	EventClass string            `json:"event_class"`
	Message    string            `json:"message,omitempty"`
	Evidence   map[string]string `json:"evidence,omitempty"`
	ProviderAt time.Time         `json:"provider_at,omitempty"`
	ObservedAt time.Time         `json:"observed_at"`
}
