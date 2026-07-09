package model

import "time"

type ReleaseGuardStatus struct {
	GeneratedAt              time.Time        `json:"generated_at"`
	Pass                     bool             `json:"pass"`
	BlockRollout             bool             `json:"block_rollout"`
	Mode                     string           `json:"mode"`
	RobustnessBaseline       RobustnessStatus `json:"robustness_baseline"`
	FailureContractCount     int              `json:"failure_contract_count"`
	PlatformArtifactKinds    []string         `json:"platform_artifact_kinds,omitempty"`
	PlatformArtifactFailures int              `json:"platform_artifact_validation_failures"`
	PlatformConsumerDrift    int              `json:"platform_consumer_drift"`
	ReleaseSignals           []ReleaseSignal  `json:"release_signals,omitempty"`
	BlockedReasons           []string         `json:"blocked_reasons,omitempty"`
	RecommendedOperatorSteps []string         `json:"recommended_operator_steps,omitempty"`
}

type ReleaseGuardStatusResponse struct {
	Status ReleaseGuardStatus `json:"status"`
}

type ServiceTrafficSafetyState struct {
	Hostname            string               `json:"hostname"`
	Pass                bool                 `json:"pass"`
	MinHealthyEdgeCount int                  `json:"min_healthy_edge_count"`
	HealthyEdgeCount    int                  `json:"healthy_edge_count"`
	EligibleEdgeGroups  []string             `json:"eligible_edge_groups,omitempty"`
	HardGatedEdgeGroups []string             `json:"hard_gated_edge_groups,omitempty"`
	HardGateReasons     map[string]string    `json:"hard_gate_reasons,omitempty"`
	Blockers            []string             `json:"blockers,omitempty"`
	FailureContracts    []string             `json:"failure_contracts,omitempty"`
	GrayReleaseScope    string               `json:"gray_release_scope,omitempty"`
	StrictProtection    bool                 `json:"strict_protection"`
	ExplorationPaused   bool                 `json:"exploration_paused"`
	RouteExplain        RouteExplainResponse `json:"route_explain"`
	GeneratedAt         time.Time            `json:"generated_at"`
}

type TrafficSafetyExplainResponse struct {
	State ServiceTrafficSafetyState `json:"state"`
}

type RequestExplainResponse struct {
	RequestID               string            `json:"request_id"`
	Found                   bool              `json:"found"`
	ErrorClass              string            `json:"error_class,omitempty"`
	FailurePlane            string            `json:"failure_plane,omitempty"`
	EdgeID                  string            `json:"edge_id,omitempty"`
	EdgeGroupID             string            `json:"edge_group_id,omitempty"`
	RuntimeNode             string            `json:"runtime_node,omitempty"`
	Hostname                string            `json:"hostname,omitempty"`
	PathPrefix              string            `json:"path_prefix,omitempty"`
	Method                  string            `json:"method,omitempty"`
	TrafficClass            string            `json:"traffic_class,omitempty"`
	RouteGeneration         string            `json:"route_generation,omitempty"`
	StatusCode              int               `json:"status_code,omitempty"`
	BodyReadBlockMS         int64             `json:"body_read_block_ms,omitempty"`
	UploadEffectiveBPS      int64             `json:"upload_effective_bps,omitempty"`
	MinWindowBPS            int64             `json:"min_window_bps,omitempty"`
	MaxReadGapMS            int64             `json:"max_read_gap_ms,omitempty"`
	RequestBodyBytes        int64             `json:"request_body_bytes,omitempty"`
	RequestBodyReadBytes    int64             `json:"request_body_read_bytes,omitempty"`
	BodyIncompleteCount     int               `json:"body_incomplete_count,omitempty"`
	BodyReadErrorCount      int               `json:"body_read_error_count,omitempty"`
	OriginDNSMS             int64             `json:"origin_dns_ms,omitempty"`
	OriginConnectMS         int64             `json:"origin_connect_ms,omitempty"`
	OriginEndpointConnectMS int64             `json:"origin_endpoint_connect_ms,omitempty"`
	OriginRequestWriteMS    int64             `json:"origin_request_write_ms,omitempty"`
	OriginResponseWaitMS    int64             `json:"origin_response_wait_ms,omitempty"`
	OriginTTFBMS            int64             `json:"origin_ttfb_ms,omitempty"`
	OriginTotalMS           int64             `json:"origin_total_ms,omitempty"`
	OriginFailureClass      string            `json:"origin_failure_class,omitempty"`
	ClientTCPRTTMS          float64           `json:"client_tcp_rtt_ms,omitempty"`
	ClientTCPRetransRate    float64           `json:"client_tcp_retrans_rate,omitempty"`
	ClientTCPRTORate        float64           `json:"client_tcp_rto_rate,omitempty"`
	ClientTCPDeliveryBPS    int64             `json:"client_tcp_delivery_rate_bps,omitempty"`
	Attribution             []string          `json:"attribution,omitempty"`
	FailureContracts        []string          `json:"failure_contracts,omitempty"`
	Evidence                map[string]string `json:"evidence,omitempty"`
	SecretSafe              bool              `json:"secret_safe"`
	SampledAt               time.Time         `json:"sampled_at,omitempty"`
	GeneratedAt             time.Time         `json:"generated_at"`
}

type RequestExplainResponseEnvelope struct {
	Explain RequestExplainResponse `json:"explain"`
}
