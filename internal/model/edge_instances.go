package model

import "time"

const (
	EdgeInstanceFencingSchemaV1 = "edge-instance-fencing/v1"
	EdgeActivationSchemaV1      = "edge-activation/v1"

	EdgeActivationPhaseLegacyAuthoritative = "legacy-authoritative"
	EdgeActivationPhaseShadow              = "shadow"
	EdgeActivationPhaseFenced              = "active-epoch-fenced"
	EdgeActivationPhaseActive              = "active-epoch-authoritative"
	EdgeActivationPhaseEnforced            = "active-epoch-enforced"
	EdgeActivationActionRollback           = "rollback"
	EdgeActivationAuthorizationSchemaV1    = "edge-activation-authorization/v1"
	EdgeRemediationSchemaV1                = "edge-auto-remediation/v1"
	EdgeRemediationAuthorizationSchemaV1   = "edge-remediation-authorization/v1"
	EdgeRemediationPhasePrepared           = "prepared"
	EdgeRemediationPhaseCommitted          = "committed"
	EdgeRemediationPhaseVerified           = "verified"
	EdgeRemediationPhaseRollbackPending    = "rollback_pending"
	EdgeRemediationReasonCrashLoop         = "crash_loop"

	EdgeRouteAuthorityLegacy      = "legacy"
	EdgeRouteAuthorityActiveEpoch = "active-epoch"

	EdgeSlotA      = "a"
	EdgeSlotB      = "b"
	EdgeSlotDirect = "direct"

	EdgeInstanceFailureNone             = ""
	EdgeInstanceFailureSignatureInvalid = "bundle_signature_invalid"
	EdgeInstanceFailureMaxStaleExceeded = "max_stale_exceeded"
	EdgeInstanceFailureIdentityDrift    = "identity_drift"
)

type EdgeExpectedInstance struct {
	EdgeID       string `json:"edge_id"`
	EdgeGroupID  string `json:"edge_group_id"`
	Slot         string `json:"slot"`
	InstanceUID  string `json:"instance_uid"`
	ReleaseEpoch string `json:"release_epoch"`
}

type EdgeActivationReceipt struct {
	Sequence                    uint64    `json:"sequence"`
	FromPhase                   string    `json:"from_phase"`
	ToPhase                     string    `json:"to_phase"`
	PlanDigest                  string    `json:"plan_digest"`
	EvidenceDigest              string    `json:"evidence_digest"`
	ReleaseID                   string    `json:"release_id"`
	Actor                       string    `json:"actor"`
	ReleaseFence                string    `json:"release_fence"`
	PhaseNonce                  string    `json:"phase_nonce"`
	Authorization               string    `json:"authorization_digest"`
	KeyID                       string    `json:"key_id"`
	KeyGeneration               string    `json:"key_generation"`
	RunnerObservedSecretUID     string    `json:"runner_observed_secret_uid"`
	RunnerObservedSecretVersion string    `json:"runner_observed_secret_version"`
	RecordedAt                  time.Time `json:"recorded_at"`
}

// EdgeActivationState is the durable, singleton migration/cutover ledger.
// Heartbeats can update instance observations but cannot advance this record.
type EdgeActivationState struct {
	Schema               string                  `json:"schema"`
	Phase                string                  `json:"phase"`
	RouteAuthority       string                  `json:"route_authority"`
	Generation           uint64                  `json:"generation"`
	PlanDigest           string                  `json:"plan_digest,omitempty"`
	ReleaseID            string                  `json:"release_id,omitempty"`
	ReleaseRecordUID     string                  `json:"release_record_uid,omitempty"`
	ReleaseRecordVersion string                  `json:"release_record_version,omitempty"`
	ReleaseRecordDigest  string                  `json:"release_record_digest,omitempty"`
	ExpectedInstances    []EdgeExpectedInstance  `json:"expected_instances,omitempty"`
	CandidateEpochs      []EdgeActiveEpoch       `json:"candidate_epochs,omitempty"`
	PreviousActiveEpochs []EdgeActiveEpoch       `json:"previous_active_epochs,omitempty"`
	PreviousAuthority    string                  `json:"previous_authority,omitempty"`
	LegacySnapshotDigest string                  `json:"legacy_snapshot_digest,omitempty"`
	APIReplicaGeneration string                  `json:"api_replica_generation,omitempty"`
	SoakStartedAt        *time.Time              `json:"soak_started_at,omitempty"`
	Receipts             []EdgeActivationReceipt `json:"receipts,omitempty"`
	Rollback             *EdgeActivationSnapshot `json:"rollback,omitempty"`
	Remediation          *EdgeRemediationAction  `json:"remediation,omitempty"`
	RemediationHistory   []EdgeRemediationAction `json:"remediation_history,omitempty"`
	CreatedAt            time.Time               `json:"created_at"`
	UpdatedAt            time.Time               `json:"updated_at"`
}

type EdgeRemediationTarget struct {
	EdgeID           string `json:"edge_id"`
	EdgeGroupID      string `json:"edge_group_id"`
	Slot             string `json:"slot"`
	InstanceUID      string `json:"instance_uid"`
	ReleaseEpoch     string `json:"release_epoch"`
	DaemonSetName    string `json:"daemonset_name"`
	DaemonSetUID     string `json:"daemonset_uid"`
	DaemonSetVersion string `json:"daemonset_version"`
	FailureClass     string `json:"failure_class"`
}

type EdgeRemediationAction struct {
	Schema                                   string                `json:"schema"`
	Sequence                                 uint64                `json:"sequence"`
	Phase                                    string                `json:"phase"`
	Nonce                                    string                `json:"nonce"`
	ReleaseFence                             string                `json:"release_fence"`
	AuthorizationDigest                      string                `json:"authorization_digest"`
	AuthorizationKeyID                       string                `json:"authorization_key_id"`
	AuthorizationKeyGeneration               string                `json:"authorization_key_generation"`
	AuthorizationRunnerObservedSecretUID     string                `json:"authorization_runner_observed_secret_uid"`
	AuthorizationRunnerObservedSecretVersion string                `json:"authorization_runner_observed_secret_version"`
	ActivationGeneration                     uint64                `json:"activation_generation"`
	PlanDigest                               string                `json:"plan_digest"`
	ReleaseID                                string                `json:"release_id"`
	ActiveEvidenceDigest                     string                `json:"active_evidence_digest"`
	PlatformEvidenceDigest                   string                `json:"platform_evidence_digest"`
	KubernetesDigest                         string                `json:"kubernetes_digest"`
	Target                                   EdgeRemediationTarget `json:"target"`
	Actor                                    string                `json:"actor"`
	CreatedAt                                time.Time             `json:"created_at"`
	UpdatedAt                                time.Time             `json:"updated_at"`
}

type EdgeRemediationAdvance struct {
	ExpectedActivationGeneration             uint64                       `json:"expected_activation_generation"`
	ExpectedActionSequence                   uint64                       `json:"expected_action_sequence"`
	ToPhase                                  string                       `json:"to_phase"`
	ActiveEvidenceDigest                     string                       `json:"active_evidence_digest"`
	PlatformEvidenceDigest                   string                       `json:"platform_evidence_digest"`
	KubernetesDigest                         string                       `json:"kubernetes_digest"`
	Target                                   EdgeRemediationTarget        `json:"target"`
	Authorization                            EdgeRemediationAuthorization `json:"authorization"`
	Actor                                    string                       `json:"-"`
	ReleaseFence                             string                       `json:"-"`
	Nonce                                    string                       `json:"-"`
	AuthorizationDigest                      string                       `json:"-"`
	AuthorizationKeyID                       string                       `json:"-"`
	AuthorizationKeyGeneration               string                       `json:"-"`
	AuthorizationRunnerObservedSecretUID     string                       `json:"-"`
	AuthorizationRunnerObservedSecretVersion string                       `json:"-"`
}

type EdgeRemediationAuthorization struct {
	Schema                      string `json:"schema"`
	KeyID                       string `json:"key_id"`
	KeyGeneration               string `json:"key_generation"`
	ReleaseFence                string `json:"release_fence"`
	ActionNonce                 string `json:"action_nonce"`
	ValidUntil                  string `json:"valid_until"`
	RunnerObservedSecretUID     string `json:"runner_observed_secret_uid"`
	RunnerObservedSecretVersion string `json:"runner_observed_secret_version"`
	Signature                   string `json:"signature"`
}

type EdgeActivationSnapshot struct {
	Phase                string                 `json:"phase"`
	RouteAuthority       string                 `json:"route_authority"`
	PlanDigest           string                 `json:"plan_digest,omitempty"`
	ReleaseID            string                 `json:"release_id,omitempty"`
	ReleaseRecordUID     string                 `json:"release_record_uid,omitempty"`
	ReleaseRecordVersion string                 `json:"release_record_version,omitempty"`
	ReleaseRecordDigest  string                 `json:"release_record_digest,omitempty"`
	ExpectedInstances    []EdgeExpectedInstance `json:"expected_instances,omitempty"`
	ActiveEpochs         []EdgeActiveEpoch      `json:"active_epochs,omitempty"`
	LegacySnapshotDigest string                 `json:"legacy_snapshot_digest,omitempty"`
	APIReplicaGeneration string                 `json:"api_replica_generation,omitempty"`
	SoakStartedAt        *time.Time             `json:"soak_started_at,omitempty"`
}

type EdgeActivationAdvance struct {
	ExpectedGeneration                       uint64                      `json:"expected_generation"`
	ToPhase                                  string                      `json:"to_phase"`
	PlanDigest                               string                      `json:"plan_digest"`
	EvidenceDigest                           string                      `json:"evidence_digest"`
	ReleaseID                                string                      `json:"release_id"`
	ReleaseRecordUID                         string                      `json:"release_record_uid"`
	ReleaseRecordVersion                     string                      `json:"release_record_version"`
	ReleaseRecordDigest                      string                      `json:"release_record_digest"`
	ExpectedInstances                        []EdgeExpectedInstance      `json:"expected_instances,omitempty"`
	ActiveEpochs                             []EdgeActiveEpoch           `json:"active_epochs,omitempty"`
	LegacySnapshotDigest                     string                      `json:"legacy_snapshot_digest"`
	APIReplicaGeneration                     string                      `json:"api_replica_generation,omitempty"`
	Actor                                    string                      `json:"actor"`
	Authorization                            EdgeActivationAuthorization `json:"authorization"`
	ReleaseFence                             string                      `json:"-"`
	PhaseNonce                               string                      `json:"-"`
	AuthorizationDigest                      string                      `json:"-"`
	AuthorizationKeyID                       string                      `json:"-"`
	AuthorizationKeyGeneration               string                      `json:"-"`
	AuthorizationRunnerObservedSecretUID     string                      `json:"-"`
	AuthorizationRunnerObservedSecretVersion string                      `json:"-"`
}

// EdgeActivationAuthorization binds one phase CAS to an immutable release
// record and the exact release runner fence. Platform-admin authentication is
// deliberately insufficient without this independently signed envelope.
type EdgeActivationAuthorization struct {
	Schema                      string `json:"schema"`
	KeyID                       string `json:"key_id"`
	KeyGeneration               string `json:"key_generation"`
	ReleaseFence                string `json:"release_fence"`
	PhaseNonce                  string `json:"phase_nonce"`
	ValidUntil                  string `json:"valid_until"`
	ExpectedInstancesDigest     string `json:"expected_instances_digest"`
	ActiveEpochsDigest          string `json:"active_epochs_digest"`
	RunnerObservedSecretUID     string `json:"runner_observed_secret_uid"`
	RunnerObservedSecretVersion string `json:"runner_observed_secret_version"`
	Signature                   string `json:"signature"`
}

// EdgeNodeInstance is one physical edge process identity. Node remains the
// compatibility-facing node snapshot, while the surrounding identity prevents
// two blue/green slots on the same machine from sharing mutable health state.
type EdgeNodeInstance struct {
	EdgeID               string    `json:"edge_id"`
	EdgeGroupID          string    `json:"edge_group_id"`
	Slot                 string    `json:"slot"`
	InstanceUID          string    `json:"instance_uid"`
	ReleaseEpoch         string    `json:"release_epoch"`
	Node                 EdgeNode  `json:"node"`
	FailureClass         string    `json:"failure_class,omitempty"`
	EffectiveHealthy     bool      `json:"effective_healthy"`
	ConsecutiveHealthy   int       `json:"consecutive_healthy"`
	ConsecutiveUnhealthy int       `json:"consecutive_unhealthy"`
	HealthStateSince     time.Time `json:"health_state_since,omitempty"`
	LastHeartbeatAt      time.Time `json:"last_heartbeat_at"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

// EdgeActiveEpoch is the centrally fenced release identity consumed by route
// generation. Heartbeats cannot create or advance it.
type EdgeActiveEpoch struct {
	EdgeGroupID         string    `json:"edge_group_id"`
	Slot                string    `json:"slot"`
	ReleaseEpoch        string    `json:"release_epoch"`
	FenceSequence       uint64    `json:"fence_sequence"`
	MinHealthyInstances int       `json:"min_healthy_instances"`
	ActivatedAt         time.Time `json:"activated_at"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}
