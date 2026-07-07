package model

import "time"

const (
	OperationEvidenceSeverityInfo    = "info"
	OperationEvidenceSeverityWarning = "warning"
	OperationEvidenceSeverityError   = "error"

	OperationEvidenceConfidenceConfirmed            = "confirmed"
	OperationEvidenceConfidenceEvidenceBacked       = "evidence_backed"
	OperationEvidenceConfidenceProbable             = "probable"
	OperationEvidenceConfidenceInsufficientEvidence = "insufficient_evidence"

	OperationEvidenceRedactionNone     = "none"
	OperationEvidenceRedactionRedacted = "redacted"

	OperationEvidenceSourceAPI                     = "api"
	OperationEvidenceSourceCLI                     = "cli"
	OperationEvidenceSourceController              = "controller"
	OperationEvidenceSourceImageTrackingController = "image_tracking_controller"
	OperationEvidenceSourceImportController        = "import_controller"
	OperationEvidenceSourceRolloutObserver         = "rollout_observer"
	OperationEvidenceSourceKubernetesAPI           = "kubernetes_api"
	OperationEvidenceSourceAppLogs                 = "app_logs"
	OperationEvidenceSourceMetricsQuery            = "metrics_query"
	OperationEvidenceSourceManualDebugBundle       = "manual_debug_bundle"

	OperationEvidenceTypeOperationCreated            = "operation_created"
	OperationEvidenceTypeOperationStarted            = "operation_started"
	OperationEvidenceTypeOperationCompleted          = "operation_completed"
	OperationEvidenceTypeOperationFailed             = "operation_failed"
	OperationEvidenceTypeOperationCancelled          = "operation_cancelled"
	OperationEvidenceTypeImageTrackingDecision       = "image_tracking_decision"
	OperationEvidenceTypeImageImportStarted          = "image_import_started"
	OperationEvidenceTypeImageImportCompleted        = "image_import_completed"
	OperationEvidenceTypeImageImportFailed           = "image_import_failed"
	OperationEvidenceTypeDeployApplyStarted          = "deploy_apply_started"
	OperationEvidenceTypeDeployApplyCompleted        = "deploy_apply_completed"
	OperationEvidenceTypeRolloutWaitStarted          = "rollout_wait_started"
	OperationEvidenceTypeRolloutProgress             = "rollout_progress"
	OperationEvidenceTypeRolloutPodFailure           = "rollout_pod_failure"
	OperationEvidenceTypeRolloutContainerTerminated  = "rollout_container_terminated"
	OperationEvidenceTypeRolloutPreviousLogs         = "rollout_previous_logs"
	OperationEvidenceTypeRolloutCurrentLogs          = "rollout_current_logs"
	OperationEvidenceTypeRolloutKubernetesEvent      = "rollout_kubernetes_event"
	OperationEvidenceTypeRolloutDeploymentSnapshot   = "rollout_deployment_snapshot"
	OperationEvidenceTypeRolloutReplicaSetSnapshot   = "rollout_replicaset_snapshot"
	OperationEvidenceTypeRolloutPodSnapshot          = "rollout_pod_snapshot"
	OperationEvidenceTypeRolloutTimeout              = "rollout_timeout"
	OperationEvidenceTypeReadinessProbeFailure       = "readiness_probe_failure"
	OperationEvidenceTypeLivenessProbeFailure        = "liveness_probe_failure"
	OperationEvidenceTypeStartupProbeFailure         = "startup_probe_failure"
	OperationEvidenceTypeSchedulerFailure            = "scheduler_failure"
	OperationEvidenceTypeImagePullFailure            = "image_pull_failure"
	OperationEvidenceTypeVolumeMountFailure          = "volume_mount_failure"
	OperationEvidenceTypeCollectorError              = "collector_error"
	OperationEvidenceTypeRedactionNotice             = "redaction_notice"
	OperationEvidenceTypeMigrationStarted            = "migration_started"
	OperationEvidenceTypeMigrationCompleted          = "migration_completed"
	OperationEvidenceTypeMigrationFailed             = "migration_failed"
	OperationEvidenceTypeMigrationSchemaVersionFound = "migration_schema_version_observed"
	OperationEvidenceTypeAppReleaseGateFailure       = "app_release_gate_failure"

	ReleaseAttemptTriggerManualDeploy            = "manual_deploy"
	ReleaseAttemptTriggerManualRestart           = "manual_restart"
	ReleaseAttemptTriggerEnvPatch                = "env_patch"
	ReleaseAttemptTriggerImageTrackingAuto       = "image_tracking_auto"
	ReleaseAttemptTriggerImageTrackingManualSync = "image_tracking_manual_sync"
	ReleaseAttemptTriggerConfigPatch             = "config_patch"
	ReleaseAttemptTriggerResourcePatch           = "resource_patch"
	ReleaseAttemptActorUser                      = "user"
	ReleaseAttemptActorSystem                    = "system"
	ReleaseAttemptStatusPending                  = "pending"
	ReleaseAttemptStatusImporting                = "importing"
	ReleaseAttemptStatusDeploying                = "deploying"
	ReleaseAttemptStatusRollingOut               = "rolling_out"
	ReleaseAttemptStatusHealthChecking           = "health_checking"
	ReleaseAttemptStatusCompleted                = "completed"
	ReleaseAttemptStatusFailed                   = "failed"
	ReleaseAttemptStatusCancelled                = "cancelled"
	ReleaseAttemptStatusSuperseded               = "superseded"
	ReleaseStepTypeTriggerReceived               = "trigger_received"
	ReleaseStepTypeImageTrackingCheck            = "image_tracking_check"
	ReleaseStepTypeImageImport                   = "image_import"
	ReleaseStepTypeImageImportWait               = "image_import_wait"
	ReleaseStepTypeDeployQueued                  = "deploy_queued"
	ReleaseStepTypeDeployApply                   = "deploy_apply"
	ReleaseStepTypeRolloutWait                   = "rollout_wait"
	ReleaseStepTypeHealthCheck                   = "health_check"
	ReleaseStepTypeMarkDeployedDigest            = "mark_deployed_digest"
	ReleaseStepTypeFinalize                      = "finalize"
	ReleaseStepStatusPending                     = "pending"
	ReleaseStepStatusRunning                     = "running"
	ReleaseStepStatusCompleted                   = "completed"
	ReleaseStepStatusFailed                      = "failed"
	ReleaseStepStatusSkipped                     = "skipped"
)

type OperationEvidence struct {
	ID               string         `json:"id"`
	TenantID         string         `json:"tenant_id"`
	ProjectID        string         `json:"project_id,omitempty"`
	AppID            string         `json:"app_id,omitempty"`
	OperationID      string         `json:"operation_id"`
	ReleaseAttemptID string         `json:"release_attempt_id,omitempty"`
	Type             string         `json:"type"`
	Source           string         `json:"source"`
	Severity         string         `json:"severity"`
	Confidence       string         `json:"confidence"`
	SubjectKind      string         `json:"subject_kind,omitempty"`
	SubjectName      string         `json:"subject_name,omitempty"`
	SubjectNamespace string         `json:"subject_namespace,omitempty"`
	SubjectUID       string         `json:"subject_uid,omitempty"`
	ObservedAt       time.Time      `json:"observed_at"`
	CollectedAt      time.Time      `json:"collected_at"`
	Summary          string         `json:"summary"`
	Message          string         `json:"message,omitempty"`
	Reason           string         `json:"reason,omitempty"`
	ExitCode         *int           `json:"exit_code,omitempty"`
	StartedAt        *time.Time     `json:"started_at,omitempty"`
	FinishedAt       *time.Time     `json:"finished_at,omitempty"`
	ContainerName    string         `json:"container_name,omitempty"`
	PodName          string         `json:"pod_name,omitempty"`
	DeploymentName   string         `json:"deployment_name,omitempty"`
	ReplicaSetName   string         `json:"replica_set_name,omitempty"`
	NodeName         string         `json:"node_name,omitempty"`
	RedactionStatus  string         `json:"redaction_status"`
	Payload          map[string]any `json:"payload,omitempty"`
	PayloadVersion   int            `json:"payload_version"`
	CreatedAt        time.Time      `json:"created_at"`
}

type OperationEvidenceFilter struct {
	TenantID         string
	PlatformAdmin    bool
	OperationID      string
	AppID            string
	ReleaseAttemptID string
	Types            []string
	Severities       []string
	Since            *time.Time
	Limit            int
}

type OperationTimelineEntry struct {
	ID               string         `json:"id"`
	OperationID      string         `json:"operation_id"`
	ReleaseAttemptID string         `json:"release_attempt_id,omitempty"`
	Type             string         `json:"type"`
	Source           string         `json:"source,omitempty"`
	Severity         string         `json:"severity,omitempty"`
	Confidence       string         `json:"confidence,omitempty"`
	Summary          string         `json:"summary"`
	Message          string         `json:"message,omitempty"`
	Reason           string         `json:"reason,omitempty"`
	EvidenceID       string         `json:"evidence_id,omitempty"`
	At               time.Time      `json:"at"`
	Payload          map[string]any `json:"payload,omitempty"`
}

type ReleaseAttempt struct {
	ID                 string         `json:"id"`
	TenantID           string         `json:"tenant_id"`
	ProjectID          string         `json:"project_id"`
	AppID              string         `json:"app_id"`
	TriggerType        string         `json:"trigger_type"`
	TriggerActorType   string         `json:"trigger_actor_type"`
	TriggerActorID     string         `json:"trigger_actor_id,omitempty"`
	SourceOperationID  string         `json:"source_operation_id,omitempty"`
	RootOperationID    string         `json:"root_operation_id,omitempty"`
	ImageRef           string         `json:"image_ref,omitempty"`
	TargetDigest       string         `json:"target_digest,omitempty"`
	PreviousDigest     string         `json:"previous_digest,omitempty"`
	DesiredSource      map[string]any `json:"desired_source,omitempty"`
	Status             string         `json:"status"`
	Confidence         string         `json:"confidence"`
	FailureOperationID string         `json:"failure_operation_id,omitempty"`
	FailureEvidenceID  string         `json:"failure_evidence_id,omitempty"`
	Summary            string         `json:"summary,omitempty"`
	StartedAt          time.Time      `json:"started_at"`
	FinishedAt         *time.Time     `json:"finished_at,omitempty"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

type ReleaseAttemptFilter struct {
	TenantID      string
	PlatformAdmin bool
	AppID         string
	Status        string
	Limit         int
}

type ReleaseStep struct {
	ID               string         `json:"id"`
	TenantID         string         `json:"tenant_id"`
	ReleaseAttemptID string         `json:"release_attempt_id"`
	OperationID      string         `json:"operation_id,omitempty"`
	Type             string         `json:"type"`
	Status           string         `json:"status"`
	Summary          string         `json:"summary"`
	EvidenceID       string         `json:"evidence_id,omitempty"`
	Payload          map[string]any `json:"payload,omitempty"`
	StartedAt        time.Time      `json:"started_at"`
	FinishedAt       *time.Time     `json:"finished_at,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
}

type ReleaseTimelineEntry struct {
	ID               string         `json:"id"`
	ReleaseAttemptID string         `json:"release_attempt_id"`
	OperationID      string         `json:"operation_id,omitempty"`
	Type             string         `json:"type"`
	Status           string         `json:"status,omitempty"`
	Summary          string         `json:"summary"`
	EvidenceID       string         `json:"evidence_id,omitempty"`
	At               time.Time      `json:"at"`
	Payload          map[string]any `json:"payload,omitempty"`
}

type OperationDebugBundle struct {
	Metadata            map[string]any           `json:"metadata"`
	Operation           Operation                `json:"operation"`
	App                 *App                     `json:"app,omitempty"`
	ImageTracking       *AppImageTracking        `json:"image_tracking,omitempty"`
	ImageTrackingChecks []AppImageTrackingCheck  `json:"image_tracking_checks,omitempty"`
	MetricsSummary      map[string]any           `json:"metrics_summary,omitempty"`
	Diagnosis           *OperationDiagnosis      `json:"diagnosis,omitempty"`
	Timeline            []OperationTimelineEntry `json:"timeline"`
	Evidence            []OperationEvidence      `json:"evidence"`
	ReleaseAttempt      *ReleaseAttempt          `json:"release_attempt,omitempty"`
	ReleaseTimeline     []ReleaseTimelineEntry   `json:"release_timeline,omitempty"`
	AppReleases         []AppRelease             `json:"app_releases,omitempty"`
	TrafficPolicies     []AppTrafficPolicy       `json:"traffic_policies,omitempty"`
	GateResults         []AppReleaseGateResult   `json:"gate_results,omitempty"`
	DrainMetricsSummary map[string]any           `json:"drain_metrics_summary,omitempty"`
	RedactionReport     []map[string]any         `json:"redaction_report,omitempty"`
}

type ReleaseDebugBundle struct {
	Metadata            map[string]any          `json:"metadata"`
	ReleaseAttempt      ReleaseAttempt          `json:"release_attempt"`
	App                 *App                    `json:"app,omitempty"`
	ImageTracking       *AppImageTracking       `json:"image_tracking,omitempty"`
	ImageTrackingChecks []AppImageTrackingCheck `json:"image_tracking_checks,omitempty"`
	MetricsSummary      map[string]any          `json:"metrics_summary,omitempty"`
	ReleaseTimeline     []ReleaseTimelineEntry  `json:"release_timeline"`
	Evidence            []OperationEvidence     `json:"evidence"`
	AppReleases         []AppRelease            `json:"app_releases,omitempty"`
	TrafficPolicies     []AppTrafficPolicy      `json:"traffic_policies,omitempty"`
	GateResults         []AppReleaseGateResult  `json:"gate_results,omitempty"`
	DrainMetricsSummary map[string]any          `json:"drain_metrics_summary,omitempty"`
	RedactionReport     []map[string]any        `json:"redaction_report,omitempty"`
}
