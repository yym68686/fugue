package store

import (
	"encoding/json"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

type OperationEvidenceRecordMetricCount struct {
	Type       string
	Severity   string
	Confidence string
	Count      int64
}

type OperationEvidenceCaptureMetricCount struct {
	Result string
	Count  int64
}

type OperationEvidenceRolloutFailureMetricCount struct {
	Reason     string
	Confidence string
	Count      int64
}

type ReleaseAttemptMetricCount struct {
	TriggerType string
	Status      string
	Count       int64
}

type ReleaseEvidenceResearchSummary struct {
	EnvPatchAttempts                 int64 `json:"env_patch_attempts"`
	TrackingSyncAttempts             int64 `json:"tracking_sync_attempts"`
	EnvPatchThenTrackingSyncAttempts int64 `json:"env_patch_then_tracking_sync_attempts"`
	WindowSeconds                    int64 `json:"window_seconds"`
}

type MigrationEvidenceMetricCount struct {
	Signal     string `json:"signal"`
	Confidence string `json:"confidence"`
	Count      int64  `json:"count"`
}

const (
	defaultOperationEvidenceLimit = 200
	maxOperationEvidenceLimit     = 1000
	// Used only by the internal, migration-only ledger reader. A 90-day safety
	// ledger must not silently truncate at the diagnostic API's page limit.
	unboundedMigrationEvidenceLimit = -1
	defaultReleaseAttemptLimit      = 50
	maxReleaseAttemptLimit          = 500

	operationEvidenceRetentionLimitPerApp       = 2000
	operationEvidenceRetentionLimitPerOperation = 500
	operationEvidenceRetentionLimitPerTenant    = 5000
	operationEvidenceRetentionWindow            = 30 * 24 * time.Hour
	operationMigrationLedgerRetentionWindow     = 90 * 24 * time.Hour
	releaseEvidenceResearchFollowupWindow       = time.Hour
)

func (s *Store) CountOperationEvidenceMetricGroups() ([]OperationEvidenceRecordMetricCount, []OperationEvidenceCaptureMetricCount, []OperationEvidenceRolloutFailureMetricCount, error) {
	return s.pgCountOperationEvidenceMetricGroups()
}

func (s *Store) CountReleaseAttemptMetricGroups() ([]ReleaseAttemptMetricCount, error) {
	return s.pgCountReleaseAttemptMetricGroups()
}

func (s *Store) CountReleaseEvidenceResearchGroups() (ReleaseEvidenceResearchSummary, []MigrationEvidenceMetricCount, error) {
	return s.pgCountReleaseEvidenceResearchGroups()
}

func rolloutFailureMetricReason(evidenceType string) string {
	switch strings.TrimSpace(evidenceType) {
	case model.OperationEvidenceTypeImagePullFailure:
		return "image_pull"
	case model.OperationEvidenceTypeSchedulerFailure:
		return "scheduling"
	case model.OperationEvidenceTypeVolumeMountFailure:
		return "volume_mount"
	case model.OperationEvidenceTypeReadinessProbeFailure:
		return "readiness_probe"
	case model.OperationEvidenceTypeLivenessProbeFailure:
		return "liveness_probe"
	case model.OperationEvidenceTypeStartupProbeFailure:
		return "startup_probe"
	case model.OperationEvidenceTypeRolloutContainerTerminated:
		return "container_terminated"
	case model.OperationEvidenceTypeRolloutPodFailure:
		return "pod_failure"
	case model.OperationEvidenceTypeRolloutTimeout:
		return "timeout"
	case model.OperationEvidenceTypeAppReleaseGateFailure:
		return "app_release_gate"
	default:
		return ""
	}
}

func operationEvidenceRolloutMetricCounts(byKey map[string]OperationEvidenceRolloutFailureMetricCount) []OperationEvidenceRolloutFailureMetricCount {
	items := make([]OperationEvidenceRolloutFailureMetricCount, 0, len(byKey))
	for _, item := range byKey {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Reason != items[j].Reason {
			return items[i].Reason < items[j].Reason
		}
		return items[i].Confidence < items[j].Confidence
	})
	return items
}

func firstNonEmptyOperationEvidenceString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (s *Store) RecordOperationEvidence(evidence model.OperationEvidence) (model.OperationEvidence, error) {
	evidence = normalizeOperationEvidence(evidence)
	if strings.TrimSpace(evidence.TenantID) == "" || strings.TrimSpace(evidence.OperationID) == "" || strings.TrimSpace(evidence.Type) == "" {
		return model.OperationEvidence{}, ErrInvalidInput
	}
	return s.pgRecordOperationEvidence(evidence)
}

func (s *Store) ListOperationEvidence(filter model.OperationEvidenceFilter) ([]model.OperationEvidence, error) {
	filter = normalizeOperationEvidenceFilter(filter)
	return s.pgListOperationEvidence(filter)
}

func (s *Store) ListOperationTimeline(tenantID string, platformAdmin bool, operationID string, includePayload bool) ([]model.OperationTimelineEntry, error) {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return nil, ErrInvalidInput
	}
	op, err := s.GetOperation(operationID)
	if err != nil {
		return nil, err
	}
	if !platformAdmin && strings.TrimSpace(op.TenantID) != strings.TrimSpace(tenantID) {
		return nil, ErrNotFound
	}
	evidence, err := s.ListOperationEvidence(model.OperationEvidenceFilter{
		TenantID:               tenantID,
		PlatformAdmin:          platformAdmin,
		OperationID:            operationID,
		Limit:                  maxOperationEvidenceLimit,
		IncludeMigrationLedger: true,
	})
	if err != nil {
		return nil, err
	}
	return BuildOperationTimeline(op, evidence, includePayload), nil
}

func BuildOperationTimeline(op model.Operation, evidence []model.OperationEvidence, includePayload bool) []model.OperationTimelineEntry {
	entries := make([]model.OperationTimelineEntry, 0, len(evidence)+3)
	if !op.CreatedAt.IsZero() {
		entries = append(entries, model.OperationTimelineEntry{
			ID:          op.ID + ":created",
			OperationID: op.ID,
			Type:        model.OperationEvidenceTypeOperationCreated,
			Source:      model.OperationEvidenceSourceAPI,
			Severity:    model.OperationEvidenceSeverityInfo,
			Confidence:  model.OperationEvidenceConfidenceConfirmed,
			Summary:     "operation created",
			At:          op.CreatedAt,
		})
	}
	if op.StartedAt != nil && !op.StartedAt.IsZero() {
		entries = append(entries, model.OperationTimelineEntry{
			ID:          op.ID + ":started",
			OperationID: op.ID,
			Type:        model.OperationEvidenceTypeOperationStarted,
			Source:      model.OperationEvidenceSourceController,
			Severity:    model.OperationEvidenceSeverityInfo,
			Confidence:  model.OperationEvidenceConfidenceConfirmed,
			Summary:     "operation started",
			At:          op.StartedAt.UTC(),
		})
	}
	for _, item := range evidence {
		entry := model.OperationTimelineEntry{
			ID:               item.ID,
			OperationID:      item.OperationID,
			ReleaseAttemptID: item.ReleaseAttemptID,
			Type:             item.Type,
			Source:           item.Source,
			Severity:         item.Severity,
			Confidence:       item.Confidence,
			Summary:          item.Summary,
			Message:          item.Message,
			Reason:           item.Reason,
			EvidenceID:       item.ID,
			At:               item.CollectedAt,
		}
		if includePayload {
			entry.Payload = clonePayloadMap(item.Payload)
		}
		if entry.At.IsZero() {
			entry.At = item.ObservedAt
		}
		if entry.At.IsZero() {
			entry.At = item.CreatedAt
		}
		entries = append(entries, entry)
	}
	if op.CompletedAt != nil && !op.CompletedAt.IsZero() {
		entryType := model.OperationEvidenceTypeOperationCompleted
		severity := model.OperationEvidenceSeverityInfo
		if strings.EqualFold(strings.TrimSpace(op.Status), model.OperationStatusFailed) {
			entryType = model.OperationEvidenceTypeOperationFailed
			severity = model.OperationEvidenceSeverityError
		}
		entries = append(entries, model.OperationTimelineEntry{
			ID:          op.ID + ":finished",
			OperationID: op.ID,
			Type:        entryType,
			Source:      model.OperationEvidenceSourceController,
			Severity:    severity,
			Confidence:  model.OperationEvidenceConfidenceConfirmed,
			Summary:     firstNonEmptyOperationEvidenceString(op.ErrorMessage, op.ResultMessage, "operation finished"),
			At:          op.CompletedAt.UTC(),
		})
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].At.Equal(entries[j].At) {
			return entries[i].ID < entries[j].ID
		}
		return entries[i].At.Before(entries[j].At)
	})
	return entries
}

func (s *Store) CreateReleaseAttempt(attempt model.ReleaseAttempt) (model.ReleaseAttempt, error) {
	attempt = normalizeReleaseAttempt(attempt)
	if strings.TrimSpace(attempt.TenantID) == "" || strings.TrimSpace(attempt.AppID) == "" || strings.TrimSpace(attempt.TriggerType) == "" {
		return model.ReleaseAttempt{}, ErrInvalidInput
	}
	return s.pgCreateReleaseAttempt(attempt)
}

func (s *Store) UpdateReleaseAttempt(attempt model.ReleaseAttempt) (model.ReleaseAttempt, error) {
	attempt = normalizeReleaseAttempt(attempt)
	if strings.TrimSpace(attempt.ID) == "" {
		return model.ReleaseAttempt{}, ErrInvalidInput
	}
	return s.pgUpdateReleaseAttempt(attempt)
}

func (s *Store) GetReleaseAttempt(id string) (model.ReleaseAttempt, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.ReleaseAttempt{}, ErrInvalidInput
	}
	return s.pgGetReleaseAttempt(id)
}

func (s *Store) ListReleaseAttempts(filter model.ReleaseAttemptFilter) ([]model.ReleaseAttempt, error) {
	filter = normalizeReleaseAttemptFilter(filter)
	return s.pgListReleaseAttempts(filter)
}

func (s *Store) FindReleaseAttemptForOperation(operationID string) (model.ReleaseAttempt, bool, error) {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return model.ReleaseAttempt{}, false, ErrInvalidInput
	}
	return s.pgFindReleaseAttemptForOperation(operationID)
}

func (s *Store) RecordReleaseStep(step model.ReleaseStep) (model.ReleaseStep, error) {
	step = normalizeReleaseStep(step)
	if strings.TrimSpace(step.TenantID) == "" || strings.TrimSpace(step.ReleaseAttemptID) == "" || strings.TrimSpace(step.Type) == "" {
		return model.ReleaseStep{}, ErrInvalidInput
	}
	return s.pgRecordReleaseStep(step)
}

func (s *Store) ListReleaseSteps(tenantID string, platformAdmin bool, releaseAttemptID string) ([]model.ReleaseStep, error) {
	releaseAttemptID = strings.TrimSpace(releaseAttemptID)
	if releaseAttemptID == "" {
		return nil, ErrInvalidInput
	}
	return s.pgListReleaseSteps(tenantID, platformAdmin, releaseAttemptID)
}

func (s *Store) ListReleaseTimeline(tenantID string, platformAdmin bool, releaseAttemptID string) ([]model.ReleaseTimelineEntry, error) {
	steps, err := s.ListReleaseSteps(tenantID, platformAdmin, releaseAttemptID)
	if err != nil {
		return nil, err
	}
	entries := make([]model.ReleaseTimelineEntry, 0, len(steps))
	for _, step := range steps {
		at := step.StartedAt
		if at.IsZero() && step.FinishedAt != nil {
			at = step.FinishedAt.UTC()
		}
		entries = append(entries, model.ReleaseTimelineEntry{
			ID:               step.ID,
			ReleaseAttemptID: step.ReleaseAttemptID,
			OperationID:      step.OperationID,
			Type:             step.Type,
			Status:           step.Status,
			Summary:          step.Summary,
			EvidenceID:       step.EvidenceID,
			At:               at,
			Payload:          clonePayloadMap(step.Payload),
		})
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].At.Equal(entries[j].At) {
			return entries[i].ID < entries[j].ID
		}
		return entries[i].At.Before(entries[j].At)
	})
	return entries, nil
}

const maxOperationEvidencePayloadBytes = 128 * 1024

func enforceOperationEvidencePayloadSize(payload map[string]any) map[string]any {
	if len(payload) == 0 {
		return payload
	}
	data, err := json.Marshal(payload)
	if err != nil || len(data) <= maxOperationEvidencePayloadBytes {
		return payload
	}
	originalBytes := len(data)
	trimmed := clonePayloadMap(payload)
	for key, value := range trimmed {
		text, ok := value.(string)
		if !ok || len(text) <= 4096 {
			continue
		}
		trimmed[key] = text[len(text)-4096:]
	}
	trimmed["payload_truncated"] = true
	trimmed["original_payload_bytes"] = originalBytes
	data, err = json.Marshal(trimmed)
	if err == nil && len(data) <= maxOperationEvidencePayloadBytes {
		return trimmed
	}
	return map[string]any{
		"schema_version":         1,
		"payload_truncated":      true,
		"original_payload_bytes": originalBytes,
		"truncation_reason":      "operation evidence payload exceeded storage limit",
	}
}

func normalizeOperationEvidence(in model.OperationEvidence) model.OperationEvidence {
	now := time.Now().UTC()
	in.ID = strings.TrimSpace(in.ID)
	if in.ID == "" {
		in.ID = model.NewID("evid")
	}
	in.TenantID = strings.TrimSpace(in.TenantID)
	in.ProjectID = strings.TrimSpace(in.ProjectID)
	in.AppID = strings.TrimSpace(in.AppID)
	in.OperationID = strings.TrimSpace(in.OperationID)
	in.ReleaseAttemptID = strings.TrimSpace(in.ReleaseAttemptID)
	in.Type = strings.TrimSpace(in.Type)
	in.Source = firstNonEmptyOperationEvidenceString(in.Source, model.OperationEvidenceSourceController)
	in.Severity = normalizeEvidenceSeverity(in.Severity)
	in.Confidence = normalizeEvidenceConfidence(in.Confidence)
	in.SubjectKind = strings.TrimSpace(in.SubjectKind)
	in.SubjectName = strings.TrimSpace(in.SubjectName)
	in.SubjectNamespace = strings.TrimSpace(in.SubjectNamespace)
	in.SubjectUID = strings.TrimSpace(in.SubjectUID)
	if in.ObservedAt.IsZero() {
		in.ObservedAt = now
	} else {
		in.ObservedAt = in.ObservedAt.UTC()
	}
	if in.CollectedAt.IsZero() {
		in.CollectedAt = now
	} else {
		in.CollectedAt = in.CollectedAt.UTC()
	}
	in.Summary = strings.TrimSpace(in.Summary)
	in.Message = strings.TrimSpace(in.Message)
	in.Reason = strings.TrimSpace(in.Reason)
	in.ContainerName = strings.TrimSpace(in.ContainerName)
	in.PodName = strings.TrimSpace(in.PodName)
	in.DeploymentName = strings.TrimSpace(in.DeploymentName)
	in.ReplicaSetName = strings.TrimSpace(in.ReplicaSetName)
	in.NodeName = strings.TrimSpace(in.NodeName)
	in.RedactionStatus = normalizeEvidenceRedactionStatus(in.RedactionStatus)
	if in.Payload == nil {
		in.Payload = map[string]any{}
	}
	in.Payload = enforceOperationEvidencePayloadSize(in.Payload)
	if in.PayloadVersion <= 0 {
		in.PayloadVersion = 1
	}
	if in.CreatedAt.IsZero() {
		in.CreatedAt = now
	} else {
		in.CreatedAt = in.CreatedAt.UTC()
	}
	return in
}

func normalizeOperationEvidenceFilter(in model.OperationEvidenceFilter) model.OperationEvidenceFilter {
	unboundedMigrationOnly := in.Limit == unboundedMigrationEvidenceLimit && in.IncludeMigrationLedger && len(in.Types) > 0
	for _, value := range in.Types {
		if !isLongLivedMigrationEvidenceType(value) {
			unboundedMigrationOnly = false
			break
		}
	}
	in.TenantID = strings.TrimSpace(in.TenantID)
	in.OperationID = strings.TrimSpace(in.OperationID)
	in.AppID = strings.TrimSpace(in.AppID)
	in.ReleaseAttemptID = strings.TrimSpace(in.ReleaseAttemptID)
	if in.Limit <= 0 && !unboundedMigrationOnly {
		in.Limit = defaultOperationEvidenceLimit
	}
	if in.Limit > maxOperationEvidenceLimit && !unboundedMigrationOnly {
		in.Limit = maxOperationEvidenceLimit
	}
	for _, value := range in.Types {
		if isLongLivedMigrationEvidenceType(value) {
			in.IncludeMigrationLedger = true
			break
		}
	}
	return in
}

func normalizeEvidenceSeverity(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case model.OperationEvidenceSeverityError:
		return model.OperationEvidenceSeverityError
	case model.OperationEvidenceSeverityWarning:
		return model.OperationEvidenceSeverityWarning
	case model.OperationEvidenceSeverityInfo, "":
		return model.OperationEvidenceSeverityInfo
	default:
		return model.OperationEvidenceSeverityInfo
	}
}

func normalizeEvidenceConfidence(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case model.OperationEvidenceConfidenceConfirmed:
		return model.OperationEvidenceConfidenceConfirmed
	case model.OperationEvidenceConfidenceEvidenceBacked:
		return model.OperationEvidenceConfidenceEvidenceBacked
	case model.OperationEvidenceConfidenceProbable:
		return model.OperationEvidenceConfidenceProbable
	case model.OperationEvidenceConfidenceInsufficientEvidence:
		return model.OperationEvidenceConfidenceInsufficientEvidence
	default:
		return model.OperationEvidenceConfidenceInsufficientEvidence
	}
}

func normalizeEvidenceRedactionStatus(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case model.OperationEvidenceRedactionRedacted:
		return model.OperationEvidenceRedactionRedacted
	case model.OperationEvidenceRedactionNone:
		return model.OperationEvidenceRedactionNone
	default:
		return model.OperationEvidenceRedactionNone
	}
}

func sortOperationEvidenceOldestFirst(items []model.OperationEvidence) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].CollectedAt.Equal(items[j].CollectedAt) {
			return items[i].ID < items[j].ID
		}
		return items[i].CollectedAt.Before(items[j].CollectedAt)
	})
}

func isLongLivedMigrationEvidenceType(evidenceType string) bool {
	if isPostgresMutationSafetyEvidence(evidenceType) {
		return true
	}
	switch strings.TrimSpace(evidenceType) {
	case model.OperationEvidenceTypeMigrationStarted,
		model.OperationEvidenceTypeMigrationCompleted,
		model.OperationEvidenceTypeMigrationFailed:
		return true
	default:
		return false
	}
}

// Physical-mutation intent must outlive diagnostic history and time limits;
// recovery may still own an unresolved, accepted Kubernetes mutation.
func isPostgresMutationSafetyEvidence(kind string) bool {
	switch kind {
	case "postgres_image_migration_baseline", "postgres_image_migration_intent", "postgres_image_migration_verified", "postgres_image_migration_drain_abort", "postgres_source_retirement_intent", "postgres_source_retirement_verified":
		return true
	default:
		return false
	}
}

func normalizeReleaseAttempt(in model.ReleaseAttempt) model.ReleaseAttempt {
	now := time.Now().UTC()
	in.ID = strings.TrimSpace(in.ID)
	if in.ID == "" {
		in.ID = model.NewID("rel")
	}
	in.TenantID = strings.TrimSpace(in.TenantID)
	in.ProjectID = strings.TrimSpace(in.ProjectID)
	in.AppID = strings.TrimSpace(in.AppID)
	in.TriggerType = strings.TrimSpace(in.TriggerType)
	in.TriggerActorType = firstNonEmptyOperationEvidenceString(in.TriggerActorType, model.ReleaseAttemptActorSystem)
	in.TriggerActorID = strings.TrimSpace(in.TriggerActorID)
	in.SourceOperationID = strings.TrimSpace(in.SourceOperationID)
	in.RootOperationID = strings.TrimSpace(in.RootOperationID)
	in.ImageRef = strings.TrimSpace(in.ImageRef)
	in.TargetDigest = strings.TrimSpace(in.TargetDigest)
	in.PreviousDigest = strings.TrimSpace(in.PreviousDigest)
	in.Status = normalizeReleaseAttemptStatus(in.Status)
	in.Confidence = normalizeEvidenceConfidence(in.Confidence)
	in.FailureOperationID = strings.TrimSpace(in.FailureOperationID)
	in.FailureEvidenceID = strings.TrimSpace(in.FailureEvidenceID)
	in.Summary = strings.TrimSpace(in.Summary)
	if in.StartedAt.IsZero() {
		in.StartedAt = now
	} else {
		in.StartedAt = in.StartedAt.UTC()
	}
	if in.CreatedAt.IsZero() {
		in.CreatedAt = now
	} else {
		in.CreatedAt = in.CreatedAt.UTC()
	}
	if in.UpdatedAt.IsZero() {
		in.UpdatedAt = now
	} else {
		in.UpdatedAt = in.UpdatedAt.UTC()
	}
	if in.DesiredSource == nil {
		in.DesiredSource = map[string]any{}
	}
	return in
}

func normalizeReleaseAttemptStatus(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case model.ReleaseAttemptStatusImporting:
		return model.ReleaseAttemptStatusImporting
	case model.ReleaseAttemptStatusDeploying:
		return model.ReleaseAttemptStatusDeploying
	case model.ReleaseAttemptStatusRollingOut:
		return model.ReleaseAttemptStatusRollingOut
	case model.ReleaseAttemptStatusHealthChecking:
		return model.ReleaseAttemptStatusHealthChecking
	case model.ReleaseAttemptStatusCompleted:
		return model.ReleaseAttemptStatusCompleted
	case model.ReleaseAttemptStatusFailed:
		return model.ReleaseAttemptStatusFailed
	case model.ReleaseAttemptStatusCancelled:
		return model.ReleaseAttemptStatusCancelled
	case model.ReleaseAttemptStatusSuperseded:
		return model.ReleaseAttemptStatusSuperseded
	default:
		return model.ReleaseAttemptStatusPending
	}
}

func normalizeReleaseAttemptFilter(in model.ReleaseAttemptFilter) model.ReleaseAttemptFilter {
	in.TenantID = strings.TrimSpace(in.TenantID)
	in.AppID = strings.TrimSpace(in.AppID)
	in.Status = strings.TrimSpace(in.Status)
	if in.Limit <= 0 {
		in.Limit = defaultReleaseAttemptLimit
	}
	if in.Limit > maxReleaseAttemptLimit {
		in.Limit = maxReleaseAttemptLimit
	}
	return in
}

func normalizeReleaseStep(in model.ReleaseStep) model.ReleaseStep {
	now := time.Now().UTC()
	in.ID = strings.TrimSpace(in.ID)
	if in.ID == "" {
		in.ID = model.NewID("relstep")
	}
	in.TenantID = strings.TrimSpace(in.TenantID)
	in.ReleaseAttemptID = strings.TrimSpace(in.ReleaseAttemptID)
	in.OperationID = strings.TrimSpace(in.OperationID)
	in.Type = strings.TrimSpace(in.Type)
	in.Status = normalizeReleaseStepStatus(in.Status)
	in.Summary = strings.TrimSpace(in.Summary)
	in.EvidenceID = strings.TrimSpace(in.EvidenceID)
	if in.Payload == nil {
		in.Payload = map[string]any{}
	}
	if in.StartedAt.IsZero() {
		in.StartedAt = now
	} else {
		in.StartedAt = in.StartedAt.UTC()
	}
	if in.CreatedAt.IsZero() {
		in.CreatedAt = now
	} else {
		in.CreatedAt = in.CreatedAt.UTC()
	}
	return in
}

func normalizeReleaseStepStatus(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case model.ReleaseStepStatusRunning:
		return model.ReleaseStepStatusRunning
	case model.ReleaseStepStatusCompleted:
		return model.ReleaseStepStatusCompleted
	case model.ReleaseStepStatusFailed:
		return model.ReleaseStepStatusFailed
	case model.ReleaseStepStatusSkipped:
		return model.ReleaseStepStatusSkipped
	default:
		return model.ReleaseStepStatusPending
	}
}

func clonePayloadMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
