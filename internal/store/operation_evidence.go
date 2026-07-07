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
	defaultReleaseAttemptLimit    = 50
	maxReleaseAttemptLimit        = 500

	operationEvidenceRetentionLimitPerApp       = 2000
	operationEvidenceRetentionLimitPerOperation = 500
	operationEvidenceRetentionLimitPerTenant    = 5000
	operationEvidenceRetentionWindow            = 30 * 24 * time.Hour
	releaseEvidenceResearchFollowupWindow       = time.Hour
)

func (s *Store) CountOperationEvidenceMetricGroups() ([]OperationEvidenceRecordMetricCount, []OperationEvidenceCaptureMetricCount, []OperationEvidenceRolloutFailureMetricCount, error) {
	if s.usingDatabase() {
		return s.pgCountOperationEvidenceMetricGroups()
	}
	recordsByKey := map[string]OperationEvidenceRecordMetricCount{}
	captureByResult := map[string]OperationEvidenceCaptureMetricCount{}
	rolloutByKey := map[string]OperationEvidenceRolloutFailureMetricCount{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, evidence := range state.OperationEvidence {
			recordKey := evidence.Type + "\x00" + evidence.Severity + "\x00" + evidence.Confidence
			record := recordsByKey[recordKey]
			record.Type = evidence.Type
			record.Severity = evidence.Severity
			record.Confidence = evidence.Confidence
			record.Count++
			recordsByKey[recordKey] = record

			if isEvidenceCaptureMetricType(evidence.Type) {
				result := evidenceCaptureMetricResult(evidence.Type)
				capture := captureByResult[result]
				capture.Result = result
				capture.Count++
				captureByResult[result] = capture
			}

			if reason := rolloutFailureMetricReason(evidence.Type); reason != "" {
				key := reason + "\x00" + evidence.Confidence
				rollout := rolloutByKey[key]
				rollout.Reason = reason
				rollout.Confidence = evidence.Confidence
				rollout.Count++
				rolloutByKey[key] = rollout
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	records := operationEvidenceRecordMetricCounts(recordsByKey)
	captures := operationEvidenceCaptureMetricCounts(captureByResult)
	rollouts := operationEvidenceRolloutMetricCounts(rolloutByKey)
	return records, captures, rollouts, nil
}

func (s *Store) CountReleaseAttemptMetricGroups() ([]ReleaseAttemptMetricCount, error) {
	if s.usingDatabase() {
		return s.pgCountReleaseAttemptMetricGroups()
	}
	byKey := map[string]ReleaseAttemptMetricCount{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, attempt := range state.ReleaseAttempts {
			key := attempt.TriggerType + "\x00" + attempt.Status
			count := byKey[key]
			count.TriggerType = attempt.TriggerType
			count.Status = attempt.Status
			count.Count++
			byKey[key] = count
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return releaseAttemptMetricCounts(byKey), nil
}

func (s *Store) CountReleaseEvidenceResearchGroups() (ReleaseEvidenceResearchSummary, []MigrationEvidenceMetricCount, error) {
	if s.usingDatabase() {
		return s.pgCountReleaseEvidenceResearchGroups()
	}
	summary := ReleaseEvidenceResearchSummary{WindowSeconds: int64(releaseEvidenceResearchFollowupWindow.Seconds())}
	migrationByKey := map[string]MigrationEvidenceMetricCount{}
	err := s.withLockedState(false, func(state *model.State) error {
		trackingAttempts := []model.ReleaseAttempt{}
		for _, attempt := range state.ReleaseAttempts {
			switch strings.TrimSpace(attempt.TriggerType) {
			case model.ReleaseAttemptTriggerEnvPatch:
				summary.EnvPatchAttempts++
			case model.ReleaseAttemptTriggerImageTrackingAuto, model.ReleaseAttemptTriggerImageTrackingManualSync:
				summary.TrackingSyncAttempts++
				trackingAttempts = append(trackingAttempts, attempt)
			}
		}
		for _, envAttempt := range state.ReleaseAttempts {
			if strings.TrimSpace(envAttempt.TriggerType) != model.ReleaseAttemptTriggerEnvPatch {
				continue
			}
			if releaseAttemptHasFollowupTrackingSync(envAttempt, trackingAttempts, releaseEvidenceResearchFollowupWindow) {
				summary.EnvPatchThenTrackingSyncAttempts++
			}
		}
		for _, evidence := range state.OperationEvidence {
			for _, signal := range migrationEvidenceSignals(evidence) {
				key := signal + "\x00" + evidence.Confidence
				item := migrationByKey[key]
				item.Signal = signal
				item.Confidence = evidence.Confidence
				item.Count++
				migrationByKey[key] = item
			}
		}
		return nil
	})
	if err != nil {
		return ReleaseEvidenceResearchSummary{}, nil, err
	}
	return summary, migrationEvidenceMetricCounts(migrationByKey), nil
}

func isEvidenceCaptureMetricType(evidenceType string) bool {
	switch strings.TrimSpace(evidenceType) {
	case model.OperationEvidenceTypeCollectorError,
		model.OperationEvidenceTypeRolloutPodFailure,
		model.OperationEvidenceTypeRolloutContainerTerminated,
		model.OperationEvidenceTypeRolloutPreviousLogs,
		model.OperationEvidenceTypeRolloutCurrentLogs,
		model.OperationEvidenceTypeRolloutKubernetesEvent,
		model.OperationEvidenceTypeRolloutDeploymentSnapshot,
		model.OperationEvidenceTypeRolloutReplicaSetSnapshot,
		model.OperationEvidenceTypeRolloutPodSnapshot,
		model.OperationEvidenceTypeImagePullFailure,
		model.OperationEvidenceTypeSchedulerFailure,
		model.OperationEvidenceTypeVolumeMountFailure,
		model.OperationEvidenceTypeReadinessProbeFailure,
		model.OperationEvidenceTypeLivenessProbeFailure,
		model.OperationEvidenceTypeStartupProbeFailure:
		return true
	default:
		return false
	}
}

func evidenceCaptureMetricResult(evidenceType string) string {
	if strings.TrimSpace(evidenceType) == model.OperationEvidenceTypeCollectorError {
		return "error"
	}
	return "success"
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

func operationEvidenceRecordMetricCounts(byKey map[string]OperationEvidenceRecordMetricCount) []OperationEvidenceRecordMetricCount {
	items := make([]OperationEvidenceRecordMetricCount, 0, len(byKey))
	for _, item := range byKey {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Type != items[j].Type {
			return items[i].Type < items[j].Type
		}
		if items[i].Severity != items[j].Severity {
			return items[i].Severity < items[j].Severity
		}
		return items[i].Confidence < items[j].Confidence
	})
	return items
}

func operationEvidenceCaptureMetricCounts(byKey map[string]OperationEvidenceCaptureMetricCount) []OperationEvidenceCaptureMetricCount {
	items := make([]OperationEvidenceCaptureMetricCount, 0, len(byKey))
	for _, item := range byKey {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Result < items[j].Result })
	return items
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

func releaseAttemptMetricCounts(byKey map[string]ReleaseAttemptMetricCount) []ReleaseAttemptMetricCount {
	items := make([]ReleaseAttemptMetricCount, 0, len(byKey))
	for _, item := range byKey {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].TriggerType != items[j].TriggerType {
			return items[i].TriggerType < items[j].TriggerType
		}
		return items[i].Status < items[j].Status
	})
	return items
}

func migrationEvidenceMetricCounts(byKey map[string]MigrationEvidenceMetricCount) []MigrationEvidenceMetricCount {
	items := make([]MigrationEvidenceMetricCount, 0, len(byKey))
	for _, item := range byKey {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Signal != items[j].Signal {
			return items[i].Signal < items[j].Signal
		}
		return items[i].Confidence < items[j].Confidence
	})
	return items
}

func releaseAttemptHasFollowupTrackingSync(envAttempt model.ReleaseAttempt, trackingAttempts []model.ReleaseAttempt, window time.Duration) bool {
	if strings.TrimSpace(envAttempt.AppID) == "" || envAttempt.StartedAt.IsZero() {
		return false
	}
	start := envAttempt.StartedAt.UTC()
	deadline := start.Add(window)
	for _, trackingAttempt := range trackingAttempts {
		if strings.TrimSpace(trackingAttempt.AppID) != strings.TrimSpace(envAttempt.AppID) {
			continue
		}
		if trackingAttempt.StartedAt.Before(start) || trackingAttempt.StartedAt.After(deadline) {
			continue
		}
		return true
	}
	return false
}

func migrationEvidenceSignals(evidence model.OperationEvidence) []string {
	evidenceType := strings.TrimSpace(evidence.Type)
	if evidenceType != model.OperationEvidenceTypeRolloutPreviousLogs && evidenceType != model.OperationEvidenceTypeRolloutCurrentLogs {
		return nil
	}
	haystack := strings.ToLower(evidence.Summary + "\n" + evidence.Message + "\n" + payloadSearchText(evidence.Payload))
	signals := []string{}
	if strings.Contains(haystack, "migration") || strings.Contains(haystack, "schema") {
		signals = append(signals, "schema_or_migration_log")
	}
	if strings.Contains(haystack, "sqlstate") {
		signals = append(signals, "sqlstate_log")
	}
	if strings.Contains(haystack, "deadlock") {
		signals = append(signals, "deadlock_log")
	}
	return signals
}

func payloadSearchText(payload map[string]any) string {
	if len(payload) == 0 {
		return ""
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return string(data)
}

func (s *Store) RecordOperationEvidence(evidence model.OperationEvidence) (model.OperationEvidence, error) {
	evidence = normalizeOperationEvidence(evidence)
	if strings.TrimSpace(evidence.TenantID) == "" || strings.TrimSpace(evidence.OperationID) == "" || strings.TrimSpace(evidence.Type) == "" {
		return model.OperationEvidence{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRecordOperationEvidence(evidence)
	}
	var out model.OperationEvidence
	err := s.withLockedState(true, func(state *model.State) error {
		if findOperation(state, evidence.OperationID) < 0 {
			return ErrNotFound
		}
		state.OperationEvidence = append(state.OperationEvidence, evidence)
		state.OperationEvidence = retainOperationEvidence(state.OperationEvidence, evidence)
		out = evidence
		return nil
	})
	return out, err
}

func (s *Store) ListOperationEvidence(filter model.OperationEvidenceFilter) ([]model.OperationEvidence, error) {
	filter = normalizeOperationEvidenceFilter(filter)
	if s.usingDatabase() {
		return s.pgListOperationEvidence(filter)
	}
	var out []model.OperationEvidence
	err := s.withLockedState(false, func(state *model.State) error {
		for _, evidence := range state.OperationEvidence {
			if !operationEvidenceMatchesFilter(evidence, filter) {
				continue
			}
			out = append(out, cloneOperationEvidence(evidence))
		}
		sortOperationEvidenceOldestFirst(out)
		out = limitOperationEvidenceToNewest(out, filter.Limit)
		return nil
	})
	return out, err
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
		TenantID:      tenantID,
		PlatformAdmin: platformAdmin,
		OperationID:   operationID,
		Limit:         maxOperationEvidenceLimit,
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
			Summary:     firstNonEmptyStoreString(op.ResultMessage, "operation started"),
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
			Summary:     firstNonEmptyStoreString(op.ErrorMessage, op.ResultMessage, "operation finished"),
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
	if s.usingDatabase() {
		return s.pgCreateReleaseAttempt(attempt)
	}
	var out model.ReleaseAttempt
	err := s.withLockedState(true, func(state *model.State) error {
		if findApp(state, attempt.AppID) < 0 {
			return ErrNotFound
		}
		state.ReleaseAttempts = append(state.ReleaseAttempts, attempt)
		out = attempt
		return nil
	})
	return out, err
}

func (s *Store) UpdateReleaseAttempt(attempt model.ReleaseAttempt) (model.ReleaseAttempt, error) {
	attempt = normalizeReleaseAttempt(attempt)
	if strings.TrimSpace(attempt.ID) == "" {
		return model.ReleaseAttempt{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpdateReleaseAttempt(attempt)
	}
	var out model.ReleaseAttempt
	err := s.withLockedState(true, func(state *model.State) error {
		idx := findReleaseAttempt(state, attempt.ID)
		if idx < 0 {
			return ErrNotFound
		}
		state.ReleaseAttempts[idx] = attempt
		out = attempt
		return nil
	})
	return out, err
}

func (s *Store) GetReleaseAttempt(id string) (model.ReleaseAttempt, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.ReleaseAttempt{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetReleaseAttempt(id)
	}
	var out model.ReleaseAttempt
	err := s.withLockedState(false, func(state *model.State) error {
		idx := findReleaseAttempt(state, id)
		if idx < 0 {
			return ErrNotFound
		}
		out = cloneReleaseAttempt(state.ReleaseAttempts[idx])
		return nil
	})
	return out, err
}

func (s *Store) ListReleaseAttempts(filter model.ReleaseAttemptFilter) ([]model.ReleaseAttempt, error) {
	filter = normalizeReleaseAttemptFilter(filter)
	if s.usingDatabase() {
		return s.pgListReleaseAttempts(filter)
	}
	var out []model.ReleaseAttempt
	err := s.withLockedState(false, func(state *model.State) error {
		for _, attempt := range state.ReleaseAttempts {
			if !releaseAttemptMatchesFilter(attempt, filter) {
				continue
			}
			out = append(out, cloneReleaseAttempt(attempt))
		}
		sortReleaseAttemptsNewestFirst(out)
		if filter.Limit > 0 && len(out) > filter.Limit {
			out = out[:filter.Limit]
		}
		return nil
	})
	return out, err
}

func (s *Store) FindReleaseAttemptForOperation(operationID string) (model.ReleaseAttempt, bool, error) {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return model.ReleaseAttempt{}, false, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgFindReleaseAttemptForOperation(operationID)
	}
	var out model.ReleaseAttempt
	var found bool
	err := s.withLockedState(false, func(state *model.State) error {
		for _, attempt := range state.ReleaseAttempts {
			if strings.TrimSpace(attempt.SourceOperationID) == operationID ||
				strings.TrimSpace(attempt.RootOperationID) == operationID ||
				strings.TrimSpace(attempt.FailureOperationID) == operationID {
				if !found || attempt.StartedAt.After(out.StartedAt) {
					out = cloneReleaseAttempt(attempt)
					found = true
				}
			}
		}
		for _, step := range state.ReleaseSteps {
			if strings.TrimSpace(step.OperationID) != operationID {
				continue
			}
			idx := findReleaseAttempt(state, step.ReleaseAttemptID)
			if idx < 0 {
				continue
			}
			attempt := state.ReleaseAttempts[idx]
			if !found || attempt.StartedAt.After(out.StartedAt) {
				out = cloneReleaseAttempt(attempt)
				found = true
			}
		}
		return nil
	})
	return out, found, err
}

func (s *Store) RecordReleaseStep(step model.ReleaseStep) (model.ReleaseStep, error) {
	step = normalizeReleaseStep(step)
	if strings.TrimSpace(step.TenantID) == "" || strings.TrimSpace(step.ReleaseAttemptID) == "" || strings.TrimSpace(step.Type) == "" {
		return model.ReleaseStep{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRecordReleaseStep(step)
	}
	var out model.ReleaseStep
	err := s.withLockedState(true, func(state *model.State) error {
		if findReleaseAttempt(state, step.ReleaseAttemptID) < 0 {
			return ErrNotFound
		}
		state.ReleaseSteps = append(state.ReleaseSteps, step)
		out = step
		return nil
	})
	return out, err
}

func (s *Store) ListReleaseSteps(tenantID string, platformAdmin bool, releaseAttemptID string) ([]model.ReleaseStep, error) {
	releaseAttemptID = strings.TrimSpace(releaseAttemptID)
	if releaseAttemptID == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListReleaseSteps(tenantID, platformAdmin, releaseAttemptID)
	}
	var out []model.ReleaseStep
	err := s.withLockedState(false, func(state *model.State) error {
		attemptIdx := findReleaseAttempt(state, releaseAttemptID)
		if attemptIdx < 0 {
			return ErrNotFound
		}
		if !platformAdmin && strings.TrimSpace(state.ReleaseAttempts[attemptIdx].TenantID) != strings.TrimSpace(tenantID) {
			return ErrNotFound
		}
		for _, step := range state.ReleaseSteps {
			if strings.TrimSpace(step.ReleaseAttemptID) == releaseAttemptID {
				out = append(out, cloneReleaseStep(step))
			}
		}
		sortReleaseStepsOldestFirst(out)
		return nil
	})
	return out, err
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
	in.Source = firstNonEmptyStoreString(in.Source, model.OperationEvidenceSourceController)
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
	in.TenantID = strings.TrimSpace(in.TenantID)
	in.OperationID = strings.TrimSpace(in.OperationID)
	in.AppID = strings.TrimSpace(in.AppID)
	in.ReleaseAttemptID = strings.TrimSpace(in.ReleaseAttemptID)
	if in.Limit <= 0 {
		in.Limit = defaultOperationEvidenceLimit
	}
	if in.Limit > maxOperationEvidenceLimit {
		in.Limit = maxOperationEvidenceLimit
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

func operationEvidenceMatchesFilter(e model.OperationEvidence, filter model.OperationEvidenceFilter) bool {
	if !filter.PlatformAdmin && strings.TrimSpace(filter.TenantID) != "" && strings.TrimSpace(e.TenantID) != strings.TrimSpace(filter.TenantID) {
		return false
	}
	if filter.PlatformAdmin && strings.TrimSpace(filter.TenantID) != "" && strings.TrimSpace(e.TenantID) != strings.TrimSpace(filter.TenantID) {
		return false
	}
	if strings.TrimSpace(filter.OperationID) != "" && strings.TrimSpace(e.OperationID) != strings.TrimSpace(filter.OperationID) {
		return false
	}
	if strings.TrimSpace(filter.AppID) != "" && strings.TrimSpace(e.AppID) != strings.TrimSpace(filter.AppID) {
		return false
	}
	if strings.TrimSpace(filter.ReleaseAttemptID) != "" && strings.TrimSpace(e.ReleaseAttemptID) != strings.TrimSpace(filter.ReleaseAttemptID) {
		return false
	}
	if !stringInNormalizedSet(e.Type, filter.Types) {
		return false
	}
	if !stringInNormalizedSet(e.Severity, filter.Severities) {
		return false
	}
	if filter.Since != nil && e.CollectedAt.Before(filter.Since.UTC()) {
		return false
	}
	return true
}

func sortOperationEvidenceOldestFirst(items []model.OperationEvidence) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].CollectedAt.Equal(items[j].CollectedAt) {
			return items[i].ID < items[j].ID
		}
		return items[i].CollectedAt.Before(items[j].CollectedAt)
	})
}

func limitOperationEvidenceToNewest(items []model.OperationEvidence, limit int) []model.OperationEvidence {
	if limit <= 0 || len(items) <= limit {
		return items
	}
	return append([]model.OperationEvidence(nil), items[len(items)-limit:]...)
}

func cloneOperationEvidence(in model.OperationEvidence) model.OperationEvidence {
	in.Payload = clonePayloadMap(in.Payload)
	return in
}

func retainOperationEvidence(items []model.OperationEvidence, inserted model.OperationEvidence) []model.OperationEvidence {
	now := inserted.CollectedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	out := retainOperationEvidenceScope(items, func(item model.OperationEvidence) bool {
		return strings.TrimSpace(item.OperationID) == strings.TrimSpace(inserted.OperationID)
	}, now, operationEvidenceRetentionLimitPerOperation)
	if strings.TrimSpace(inserted.AppID) != "" {
		out = retainOperationEvidenceScope(out, func(item model.OperationEvidence) bool {
			return strings.TrimSpace(item.AppID) == strings.TrimSpace(inserted.AppID)
		}, now, operationEvidenceRetentionLimitPerApp)
		return out
	}
	if strings.TrimSpace(inserted.TenantID) != "" {
		out = retainOperationEvidenceScope(out, func(item model.OperationEvidence) bool {
			return strings.TrimSpace(item.TenantID) == strings.TrimSpace(inserted.TenantID) && strings.TrimSpace(item.AppID) == ""
		}, now, operationEvidenceRetentionLimitPerTenant)
	}
	return out
}

func retainOperationEvidenceScope(items []model.OperationEvidence, inScope func(model.OperationEvidence) bool, now time.Time, limit int) []model.OperationEvidence {
	if limit <= 0 || len(items) == 0 {
		return items
	}
	cutoff := now.UTC().Add(-operationEvidenceRetentionWindow)
	out := items[:0]
	scoped := []model.OperationEvidence{}
	for _, item := range items {
		if !inScope(item) {
			out = append(out, item)
			continue
		}
		if !item.CollectedAt.IsZero() && item.CollectedAt.Before(cutoff) {
			continue
		}
		scoped = append(scoped, item)
	}
	sort.SliceStable(scoped, func(i, j int) bool {
		if scoped[i].CollectedAt.Equal(scoped[j].CollectedAt) {
			return scoped[i].ID > scoped[j].ID
		}
		return scoped[i].CollectedAt.After(scoped[j].CollectedAt)
	})
	if len(scoped) > limit {
		scoped = scoped[:limit]
	}
	sortOperationEvidenceOldestFirst(scoped)
	out = append(out, scoped...)
	return out
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
	in.TriggerActorType = firstNonEmptyStoreString(in.TriggerActorType, model.ReleaseAttemptActorSystem)
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

func releaseAttemptMatchesFilter(attempt model.ReleaseAttempt, filter model.ReleaseAttemptFilter) bool {
	if strings.TrimSpace(filter.TenantID) != "" && strings.TrimSpace(attempt.TenantID) != strings.TrimSpace(filter.TenantID) {
		return false
	}
	if strings.TrimSpace(filter.AppID) != "" && strings.TrimSpace(attempt.AppID) != strings.TrimSpace(filter.AppID) {
		return false
	}
	if strings.TrimSpace(filter.Status) != "" && !strings.EqualFold(strings.TrimSpace(attempt.Status), strings.TrimSpace(filter.Status)) {
		return false
	}
	return true
}

func sortReleaseAttemptsNewestFirst(items []model.ReleaseAttempt) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].StartedAt.Equal(items[j].StartedAt) {
			return items[i].ID > items[j].ID
		}
		return items[i].StartedAt.After(items[j].StartedAt)
	})
}

func cloneReleaseAttempt(in model.ReleaseAttempt) model.ReleaseAttempt {
	in.DesiredSource = clonePayloadMap(in.DesiredSource)
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

func sortReleaseStepsOldestFirst(items []model.ReleaseStep) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].StartedAt.Equal(items[j].StartedAt) {
			return items[i].ID < items[j].ID
		}
		return items[i].StartedAt.Before(items[j].StartedAt)
	})
}

func cloneReleaseStep(in model.ReleaseStep) model.ReleaseStep {
	in.Payload = clonePayloadMap(in.Payload)
	return in
}

func findReleaseAttempt(state *model.State, id string) int {
	for idx, attempt := range state.ReleaseAttempts {
		if strings.TrimSpace(attempt.ID) == strings.TrimSpace(id) {
			return idx
		}
	}
	return -1
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
