package store

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func setupOperationEvidenceStore(t *testing.T) (*Store, model.Tenant, model.Project, model.App, model.Operation) {
	t.Helper()
	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Evidence Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "ops", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "api", "", model.AppSpec{Image: "ghcr.io/example/api", Replicas: 1})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	spec := app.Spec
	op, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDeploy, AppID: app.ID, DesiredSpec: &spec})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	return s, tenant, project, app, op
}

func TestOperationEvidenceStoreRecordsListsTimelinePayloadLimitAndMetrics(t *testing.T) {
	t.Parallel()

	s, tenant, project, app, op := setupOperationEvidenceStore(t)
	observed := time.Now().UTC().Add(-2 * time.Minute)
	first, err := s.RecordOperationEvidence(model.OperationEvidence{
		TenantID:        tenant.ID,
		ProjectID:       project.ID,
		AppID:           app.ID,
		OperationID:     op.ID,
		Type:            model.OperationEvidenceTypeRolloutPreviousLogs,
		Source:          model.OperationEvidenceSourceAppLogs,
		Severity:        model.OperationEvidenceSeverityError,
		Confidence:      model.OperationEvidenceConfidenceConfirmed,
		ObservedAt:      observed,
		Summary:         "captured previous logs",
		Message:         "startup failed",
		RedactionStatus: model.OperationEvidenceRedactionRedacted,
		Payload: map[string]any{
			"log_tail": strings.Repeat("x", maxOperationEvidencePayloadBytes+4096),
		},
	})
	if err != nil {
		t.Fatalf("record evidence: %v", err)
	}
	if first.Payload["payload_truncated"] != true {
		t.Fatalf("expected oversized payload to be marked truncated, got %#v", first.Payload)
	}
	_, err = s.RecordOperationEvidence(model.OperationEvidence{
		TenantID:        tenant.ID,
		ProjectID:       project.ID,
		AppID:           app.ID,
		OperationID:     op.ID,
		Type:            model.OperationEvidenceTypeRolloutDeploymentSnapshot,
		Source:          model.OperationEvidenceSourceKubernetesAPI,
		Severity:        model.OperationEvidenceSeverityInfo,
		Confidence:      model.OperationEvidenceConfidenceConfirmed,
		Summary:         "captured deployment snapshot",
		RedactionStatus: model.OperationEvidenceRedactionRedacted,
	})
	if err != nil {
		t.Fatalf("record snapshot evidence: %v", err)
	}
	third, err := s.RecordOperationEvidence(model.OperationEvidence{
		TenantID:        tenant.ID,
		ProjectID:       project.ID,
		AppID:           app.ID,
		OperationID:     op.ID,
		Type:            model.OperationEvidenceTypeRolloutContainerTerminated,
		Source:          model.OperationEvidenceSourceRolloutObserver,
		Severity:        model.OperationEvidenceSeverityError,
		Confidence:      model.OperationEvidenceConfidenceConfirmed,
		Summary:         "container exited",
		RedactionStatus: model.OperationEvidenceRedactionNone,
	})
	if err != nil {
		t.Fatalf("record container evidence: %v", err)
	}

	listed, err := s.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: tenant.ID, OperationID: op.ID, Limit: 1})
	if err != nil {
		t.Fatalf("list evidence: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != third.ID {
		t.Fatalf("expected limit to return newest evidence %s, got %+v", third.ID, listed)
	}
	logsOnly, err := s.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: tenant.ID, OperationID: op.ID, Types: []string{model.OperationEvidenceTypeRolloutPreviousLogs}})
	if err != nil {
		t.Fatalf("list logs evidence: %v", err)
	}
	if len(logsOnly) != 1 || logsOnly[0].ID != first.ID {
		t.Fatalf("expected logs evidence, got %+v", logsOnly)
	}

	timeline, err := s.ListOperationTimeline(tenant.ID, false, op.ID, false)
	if err != nil {
		t.Fatalf("list timeline: %v", err)
	}
	if len(timeline) < 3 || timeline[0].Type != model.OperationEvidenceTypeOperationCreated {
		t.Fatalf("expected operation-created timeline plus evidence, got %+v", timeline)
	}
	if timelineHasPayload(timeline) {
		t.Fatalf("expected payload omitted from timeline when includePayload=false: %+v", timeline)
	}
	timeline, err = s.ListOperationTimeline(tenant.ID, false, op.ID, true)
	if err != nil {
		t.Fatalf("list timeline with payload: %v", err)
	}
	if !timelineHasPayload(timeline) {
		t.Fatalf("expected payload in timeline when includePayload=true: %+v", timeline)
	}

	records, captures, rollouts, err := s.CountOperationEvidenceMetricGroups()
	if err != nil {
		t.Fatalf("count evidence metrics: %v", err)
	}
	if len(records) == 0 || len(captures) == 0 || len(rollouts) == 0 {
		t.Fatalf("expected evidence metric counts, records=%+v captures=%+v rollouts=%+v", records, captures, rollouts)
	}
}

func TestReleaseAttemptStoreRecordsTimelineAndMetrics(t *testing.T) {
	t.Parallel()

	s, tenant, project, app, op := setupOperationEvidenceStore(t)
	attempt, err := s.CreateReleaseAttempt(model.ReleaseAttempt{
		TenantID:          tenant.ID,
		ProjectID:         project.ID,
		AppID:             app.ID,
		TriggerType:       model.ReleaseAttemptTriggerImageTrackingManualSync,
		TriggerActorType:  model.ReleaseAttemptActorUser,
		TriggerActorID:    "tester",
		SourceOperationID: op.ID,
		RootOperationID:   op.ID,
		Status:            model.ReleaseAttemptStatusImporting,
		Confidence:        model.OperationEvidenceConfidenceEvidenceBacked,
	})
	if err != nil {
		t.Fatalf("create release attempt: %v", err)
	}
	step, err := s.RecordReleaseStep(model.ReleaseStep{
		TenantID:         tenant.ID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             model.ReleaseStepTypeImageImport,
		Status:           model.ReleaseStepStatusRunning,
		Summary:          "image import running",
	})
	if err != nil {
		t.Fatalf("record release step: %v", err)
	}
	found, ok, err := s.FindReleaseAttemptForOperation(op.ID)
	if err != nil || !ok {
		t.Fatalf("find release attempt found=%v err=%v", ok, err)
	}
	if found.ID != attempt.ID {
		t.Fatalf("expected attempt %s, got %+v", attempt.ID, found)
	}
	timeline, err := s.ListReleaseTimeline(tenant.ID, false, attempt.ID)
	if err != nil {
		t.Fatalf("list release timeline: %v", err)
	}
	if len(timeline) != 1 || timeline[0].ID != step.ID {
		t.Fatalf("expected release step in timeline, got %+v", timeline)
	}
	counts, err := s.CountReleaseAttemptMetricGroups()
	if err != nil {
		t.Fatalf("count release attempts: %v", err)
	}
	if len(counts) != 1 || counts[0].TriggerType != model.ReleaseAttemptTriggerImageTrackingManualSync || counts[0].Status != model.ReleaseAttemptStatusImporting || counts[0].Count != 1 {
		t.Fatalf("unexpected release attempt metrics: %+v", counts)
	}
}

func TestOperationEvidenceRetentionKeepsRecentBoundedHistory(t *testing.T) {
	t.Parallel()

	s, tenant, project, app, op := setupOperationEvidenceStore(t)
	now := time.Now().UTC()
	old, err := s.RecordOperationEvidence(model.OperationEvidence{
		TenantID:    tenant.ID,
		ProjectID:   project.ID,
		AppID:       app.ID,
		OperationID: op.ID,
		Type:        model.OperationEvidenceTypeRolloutProgress,
		Source:      model.OperationEvidenceSourceRolloutObserver,
		Severity:    model.OperationEvidenceSeverityInfo,
		Confidence:  model.OperationEvidenceConfidenceEvidenceBacked,
		CollectedAt: now.Add(-operationEvidenceRetentionWindow - time.Hour),
		Summary:     "old progress sample",
	})
	if err != nil {
		t.Fatalf("record old evidence: %v", err)
	}

	for i := 0; i < operationEvidenceRetentionLimitPerOperation+5; i++ {
		if _, err := s.RecordOperationEvidence(model.OperationEvidence{
			TenantID:    tenant.ID,
			ProjectID:   project.ID,
			AppID:       app.ID,
			OperationID: op.ID,
			Type:        model.OperationEvidenceTypeRolloutProgress,
			Source:      model.OperationEvidenceSourceRolloutObserver,
			Severity:    model.OperationEvidenceSeverityInfo,
			Confidence:  model.OperationEvidenceConfidenceEvidenceBacked,
			CollectedAt: now.Add(time.Duration(i) * time.Millisecond),
			Summary:     "recent progress sample",
		}); err != nil {
			t.Fatalf("record evidence %d: %v", i, err)
		}
	}

	listed, err := s.ListOperationEvidence(model.OperationEvidenceFilter{
		TenantID:    tenant.ID,
		OperationID: op.ID,
		Limit:       maxOperationEvidenceLimit,
	})
	if err != nil {
		t.Fatalf("list evidence: %v", err)
	}
	if len(listed) != operationEvidenceRetentionLimitPerOperation {
		t.Fatalf("expected %d retained evidence records, got %d", operationEvidenceRetentionLimitPerOperation, len(listed))
	}
	for _, item := range listed {
		if item.ID == old.ID {
			t.Fatalf("expected old evidence %s to be pruned", old.ID)
		}
	}
	if listed[0].CollectedAt.After(listed[len(listed)-1].CollectedAt) {
		t.Fatalf("expected oldest-first order after retention, got first=%s last=%s", listed[0].CollectedAt, listed[len(listed)-1].CollectedAt)
	}
}

func TestReleaseEvidenceResearchCountsEnvPatchTrackingSyncAndMigrationSignals(t *testing.T) {
	t.Parallel()

	s, tenant, project, app, op := setupOperationEvidenceStore(t)
	now := time.Now().UTC()
	envAttempt, err := s.CreateReleaseAttempt(model.ReleaseAttempt{
		TenantID:          tenant.ID,
		ProjectID:         project.ID,
		AppID:             app.ID,
		TriggerType:       model.ReleaseAttemptTriggerEnvPatch,
		TriggerActorType:  model.ReleaseAttemptActorUser,
		TriggerActorID:    "tester",
		SourceOperationID: op.ID,
		RootOperationID:   op.ID,
		Status:            model.ReleaseAttemptStatusDeploying,
		Confidence:        model.OperationEvidenceConfidenceEvidenceBacked,
		StartedAt:         now,
	})
	if err != nil {
		t.Fatalf("create env patch attempt: %v", err)
	}
	_, err = s.CreateReleaseAttempt(model.ReleaseAttempt{
		TenantID:          tenant.ID,
		ProjectID:         project.ID,
		AppID:             app.ID,
		TriggerType:       model.ReleaseAttemptTriggerImageTrackingManualSync,
		TriggerActorType:  model.ReleaseAttemptActorUser,
		TriggerActorID:    "tester",
		SourceOperationID: op.ID,
		RootOperationID:   op.ID,
		Status:            model.ReleaseAttemptStatusImporting,
		Confidence:        model.OperationEvidenceConfidenceEvidenceBacked,
		StartedAt:         envAttempt.StartedAt.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("create tracking sync attempt: %v", err)
	}
	if _, err := s.RecordOperationEvidence(model.OperationEvidence{
		TenantID:    tenant.ID,
		ProjectID:   project.ID,
		AppID:       app.ID,
		OperationID: op.ID,
		Type:        model.OperationEvidenceTypeRolloutPreviousLogs,
		Source:      model.OperationEvidenceSourceAppLogs,
		Severity:    model.OperationEvidenceSeverityError,
		Confidence:  model.OperationEvidenceConfidenceConfirmed,
		Summary:     "captured migration failure logs",
		Message:     "startup failed: apply schema: ERROR: deadlock detected (SQLSTATE 40P01)",
		Payload: map[string]any{
			"log_tail": "startup failed: apply schema: ERROR: deadlock detected (SQLSTATE 40P01)",
		},
	}); err != nil {
		t.Fatalf("record migration evidence: %v", err)
	}

	releaseResearch, migrationCounts, err := s.CountReleaseEvidenceResearchGroups()
	if err != nil {
		t.Fatalf("count release research: %v", err)
	}
	if releaseResearch.EnvPatchAttempts != 1 || releaseResearch.TrackingSyncAttempts != 1 || releaseResearch.EnvPatchThenTrackingSyncAttempts != 1 {
		t.Fatalf("unexpected release research summary: %+v", releaseResearch)
	}
	if !migrationCountHasSignal(migrationCounts, "schema_or_migration_log", model.OperationEvidenceConfidenceConfirmed) ||
		!migrationCountHasSignal(migrationCounts, "sqlstate_log", model.OperationEvidenceConfidenceConfirmed) ||
		!migrationCountHasSignal(migrationCounts, "deadlock_log", model.OperationEvidenceConfidenceConfirmed) {
		t.Fatalf("expected migration evidence signals, got %+v", migrationCounts)
	}
}

func migrationCountHasSignal(items []MigrationEvidenceMetricCount, signal, confidence string) bool {
	for _, item := range items {
		if item.Signal == signal && item.Confidence == confidence && item.Count > 0 {
			return true
		}
	}
	return false
}

func timelineHasPayload(entries []model.OperationTimelineEntry) bool {
	for _, entry := range entries {
		if len(entry.Payload) > 0 {
			return true
		}
	}
	return false
}
