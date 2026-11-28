package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"fugue/internal/storageaudit"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestStorageLifecycleStatusReadsOnlyWorkerProjection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db}
	policy := storageaudit.Policy{Version: 1, Mode: "audit", EffectiveFrom: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		IntervalSeconds: 300, SourcePageRows: 2, HistoryDays: 7, MinimumVerifiedVersions: 3,
		UnusedUploadHours: 24, UnreferencedUploadDays: 30, WarningFreePercent: 20, PressureFreePercent: 15, CriticalFreePercent: 10}
	now := time.Now().UTC()
	sample := storageaudit.Sample{ObservedAt: now, PolicySHA256: policy.Digest(), DatabaseBytes: 8192}
	for _, name := range []string{"fugue_source_uploads", "fugue_platform_artifacts", "fugue_platform_artifact_contents",
		"fugue_audit_events", "fugue_platform_input_shared_packs", "fugue_platform_input_shared_objects",
		"fugue_platform_input_component_refs", "fugue_platform_input_component_recipes", "fugue_platform_input_separated_refs",
		"fugue_platform_input_components", "fugue_platform_quality_evidence"} {
		sample.Relations = append(sample.Relations, storageaudit.RelationAllocation{Relation: name})
	}
	sampleJSON, _ := json.Marshal(sample)
	policyJSON, _ := json.Marshal(policy)
	query := regexp.QuoteMeta("FROM fugue_storage_audit.lifecycle_status_v1")
	columns := []string{"owner", "mode", "policy_sha256", "last_attempt_at", "duration_seconds", "last_run_failed", "failure_count", "completed_passes", "sample", "policy_document"}
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(columns).
		AddRow("worker", policy.Mode, policy.Digest(), now, 0.04, false, 0, 5, sampleJSON, policyJSON))
	s.refreshStorageLifecycleStatus(t.Context())
	if got := s.StorageLifecycleObservation(); got.Sample == nil || got.Sample.DatabaseBytes != 8192 ||
		!got.Sample.ObservedAt.Equal(now) || got.Policy != policy || got.LastRunFailed {
		t.Fatalf("invalid diagnostic projection: %+v", got)
	}
	copy := s.StorageLifecycleObservation()
	copy.Sample.Relations[0].TotalBytes = 999
	if s.StorageLifecycleObservation().Sample.Relations[0].TotalBytes != 0 {
		t.Fatal("diagnostic caller changed the sample cache")
	}
	mock.ExpectQuery(query).WillReturnError(errors.New("transient database failure"))
	s.refreshStorageLifecycleStatus(t.Context())
	if got := s.StorageLifecycleObservation(); got.Sample == nil || got.Sample.DatabaseBytes != 8192 ||
		!got.LastRunFailed || got.FailureCategory != "owner_status_unavailable" {
		t.Fatalf("transient error lost the last worker sample: %+v", got)
	}
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(columns).
		AddRow("api", "disabled", "", nil, 0, false, 0, 0, nil, nil))
	s.refreshStorageLifecycleStatus(t.Context())
	if got := s.StorageLifecycleObservation(); got.Sample == nil || !got.LastRunFailed {
		t.Fatalf("unexpected owner replaced the worker sample: %+v", got)
	}
	var metrics bytes.Buffer
	s.WriteObservationRetentionMetrics(&metrics)
	if strings.Contains(metrics.String(), "fugue_storage_lifecycle_") ||
		!strings.Contains(metrics.String(), "fugue_observation_retention_enabled") {
		t.Fatalf("API emitted audit metrics or lost retention metrics: %s", metrics.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
