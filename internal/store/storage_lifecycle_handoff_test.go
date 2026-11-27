package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"fugue/internal/storageaudit"

	"github.com/DATA-DOG/go-sqlmock"
)

func writeLifecycleHandoffTestPolicy(t *testing.T, p StorageLifecyclePolicy) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "storage-lifecycle.json")
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func expectLifecycleAcquire(mock sqlmock.Sqlmock, held bool) {
	mock.ExpectQuery(regexp.QuoteMeta(storageLifecycleReaderAcquire)).WithArgs(storageaudit.OwnershipLock).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(held))
}

func expectLifecycleRelease(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(regexp.QuoteMeta(storageLifecycleReaderRelease)).WithArgs(storageaudit.OwnershipLock).
		WillReturnRows(sqlmock.NewRows([]string{"unlocked"}).AddRow(true))
}

func TestStorageLifecycleHandoffHoldsFenceAndDoesNotRateLimitBusyOrUnavailable(t *testing.T) {
	s, mock := lifecycleMock(t)
	s.EnableStorageLifecycleHandoff()
	p := testLifecyclePolicy()
	path := writeLifecycleHandoffTestPolicy(t, p)
	now := time.Now().UTC()

	expectLifecycleAcquire(mock, false)
	s.storageLifecycleTick(t.Context(), path, now)
	if !s.StorageLifecycleObservation().LastAttemptAt.IsZero() {
		t.Fatal("a busy fence incorrectly consumed one sampling interval")
	}
	expectLifecycleAcquire(mock, true)
	mock.ExpectQuery(regexp.QuoteMeta(storageLifecycleReaderOwner)).WillReturnError(errors.New("private driver error"))
	expectLifecycleRelease(mock)
	s.storageLifecycleTick(t.Context(), path, now)
	if o := s.StorageLifecycleObservation(); !o.LastAttemptAt.IsZero() || o.Sample != nil || o.FailureCategory != "owner_status_unavailable" {
		t.Fatal("an unreadable owner did not close the audit gate", o)
	}
	expectLifecycleAcquire(mock, true)
	mock.ExpectQuery(regexp.QuoteMeta(storageLifecycleReaderOwner)).WillReturnRows(sqlmock.NewRows([]string{"owner"}).AddRow("api"))
	expectLifecyclePage(mock, "", p, sourceAuditRows().AddRow("synthetic", now, 1, false, "", "", ""))
	expectLifecycleRelease(mock)
	s.storageLifecycleTick(t.Context(), path, now)
	if o := s.StorageLifecycleObservation(); o.Sample == nil || !o.LastAttemptAt.Equal(now) || o.LastRunFailed {
		t.Fatal("the legacy API could not sample with the owner fence held", o)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageLifecycleHandoffNeverReopensLegacyScanAndKeepsDiagnosticCache(t *testing.T) {
	s, mock := lifecycleMock(t)
	s.EnableStorageLifecycleHandoff()
	p := testLifecyclePolicy()
	path := writeLifecycleHandoffTestPolicy(t, p)
	now := time.Now().UTC()
	s.lifecycle.cursor.Source = "legacy-cursor"
	expectLifecycleAcquire(mock, true)
	mock.ExpectQuery(regexp.QuoteMeta(storageLifecycleReaderOwner)).WillReturnRows(sqlmock.NewRows([]string{"owner"}).AddRow("worker"))
	expectLifecycleRelease(mock)
	s.storageLifecycleTick(t.Context(), path, now)
	if !s.lifecycle.workerOwned || s.lifecycle.cursor != (storageLifecycleCursor{}) {
		t.Fatal("the legacy collector ignored a worker ownership witness")
	}
	const statusQuery = `SELECT owner, mode, policy_sha256, last_attempt_at,`
	columns := []string{"owner", "mode", "policy_sha256", "last_attempt_at", "duration_seconds", "last_run_failed", "failure_count", "completed_passes", "sample", "policy_document"}
	sample := StorageLifecycleSample{ObservedAt: now, PolicySHA256: p.Digest(), DatabaseBytes: 8192}
	for _, name := range []string{"fugue_source_uploads", "fugue_platform_artifacts", "fugue_platform_artifact_contents", "fugue_audit_events", "fugue_platform_input_shared_packs", "fugue_platform_input_shared_objects", "fugue_platform_input_component_refs", "fugue_platform_input_component_recipes", "fugue_platform_input_separated_refs", "fugue_platform_input_components", "fugue_platform_quality_evidence"} {
		sample.Relations = append(sample.Relations, StorageRelationAllocation{Relation: name})
	}
	raw, _ := json.Marshal(sample)
	policy, _ := json.Marshal(p)
	mock.ExpectQuery(regexp.QuoteMeta(statusQuery)).WillReturnRows(sqlmock.NewRows(columns).
		AddRow("worker", p.Mode, p.Digest(), now, 0.04, false, 0, 5, raw, policy))
	s.refreshStorageLifecycleStatus(t.Context())
	o := s.StorageLifecycleObservation()
	if o.Sample == nil || o.Sample.DatabaseBytes != sample.DatabaseBytes || !o.Sample.ObservedAt.Equal(now) || o.Policy != p || o.LastRunFailed {
		t.Fatal("a worker-owned sample no longer has the legacy diagnostic shape", o)
	}

	mock.ExpectQuery(regexp.QuoteMeta(statusQuery)).WillReturnError(errors.New("details must not escape"))
	s.refreshStorageLifecycleStatus(t.Context())
	s.storageLifecycleTick(t.Context(), path, now.Add(time.Hour)) // no inventory query after handoff
	o = s.StorageLifecycleObservation()
	if o.Sample == nil || o.Sample.DatabaseBytes != sample.DatabaseBytes || !o.LastRunFailed || o.FailureCategory != "owner_status_unavailable" {
		t.Fatal("a transient status failure discarded the last durable diagnostic or enabled legacy", o)
	}
	mock.ExpectQuery(regexp.QuoteMeta(statusQuery)).WillReturnRows(sqlmock.NewRows(columns).
		AddRow("api", "disabled", "", nil, 0, false, 0, 0, nil, nil))
	s.refreshStorageLifecycleStatus(t.Context())
	if got := s.StorageLifecycleObservation(); got.Sample == nil || !got.LastRunFailed {
		t.Fatal("an unexpected owner regression restored the API collector", got)
	}
	var out bytes.Buffer
	s.WriteStorageLifecycleMetrics(&out)
	if strings.Contains(out.String(), "fugue_storage_lifecycle_") || strings.Contains(out.String(), "fugue_storage_database_") ||
		!strings.Contains(out.String(), "fugue_observation_retention_enabled") {
		t.Fatal("worker metrics overlap the API, or the unrelated retention series was lost", out.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
