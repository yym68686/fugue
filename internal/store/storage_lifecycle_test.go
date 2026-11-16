package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func testLifecyclePolicy() StorageLifecyclePolicy {
	return StorageLifecyclePolicy{Version: 1, Mode: "audit", EffectiveFrom: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		IntervalSeconds: 300, SourcePageRows: 2, HistoryDays: 7, MinimumVerifiedVersions: 3,
		UnusedUploadHours: 24, UnreferencedUploadDays: 30, WarningFreePercent: 20, PressureFreePercent: 15, CriticalFreePercent: 10}
}

func TestStorageLifecyclePolicyCannotAuthorizeDeletionOrWeakenProtection(t *testing.T) {
	p := testLifecyclePolicy()
	raw, _ := json.Marshal(p)
	if _, err := ParseStorageLifecyclePolicy(raw); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*StorageLifecyclePolicy){
		func(p *StorageLifecyclePolicy) { p.Mode = "enforce" },
		func(p *StorageLifecyclePolicy) { p.Mode = "" },
		func(p *StorageLifecyclePolicy) { p.Version = 2 },
		func(p *StorageLifecyclePolicy) { p.EffectiveFrom = time.Time{} },
		func(p *StorageLifecyclePolicy) { p.IntervalSeconds = 299 },
		func(p *StorageLifecyclePolicy) { p.SourcePageRows = 129 },
		func(p *StorageLifecyclePolicy) { p.HistoryDays = 6 },
		func(p *StorageLifecyclePolicy) { p.MinimumVerifiedVersions = 2 },
		func(p *StorageLifecyclePolicy) { p.UnusedUploadHours = 23 },
		func(p *StorageLifecyclePolicy) { p.UnreferencedUploadDays = 29 },
		func(p *StorageLifecyclePolicy) { p.CriticalFreePercent = 15 },
		func(p *StorageLifecyclePolicy) { p.WarningFreePercent = 51 },
	} {
		changed := p
		change(&changed)
		raw, _ := json.Marshal(changed)
		if _, err := ParseStorageLifecyclePolicy(raw); err == nil {
			t.Fatalf("unsafe policy accepted: %s", raw)
		}
	}
	raw, _ = json.Marshal(p)
	for _, extra := range []string{`,"tables":["fugue_apps"]}`, `,"delete_unknown":true}`, `} {}`} {
		if _, err := ParseStorageLifecyclePolicy(append(append([]byte{}, raw[:len(raw)-1]...), []byte(extra)...)); err == nil {
			t.Fatal("unknown fields/trailing document accepted")
		}
	}
	var production struct {
		Lifecycle StorageLifecyclePolicy `json:"storageLifecycle"`
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "environments", "production", "storage-policy", "policy.json"))
	if err != nil || json.Unmarshal(raw, &production) != nil {
		t.Fatal("production policy unavailable", err)
	}
	raw, _ = json.Marshal(production.Lifecycle)
	if _, err = ParseStorageLifecyclePolicy(raw); err != nil || production.Lifecycle.Mode != "audit" {
		t.Fatal("production must start audit-only", err)
	}
}

func TestStorageLifecycleRejectsNegativeSourceSizeWithoutPublishingSample(t *testing.T) {
	s, mock := lifecycleMock(t)
	p := testLifecyclePolicy()
	expectLifecyclePrefix(mock)
	mock.ExpectQuery(regexp.QuoteMeta(storageLifecycleSourceSQL)).WithArgs("", p.SourcePageRows+1).
		WillReturnRows(sourceAuditRows().AddRow("a", time.Now(), -1, false)).RowsWillBeClosed()
	mock.ExpectRollback()
	if _, _, err := s.collectStorageLifecycleSample(t.Context(), p, "policy", "", time.Now()); err == nil {
		t.Fatal("negative declared size accepted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func lifecycleMock(t *testing.T) (*Store, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return &Store{db: db}, mock
}

func expectLifecyclePrefix(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	for _, q := range []string{`SET LOCAL statement_timeout='2s'`, `SET LOCAL lock_timeout='100ms'`, `SET LOCAL temp_file_limit='0'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		mock.ExpectExec(regexp.QuoteMeta(q)).WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT pg_database_size(current_database())`)).WillReturnRows(sqlmock.NewRows([]string{"size"}).AddRow(4096))
	mock.ExpectQuery(regexp.QuoteMeta(storageLifecycleRelationSQL)).WillReturnRows(sqlmock.NewRows([]string{"name", "present", "heap", "table", "index", "total"}).
		AddRow("fugue_source_uploads", true, 8192, 16384, 8192, 24576).
		AddRow("fugue_audit_events", false, 0, 0, 0, 0))
}

func expectLifecyclePage(mock sqlmock.Sqlmock, cursor string, p StorageLifecyclePolicy, rows *sqlmock.Rows) {
	expectLifecyclePrefix(mock)
	mock.ExpectQuery(regexp.QuoteMeta(storageLifecycleSourceSQL)).WithArgs(cursor, p.SourcePageRows+1).WillReturnRows(rows).RowsWillBeClosed()
	mock.ExpectCommit()
}

func sourceAuditRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "created", "size", "referenced"})
}

func TestStorageLifecycleBoundedPageNeverTreatsUnknownOrAgeAsDeletable(t *testing.T) {
	s, mock := lifecycleMock(t)
	p := testLifecyclePolicy()
	now := p.EffectiveFrom.Add(100 * 24 * time.Hour)
	expectLifecyclePage(mock, "", p, sourceAuditRows().
		AddRow("private-upload-a", p.EffectiveFrom.Add(-time.Hour), 100, false).
		AddRow("private-upload-b", now.Add(-time.Hour), 200, true).
		AddRow("sentinel", now, 5000, false))
	sample, next, err := s.collectStorageLifecycleSample(t.Context(), p, "policy-one", "", now)
	if err != nil {
		t.Fatal(err)
	}
	page := sample.SourcePage
	if next != "private-upload-b" || page.Rows != 2 || page.DeclaredBytes != 300 || page.ReferencedRows != 1 || page.UnknownRows != 1 || page.LegacyRows != 1 || page.OlderThan30DaysBytes != 100 || page.PageAtEnd || page.ReferencesComplete || page.RetirementAuthorized || page.EligibleRows != 0 {
		t.Fatalf("unsafe or unbounded page: %+v next=%s", page, next)
	}
	s.lifecycle.observation.Sample = &sample
	observation := s.StorageLifecycleObservation()
	observation.Sample.Relations[0].TotalBytes = 999
	if s.StorageLifecycleObservation().Sample.Relations[0].TotalBytes == 999 {
		t.Fatal("observation aliases mutable state")
	}
	raw, _ := json.Marshal(observation)
	if strings.Contains(string(raw), "private-upload") {
		t.Fatal("upload identity leaked")
	}
	var out bytes.Buffer
	s.WriteStorageLifecycleMetrics(&out)
	metrics := out.String()
	if !strings.Contains(metrics, `fugue_storage_source_page_rows{class="unknown"} 1.000000`) || strings.Contains(metrics, "private-upload") || strings.Contains(metrics, `allocated_bytes{kind="total",relation="fugue_audit_events"}`) {
		t.Fatal("invalid metrics", metrics)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageLifecycleEndOfSweepResetsWithoutClaimingGlobalCompleteness(t *testing.T) {
	s, mock := lifecycleMock(t)
	p := testLifecyclePolicy()
	expectLifecyclePage(mock, "last-id", p, sourceAuditRows())
	sample, next, err := s.collectStorageLifecycleSample(t.Context(), p, "policy", "last-id", time.Now())
	if err != nil || next != "" || !sample.SourcePage.PageAtEnd || sample.SourcePage.ReferencesComplete {
		t.Fatal(sample, next, err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageLifecycleFailedReadKeepsLastSampleAndCursor(t *testing.T) {
	s, mock := lifecycleMock(t)
	p := testLifecyclePolicy()
	path := filepath.Join(t.TempDir(), "storage-lifecycle.json")
	raw, _ := json.Marshal(p)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	expectLifecyclePage(mock, "", p, sourceAuditRows().AddRow("a", now, 1, false).AddRow("b", now, 2, false).AddRow("c", now, 3, false))
	s.storageLifecycleTick(t.Context(), path, now)
	expectLifecyclePrefix(mock)
	mock.ExpectQuery(regexp.QuoteMeta(storageLifecycleSourceSQL)).WithArgs("b", p.SourcePageRows+1).WillReturnError(errors.New("secret driver detail"))
	mock.ExpectRollback()
	s.storageLifecycleTick(t.Context(), path, now.Add(300*time.Second))
	o := s.StorageLifecycleObservation()
	if !o.LastRunFailed || o.Failures != 1 || o.FailureCategory != "bounded_read_failed" || s.lifecycle.cursor != "b" || !o.Sample.ObservedAt.Equal(now) {
		t.Fatalf("failure advanced or replaced last successful page: %+v", o)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageLifecycleInvalidMissingDisabledAndFuturePolicyDoNoDatabaseWork(t *testing.T) {
	s, mock := lifecycleMock(t)
	path := filepath.Join(t.TempDir(), "storage-lifecycle.json")
	p := testLifecyclePolicy()
	now := time.Now().UTC()
	for _, mode := range []string{"disabled", "future", "enforce", "missing"} {
		changed := p
		changed.Mode = mode
		if mode == "future" {
			changed.Mode, changed.EffectiveFrom = "audit", now.Add(time.Hour)
		}
		raw, _ := json.Marshal(changed)
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if mode == "missing" {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}
		s.storageLifecycleTick(t.Context(), path, now)
		o := s.StorageLifecycleObservation()
		if !o.LastAttemptAt.IsZero() || o.ExecutionSupported || (mode == "enforce" && (!o.PolicyReadFailed || o.Policy.Mode != "disabled")) {
			t.Fatal("unsupported configuration ran", mode, o)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s.RunStorageLifecycleAudit(ctx, "")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
