package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/store"
)

type evidenceRetentionFixture struct {
	progress store.QualityEvidenceRetentionProgress
	err      error
}

func (f evidenceRetentionFixture) RunQualityEvidenceRetentionPass(context.Context) (store.QualityEvidenceRetentionPass, error) {
	return store.QualityEvidenceRetentionPass{}, errors.New("metrics must not prune")
}

func (f evidenceRetentionFixture) ReadQualityEvidenceRetentionProgress(context.Context) (store.QualityEvidenceRetentionProgress, error) {
	return f.progress, f.err
}

func TestEvidenceRetentionMetricsDoNotManufactureProgress(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture evidenceRetentionFixture
		in      []string
		out     []string
	}{
		{"unavailable", evidenceRetentionFixture{err: errors.New("DB failure")},
			[]string{"fugue_quality_evidence_retention_storage_available 0"}, []string{"DB failure", "retention_completed_timestamp"}},
		{"standby", evidenceRetentionFixture{progress: store.QualityEvidenceRetentionProgress{Owner: "api"}},
			[]string{"retention_storage_available 1", `retention_writer_owner{owner="api"} 1`, "retention_completed_passes 0"},
			[]string{"retention_completed_timestamp"}},
		{"reopened", evidenceRetentionFixture{progress: store.QualityEvidenceRetentionProgress{
			Owner: "worker", CompletedAt: time.Unix(1700000000, 0), CompletedPasses: 2, TotalDeleted: 257,
		}}, []string{`retention_writer_owner{owner="worker"} 1`, "retention_completed_passes 2", "retention_deleted_objects 257",
			"retention_completed_timestamp_seconds"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			evidenceRetentionMetrics(tc.fixture).ServeHTTP(recorder, httptest.NewRequest("GET", "/maintenance/metrics", nil))
			body := recorder.Body.String()
			for _, pattern := range tc.in {
				if !strings.Contains(body, pattern) {
					t.Fatalf("missing %q in %q", pattern, body)
				}
			}
			for _, pattern := range tc.out {
				if strings.Contains(body, pattern) {
					t.Fatalf("unexpected %q in %q", pattern, body)
				}
			}
		})
	}
}

func TestEvidenceRetentionRetryWaitsOutsideTheWriteTransaction(t *testing.T) {
	for _, tc := range []struct {
		state string
		count int64
		err   error
		want  time.Duration
	}{
		{"busy", 0, nil, 10 * time.Second},
		{"completed", store.QualityEvidenceRetentionBatchRows, nil, 10 * time.Second},
		{"completed", 255, nil, time.Minute},
		{"standby", 0, nil, time.Minute},
		{"", 0, errors.New("DB failure"), time.Minute},
	} {
		got := evidenceRetentionRetryDelay(store.QualityEvidenceRetentionPass{State: tc.state, Deleted: tc.count}, tc.err)
		if got != tc.want {
			t.Fatalf("state=%q count=%d err=%v: %v != %v", tc.state, tc.count, tc.err, got, tc.want)
		}
	}
}
