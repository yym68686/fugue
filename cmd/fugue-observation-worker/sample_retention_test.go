package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/store"
)

const sampleRetentionFixturePolicy = `{"version":1,"enabled":true,"interval_seconds":30,"batch_rows":500,
"heartbeat_days":7,"heartbeat_rows_per_chain":10000,"probe_days":7,"probe_rows_per_table":1000000}`

type sampleRetentionFixture struct {
	progress []store.ObservationSampleRetentionProgress
	err      error
}

func (f sampleRetentionFixture) RunObservationSampleRetentionPass(context.Context, string, store.ObservationRetentionPolicy) (store.ObservationSampleRetentionPass, error) {
	panic("a health or metrics read must not run deletion")
}

func (f sampleRetentionFixture) ReadObservationSampleRetentionProgress(context.Context) ([]store.ObservationSampleRetentionProgress, error) {
	return f.progress, f.err
}

func TestSampleRetentionMetricsAndReadinessAreReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observation-retention.json")
	if err := os.WriteFile(path, []byte(sampleRetentionFixturePolicy), 0600); err != nil {
		t.Fatal(err)
	}
	storage := sampleRetentionFixture{progress: []store.ObservationSampleRetentionProgress{
		{Target: "network", Owner: "worker", CompletedAt: time.Unix(1700000000, 0), CompletedPasses: 2, TotalDeleted: 501},
		{Target: "performance", Owner: "worker", CompletedPasses: 0},
	}}
	if !sampleRetentionReady(t.Context(), storage, path) || sampleRetentionReady(t.Context(), storage, path+".missing") {
		t.Fatal("readiness did not validate the policy projection")
	}
	recorder := httptest.NewRecorder()
	sampleRetentionMetrics(storage, path).ServeHTTP(recorder, httptest.NewRequest("GET", "/sample-retention/metrics", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		"fugue_observation_sample_retention_policy_valid 1",
		"fugue_observation_sample_retention_storage_available 1",
		`fugue_observation_sample_retention_writer_owner{owner="worker",target="network"} 1`,
		`fugue_observation_sample_retention_deleted_rows{target="network"} 501`,
		`fugue_observation_sample_retention_completed_timestamp_seconds{target="network"}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing metric %q from %q", want, body)
		}
	}
	if strings.Contains(body, `fugue_observation_sample_retention_completed_timestamp_seconds{target="performance"}`) {
		t.Fatal("metrics claimed uncommitted progress")
	}
	storage.err = errors.New("private database details")
	recorder = httptest.NewRecorder()
	sampleRetentionMetrics(storage, path).ServeHTTP(recorder, httptest.NewRequest("GET", "/sample-retention/metrics", nil))
	if body := recorder.Body.String(); strings.Contains(body, "private database details") || !strings.Contains(body, "sample_retention_storage_available 0") {
		t.Fatal("metrics leaked or masked a database failure")
	}
}

func TestSampleRetentionRetryDrainsWithoutBusyLooping(t *testing.T) {
	policy, err := store.ParseObservationRetentionPolicy([]byte(sampleRetentionFixturePolicy))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		state string
		count int64
		err   error
		want  time.Duration
	}{
		{"completed", 500, nil, 5 * time.Second},
		{"completed", 499, nil, 30 * time.Second},
		{"busy", 0, nil, 5 * time.Second},
		{"standby", 0, nil, 30 * time.Second},
		{"busy", 0, errors.New("database failed"), 30 * time.Second},
	} {
		got := sampleRetentionRetryDelay(policy, store.ObservationSampleRetentionPass{State: tc.state, Deleted: tc.count}, tc.err)
		if got != tc.want {
			t.Fatalf("state=%q rows=%d err=%v: %v, want %v", tc.state, tc.count, tc.err, got, tc.want)
		}
	}
}

func TestSampleRetentionStandbyChecksOwnershipDuringLongIntervals(t *testing.T) {
	policy := store.ObservationRetentionPolicy{IntervalSeconds: 3600}
	next := sampleRetentionRetryDelay(policy, store.ObservationSampleRetentionPass{State: "standby"}, nil)
	if next != 30*time.Second {
		t.Fatalf("standby would wait %v to observe the writer handoff", next)
	}
}
