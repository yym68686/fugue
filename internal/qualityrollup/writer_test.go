package qualityrollup

import (
	"context"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/qualityrollup/rolluptest"
	"fugue/internal/store"
)

func TestCompatibilityWriterHonorsPersistedOwner(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, owner  string
		runs, failed int64
	}{
		{name: "before handover", runs: 1},
		{name: "after handover", owner: string(store.EdgeQualityRollupWorkerWriter)},
		{name: "unknown owner", owner: "invalid", runs: 1, failed: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			const fixture = "writer-ownership"
			storage := rolluptest.Store(t, fixture)
			if tc.owner != "" {
				ok, err := storage.UpdateObservationCheckpoint(ctx, store.EdgeQualityRollupWriterCheckpoint, func([]byte) ([]byte, error) {
					return []byte(tc.owner), nil
				})
				if err != nil || !ok {
					t.Fatalf("write owner: acquired=%t err=%v", ok, err)
				}
			}
			restarted := New(rolluptest.Store(t, fixture), nil)
			restarted.runOnce(ctx, time.Now().UTC())
			if restarted.runCount != tc.runs || restarted.errorCount != tc.failed {
				t.Fatalf("runs=%d failures=%d, want %d/%d", restarted.runCount, restarted.errorCount, tc.runs, tc.failed)
			}
		})
	}
}

func TestPostgresWriterHandoffFencesOldWriterAndResumes(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 5, 12, 3, 0, 0, time.UTC)
	raw := model.EdgePerformanceSample{
		ID: "first", EdgeID: "edge-a", EdgeGroupID: "group-a", Hostname: "app.example.test",
		TTFBMS: 50, SampleCount: 6, SampledAt: now.Add(-4 * time.Minute),
	}
	storage := rolluptest.PostgresStore(t, []model.EdgePerformanceSample{raw})
	api, worker := New(storage, nil), NewWorker(storage, nil)
	if count, ran, err := worker.runOwnedPass(ctx, now); err != nil || ran || count != 0 {
		t.Fatalf("standby wrote before handoff: count=%d ran=%t err=%v", count, ran, err)
	}
	if count, ran, err := api.runOwnedPass(ctx, now); err != nil || !ran || count == 0 {
		t.Fatalf("compatibility API did not write: count=%d ran=%t err=%v", count, ran, err)
	}
	if locked, err := storage.WithAdvisoryLock(ctx, store.EdgeQualityRollupWriterLock, func() error {
		if ok, err := storage.TransferEdgeQualityRollupsToWorker(ctx); err != nil || ok {
			t.Fatalf("handoff over a legacy session owner: transferred=%t err=%v", ok, err)
		}
		return nil
	}); err != nil || !locked {
		t.Fatalf("legacy writer could not take the shared lock: acquired=%t err=%v", locked, err)
	}

	inFlight, cancel := context.WithCancel(ctx)
	defer cancel()
	old, err := storage.BeginEdgeQualityRollupBuild(inFlight)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Rollback()
	if ok, err := old.AcquireWriter(store.EdgeQualityRollupAPIWriter); err != nil || !ok {
		t.Fatalf("in-flight writer did not take fence: acquired=%t err=%v", ok, err)
	}
	if ok, err := storage.TransferEdgeQualityRollupsToWorker(ctx); err != nil || ok {
		t.Fatalf("handoff succeeded while write transaction is open: transferred=%t err=%v", ok, err)
	}
	cancel()
	if err := old.Commit(nil, nil, map[string]time.Time{"5m": now.Add(time.Hour)}); err == nil {
		t.Fatal("canceled writer incorrectly committed progress")
	}
	old.Rollback()
	for i := 0; i < 2; i++ {
		if ok, err := storage.TransferEdgeQualityRollupsToWorker(ctx); err != nil || !ok {
			t.Fatalf("idempotent handoff failed: transferred=%t err=%v", ok, err)
		}
	}

	if count, ran, err := NewWorker(storage, nil).runOwnedPass(ctx, now); err != nil || !ran || count != 0 {
		t.Fatalf("restart did not reuse committed windows: count=%d ran=%t err=%v", count, ran, err)
	}
	raw.ID, raw.SampledAt = "second", now.Add(time.Minute)
	if err := storage.RecordEdgePerformanceSamples([]model.EdgePerformanceSample{raw}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	next := now.Add(5 * time.Minute)
	if count, ran, err := New(storage, nil).runOwnedPass(ctx, next); err != nil || ran || count != 0 {
		t.Fatalf("API rollback reopened the fence: count=%d ran=%t err=%v", count, ran, err)
	}
	if count, ran, err := NewWorker(storage, nil).runOwnedPass(ctx, next); err != nil || !ran || count == 0 {
		t.Fatalf("worker did not catch up after handoff: count=%d ran=%t err=%v", count, ran, err)
	}
	if count, ran, err := NewWorker(storage, nil).runOwnedPass(ctx, next); err != nil || !ran || count != 0 {
		t.Fatalf("same window was replayed: count=%d ran=%t err=%v", count, ran, err)
	}
}
