package imagetracking

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

type manualFixture struct {
	ManualRepository
	tracking model.AppImageTracking
	checked  []model.AppImageTrackingCheck
	writes   int
}

func (f *manualFixture) GetAppImageTracking(tenant string, admin bool, app string) (model.AppImageTracking, error) {
	if tenant != f.tracking.TenantID || admin || app != f.tracking.AppID {
		panic("unexpected tracking scope")
	}
	return f.tracking, nil
}

func (f *manualFixture) RecordAppImageTrackingCheck(_, _, _, _, _ string) (model.AppImageTracking, error) {
	f.writes++
	return f.tracking, nil
}

func (f *manualFixture) CreateAppImageTrackingCheck(check model.AppImageTrackingCheck) (model.AppImageTrackingCheck, error) {
	f.checked = append(f.checked, check)
	return check, nil
}

func TestManualSyncRejectsDisabledOrWrongRefBeforeAnyMutation(t *testing.T) {
	app := model.App{ID: "app", TenantID: "tenant"}
	principal := model.Principal{TenantID: app.TenantID}
	repository := &manualFixture{tracking: model.AppImageTracking{TenantID: app.TenantID, AppID: app.ID, ImageRef: "example.invalid/app:stable"}}
	service := NewManual(repository, nil, nil, func(context.Context, string) (string, error) {
		t.Fatal("invalid request called resolver")
		return "", nil
	}, nil, nil, nil)
	if _, err := service.Sync(context.Background(), principal, app, SyncRequest{}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled tracking: %v", err)
	}
	repository.tracking.Enabled = true
	other := "example.invalid/app:other"
	if _, err := service.Sync(context.Background(), principal, app, SyncRequest{ImageRef: &other}); !errors.Is(err, ErrReferenceMismatch) {
		t.Fatalf("mismatched ref: %v", err)
	}
	if repository.writes != 0 || len(repository.checked) != 0 {
		t.Fatalf("invalid requests made state changes: %+v", repository)
	}
}

func TestManualSyncResolverErrorRecordsDecisionAndPreservesError(t *testing.T) {
	app := model.App{ID: "app", TenantID: "tenant"}
	principal := model.Principal{TenantID: app.TenantID}
	repository := &manualFixture{tracking: model.AppImageTracking{ID: "tracking", TenantID: app.TenantID, AppID: app.ID, Enabled: true, ImageRef: "example.invalid/app:stable"}}
	service := NewManual(repository, nil, nil, func(context.Context, string) (string, error) {
		return "", errors.New("registry unavailable")
	}, nil, nil, nil)
	_, err := service.Sync(context.Background(), principal, app, SyncRequest{DeliveryID: " delivery "})
	var resolveErr *ResolverError
	if !errors.As(err, &resolveErr) || resolveErr.Error() != "registry unavailable" {
		t.Fatalf("resolver error: %v", err)
	}
	if repository.writes != 1 || len(repository.checked) != 1 {
		t.Fatalf("resolver decision writes: %+v", repository)
	}
	got := repository.checked[0]
	if got.Decision != model.AppImageTrackingDecisionResolverError || got.Event != "manual" || got.DeliveryID != "delivery" {
		t.Fatalf("unexpected check: %+v", got)
	}
}

func TestPollWorkerDisabledOrCanceledNeverStarts(t *testing.T) {
	for _, interval := range []time.Duration{-time.Second, 0, time.Second} {
		ctx, cancel := context.WithCancel(context.Background())
		if interval > 0 {
			cancel()
		}
		stop := NewPoller(PollOptions{Interval: interval}).Start(ctx, func() { t.Error("inactive worker triggered operations") })
		stop()
		cancel()
	}
}

func TestMetricsOwnConcurrentCountersAndReturnDetachedSnapshot(t *testing.T) {
	var metrics Metrics
	var workers sync.WaitGroup
	check := model.AppImageTrackingCheck{Decision: model.AppImageTrackingDecisionQueued, CheckedAt: time.Now().UTC()}
	for range 8 {
		workers.Go(func() { metrics.observeImageTrackingDecision(check) })
	}
	workers.Wait()
	counts, _, queuedAt, _, _ := metrics.imageTrackingMetricsSnapshot()
	if counts[check.Decision] != 8 || !queuedAt.Equal(check.CheckedAt) {
		t.Fatalf("count=%v last queued=%v", counts, queuedAt)
	}
	clear(counts)
	again, _, _, _, _ := metrics.imageTrackingMetricsSnapshot()
	if !reflect.DeepEqual(again, map[string]int64{check.Decision: 8}) {
		t.Fatalf("external snapshot rewrote metrics: %v", again)
	}
}
