package inventory

import (
	"context"
	"errors"
	"slices"
	"testing"

	"fugue/internal/model"
)

type recordingQueue struct {
	updaters      []model.NodeUpdater
	calls         []string
	reportFailure error
}

func (q *recordingQueue) ListUpdaters() ([]model.NodeUpdater, error) {
	return q.updaters, nil
}

func (q *recordingQueue) SupportsReport(u model.NodeUpdater) (bool, error) {
	q.calls = append(q.calls, "can-report:"+u.ID)
	return u.ID != "unsupported", nil
}

func (q *recordingQueue) SupportsUpgrade(u model.NodeUpdater) (bool, error) {
	q.calls = append(q.calls, "can-upgrade:"+u.ID)
	return u.ID != "cannot-upgrade", nil
}

func (q *recordingQueue) UpgradeActive(id string) (bool, error) {
	q.calls = append(q.calls, "has-upgrade:"+id)
	return id == "already-queued", nil
}

func (q *recordingQueue) QueueUpgrade(u model.NodeUpdater) error {
	q.calls = append(q.calls, "upgrade:"+u.ID)
	return nil
}

func (q *recordingQueue) QueueReport(u model.NodeUpdater) error {
	q.calls = append(q.calls, "report:"+u.ID)
	return q.reportFailure
}

func TestScheduleReportsOnlyQueuesSupportedWork(t *testing.T) {
	active := model.NodeUpdaterStatusActive
	q := &recordingQueue{updaters: []model.NodeUpdater{
		{ID: "inactive", Status: "inactive", UpdaterVersion: "v1"},
		{ID: "unsupported", Status: active, UpdaterVersion: "v1"},
		{ID: "current", Status: active, UpdaterVersion: "v2"},
		{ID: "cannot-upgrade", Status: active, UpdaterVersion: "v1"},
		{ID: "already-queued", Status: active, UpdaterVersion: "v1"},
		{ID: "upgrade", Status: active, UpdaterVersion: "v1"},
	}}
	if err := ScheduleReports(context.Background(), q, "v2", func(current, target string) bool { return current != target }); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"can-report:unsupported",
		"can-report:current", "report:current",
		"can-report:cannot-upgrade", "can-upgrade:cannot-upgrade", "report:cannot-upgrade",
		"can-report:already-queued", "can-upgrade:already-queued", "has-upgrade:already-queued", "report:already-queued",
		"can-report:upgrade", "can-upgrade:upgrade", "has-upgrade:upgrade", "upgrade:upgrade", "report:upgrade",
	}
	if !slices.Equal(q.calls, want) {
		t.Fatalf("commands = %v, want %v", q.calls, want)
	}
}

func TestScheduleReportsHonorsCancellationAndQueueErrors(t *testing.T) {
	schedule := func(ctx context.Context, q *recordingQueue) error {
		return ScheduleReports(ctx, q, "v1", func(current, target string) bool { return current != target })
	}
	q := &recordingQueue{updaters: []model.NodeUpdater{{ID: "worker", Status: model.NodeUpdaterStatusActive, UpdaterVersion: "v1"}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := schedule(ctx, q); !errors.Is(err, context.Canceled) || len(q.calls) != 0 {
		t.Fatalf("canceled pass made calls %v and returned %v", q.calls, err)
	}
	queueFailed := errors.New("write failed")
	q.reportFailure = queueFailed
	if err := schedule(context.Background(), q); !errors.Is(err, queueFailed) || err.Error() != "schedule LocalPV inventory for node updater worker: write failed" {
		t.Fatalf("queue error was lost: %v", err)
	}
}
