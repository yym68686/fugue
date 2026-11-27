package operationqueue

import (
	"errors"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
)

type testRepository struct {
	ops          []model.Operation
	apps         []model.App
	writes       []string
	snapshotLost string
}

func (r *testRepository) ListActiveOperations() ([]model.Operation, error) {
	return slices.Clone(r.ops), nil
}
func (r *testRepository) ListAppsMetadataByIDs(ids []string) ([]model.App, error) {
	var out []model.App
	for _, app := range r.apps {
		if slices.Contains(ids, app.ID) {
			out = append(out, app)
		}
	}
	return out, nil
}
func (r *testRepository) ListAppsMetadataByProjectIDs(ids []string) ([]model.App, error) {
	var out []model.App
	for _, app := range r.apps {
		if slices.Contains(ids, app.ProjectID) {
			out = append(out, app)
		}
	}
	return out, nil
}
func (r *testRepository) consume(id string) (model.Operation, error) {
	i := slices.IndexFunc(r.ops, func(op model.Operation) bool { return op.ID == id })
	if i < 0 {
		return model.Operation{}, errors.New("unexpected write for missing operation")
	}
	op := r.ops[i]
	r.ops = slices.Delete(r.ops, i, i+1)
	return op, nil
}
func (r *testRepository) TryClaimPendingOperation(id string) (model.Operation, bool, error) {
	r.writes = append(r.writes, "claim:"+id)
	op, err := r.consume(id)
	return op, op.ID != r.snapshotLost && err == nil, err
}
func (r *testRepository) FailBeforeClaim(expected model.Operation, _ string) (model.Operation, bool, error) {
	id := expected.ID
	r.writes = append(r.writes, "fail:"+id)
	op, err := r.consume(id)
	return op, op.ID != r.snapshotLost && err == nil, err
}
func (r *testRepository) CancelSupersededDeploy(id, _ string) (bool, error) {
	r.writes = append(r.writes, "cancel:"+id)
	_, err := r.consume(id)
	return err == nil, err
}

func pending(id, app, kind string) model.Operation {
	return model.Operation{ID: id, AppID: app, Type: kind, Status: model.OperationStatusPending}
}

func TestClaimRefreshesAfterConcurrentTransitionAndIsolatesLanes(t *testing.T) {
	r := &testRepository{
		ops: []model.Operation{
			pending("import", "first", model.OperationTypeImport),
			pending("gone", "second", model.OperationTypeDeploy),
			pending("ready", "third", model.OperationTypeDeploy),
		},
		snapshotLost: "gone",
	}
	var q Queue
	var events []Event
	claimed, found, err := q.Claim(r, ForegroundActivate, nil, func(e Event) { events = append(events, e) })
	if err != nil || !found || claimed.ID != "ready" {
		t.Fatalf("claim = %q, %v, %v", claimed.ID, found, err)
	}
	if !slices.Equal(r.writes, []string{"claim:gone", "claim:ready"}) || len(events) != 1 || events[0].Kind != Claimed {
		t.Fatalf("writes=%v, events=%v", r.writes, events)
	}
	if !q.IsExecuting(claimed.ID) || q.ReserveAttempt(claimed) {
		t.Fatal("claimed attempt not registered")
	}
	if _, ok := q.ReserveRecovery(claimed.ID); ok {
		t.Fatal("recovery acquired a live attempt")
	}
	q.FinishAttempt(claimed)
	release, ok := q.ReserveRecovery(claimed.ID)
	if !ok {
		t.Fatal("recovery cannot acquire a finished attempt")
	}
	q.FinishAttempt(claimed)
	if !q.IsExecuting(claimed.ID) {
		t.Fatal("stale worker cleared recovery reservation")
	}
	release()
	if !q.ReserveAttempt(claimed) {
		t.Fatal("worker cannot reserve attempt after recovery release")
	}
	release()
	if !q.IsExecuting(claimed.ID) {
		t.Fatal("stale recovery cleared worker reservation")
	}
}

func TestClaimCombinesDependencyAndDeletionRulesWithQueueWrites(t *testing.T) {
	r := &testRepository{
		ops: []model.Operation{
			pending("obsolete", "removed", model.OperationTypeDeploy),
			pending("blocked", "compose", model.OperationTypeDeploy),
			pending("delete", "removed", model.OperationTypeDelete),
		},
		apps: []model.App{
			{ID: "compose", ProjectID: "same", Source: &model.AppSource{ComposeService: "web", ComposeDependsOn: []string{"db"}}},
			{ID: "database", ProjectID: "other", Source: &model.AppSource{ComposeService: "db"}},
		},
	}
	var q Queue
	var kinds []EventKind
	claimed, found, err := q.Claim(r, ForegroundActivate, nil, func(e Event) { kinds = append(kinds, e.Kind) })
	if err != nil || !found || claimed.ID != "delete" {
		t.Fatalf("claim = %q, %v, %v", claimed.ID, found, err)
	}
	if !slices.Equal(r.writes, []string{"cancel:obsolete", "fail:blocked", "claim:delete"}) ||
		!slices.Equal(kinds, []EventKind{Blocking, Failed, Claimed}) {
		t.Fatalf("writes=%v, events=%v", r.writes, kinds)
	}
}

func TestRetryBackoffAndDurableAttemptIdentity(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC)
	op := pending("operation", "app", model.OperationTypeDeploy)
	op.UpdatedAt = now
	op.ResultMessage = model.OperationResultDeployImageReplicationPending + ": image is being replicated"
	if RetryReady(op, now.Add(29*time.Second)) || !RetryReady(op, now.Add(30*time.Second)) {
		t.Fatal("image replication wait must become claimable exactly at the retry interval")
	}
	op.ResultMessage = "operation is pending"
	if !RetryReady(op, now) {
		t.Fatal("ordinary pending operation should be immediately claimable")
	}
	op.StartedAt = &now
	stored := now.Truncate(time.Microsecond).In(time.FixedZone("offset", 3600))
	if ExecutionKey(op) != ExecutionKey(model.Operation{StartedAt: &stored}) {
		t.Fatal("persisting the claim changed its attempt identity")
	}
}

func TestAppCPUResizeRestoresUseDedicatedWorkerLane(t *testing.T) {
	op := model.Operation{Type: model.OperationTypeAppCPUResize, RequestedByType: model.ActorTypeSystem, RequestedByID: "app-cpu-resize-reconcile"}
	for _, lane := range []Lane{ForegroundImport, ForegroundActivate, GitHubSyncImport, GitHubSyncActivate, CPURequestRestore} {
		if pendingOperationMatchesLane(op, lane) != (lane == CPURequestRestore) {
			t.Fatal("restore consumed an unrelated worker", lane)
		}
	}
	op.RequestedByType = model.ActorTypeAPIKey
	if pendingOperationMatchesLane(op, CPURequestRestore) || !pendingOperationMatchesLane(op, ForegroundActivate) {
		t.Fatal("user resize routed as background restore")
	}
}
