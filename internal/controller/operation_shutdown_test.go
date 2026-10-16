package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

func TestCanceledOperationDrainDoesNotClaimPendingWork(t *testing.T) {
	for _, unbounded := range []bool{false, true} {
		t.Run(fmt.Sprint(unbounded), func(t *testing.T) {
			st, app := newSourceSyncFixture(t)
			op, err := st.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDelete})
			if err != nil {
				t.Fatal(err)
			}
			svc := &Service{Store: st, Logger: log.New(io.Discard, "", 0)}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var drainErr error
			if unbounded {
				var workers sync.WaitGroup
				drainErr = svc.dispatchPendingOperationsInLane(ctx, operationLaneForegroundActivate, &workers)
				workers.Wait()
			} else {
				drainErr = svc.drainPendingOperationsInLane(ctx, operationLaneForegroundActivate)
			}
			if !errors.Is(drainErr, context.Canceled) {
				t.Fatalf("canceled drain did not stop: %v", drainErr)
			}
			got, err := st.GetOperation(op.ID)
			if err != nil || got.Status != op.Status || got.UpdatedAt != op.UpdatedAt {
				t.Fatalf("canceled drain mutated pending work: before=%+v after=%+v err=%v", op, got, err)
			}
		})
	}
}

type shutdownImporter struct {
	blockingImporter
	canceled chan struct{}
	finish   chan struct{}
}

func (i *shutdownImporter) ImportGitHubSource(ctx context.Context, _ sourceimport.GitHubSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
	close(i.started)
	<-ctx.Done()
	close(i.canceled)
	<-i.finish
	return sourceimport.GitHubSourceImportOutput{}, ctx.Err()
}

func TestPendingOperationWorkersJoinCancellationBeforeReturning(t *testing.T) {
	for _, count := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			st, app := newSourceSyncFixture(t)
			source := *model.AppOriginSource(app)
			op, err := st.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeImport,
				RequestedByType: model.ActorTypeBootstrap, RequestedByID: model.OperationRequestedByGitHubSyncController,
				DesiredSpec: &app.Spec, DesiredSource: &source})
			if err != nil {
				t.Fatal(err)
			}
			importer := &shutdownImporter{blockingImporter: blockingImporter{started: make(chan struct{})}, canceled: make(chan struct{}), finish: make(chan struct{})}
			svc := New(st, config.ControllerConfig{PollInterval: time.Hour, RenderDir: t.TempDir()}, log.New(io.Discard, "", 0))
			svc.importer = importer
			svc.registryPushBase, svc.registryPullBase = "registry.example.test", "registry.example.test"
			svc.inspectManagedImage = inspectManagedImageAlwaysExists
			triggers, stop := svc.startPendingOperationWorkers(context.Background(), operationLaneGitHubSyncImport, count)
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(importer.finish) }); stop() })
			triggerPendingOperationWorkers(triggers...)
			select {
			case <-importer.started:
			case <-time.After(2 * time.Second):
				t.Fatal("operation did not start")
			}
			stopped := make(chan struct{})
			go func() { stop(); close(stopped) }()
			select {
			case <-importer.canceled:
			case <-time.After(time.Second):
				t.Fatal("stop did not cancel running operation")
			}
			select {
			case <-stopped:
				t.Fatal("stop returned before running operation completed cancellation")
			case <-time.After(20 * time.Millisecond):
			}
			release.Do(func() { close(importer.finish) })
			select {
			case <-stopped:
			case <-time.After(2 * time.Second):
				t.Fatal("stop did not join canceled operation")
			}
			got, err := st.GetOperation(op.ID)
			if err != nil || got.Status != model.OperationStatusPending || got.ResultMessage != "operation requeued after controller interruption" {
				t.Fatalf("guarded cancellation was not persisted before stop returned: %+v %v", got, err)
			}
			if _, running := svc.runningManagedOperations.Load(op.ID); running {
				t.Fatal("stopped worker retained an in-flight operation")
			}
		})
	}
}

func TestDatabaseMigrationWorkerJoinsInFlightPass(t *testing.T) {
	st, _ := newSourceSyncFixture(t)
	_, err := st.CreateDatabaseMigration(model.DatabaseMigration{ID: "migration-one", Kind: controlPlaneDatabaseMigrationKind, ResourceID: "database-one", TargetStorageClassName: "target-storage"})
	if err != nil {
		t.Fatal(err)
	}
	entered, finish := make(chan struct{}), make(chan struct{})
	svc := &Service{Store: st, Logger: log.New(io.Discard, "", 0), newKubeClient: func(string) (*kubeClient, error) {
		close(entered)
		<-finish
		return nil, errors.New("synthetic client unavailable")
	}}
	stop := svc.startDatabaseMigrationWorker(context.Background())
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(finish) }); stop() })
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("migration worker did not start")
	}
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("migration stop returned before active pass finished")
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(finish) })
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("migration stop did not join")
	}
	got, err := st.GetDatabaseMigration("migration-one")
	if err != nil || got.Status != model.DatabaseMigrationStatusFailed {
		t.Fatalf("worker returned before store completion: %+v %v", got, err)
	}
}

func TestPostOperationMaintenanceUsesWorkerLifetime(t *testing.T) {
	workerCtx, stopWorker := context.WithCancel(context.Background())
	var tasks sync.WaitGroup
	tasks.Add(1) // The operation is still running when it registers maintenance.
	opCtx, finishOperation := context.WithCancel(context.WithValue(workerCtx, operationWorkerLifetimeKey{}, operationWorkerLifetime{ctx: workerCtx, tasks: &tasks}))
	started, canceled, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var release sync.Once
	var completeOperation sync.Once
	t.Cleanup(func() {
		finishOperation()
		completeOperation.Do(tasks.Done)
		stopWorker()
		release.Do(func() { close(finish) })
		tasks.Wait()
	})
	runPostOperationMaintenance(opCtx, log.New(io.Discard, "", 0), "test maintenance", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-finish
		return ctx.Err()
	})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("maintenance did not start asynchronously")
	}
	finishOperation()
	completeOperation.Do(tasks.Done)
	select {
	case <-canceled:
		t.Fatal("operation completion canceled asynchronous maintenance")
	case <-time.After(20 * time.Millisecond):
	}
	stopWorker()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("worker shutdown did not cancel maintenance")
	}
	joined := make(chan struct{})
	go func() { tasks.Wait(); close(joined) }()
	select {
	case <-joined:
		t.Fatal("worker returned before maintenance completed cancellation")
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(finish) })
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("maintenance did not join after completion")
	}
}
