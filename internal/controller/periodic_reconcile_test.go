package controller

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/store"
)

type periodicLoopStartWriter struct {
	once    sync.Once
	started chan struct{}
}

func (w *periodicLoopStartWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "controller listen connect error") {
		w.once.Do(func() { close(w.started) })
	}
	return len(p), nil
}

func TestEventDrivenPeriodicTasksHandleChangesAfterStartupWithoutNotifications(t *testing.T) {
	for _, backend := range []string{"json", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			dsn := ""
			if backend == "postgres" {
				dsn = strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
				if dsn == "" {
					t.Skip("set FUGUE_TEST_DATABASE_URL for real PostgreSQL periodic scheduling")
				}
				if !strings.Contains(dsn, "fugue_test") && !strings.Contains(dsn, "fugue-pgtest") {
					t.Fatal("use a dedicated fugue_test database")
				}
			}
			storePath := filepath.Join(os.TempDir(), "fugue-periodic-"+fmt.Sprint(time.Now().UnixNano())+".json")
			defer os.Remove(storePath)
			defer os.Remove(storePath + ".lock")
			state := store.New(storePath, dsn)
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := state.CreateTenant("Periodic scheduling " + fmt.Sprint(time.Now().UnixNano()))
			if err != nil {
				t.Fatal(err)
			}
			project, err := state.CreateProject(tenant.ID, "periodic-apps", "")
			if err != nil {
				t.Fatal(err)
			}
			_, key, err := state.CreateNodeKey(tenant.ID, "periodic-source")
			if err != nil {
				t.Fatal(err)
			}
			suffix := fmt.Sprint(time.Now().UnixNano())
			_, source, err := state.BootstrapClusterNode(key, "source-"+suffix, "https://source.example.test", nil, "source-"+suffix, "source-"+suffix)
			if err != nil {
				t.Fatal(err)
			}
			_, targetKey, err := state.CreateNodeKey(tenant.ID, "periodic-target-"+suffix)
			if err != nil {
				t.Fatal(err)
			}
			_, target, err := state.BootstrapClusterNode(targetKey, "target-"+suffix, "https://target.example.test", nil, "target-"+suffix, "target-"+suffix)
			if err != nil {
				t.Fatal(err)
			}
			app, err := state.CreateApp(tenant.ID, project.ID, "periodic-app", "", model.AppSpec{Image: "registry.example.test/app:v1", Ports: []int{8080}, Replicas: 1, RuntimeID: source.ID, Failover: &model.AppFailoverSpec{TargetRuntimeID: target.ID, Auto: true}})
			if err != nil {
				t.Fatal(err)
			}
			writer := &periodicLoopStartWriter{started: make(chan struct{})}
			svc := New(state, config.ControllerConfig{DatabaseURL: "postgres://127.0.0.1:1/unavailable?sslmode=disable", PollInterval: 10 * time.Millisecond, RuntimeOfflineAfter: 40 * time.Millisecond, FallbackPollInterval: time.Hour, ManagedAppReconcileFallbackInterval: time.Hour, RenderDir: t.TempDir()}, log.New(writer, "", 0))
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- svc.runActiveLoop(ctx) }()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("active loop did not stop")
				}
			}()
			select {
			case <-writer.started:
			case e := <-done:
				t.Fatalf("active loop exited: %v", e)
			case <-time.After(5 * time.Second):
				t.Fatal("DB listener did not start")
			}
			// Both changes happen after the only startup reconciliation, while LISTEN
			// remains unavailable. No notification or leader restart can hide the gap.
			if _, err := state.SyncManagedOwnedClusterRuntimeStatuses(map[string]bool{source.ClusterNodeName: false, target.ClusterNodeName: true}); err != nil {
				t.Fatal(err)
			}
			updater, _, err := state.EnrollNodeUpdater(key, "updater-"+suffix, "https://updater.example.test", nil, "updater-"+suffix, "updater-"+suffix, "v1", "join-v10", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypeUpgradeUpdater})
			if err != nil {
				t.Fatal(err)
			}
			failovers, upgrades := 0, 0
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				failovers, upgrades = 0, 0
				ops, err := state.ListOperations(tenant.ID, false)
				if err != nil {
					t.Fatal(err)
				}
				for _, op := range ops {
					if op.AppID == app.ID && op.Type == model.OperationTypeFailover && op.TargetRuntimeID == target.ID {
						failovers++
					}
				}
				tasks, err := state.ListNodeUpdateTasks(tenant.ID, false, updater.ID, model.NodeUpdateTaskStatusPending)
				if err != nil {
					t.Fatal(err)
				}
				for _, task := range tasks {
					if task.Type == model.NodeUpdateTaskTypeUpgradeUpdater && task.Payload["target_version"] == model.NodeUpdaterCurrentVersion {
						upgrades++
					}
				}
				if failovers == 1 && upgrades == 1 {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			runtimes, _ := state.ListRuntimes(tenant.ID, true)
			t.Fatalf("post-start periodic changes were missed: failovers=%d upgrades=%d runtimes=%+v", failovers, upgrades, runtimes)
		})
	}
}

func TestPeriodicReconcileRetriesFailuresAndStopsSlowTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	calls, active, maxActive := 0, 0, 0
	succeeded := make(chan struct{})
	stop := (&Service{}).startPeriodicReconcile(ctx, 5*time.Millisecond, "fault-test", func(taskCtx context.Context) error {
		mu.Lock()
		calls++
		active++
		if active > maxActive {
			maxActive = active
		}
		n := calls
		mu.Unlock()
		defer func() { mu.Lock(); active--; mu.Unlock() }()
		if n == 1 {
			return fmt.Errorf("temporary database failure")
		}
		close(succeeded)
		return nil
	})
	select {
	case <-succeeded:
	case <-time.After(time.Second):
		t.Fatal("periodic task did not retry")
	}
	stop()
	mu.Lock()
	gotCalls, gotMax := calls, maxActive
	mu.Unlock()
	if gotCalls < 2 || gotMax != 1 {
		t.Fatalf("periodic task calls=%d maxActive=%d", gotCalls, gotMax)
	}

	blocked := make(chan struct{})
	stopSlow := (&Service{}).startPeriodicReconcile(ctx, 5*time.Millisecond, "slow-test", func(taskCtx context.Context) error {
		select {
		case <-blocked:
		case <-taskCtx.Done():
		}
		return taskCtx.Err()
	})
	time.Sleep(20 * time.Millisecond)
	begin := time.Now()
	stopSlow()
	if time.Since(begin) > time.Second {
		t.Fatal("slow periodic task did not stop after cancellation")
	}
}
