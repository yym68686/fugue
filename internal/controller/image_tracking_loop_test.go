package controller

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/store"
)

const imageTrackingLoopTestDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

func newImageTrackingLoopFixture(t *testing.T) (*store.Store, model.App) {
	t.Helper()
	st, tenant, project, _ := newImageTrackingSyncTestStore(t)
	image := "registry.example.test/service:image-111111111111"
	source := model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "ghcr.io/example/service:main", ResolvedImageRef: image}
	app, err := st.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "tracked-service", "", model.AppSpec{Image: image, Replicas: 1, RuntimeID: "runtime_managed_shared"}, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertAppImageTracking(model.AppImageTracking{TenantID: tenant.ID, AppID: app.ID, ImageRef: source.ImageRef, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	return st, app
}

func TestImageTrackingChecksContinueDuringBlockedFullReconcile(t *testing.T) {
	for _, eventDriven := range []bool{false, true} {
		name := "polling"
		if eventDriven {
			name = "event-driven"
		}
		t.Run(name, func(t *testing.T) {
			st, _ := newImageTrackingLoopFixture(t)
			blocked := make(chan struct{})
			var once sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/managedapps") {
					once.Do(func() { close(blocked) })
					<-r.Context().Done()
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"items":[]}`)
			}))
			defer server.Close()
			cfg := config.ControllerConfig{KubectlApply: true, PollInterval: time.Hour, FallbackPollInterval: time.Hour, ManagedAppReconcileFallbackInterval: time.Hour, ImageTrackingInterval: 20 * time.Millisecond, ImageTrackingTimeout: time.Second, RenderDir: t.TempDir()}
			if eventDriven {
				cfg.DatabaseURL = "postgres://127.0.0.1:1/unavailable?sslmode=disable"
			}
			svc := New(st, cfg, log.New(io.Discard, "", 0))
			svc.newKubeClient = func(namespace string) (*kubeClient, error) {
				return &kubeClient{client: server.Client(), baseURL: server.URL, namespace: namespace}, nil
			}
			checks := make(chan struct{}, 10)
			svc.resolveRemoteImageDigest = func(context.Context, string) (string, error) {
				select {
				case checks <- struct{}{}:
				default:
				}
				return imageTrackingLoopTestDigest, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- svc.runActiveLoop(ctx) }()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("leader shutdown did not join blocked tasks")
				}
			}()
			select {
			case <-blocked:
			case err := <-done:
				t.Fatalf("active loop exited before reconciliation: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("full reconciliation did not block")
			}
			timer := time.NewTimer(time.Second)
			defer timer.Stop()
			for i := 0; i < 2; i++ {
				select {
				case <-checks:
				case <-timer.C:
					t.Fatal("full reconciliation blocked periodic image checks")
				}
			}
		})
	}
}

func TestImageTrackingWorkerCoalescesTicksAndJoinsCanceledCheck(t *testing.T) {
	st, app := newImageTrackingLoopFixture(t)
	svc := newImageTrackingSyncTestService(st, imageTrackingLoopTestDigest)
	svc.Config.ImageTrackingInterval = time.Millisecond
	svc.Config.ImageTrackingTimeout = 5 * time.Second
	started := make(chan int32, 10)
	releaseFirst := make(chan struct{})
	canceled := make(chan struct{})
	allowExit := make(chan struct{})
	var calls, triggers atomic.Int32
	svc.resolveRemoteImageDigest = func(ctx context.Context, _ string) (string, error) {
		n := calls.Add(1)
		started <- n
		if n == 1 {
			select {
			case <-releaseFirst:
				return imageTrackingLoopTestDigest, nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		<-ctx.Done()
		if n == 2 {
			close(canceled)
			<-allowExit
		}
		// Even a resolver that returns a digest on cancellation must not queue it.
		return "sha256:2222222222222222222222222222222222222222222222222222222222222222", nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := svc.startAppImageTracking(ctx, func() { triggers.Add(1) })
	defer stop()
	var releaseExit sync.Once
	defer releaseExit.Do(func() { close(allowExit) })
	waitStart := func(want int32) {
		t.Helper()
		select {
		case got := <-started:
			if got != want {
				t.Fatalf("resolver start=%d, want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("resolver check %d did not start", want)
		}
	}
	waitStart(1)
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("checks overlapped while first was blocked: calls=%d", got)
	}
	close(releaseFirst)
	waitStart(2)
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("ticks accumulated overlapping checks: calls=%d", got)
	}
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel the active registry check")
	}
	select {
	case <-stopped:
		t.Fatal("stop returned before the registry check exited")
	default:
	}
	releaseExit.Do(func() { close(allowExit) })
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not join the registry check")
	}
	if calls.Load() != 2 || triggers.Load() != 1 {
		t.Fatalf("calls=%d triggers=%d; canceled scan must not wake operation workers", calls.Load(), triggers.Load())
	}
	checks, err := st.ListAppImageTrackingChecks(model.AppImageTrackingCheckFilter{TenantID: app.TenantID, AppID: app.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) != 1 || checks[0].Decision != model.AppImageTrackingDecisionAlreadyDeployed {
		t.Fatalf("leadership cancellation recorded an image decision: %+v", checks)
	}
	tracking, err := st.GetAppImageTracking(app.TenantID, false, app.ID)
	if err != nil {
		t.Fatal(err)
	}
	ops, err := st.ListOperationsByApp(app.TenantID, true, app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if tracking.LastError != "" || len(ops) != 0 || !svc.imageTrackingLastSyncErrorAt.IsZero() || svc.imageTrackingSyncRunning {
		t.Fatalf("cancellation left failure or work behind: tracking=%+v operations=%+v", tracking, ops)
	}
}

func TestImageTrackingWorkerRetriesAfterRegistryTimeout(t *testing.T) {
	st, app := newImageTrackingLoopFixture(t)
	svc := newImageTrackingSyncTestService(st, imageTrackingLoopTestDigest)
	svc.Config.ImageTrackingInterval = 10 * time.Millisecond
	svc.Config.ImageTrackingTimeout = 5 * time.Millisecond
	var calls atomic.Int32
	svc.resolveRemoteImageDigest = func(ctx context.Context, _ string) (string, error) {
		if calls.Add(1) == 1 {
			<-ctx.Done()
			return "", ctx.Err()
		}
		return imageTrackingLoopTestDigest, nil
	}
	passes := make(chan struct{}, 10)
	stop := svc.startAppImageTracking(context.Background(), func() {
		select {
		case passes <- struct{}{}:
		default:
		}
	})
	defer stop()
	for i := 0; i < 2; i++ {
		select {
		case <-passes:
		case <-time.After(time.Second):
			t.Fatal("registry timeout prevented the next image scan")
		}
	}
	stop()
	checks, err := st.ListAppImageTrackingChecks(model.AppImageTrackingCheckFilter{TenantID: app.TenantID, AppID: app.ID})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, check := range checks {
		seen[check.Decision] = true
	}
	if !seen[model.AppImageTrackingDecisionResolverError] || !seen[model.AppImageTrackingDecisionAlreadyDeployed] {
		t.Fatalf("timeout and subsequent recovery were not both recorded: %+v", checks)
	}
}

func TestImageTrackingWorkerDisabledOrAlreadyCanceled(t *testing.T) {
	for _, interval := range []time.Duration{-time.Second, 0, time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			// A disabled or canceled worker must never access the nil store.
			svc := &Service{Config: config.ControllerConfig{ImageTrackingInterval: interval}}
			ctx, cancel := context.WithCancel(context.Background())
			if interval > 0 {
				cancel()
			}
			defer cancel()
			stop := svc.startAppImageTracking(ctx, func() { t.Error("inactive worker triggered operations") })
			stop()
		})
	}
}
