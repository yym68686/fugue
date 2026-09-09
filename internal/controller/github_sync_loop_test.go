package controller

import (
	"context"
	"errors"
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

func newSourceSyncFixture(t *testing.T) (*store.Store, model.App) {
	t.Helper()
	st := store.New(t.TempDir() + "/state.json")
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := st.CreateTenant("Source fixture")
	if err != nil {
		t.Fatal(err)
	}
	project, err := st.CreateProject(tenant.ID, "source-fixture", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := st.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "source-fixture", "", model.AppSpec{Image: "registry.example.test/fixture:stable", Replicas: 1, RuntimeID: "runtime_managed_shared"}, model.AppSource{Type: model.AppSourceTypeGitHubPublic, RepoURL: "https://github.com/example/fixture", RepoBranch: "main", CommitSHA: "stable", BuildStrategy: model.AppBuildStrategyDockerfile})
	if err != nil {
		t.Fatal(err)
	}
	return st, app
}

func TestGitHubChecksAdvanceHealthyFactsAndRejectLateRebind(t *testing.T) {
	st, app := newSourceSyncFixture(t)
	now := time.Now().UTC()
	svc := &Service{Store: st, Logger: log.New(io.Discard, "", 0), Config: config.ControllerConfig{GitHubSyncTimeout: time.Second}, now: func() time.Time { return now }, latestGitHubCommit: func(context.Context, string, string, string) (string, string, error) { return "stable", "main", nil }}
	for i := 0; i < 2; i++ {
		if err := svc.syncGitHubApps(context.Background()); err != nil {
			t.Fatal(err)
		}
		got, err := st.GetApp(app.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status.SourceSync == nil || !got.Status.SourceSync.LastCheckedAt.Equal(now) || !got.Status.SourceSync.LastSuccessAt.Equal(now) {
			t.Fatal("successful healthy check did not advance fact")
		}
		now = now.Add(time.Minute)
	}
	svc.latestGitHubCommit = func(context.Context, string, string, string) (string, string, error) {
		source := *model.AppOriginSource(app)
		source.RepoBranch = "replacement"
		if _, err := st.UpdateAppOriginSource(app.ID, source); err != nil {
			t.Fatal(err)
		}
		return "new-commit", "main", nil
	}
	if err := svc.syncGitHubApps(context.Background()); err != nil {
		t.Fatal(err)
	}
	ops, err := st.ListOperationsByApp(app.TenantID, true, app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 0 {
		t.Fatal("late source check queued a build after rebind")
	}
	got, err := st.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.SourceSync != nil || model.AppOriginSource(got).RepoBranch != "replacement" {
		t.Fatal("late check changed rebound state")
	}
}

func TestGitHubChecksContinueDuringBlockedFullReconcile(t *testing.T) {
	st, _ := newSourceSyncFixture(t)
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
	var checks atomic.Int32
	svc := New(st, config.ControllerConfig{DatabaseURL: "postgres://127.0.0.1:1/unavailable?sslmode=disable", KubectlApply: true, PollInterval: time.Hour, FallbackPollInterval: time.Hour, ManagedAppReconcileFallbackInterval: time.Hour, GitHubSyncInterval: 20 * time.Millisecond, GitHubSyncTimeout: time.Second, RenderDir: t.TempDir()}, log.New(io.Discard, "", 0))
	svc.newKubeClient = func(namespace string) (*kubeClient, error) {
		return &kubeClient{client: server.Client(), baseURL: server.URL, namespace: namespace}, nil
	}
	svc.latestGitHubCommit = func(context.Context, string, string, string) (string, string, error) {
		checks.Add(1)
		return "stable", "main", nil
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
	case <-time.After(5 * time.Second):
		t.Fatal("full reconcile did not block")
	}
	deadline := time.Now().Add(2 * time.Second)
	for checks.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if checks.Load() < 2 {
		t.Fatal("full reconcile blocked source checks")
	}
}

func TestGitHubCheckWorkerDoesNotOverlapOrRecordCancellationAsFailure(t *testing.T) {
	st, app := newSourceSyncFixture(t)
	started := make(chan struct{})
	var calls atomic.Int32
	svc := &Service{Store: st, Config: config.ControllerConfig{GitHubSyncInterval: time.Millisecond, GitHubSyncTimeout: time.Second}, Logger: log.New(io.Discard, "", 0), latestGitHubCommit: func(ctx context.Context, _, _, _ string) (string, string, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		return "", "", ctx.Err()
	}}
	stop := svc.startGitHubSourceSync(context.Background(), func() { t.Error("canceled pass triggered workers") })
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	time.Sleep(20 * time.Millisecond)
	stop()
	if calls.Load() != 1 {
		t.Fatal("overlapping checks")
	}
	got, err := st.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.SourceSync != nil {
		t.Fatal("leadership cancellation became a repository failure")
	}
}

func TestCompletedDeployReadRetriesOnlyTransientErrors(t *testing.T) {
	transientCalls := 0
	_, err := retryCompletedDeployRead(context.Background(), func() ([]model.Operation, error) {
		transientCalls++
		if transientCalls == 1 {
			return nil, context.DeadlineExceeded
		}
		return nil, nil
	})
	if err != nil || transientCalls != 2 {
		t.Fatalf("transient calls=%d err=%v", transientCalls, err)
	}
	terminalCalls := 0
	invalid := errors.New("invalid query")
	_, err = retryCompletedDeployRead(context.Background(), func() ([]model.Operation, error) { terminalCalls++; return nil, invalid })
	if !errors.Is(err, invalid) || terminalCalls != 1 {
		t.Fatal("permanent failure retried or hidden")
	}
	ctx, cancel := context.WithCancel(context.Background())
	_, err = retryCompletedDeployRead(ctx, func() ([]model.Operation, error) { cancel(); return nil, context.DeadlineExceeded })
	if err == nil {
		t.Fatal("canceled guard succeeded")
	}
}

func TestStaleDeployGuardFindsMatchingCandidateBeyondFirstPage(t *testing.T) {
	st, app := newSourceSyncFixture(t)
	stale, err := st.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec})
	if err != nil {
		t.Fatal(err)
	}
	live := app.Spec
	live.Image = "registry.example.test/fixture:live"
	matched, err := st.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &live})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CompleteManagedOperation(matched.ID, "", "done"); err != nil {
		t.Fatal(err)
	}
	current, err := st.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < store.CompletedDeployCandidatePageSize+2; i++ {
		other := live
		other.Image = "registry.example.test/fixture:other"
		op, err := st.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &other})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.CompleteManagedOperation(op.ID, "", "done"); err != nil {
			t.Fatal(err)
		}
	}
	svc := &Service{Store: st}
	got, found, err := svc.completedDeployAfterOperationMatchingCurrentApp(context.Background(), stale, current)
	if err != nil || !found || got.ID != matched.ID {
		t.Fatalf("found=%v id=%s err=%v", found, got.ID, err)
	}
}
