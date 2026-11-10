package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

func TestOOMHistoryIsMonotonicDurableAndInputScoped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	s := &Service{Store: store.New(path)}
	if err := s.Store.Init(); err != nil {
		t.Fatal(err)
	}
	app := model.App{ID: "app-fixture", TenantID: "tenant-fixture"}
	src := model.AppSource{CommitSHA: strings.Repeat("a", 40)}
	spec := model.AppSpec{Env: map[string]string{"BUILD_MODE": "release"}}
	fp := buildOOMInputFingerprint(src, spec)
	var wg sync.WaitGroup
	for _, limit := range []int64{6 << 30, 5 << 30, 7 << 30, 4 << 30} {
		wg.Add(1)
		go func(limit int64) {
			defer wg.Done()
			if err := s.recordBuildOOM(context.Background(), app, "op-fixture", fp, sourceimport.BuilderOOMObservation{MemoryLimitBytes: limit, Attempt: 1, ObservedAt: time.Now()}); err != nil {
				t.Error(err)
			}
		}(limit)
	}
	wg.Wait()
	s = &Service{Store: store.New(path)}
	history, err := s.readBuildOOMHistory(context.Background(), app, fp)
	if err != nil || history.MemoryLimitBytes != 7<<30 {
		t.Fatal(history, err)
	}
	// Policy ceilings are intentionally absent from this input key.
	if fp != buildOOMInputFingerprint(src, spec) {
		t.Fatal("unstable input identity")
	}
	src.CommitSHA = strings.Repeat("b", 40)
	other, err := s.readBuildOOMHistory(context.Background(), app, buildOOMInputFingerprint(src, spec))
	if err != nil || other.MemoryLimitBytes != 0 {
		t.Fatal("new input inherited OOM", other, err)
	}
	app.TenantID = "another-tenant"
	other, err = s.readBuildOOMHistory(context.Background(), app, fp)
	if err != nil || other.MemoryLimitBytes != 0 {
		t.Fatal("tenant history leaked", other, err)
	}
}

func TestOOMHistoryWaitsForCheckpointContentionAndHonorsCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "released", true: "canceled"}[canceled], func(t *testing.T) {
			state := store.New(filepath.Join(t.TempDir(), "store.json"))
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			app := model.App{ID: "app-neutral", TenantID: "tenant-neutral"}
			fingerprint := buildOOMInputFingerprint(model.AppSource{CommitSHA: strings.Repeat("c", 40)}, model.AppSpec{})
			locked, release, holderDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			go func() {
				_, err := state.UpdateObservationCheckpoint(t.Context(), buildOOMHistoryKey(app, fingerprint), func([]byte) ([]byte, error) {
					close(locked)
					<-release
					return nil, nil
				})
				holderDone <- err
			}()
			<-locked
			var once sync.Once
			unlock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unlock)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- (&Service{Store: state}).recordBuildOOM(ctx, app, "operation-neutral", fingerprint, sourceimport.BuilderOOMObservation{MemoryLimitBytes: 7 << 30})
			}()
			if canceled {
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled lock wait: %v", err)
				}
				unlock()
			} else {
				// Longer than the old eight 10ms retries; the lock owner is a
				// live, bounded writer, not a corrupt checkpoint or failed write.
				select {
				case err := <-done:
					t.Fatalf("returned before lock release: %v", err)
				case <-time.After(150 * time.Millisecond):
				}
				unlock()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				raw, err := state.ReadObservationCheckpoint(t.Context(), buildOOMHistoryKey(app, fingerprint))
				history, decodeErr := decodeBuildOOMHistory(raw)
				if err != nil || decodeErr != nil || history.MemoryLimitBytes != 7<<30 {
					t.Fatalf("lost OOM after contention: %+v %v %v", history, err, decodeErr)
				}
			}
			if err := <-holderDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOOMHistorySeedsOnlyExecutedOOMForMatchingInputs(t *testing.T) {
	state := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := state.CreateTenant("Fixture")
	project, _ := state.CreateProject(tenant.ID, "fixture", "")
	src := model.AppSource{Type: model.AppSourceTypeGitHubPublic, RepoURL: "https://github.com/example/service", CommitSHA: strings.Repeat("a", 40)}
	app, err := state.CreateImportedApp(tenant.ID, project.ID, "fixture", "", model.AppSpec{Replicas: 1, Image: "registry.example/service:old"}, src, model.AppRoute{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		outcome, cause, commit string
		limit                  int64
	}{
		{"failed", "memory_limit_exceeded", src.CommitSHA, 5 << 30},
		{"failed", "memory_limit_exceeded", src.CommitSHA, 6 << 30},
		{"rejected", "memory_limit_exceeded", src.CommitSHA, 9 << 30},
		{"failed", "process_exit_nonzero", src.CommitSHA, 10 << 30},
		{"failed", "memory_limit_exceeded", strings.Repeat("b", 40), 11 << 30},
	} {
		input := src
		input.CommitSHA = tc.commit
		op, err := state.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeImport, DesiredSource: &input, DesiredSpec: &app.Spec})
		if err != nil {
			t.Fatal(err)
		}
		fact := model.BuilderAttemptEvidence{Attempt: 1, Outcome: tc.outcome, MemoryLimitBytes: tc.limit, Causes: []string{tc.cause}}
		_, err = state.RecordOperationEvidence(model.OperationEvidence{TenantID: tenant.ID, AppID: app.ID, OperationID: op.ID, Type: model.OperationEvidenceTypeBuildAttempt, ObservedAt: time.Now(), Payload: sourceimport.BuilderAttemptPayload(fact)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := state.FailOperation(op.ID, "fixture build failure"); err != nil {
			t.Fatal(err)
		}
	}
	svc := &Service{Store: state}
	fp := buildOOMInputFingerprint(src, app.Spec)
	history, err := svc.readBuildOOMHistory(context.Background(), app, fp)
	if err != nil || history.MemoryLimitBytes != 6<<30 {
		t.Fatal(history, err)
	}
	app.Spec.Env = map[string]string{"BUILD_MODE": "changed"}
	history, err = svc.readBuildOOMHistory(context.Background(), app, buildOOMInputFingerprint(src, app.Spec))
	if err != nil || history.MemoryLimitBytes != 0 {
		t.Fatal("changed inputs inherited history", history, err)
	}
	if _, err := decodeBuildOOMHistory([]byte("corrupt")); err == nil {
		t.Fatal("corrupt history accepted")
	}
	negative, _ := json.Marshal(buildOOMHistory{Version: 1, BuilderOOMObservation: sourceimport.BuilderOOMObservation{MemoryLimitBytes: -1}})
	if _, err := decodeBuildOOMHistory(negative); err == nil {
		t.Fatal("negative history accepted")
	}
}

func TestGitHubOOMAdmissionStopsRepeatedOperationsAndResumes(t *testing.T) {
	state := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := state.CreateTenant("Fixture")
	if err != nil {
		t.Fatal(err)
	}
	project, err := state.CreateProject(tenant.ID, "fixture", "")
	if err != nil {
		t.Fatal(err)
	}
	src := model.AppSource{Type: model.AppSourceTypeGitHubPublic, RepoURL: "https://github.com/example/service", RepoBranch: "main", CommitSHA: strings.Repeat("a", 40), BuildStrategy: model.AppBuildStrategyDockerfile}
	app, err := state.CreateImportedApp(tenant.ID, project.ID, "fixture", "", model.AppSpec{Replicas: 1, Image: "registry.example/service:old"}, src, model.AppRoute{})
	if err != nil {
		t.Fatal(err)
	}
	next := src
	next.CommitSHA = strings.Repeat("b", 40)
	// Capacity recovery must also resume an input whose old automatic failure
	// budget was exhausted, without creating duplicate pending operations.
	for i := 0; i < gitHubTrackedCommitMaxAutomaticFailures; i++ {
		op, err := state.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeImport, RequestedByType: model.ActorTypeBootstrap, RequestedByID: model.OperationRequestedByGitHubSyncController, DesiredSpec: &app.Spec, DesiredSource: &next})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := state.FailOperation(op.ID, "fixture resource failure"); err != nil {
			t.Fatal(err)
		}
	}
	svc := &Service{Store: state, Logger: log.New(io.Discard, "", 0), Config: config.ControllerConfig{GitHubSyncTimeout: time.Second}, latestGitHubCommit: func(context.Context, string, string, string) (string, string, error) {
		return next.CommitSHA, "main", nil
	}}
	fp := buildOOMInputFingerprint(next, app.Spec)
	if err := svc.recordBuildOOM(context.Background(), app, "op-fixture", fp, sourceimport.BuilderOOMObservation{MemoryLimitBytes: 5 << 30}); err != nil {
		t.Fatal(err)
	}
	available := int64(5 << 30)
	svc.inspectBuilderOOMPlacement = func(_ context.Context, _ string, _ sourceimport.BuilderPodPolicy, _ string, _ bool, _ map[string]string, previous int64) (model.BuilderPlacementInspection, error) {
		if previous != 5<<30 {
			t.Fatal(previous)
		}
		return model.BuilderPlacementInspection{Nodes: []model.BuilderPlacementNodeInspection{{Eligible: true, Allocation: model.BuilderResourceSnapshot{MemoryBytes: available}}}}, nil
	}
	for i := 0; i < 3; i++ {
		if err := svc.syncGitHubApps(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	ops, err := state.ListOperations(tenant.ID, false)
	if err != nil || len(ops) != gitHubTrackedCommitMaxAutomaticFailures {
		t.Fatal("blocked sync created operations", ops, err)
	}
	observed, err := state.GetApp(app.ID)
	if err != nil || observed.Status.SourceSync == nil || observed.Status.SourceSync.LastErrorCode != "builder_memory_not_increased" {
		t.Fatal(observed.Status, err)
	}
	available = 5<<30 + 1<<20
	if err := svc.syncGitHubApps(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.syncGitHubApps(context.Background()); err != nil {
		t.Fatal(err)
	}
	ops, err = state.ListOperations(tenant.ID, false)
	if err != nil || len(ops) != gitHubTrackedCommitMaxAutomaticFailures+1 {
		t.Fatal("capacity recovery did not queue exactly once", ops, err)
	}
}

func TestAutoOOMAdmissionUsesResolvedProfile(t *testing.T) {
	for _, profile := range []string{"heavy", "light"} {
		t.Run(profile, func(t *testing.T) {
			state := store.New(filepath.Join(t.TempDir(), "store.json"))
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			svc := &Service{Store: state}
			app := model.App{ID: "app-fixture", TenantID: "tenant-fixture"}
			source := model.AppSource{CommitSHA: strings.Repeat("a", 40), BuildStrategy: model.AppBuildStrategyAuto}
			spec := model.AppSpec{}
			fp := buildOOMInputFingerprint(source, spec)
			if err := svc.recordBuildOOM(context.Background(), app, "op-fixture", fp, sourceimport.BuilderOOMObservation{MemoryLimitBytes: 5 << 30}); err != nil {
				t.Fatal(err)
			}
			// Old evidence may have no profile. Resolve it once without
			// running a Pod; the scheduler still checks the same OOM limit.
			admitted, err := svc.githubBuildOOMAdmission(context.Background(), app, source, spec, sourceimport.BuilderPodPolicy{})
			if err != nil || !admitted {
				t.Fatal(admitted, err)
			}
			history, err := svc.resolveBuildOOMProfile(context.Background(), app, fp, profile)
			if err != nil || history.MemoryLimitBytes != 5<<30 || history.WorkloadProfile != profile {
				t.Fatal(history, err)
			}
			// A smaller observation must not lower the memory fact when
			// enriching its resolved profile.
			if err := svc.recordBuildOOM(context.Background(), app, "op-smaller", fp, sourceimport.BuilderOOMObservation{MemoryLimitBytes: 4 << 30, WorkloadProfile: profile}); err != nil {
				t.Fatal(err)
			}
			svc.inspectBuilderOOMPlacement = func(_ context.Context, _ string, _ sourceimport.BuilderPodPolicy, strategy string, _ bool, _ map[string]string, previous int64) (model.BuilderPlacementInspection, error) {
				expected := model.AppBuildStrategyDockerfile
				if profile == "light" {
					expected = model.AppBuildStrategyStaticSite
				}
				if strategy != expected || previous != 5<<30 {
					t.Fatal("incorrect auto profile admission", strategy, previous)
				}
				return model.BuilderPlacementInspection{Nodes: []model.BuilderPlacementNodeInspection{{Eligible: true, Allocation: model.BuilderResourceSnapshot{MemoryBytes: 5<<30 + 1<<20}}}}, nil
			}
			admitted, err = svc.githubBuildOOMAdmission(context.Background(), app, source, spec, sourceimport.BuilderPodPolicy{})
			if err != nil || !admitted {
				t.Fatal("resolved auto input did not recover", admitted, err)
			}
		})
	}
}
