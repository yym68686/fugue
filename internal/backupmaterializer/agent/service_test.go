package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
	"fugue/internal/backupmaterializer"
	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
	"fugue/internal/backupmaterializer/reconciler"
)

func TestDisabledAgentIsInertAndReadinessFailsClosed(t *testing.T) {
	t.Parallel()
	cycle := &sequenceCycle{results: []cycleResult{{err: errors.New("must not run")}}}
	config := Config{
		Enabled: false, CellKey: "private-invalid-cell", Cycle: cycle,
		Interval: -1, AttemptTimeout: -1, Now: func() time.Time { panic("disabled agent read clock") },
	}
	service, err := New(config, log.New(&bytes.Buffer{}, "private-prefix", 0))
	if err != nil || service.Enabled() || service.cellKey != "" || service.cycle != nil || service.now != nil || service.logger != nil {
		t.Fatalf("disabled construction retained capability: service=%#v err=%v", service, err)
	}
	if err := service.ReconcileOnce(context.Background()); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled reconcile error = %v", err)
	}
	if cycle.Calls() != 0 {
		t.Fatalf("disabled agent invoked cycle %d time(s)", cycle.Calls())
	}
	snapshot := service.Snapshot()
	if ValidateSnapshot(snapshot) != nil || snapshot.Mode != ModeDisabled || snapshot.Ready || snapshot.AttemptCount != 0 {
		t.Fatalf("disabled snapshot drifted: %#v", snapshot)
	}
	for path, wantStatus := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable, "/v1/status": http.StatusOK} {
		recorder := httptest.NewRecorder()
		service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != wantStatus || recorder.Header().Get("Cache-Control") != "private, no-store" ||
			recorder.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("disabled %s response drifted: status=%d headers=%v body=%s", path, recorder.Code, recorder.Header(), recorder.Body.String())
		}
	}
	rendered := strings.Join([]string{fmt.Sprint(config), fmt.Sprintf("%#v", config), fmt.Sprint(service), fmt.Sprintf("%#v", service)}, "\n")
	if !strings.Contains(rendered, "[REDACTED]") || strings.Contains(rendered, config.CellKey) || strings.Contains(rendered, "private-prefix") {
		t.Fatalf("disabled diagnostics exposed retained input: %s", rendered)
	}
}

func TestAgentTracksExactReadyConvergedAndMutationCandidateStates(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t)
	cycle := &sequenceCycle{results: []cycleResult{{status: fixture.create}, {status: fixture.noop}}}
	service := testService(t, fixture.cellKey, cycle)

	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("create-candidate cycle: %v", err)
	}
	create := service.Snapshot()
	if ValidateSnapshot(create) != nil || create.Ready || create.Converged || create.Blocked || !create.MutationCandidate ||
		create.Retryable || create.AttemptCount != 1 || create.ConsecutiveFailures != 0 || create.FailureCode != "" ||
		create.CurrentStatus == nil || create.CurrentStatus.Action != reconcile.ActionCreateIfAbsent ||
		create.LastValidStatus == nil || create.LastValidStatus.Digest != create.CurrentStatus.Digest ||
		create.LastAttemptAt == nil || create.LastEvaluationAt == nil {
		t.Fatalf("create snapshot drifted: %#v", create)
	}

	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("converged cycle: %v", err)
	}
	noop := service.Snapshot()
	if ValidateSnapshot(noop) != nil || !noop.Ready || !noop.Converged || noop.LastKnownGoodServing ||
		noop.Blocked || noop.Retryable || noop.MutationCandidate || noop.AttemptCount != 2 ||
		noop.CurrentStatus == nil || noop.CurrentStatus.Action != reconcile.ActionNoop {
		t.Fatalf("noop snapshot drifted: %#v", noop)
	}

	noop.CurrentStatus.Decision.Action = reconcile.ActionBlock
	noop.LastValidStatus.Decision.Action = reconcile.ActionBlock
	fresh := service.Snapshot()
	if fresh.CurrentStatus == nil || fresh.CurrentStatus.Action != reconcile.ActionNoop ||
		fresh.CurrentStatus.Decision.Action != reconcile.ActionNoop || fresh.LastValidStatus.Decision.Action != reconcile.ActionNoop {
		t.Fatalf("caller mutated retained status: %#v", fresh)
	}
}

func TestAgentFailureIsLaneLocalAndNextCycleRecovers(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t)
	cycle := &sequenceCycle{results: []cycleResult{
		{status: fixture.noop},
		{err: errors.New("bearer private-remote-body")},
		{status: fixture.currentUnavailable},
		{status: fixture.noop},
	}}
	service := testService(t, fixture.cellKey, cycle)
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("initial cycle: %v", err)
	}
	if err := service.ReconcileOnce(context.Background()); !errors.Is(err, ErrService) {
		t.Fatalf("cycle error = %v, want fixed service error", err)
	}
	failed := service.Snapshot()
	if ValidateSnapshot(failed) != nil || failed.Ready || failed.CurrentStatus != nil || failed.FailureCode != FailureCycleError ||
		failed.ConsecutiveFailures != 1 || failed.AttemptCount != 2 || failed.LastValidStatus == nil ||
		failed.LastValidStatus.Digest != fixture.noop.Digest || strings.Contains(fmt.Sprint(failed), "private-remote-body") {
		t.Fatalf("lane-local failure snapshot drifted: %#v", failed)
	}
	missingEvaluation := cloneSnapshot(failed)
	missingEvaluation.LastEvaluationAt = nil
	missingEvaluation.Digest = DigestSnapshot(missingEvaluation)
	if err := ValidateSnapshot(missingEvaluation); !errors.Is(err, ErrService) {
		t.Fatalf("failure snapshot detached LKG evaluation time: %v", err)
	}
	driftedEvaluation := cloneSnapshot(failed)
	value := driftedEvaluation.LastEvaluationAt.Add(time.Second)
	driftedEvaluation.LastEvaluationAt = &value
	driftedEvaluation.Digest = DigestSnapshot(driftedEvaluation)
	if err := ValidateSnapshot(driftedEvaluation); !errors.Is(err, ErrService) {
		t.Fatalf("failure snapshot accepted mismatched LKG evaluation time: %v", err)
	}
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("valid unavailable cycle: %v", err)
	}
	unavailable := service.Snapshot()
	if unavailable.Ready || !unavailable.Blocked || !unavailable.Retryable || unavailable.FailureCode != "" ||
		unavailable.ConsecutiveFailures != 0 || unavailable.CurrentStatus == nil ||
		unavailable.CurrentStatus.CurrentState != reconciler.CurrentUnavailable {
		t.Fatalf("current-unavailable status escaped valid cycle semantics: %#v", unavailable)
	}
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("recovery cycle: %v", err)
	}
	recovered := service.Snapshot()
	if !recovered.Ready || !recovered.Converged || recovered.FailureCode != "" || recovered.ConsecutiveFailures != 0 ||
		recovered.AttemptCount != 4 {
		t.Fatalf("agent did not recover lane-locally: %#v", recovered)
	}
}

func TestAgentRejectsInvalidAndCrossCellCycleResults(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t)
	other := testStatusFixtureForTarget(t, backupcontrol.BackupTarget{
		Type: backupcontrol.TargetRegistry, ScopeKey: "platform/registry",
	})
	invalid := fixture.noop
	invalid.ProductionMutationAllowed = true
	invalid.Digest = reconciler.DigestStatus(invalid)
	for name, status := range map[string]reconciler.Status{"invalid": invalid, "cross-cell": other.create} {
		status := status
		t.Run(name, func(t *testing.T) {
			service := testService(t, fixture.cellKey, &sequenceCycle{results: []cycleResult{{status: status}}})
			if err := service.ReconcileOnce(context.Background()); !errors.Is(err, ErrService) {
				t.Fatalf("invalid result error = %v", err)
			}
			snapshot := service.Snapshot()
			if ValidateSnapshot(snapshot) != nil || snapshot.FailureCode != FailureCycleInvalid || snapshot.Ready ||
				snapshot.CurrentStatus != nil || snapshot.LastValidStatus != nil || snapshot.ConsecutiveFailures != 1 {
				t.Fatalf("invalid cycle result was retained: %#v", snapshot)
			}
		})
	}
}

func TestAgentTimeoutCancellationAndSerialAttemptBoundary(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t)

	t.Run("attempt timeout", func(t *testing.T) {
		cycle := cycleFunc(func(ctx context.Context) (reconciler.Status, error) {
			<-ctx.Done()
			return reconciler.Status{}, ctx.Err()
		})
		service := testService(t, fixture.cellKey, cycle)
		service.attemptTimeout = 10 * time.Millisecond
		if err := service.ReconcileOnce(context.Background()); !errors.Is(err, ErrService) {
			t.Fatalf("timeout error = %v", err)
		}
		snapshot := service.Snapshot()
		if snapshot.FailureCode != FailureAttemptTimeout || snapshot.Reconciling || snapshot.Ready || snapshot.ConsecutiveFailures != 1 {
			t.Fatalf("timeout snapshot drifted: %#v", snapshot)
		}
	})

	t.Run("parent cancellation", func(t *testing.T) {
		started := make(chan struct{})
		cycle := cycleFunc(func(ctx context.Context) (reconciler.Status, error) {
			close(started)
			<-ctx.Done()
			return reconciler.Status{}, ctx.Err()
		})
		service := testService(t, fixture.cellKey, cycle)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- service.ReconcileOnce(ctx) }()
		<-started
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
		snapshot := service.Snapshot()
		if snapshot.FailureCode != FailureCanceled || snapshot.Reconciling || snapshot.Ready {
			t.Fatalf("canceled snapshot drifted: %#v", snapshot)
		}
	})

	t.Run("serial attempts and reconciling snapshot", func(t *testing.T) {
		var active atomic.Int64
		var maximum atomic.Int64
		started := make(chan struct{}, 2)
		release := make(chan struct{}, 2)
		cycle := cycleFunc(func(context.Context) (reconciler.Status, error) {
			current := active.Add(1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			active.Add(-1)
			return fixture.noop, nil
		})
		service := testService(t, fixture.cellKey, cycle)
		errorsFound := make(chan error, 2)
		go func() { errorsFound <- service.ReconcileOnce(context.Background()) }()
		<-started
		midflight := service.Snapshot()
		if !midflight.Reconciling || midflight.AttemptCount != 1 || ValidateSnapshot(midflight) != nil {
			t.Fatalf("midflight snapshot drifted: %#v", midflight)
		}
		go func() { errorsFound <- service.ReconcileOnce(context.Background()) }()
		time.Sleep(10 * time.Millisecond)
		if maximum.Load() != 1 {
			t.Fatalf("overlapping cycle entered before release: maximum=%d", maximum.Load())
		}
		release <- struct{}{}
		<-started
		release <- struct{}{}
		for range 2 {
			if err := <-errorsFound; err != nil {
				t.Fatalf("serialized cycle failed: %v", err)
			}
		}
		if maximum.Load() != 1 || service.Snapshot().AttemptCount != 2 {
			t.Fatalf("attempts overlapped or disappeared: maximum=%d snapshot=%#v", maximum.Load(), service.Snapshot())
		}
	})
}

func TestAgentRunRetriesWithoutLoggingCycleErrorsAndStopsCleanly(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t)
	cycle := &sequenceCycle{results: []cycleResult{{err: errors.New("bearer secret response")}, {status: fixture.noop}}}
	var logs bytes.Buffer
	service, err := New(Config{
		Enabled: true, CellKey: fixture.cellKey, Cycle: cycle, Interval: time.Second, AttemptTimeout: time.Second,
		Now: func() time.Time { return fixture.now },
	}, log.New(&logs, "", 0))
	if err != nil {
		t.Fatalf("construct service: %v", err)
	}
	service.interval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		// Close the observation-to-cancellation gap against the same mutex
		// that gates a new attempt. Otherwise a heavily instrumented runner
		// can start attempt three after Ready was observed but before cancel,
		// and that legitimately records the parent cancellation as a failure.
		service.attemptMu.Lock()
		ready := service.Snapshot().Ready
		if ready {
			cancel()
			service.attemptMu.Unlock()
			break
		}
		service.attemptMu.Unlock()
		select {
		case <-deadline.C:
			t.Fatalf("run did not retry: calls=%d snapshot=%#v", cycle.Calls(), service.Snapshot())
		case <-poll.C:
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("run shutdown: %v", err)
	}
	if strings.Contains(logs.String(), "bearer secret response") || !strings.Contains(logs.String(), FailureCycleError) {
		t.Fatalf("run log leaked cause or omitted fixed code: %q", logs.String())
	}
	if !service.Snapshot().Ready {
		t.Fatalf("retry did not recover: %#v", service.Snapshot())
	}
}

func TestAgentHandlerIsReadOnlySecretFreeAndStatusExact(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t)
	service := testService(t, fixture.cellKey, &sequenceCycle{results: []cycleResult{{status: fixture.noop}}})
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for path, wantStatus := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusOK, "/v1/status": http.StatusOK} {
		recorder := httptest.NewRecorder()
		service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		body := append([]byte(nil), recorder.Body.Bytes()...)
		if recorder.Code != wantStatus || recorder.Header().Get("Content-Type") != "application/json" ||
			recorder.Header().Get("Cache-Control") != "private, no-store" {
			t.Fatalf("%s response drifted: status=%d headers=%v body=%s", path, recorder.Code, recorder.Header(), body)
		}
		var snapshot Snapshot
		decoder := json.NewDecoder(bytes.NewReader(body))
		if err := decoder.Decode(&snapshot); err != nil || ValidateSnapshot(snapshot) != nil {
			t.Fatalf("decode %s snapshot: snapshot=%#v err=%v", path, snapshot, err)
		}
		if strings.Contains(string(body), "observer-token") || strings.Contains(string(body), "tenant-1") {
			t.Fatalf("%s exposed private input: %s", path, body)
		}
	}
	for method, path := range map[string]string{http.MethodPost: "/v1/status", http.MethodPut: "/readyz", http.MethodDelete: "/healthz"} {
		recorder := httptest.NewRecorder()
		service.Handler().ServeHTTP(recorder, httptest.NewRequest(method, path, strings.NewReader("command")))
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s status = %d, want method not allowed", method, path, recorder.Code)
		}
	}
	recorder := httptest.NewRecorder()
	service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("unknown route status = %d", recorder.Code)
	}
}

func TestAgentConfigurationAndSnapshotValidationFailClosed(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t)
	var typedNil *sequenceCycle
	configs := []Config{
		{Enabled: true, CellKey: "invalid", Cycle: &sequenceCycle{}},
		{Enabled: true, CellKey: fixture.cellKey},
		{Enabled: true, CellKey: fixture.cellKey, Cycle: typedNil},
		{Enabled: true, CellKey: fixture.cellKey, Cycle: &sequenceCycle{}, Interval: time.Second - time.Millisecond},
		{Enabled: true, CellKey: fixture.cellKey, Cycle: &sequenceCycle{}, Interval: maximumInterval + time.Second},
		{Enabled: true, CellKey: fixture.cellKey, Cycle: &sequenceCycle{}, AttemptTimeout: time.Second + time.Nanosecond},
		{Enabled: true, CellKey: fixture.cellKey, Cycle: &sequenceCycle{}, AttemptTimeout: maximumAttemptTimeout + time.Second},
	}
	for index, config := range configs {
		if service, err := New(config, nil); !errors.Is(err, ErrConfig) || service != nil {
			t.Fatalf("config %d result = %#v, %v; want nil ErrConfig", index, service, err)
		}
	}
	var nilService *Service
	if nilService.Enabled() || nilService.Snapshot() != (Snapshot{}) ||
		nilService.String() != "backup materializer agent <nil>" || nilService.GoString() != nilService.String() {
		t.Fatal("nil service behavior drifted")
	}
	if err := nilService.ReconcileOnce(context.Background()); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil service reconcile error = %v", err)
	}
	if err := testService(t, fixture.cellKey, &sequenceCycle{}).ReconcileOnce(nil); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil context error = %v", err)
	}

	service := testService(t, fixture.cellKey, &sequenceCycle{results: []cycleResult{{status: fixture.noop}}})
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("baseline reconcile: %v", err)
	}
	baseline := service.Snapshot()
	mutations := map[string]func(*Snapshot){
		"API":                  func(value *Snapshot) { value.APIVersion = "v2" },
		"kind":                 func(value *Snapshot) { value.Kind = "Other" },
		"mode":                 func(value *Snapshot) { value.Mode = ModeDisabled },
		"cell":                 func(value *Snapshot) { value.CellKey = "backup/registry/0123456789abcdef" },
		"ready":                func(value *Snapshot) { value.Ready = false },
		"converged":            func(value *Snapshot) { value.Converged = false },
		"blocked":              func(value *Snapshot) { value.Blocked = true },
		"retryable":            func(value *Snapshot) { value.Retryable = true },
		"mutation":             func(value *Snapshot) { value.MutationCandidate = true },
		"delete":               func(value *Snapshot) { value.DeleteAllowed = true },
		"observation":          func(value *Snapshot) { value.ObservationOnly = false },
		"execution":            func(value *Snapshot) { value.ExecutionAllowed = true },
		"production":           func(value *Snapshot) { value.ProductionMutationAllowed = true },
		"attempt count":        func(value *Snapshot) { value.AttemptCount = 0 },
		"failures":             func(value *Snapshot) { value.ConsecutiveFailures = 1 },
		"attempt time":         func(value *Snapshot) { value.LastAttemptAt = nil },
		"evaluation time":      func(value *Snapshot) { value.LastEvaluationAt = nil },
		"failure code":         func(value *Snapshot) { value.FailureCode = "private-error" },
		"current missing":      func(value *Snapshot) { value.CurrentStatus = nil },
		"last valid missing":   func(value *Snapshot) { value.LastValidStatus = nil },
		"nested current drift": func(value *Snapshot) { value.CurrentStatus.ExecutionAllowed = true },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := cloneSnapshot(baseline)
			mutate(&candidate)
			candidate.Digest = DigestSnapshot(candidate)
			if err := ValidateSnapshot(candidate); !errors.Is(err, ErrService) {
				t.Fatalf("mutated snapshot error = %v, want ErrService: %#v", err, candidate)
			}
		})
	}
	badDigest := cloneSnapshot(baseline)
	badDigest.Digest = "sha256:" + strings.Repeat("0", 64)
	if err := ValidateSnapshot(badDigest); !errors.Is(err, ErrService) {
		t.Fatalf("snapshot digest drift error = %v", err)
	}
}

func TestAgentProductionDependencyBoundary(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-deps", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list agent dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{
			"database/sql", "os/exec", "fugue/internal/backupidentity", "fugue/internal/backupmaterializer",
		} {
			if dependency == forbidden {
				t.Fatalf("agent dependency widened to %q", dependency)
			}
		}
		for _, prefix := range []string{
			"k8s.io/", "fugue/internal/api", "fugue/internal/store", "fugue/internal/model",
			"fugue/internal/backupmaterializer/client", "fugue/internal/backupmaterializer/secretreader",
			"fugue/internal/backupmaterializerreview", "fugue/internal/backupmaterializeridentity",
		} {
			if strings.HasPrefix(dependency, prefix) {
				t.Fatalf("agent crossed capability boundary through %q", dependency)
			}
		}
	}
	sort.Strings(local)
	wantLocal := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/agent",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconcile",
		"fugue/internal/backupmaterializer/reconciler",
	}
	if !reflect.DeepEqual(local, wantLocal) {
		t.Fatalf("agent local dependency closure drifted: got=%v want=%v", local, wantLocal)
	}
	direct := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	directOutput, err := direct.Output()
	if err != nil {
		t.Fatalf("list direct agent imports: %v", err)
	}
	gotDirect := strings.Fields(string(directOutput))
	sort.Strings(gotDirect)
	wantDirect := []string{
		"context",
		"crypto/sha256",
		"encoding/hex",
		"encoding/json",
		"errors",
		"fmt",
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconciler",
		"io",
		"log",
		"net/http",
		"reflect",
		"sync",
		"time",
	}
	if !reflect.DeepEqual(gotDirect, wantDirect) {
		t.Fatalf("agent direct dependency boundary drifted: got=%v want=%v", gotDirect, wantDirect)
	}
}

type cycleResult struct {
	status reconciler.Status
	err    error
}

type sequenceCycle struct {
	mu      sync.Mutex
	results []cycleResult
	calls   int
}

func (cycle *sequenceCycle) ReconcileOnce(context.Context) (reconciler.Status, error) {
	cycle.mu.Lock()
	defer cycle.mu.Unlock()
	cycle.calls++
	if len(cycle.results) == 0 {
		return reconciler.Status{}, errors.New("cycle result unavailable")
	}
	index := cycle.calls - 1
	if index >= len(cycle.results) {
		index = len(cycle.results) - 1
	}
	return cycle.results[index].status, cycle.results[index].err
}

func (cycle *sequenceCycle) Calls() int {
	cycle.mu.Lock()
	defer cycle.mu.Unlock()
	return cycle.calls
}

type cycleFunc func(context.Context) (reconciler.Status, error)

func (function cycleFunc) ReconcileOnce(ctx context.Context) (reconciler.Status, error) {
	return function(ctx)
}

type statusFixture struct {
	now                time.Time
	cellKey            string
	create             reconciler.Status
	noop               reconciler.Status
	currentUnavailable reconciler.Status
}

func testStatusFixture(t *testing.T) statusFixture {
	t.Helper()
	return testStatusFixtureForTarget(t, backupcontrol.BackupTarget{
		Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database",
	})
}

func testStatusFixtureForTarget(t *testing.T, target backupcontrol.BackupTarget) statusFixture {
	t.Helper()
	now := time.Date(2026, 8, 3, 1, 0, 30, 0, time.UTC)
	bundle, plan := testGeneration(t, target, now.Add(-30*time.Second), now)
	absent, err := reconcile.ObserveAbsent(plan.CellKey)
	if err != nil {
		t.Fatalf("observe absent: %v", err)
	}
	create := runReconciler(t, plan.CellKey, now, &desiredSource{bundle: bundle}, &currentSource{observation: absent})
	managed := testManaged(t, plan, now)
	noop := runReconciler(t, plan.CellKey, now, &desiredSource{bundle: bundle}, &currentSource{observation: managed})
	currentUnavailable := runReconciler(t, plan.CellKey, now, &desiredSource{bundle: bundle}, &currentSource{err: errors.New("unavailable")})
	return statusFixture{now: now, cellKey: plan.CellKey, create: create, noop: noop, currentUnavailable: currentUnavailable}
}

type desiredSource struct {
	bundle materializercontract.ObserverInputBundle
}

func (source *desiredSource) Fetch(context.Context) (materializercontract.ObserverInputBundle, error) {
	return source.bundle, nil
}

type currentSource struct {
	observation reconcile.Observation
	err         error
}

func (source *currentSource) Observe(context.Context) (reconcile.Observation, error) {
	return source.observation, source.err
}

func runReconciler(t *testing.T, cellKey string, now time.Time, desired reconciler.DesiredSource, current reconciler.CurrentSource) reconciler.Status {
	t.Helper()
	cycle, err := reconciler.New(reconciler.Config{
		Enabled: true, CellKey: cellKey, DesiredSource: desired, CurrentSource: current,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("construct reconciler: %v", err)
	}
	status, err := cycle.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("run reconciler: %v", err)
	}
	return status
}

func testGeneration(
	t *testing.T,
	target backupcontrol.BackupTarget,
	issuedAt time.Time,
	buildAt time.Time,
) (materializercontract.ObserverInputBundle, materialization.Plan) {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		"run-1", "run-1", target, "backend-1",
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 4, 120, 1800,
	)
	if err != nil {
		t.Fatalf("build backup spec: %v", err)
	}
	keyring := backupidentity.DeriveKeyring(strings.Repeat("k", 32), "backup-key-1", "", "", nil)
	bundle, err := backupmaterializer.IssueObserverInputBundle(keyring, spec, "tenant-1", issuedAt)
	if err != nil {
		t.Fatalf("issue input bundle: %v", err)
	}
	plan, err := materialization.Build(bundle, buildAt)
	if err != nil {
		t.Fatalf("build materialization plan: %v", err)
	}
	return bundle, plan
}

func testManaged(t *testing.T, plan materialization.Plan, now time.Time) reconcile.Observation {
	t.Helper()
	manifest, err := reconcile.BuildManifest(plan, now)
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	data, err := plan.Data(now)
	if err != nil {
		t.Fatalf("read plan data: %v", err)
	}
	snapshot, err := reconcile.SealCurrent(plan, reconcile.SecretEvidence{
		Namespace: manifest.Namespace, SecretName: manifest.SecretName,
		UID: "01234567-89ab-cdef-0123-456789abcdef", ResourceVersion: "42", SecretType: manifest.SecretType,
		Labels: cloneMap(manifest.Labels), Annotations: cloneMap(manifest.Annotations),
		Data: map[string][]byte{
			data.SpecKey: append([]byte(nil), data.SpecDocument...), data.TokenKey: append([]byte(nil), data.ObserverToken...),
		},
	})
	if err != nil {
		t.Fatalf("seal current: %v", err)
	}
	observation, err := reconcile.ObserveManaged(snapshot)
	if err != nil {
		t.Fatalf("observe managed: %v", err)
	}
	return observation
}

func cloneMap(value map[string]string) map[string]string {
	cloned := make(map[string]string, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}

func testService(t *testing.T, cellKey string, cycle Cycle) *Service {
	t.Helper()
	service, err := New(Config{
		Enabled: true, CellKey: cellKey, Cycle: cycle, Interval: time.Second, AttemptTimeout: time.Second,
		Now: func() time.Time { return time.Date(2026, 8, 3, 1, 0, 29, 0, time.UTC) },
	}, nil)
	if err != nil {
		t.Fatalf("construct agent: %v", err)
	}
	return service
}
