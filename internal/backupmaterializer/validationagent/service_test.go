package validationagent

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
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
	"fugue/internal/backupmaterializer"
	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/dryrunreconciler"
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
	"fugue/internal/backupmaterializer/reconciler"
	"fugue/internal/backupmaterializer/secretwriter"
	"fugue/internal/backupmaterializer/validationcycle"
)

var _ Cycle = (*validationcycle.Cycle)(nil)

type cycleResult struct {
	status validationcycle.Status
	err    error
}

type sequenceCycle struct {
	enabled bool
	results []cycleResult
	calls   atomic.Int64
}

func (cycle *sequenceCycle) Enabled() bool { return cycle != nil && cycle.enabled }

func (cycle *sequenceCycle) ReconcileOnce(context.Context) (validationcycle.Status, error) {
	index := int(cycle.calls.Add(1) - 1)
	if len(cycle.results) == 0 {
		return validationcycle.Status{}, errors.New("empty private cycle")
	}
	if index >= len(cycle.results) {
		index = len(cycle.results) - 1
	}
	return cycle.results[index].status, cycle.results[index].err
}

func (cycle *sequenceCycle) Calls() int64 { return cycle.calls.Load() }

type cycleFunc struct {
	fn func(context.Context) (validationcycle.Status, error)
}

func (*cycleFunc) Enabled() bool { return true }

func (cycle *cycleFunc) ReconcileOnce(ctx context.Context) (validationcycle.Status, error) {
	return cycle.fn(ctx)
}

func TestDisabledValidationAgentIsInertAndFailsReadinessClosed(t *testing.T) {
	t.Parallel()
	cycle := &sequenceCycle{enabled: true, results: []cycleResult{{err: errors.New("must not run private")}}}
	config := Config{
		Enabled: false, CellKey: "private-invalid-cell", Cycle: cycle,
		Interval: -1, AttemptTimeout: -1, Now: func() time.Time { panic("disabled agent read clock") },
	}
	service, err := New(config, log.New(&bytes.Buffer{}, "private-prefix", 0))
	if err != nil || service.Enabled() || service.cellKey != "" || service.cycle != nil ||
		service.now != nil || service.logger != nil {
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
	for path, wantStatus := range map[string]int{
		"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable, "/v1/status": http.StatusOK,
	} {
		recorder := httptest.NewRecorder()
		service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != wantStatus || recorder.Header().Get("Cache-Control") != "private, no-store" ||
			recorder.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("disabled %s response drifted: status=%d headers=%v body=%s", path, recorder.Code, recorder.Header(), recorder.Body.String())
		}
	}
	rendered := strings.Join([]string{
		fmt.Sprint(config), fmt.Sprintf("%#v", config), fmt.Sprint(service), fmt.Sprintf("%#v", service),
	}, "\n")
	if !strings.Contains(rendered, "[REDACTED]") || strings.Contains(rendered, config.CellKey) ||
		strings.Contains(rendered, "private-prefix") {
		t.Fatalf("disabled diagnostics exposed retained input: %s", rendered)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := service.Run(ctx); err != nil {
		t.Fatalf("disabled run after cancellation: %v", err)
	}
}

func TestValidationAgentTracksAcceptedExpectedFailureAndConvergence(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t, testAppTarget(), "agent-track")
	cycle := &sequenceCycle{enabled: true, results: []cycleResult{
		{status: fixture.accepted}, {status: fixture.conflict}, {status: fixture.noop},
	}}
	service := testService(t, fixture.cellKey, cycle, nil)

	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("accepted cycle: %v", err)
	}
	accepted := service.Snapshot()
	if ValidateSnapshot(accepted) != nil || accepted.Ready || accepted.Converged || accepted.Blocked || accepted.Retryable ||
		!accepted.MutationCandidate || !accepted.ValidationRequired || !accepted.ValidationAttempted ||
		!accepted.ValidationAccepted || accepted.ValidationOutcome != dryrunreconciler.OutcomeAccepted ||
		accepted.ExistingObjectPreserved || accepted.Persisted || accepted.DeleteAllowed || !accepted.ObservationOnly ||
		accepted.ExecutionAllowed || accepted.ProductionMutationAllowed || accepted.AttemptCount != 1 ||
		accepted.ConsecutiveFailures != 0 || accepted.CurrentStatus == nil || accepted.LastValidStatus == nil {
		t.Fatalf("accepted snapshot drifted: %#v", accepted)
	}
	assertSnapshotSecretFree(t, accepted, fixture)

	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("expected conflict cycle became service failure: %v", err)
	}
	conflict := service.Snapshot()
	if ValidateSnapshot(conflict) != nil || conflict.Ready || !conflict.Blocked || !conflict.Retryable ||
		!conflict.ValidationRequired || !conflict.ValidationAttempted || conflict.ValidationAccepted ||
		conflict.ValidationOutcome != dryrunreconciler.OutcomeConflict || conflict.FailureCode != "" ||
		conflict.ConsecutiveFailures != 0 || conflict.AttemptCount != 2 {
		t.Fatalf("cell-local conflict snapshot drifted: %#v", conflict)
	}

	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("converged cycle: %v", err)
	}
	converged := service.Snapshot()
	if ValidateSnapshot(converged) != nil || !converged.Ready || !converged.Converged ||
		converged.LastKnownGoodServing || converged.Blocked || converged.Retryable || converged.MutationCandidate ||
		converged.ValidationRequired || converged.ValidationAttempted || converged.ValidationAccepted ||
		converged.ValidationOutcome != "" || converged.AttemptCount != 3 || converged.CurrentStatus == nil ||
		converged.LastValidStatus == nil || converged.LastEvaluationAt == nil {
		t.Fatalf("converged snapshot drifted: %#v", converged)
	}

	originalDigest := converged.CurrentStatus.Digest
	converged.CurrentStatus.PreparedCycle.Status.Action = reconcile.ActionBlock
	converged.CurrentStatus.PreparedCycle.Status.Decision.Action = reconcile.ActionBlock
	converged.LastValidStatus.PreparedCycle.Status.Decision.Action = reconcile.ActionBlock
	*converged.LastEvaluationAt = time.Time{}
	fresh := service.Snapshot()
	if fresh.CurrentStatus == nil || fresh.CurrentStatus.Digest != originalDigest ||
		fresh.CurrentStatus.PreparedCycle.Status.Action != reconcile.ActionNoop ||
		fresh.CurrentStatus.PreparedCycle.Status.Decision.Action != reconcile.ActionNoop ||
		fresh.LastValidStatus.PreparedCycle.Status.Decision.Action != reconcile.ActionNoop ||
		fresh.LastEvaluationAt == nil || fresh.LastEvaluationAt.IsZero() {
		t.Fatalf("caller mutated retained validation evidence: %#v", fresh)
	}
}

func TestValidationAgentFailureIsLaneLocalAndNextCycleRecovers(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t, testAppTarget(), "agent-recover")
	cycle := &sequenceCycle{enabled: true, results: []cycleResult{
		{status: fixture.noop}, {err: errors.New("bearer private-remote-body")}, {status: fixture.noop},
	}}
	service := testService(t, fixture.cellKey, cycle, nil)
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("initial cycle: %v", err)
	}
	if err := service.ReconcileOnce(context.Background()); !errors.Is(err, ErrService) {
		t.Fatalf("cycle error = %v, want fixed service error", err)
	}
	failed := service.Snapshot()
	if ValidateSnapshot(failed) != nil || failed.Ready || failed.CurrentStatus != nil ||
		failed.FailureCode != FailureCycleError || failed.ConsecutiveFailures != 1 || failed.AttemptCount != 2 ||
		failed.LastValidStatus == nil || failed.LastValidStatus.Digest != fixture.noop.Digest ||
		failed.LastEvaluationAt == nil || strings.Contains(fmt.Sprint(failed), "private-remote-body") {
		t.Fatalf("lane-local failure snapshot drifted: %#v", failed)
	}
	missingEvaluation := cloneSnapshot(failed)
	missingEvaluation.LastEvaluationAt = nil
	missingEvaluation.Digest = DigestSnapshot(missingEvaluation)
	if err := ValidateSnapshot(missingEvaluation); !errors.Is(err, ErrService) {
		t.Fatalf("failure snapshot detached LKG evaluation time: %v", err)
	}
	impossibleFailures := cloneSnapshot(failed)
	impossibleFailures.ConsecutiveFailures = impossibleFailures.AttemptCount + 1
	impossibleFailures.Digest = DigestSnapshot(impossibleFailures)
	if err := ValidateSnapshot(impossibleFailures); !errors.Is(err, ErrService) {
		t.Fatalf("failure snapshot exceeded attempt count: %v", err)
	}
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("recovery cycle: %v", err)
	}
	recovered := service.Snapshot()
	if ValidateSnapshot(recovered) != nil || !recovered.Ready || !recovered.Converged || recovered.FailureCode != "" ||
		recovered.ConsecutiveFailures != 0 || recovered.AttemptCount != 3 {
		t.Fatalf("agent did not recover lane-locally: %#v", recovered)
	}
}

func TestValidationAgentContainsCyclePanicAndDoesNotExposeValue(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t, testAppTarget(), "agent-panic")
	service := testService(t, fixture.cellKey, &cycleFunc{fn: func(context.Context) (validationcycle.Status, error) {
		panic("bearer private-panic-value")
	}}, nil)
	if err := service.ReconcileOnce(context.Background()); !errors.Is(err, ErrService) {
		t.Fatalf("panic boundary error = %v", err)
	}
	snapshot := service.Snapshot()
	if ValidateSnapshot(snapshot) != nil || snapshot.FailureCode != FailureCycleError || snapshot.CurrentStatus != nil ||
		strings.Contains(fmt.Sprintf("%#v %v", snapshot, snapshot), "private-panic-value") {
		t.Fatalf("panic escaped fixed failure boundary: %#v", snapshot)
	}
}

func TestValidationAgentRejectsInvalidAndCrossCellCycleResults(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t, testAppTarget(), "agent-reject")
	other := testStatusFixture(t, backupcontrol.BackupTarget{
		Type: backupcontrol.TargetRegistry, ScopeKey: "platform/registry",
	}, "agent-other")
	invalid := cloneStatus(fixture.accepted)
	invalid.ProductionMutationAllowed = true
	invalid.Digest = validationcycle.DigestStatus(invalid)
	for name, status := range map[string]validationcycle.Status{"invalid": invalid, "cross-cell": other.accepted} {
		status := status
		t.Run(name, func(t *testing.T) {
			service := testService(t, fixture.cellKey, &sequenceCycle{
				enabled: true, results: []cycleResult{{status: status}},
			}, nil)
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

func TestValidationAgentTimeoutCancellationAndSerialAttemptBoundary(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t, testAppTarget(), "agent-boundary")

	t.Run("attempt timeout", func(t *testing.T) {
		cycle := &cycleFunc{fn: func(ctx context.Context) (validationcycle.Status, error) {
			<-ctx.Done()
			return validationcycle.Status{}, ctx.Err()
		}}
		service := testService(t, fixture.cellKey, cycle, nil)
		service.attemptTimeout = 10 * time.Millisecond
		if err := service.ReconcileOnce(context.Background()); !errors.Is(err, ErrService) {
			t.Fatalf("timeout error = %v", err)
		}
		snapshot := service.Snapshot()
		if ValidateSnapshot(snapshot) != nil || snapshot.FailureCode != FailureAttemptTimeout ||
			snapshot.Reconciling || snapshot.Ready || snapshot.ConsecutiveFailures != 1 {
			t.Fatalf("timeout snapshot drifted: %#v", snapshot)
		}
	})

	t.Run("parent cancellation", func(t *testing.T) {
		started := make(chan struct{})
		cycle := &cycleFunc{fn: func(ctx context.Context) (validationcycle.Status, error) {
			close(started)
			<-ctx.Done()
			return validationcycle.Status{}, ctx.Err()
		}}
		service := testService(t, fixture.cellKey, cycle, nil)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- service.ReconcileOnce(ctx) }()
		<-started
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
		snapshot := service.Snapshot()
		if ValidateSnapshot(snapshot) != nil || snapshot.FailureCode != FailureCanceled ||
			snapshot.Reconciling || snapshot.Ready {
			t.Fatalf("canceled snapshot drifted: %#v", snapshot)
		}
	})

	t.Run("serial attempts and reconciling snapshot", func(t *testing.T) {
		var active atomic.Int64
		var maximum atomic.Int64
		started := make(chan struct{}, 2)
		release := make(chan struct{}, 2)
		cycle := &cycleFunc{fn: func(context.Context) (validationcycle.Status, error) {
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
		}}
		service := testService(t, fixture.cellKey, cycle, nil)
		results := make(chan error, 2)
		go func() { results <- service.ReconcileOnce(context.Background()) }()
		<-started
		go func() { results <- service.ReconcileOnce(context.Background()) }()
		select {
		case <-started:
			t.Fatal("second attempt crossed the serial boundary")
		case <-time.After(20 * time.Millisecond):
		}
		reconciling := service.Snapshot()
		if ValidateSnapshot(reconciling) != nil || !reconciling.Reconciling || reconciling.AttemptCount != 1 {
			t.Fatalf("in-flight snapshot drifted: %#v", reconciling)
		}
		release <- struct{}{}
		<-started
		release <- struct{}{}
		for range 2 {
			if err := <-results; err != nil {
				t.Fatalf("serial attempt: %v", err)
			}
		}
		if maximum.Load() != 1 || service.Snapshot().AttemptCount != 2 {
			t.Fatalf("attempts overlapped: max=%d snapshot=%#v", maximum.Load(), service.Snapshot())
		}
	})
}

func TestValidationAgentRunRecoversAndLogsOnlyFixedEvidence(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t, testAppTarget(), "agent-run")
	cycle := &sequenceCycle{enabled: true, results: []cycleResult{
		{err: errors.New("private bearer and response")}, {status: fixture.noop},
	}}
	var logs bytes.Buffer
	service := testService(t, fixture.cellKey, cycle, log.New(&logs, "validation ", 0))
	service.interval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for cycle.Calls() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run loop: %v", err)
	}
	snapshot := service.Snapshot()
	if cycle.Calls() < 2 || ValidateSnapshot(snapshot) != nil || !snapshot.Ready || snapshot.ConsecutiveFailures != 0 {
		t.Fatalf("run loop did not recover: calls=%d snapshot=%#v", cycle.Calls(), snapshot)
	}
	rendered := logs.String()
	if !strings.Contains(rendered, "code="+FailureCycleError) || strings.Contains(rendered, "private bearer") ||
		strings.Contains(rendered, "response") {
		t.Fatalf("run log drifted: %q", rendered)
	}
}

func TestValidationAgentHandlerIsReadOnlyAndReturnsIndependentStatus(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t, testAppTarget(), "agent-handler")
	cycle := &sequenceCycle{enabled: true, results: []cycleResult{{status: fixture.noop}}}
	service := testService(t, fixture.cellKey, cycle, nil)
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("prime status: %v", err)
	}
	for path, wantStatus := range map[string]int{
		"/healthz": http.StatusOK, "/readyz": http.StatusOK, "/v1/status": http.StatusOK,
	} {
		recorder := httptest.NewRecorder()
		service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != wantStatus || recorder.Header().Get("Content-Type") != "application/json" ||
			recorder.Header().Get("Cache-Control") != "private, no-store" ||
			recorder.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("GET %s drifted: status=%d headers=%v body=%s", path, recorder.Code, recorder.Header(), recorder.Body.String())
		}
		var snapshot Snapshot
		if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil || ValidateSnapshot(snapshot) != nil {
			t.Fatalf("decode GET %s: snapshot=%#v err=%v validation=%v", path, snapshot, err, ValidateSnapshot(snapshot))
		}
	}
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodPost, "/v1/status", strings.NewReader("private-command")),
		httptest.NewRequest(http.MethodPut, "/readyz", nil),
		httptest.NewRequest(http.MethodDelete, "/healthz", nil),
		httptest.NewRequest(http.MethodGet, "/v1/reconcile", nil),
	} {
		recorder := httptest.NewRecorder()
		service.Handler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusMethodNotAllowed && recorder.Code != http.StatusNotFound {
			t.Fatalf("unexpected command route %s %s returned %d", request.Method, request.URL.Path, recorder.Code)
		}
	}
	if cycle.Calls() != 1 {
		t.Fatalf("HTTP reads invoked cycle: calls=%d", cycle.Calls())
	}
}

func TestValidationAgentConfigurationAndFixedErrorBoundary(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t, testAppTarget(), "agent-config")
	validCycle := &sequenceCycle{enabled: true, results: []cycleResult{{status: fixture.noop}}}
	var typedNil *sequenceCycle
	for name, config := range map[string]Config{
		"invalid cell":       {Enabled: true, CellKey: "private-invalid", Cycle: validCycle},
		"nil cycle":          {Enabled: true, CellKey: fixture.cellKey},
		"typed nil cycle":    {Enabled: true, CellKey: fixture.cellKey, Cycle: typedNil},
		"disabled cycle":     {Enabled: true, CellKey: fixture.cellKey, Cycle: &sequenceCycle{}},
		"short interval":     {Enabled: true, CellKey: fixture.cellKey, Cycle: validCycle, Interval: time.Millisecond},
		"long interval":      {Enabled: true, CellKey: fixture.cellKey, Cycle: validCycle, Interval: 11 * time.Minute},
		"unaligned interval": {Enabled: true, CellKey: fixture.cellKey, Cycle: validCycle, Interval: time.Second + time.Microsecond},
		"short timeout":      {Enabled: true, CellKey: fixture.cellKey, Cycle: validCycle, AttemptTimeout: time.Millisecond},
		"long timeout":       {Enabled: true, CellKey: fixture.cellKey, Cycle: validCycle, AttemptTimeout: 61 * time.Second},
		"unaligned timeout":  {Enabled: true, CellKey: fixture.cellKey, Cycle: validCycle, AttemptTimeout: time.Second + time.Microsecond},
	} {
		config := config
		t.Run(name, func(t *testing.T) {
			if service, err := New(config, nil); !errors.Is(err, ErrConfig) || service != nil {
				t.Fatalf("invalid config accepted: service=%#v err=%v", service, err)
			}
		})
	}
	service, err := New(Config{Enabled: true, CellKey: fixture.cellKey, Cycle: validCycle}, nil)
	if err != nil || !service.Enabled() || service.interval != defaultInterval ||
		service.attemptTimeout != defaultAttemptTimeout {
		t.Fatalf("default config drifted: service=%#v err=%v", service, err)
	}
	if err := (*Service)(nil).ReconcileOnce(context.Background()); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil service error = %v", err)
	}
	if err := service.ReconcileOnce(nil); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := service.ReconcileOnce(canceled); !errors.Is(err, context.Canceled) || validCycle.Calls() != 0 {
		t.Fatalf("pre-canceled boundary drifted: err=%v calls=%d", err, validCycle.Calls())
	}
	zeroClock := testService(t, fixture.cellKey, validCycle, nil)
	zeroClock.now = func() time.Time { return time.Time{} }
	if err := zeroClock.ReconcileOnce(context.Background()); !errors.Is(err, ErrConfig) || validCycle.Calls() != 0 {
		t.Fatalf("zero clock boundary drifted: err=%v calls=%d", err, validCycle.Calls())
	}
	if err := (*Service)(nil).Run(context.Background()); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil run error = %v", err)
	}
	if err := service.Run(nil); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil run context error = %v", err)
	}
	if snapshot := (*Service)(nil).Snapshot(); !reflect.DeepEqual(snapshot, Snapshot{}) {
		t.Fatalf("nil snapshot = %#v", snapshot)
	}
}

func TestValidationAgentSnapshotRejectsIndependentMutationMatrix(t *testing.T) {
	t.Parallel()
	fixture := testStatusFixture(t, testAppTarget(), "agent-matrix")
	service := testService(t, fixture.cellKey, &sequenceCycle{
		enabled: true, results: []cycleResult{{status: fixture.accepted}},
	}, nil)
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("prime accepted status: %v", err)
	}
	base := service.Snapshot()
	mutations := map[string]func(*Snapshot){
		"api version":          func(value *Snapshot) { value.APIVersion = "v2" },
		"kind":                 func(value *Snapshot) { value.Kind = "Other" },
		"mode":                 func(value *Snapshot) { value.Mode = "active" },
		"cell":                 func(value *Snapshot) { value.CellKey = "app-database/other/database" },
		"ready":                func(value *Snapshot) { value.Ready = true },
		"converged":            func(value *Snapshot) { value.Converged = true },
		"lkg":                  func(value *Snapshot) { value.LastKnownGoodServing = true },
		"blocked":              func(value *Snapshot) { value.Blocked = true },
		"retryable":            func(value *Snapshot) { value.Retryable = true },
		"mutation candidate":   func(value *Snapshot) { value.MutationCandidate = false },
		"validation required":  func(value *Snapshot) { value.ValidationRequired = false },
		"validation attempted": func(value *Snapshot) { value.ValidationAttempted = false },
		"validation accepted":  func(value *Snapshot) { value.ValidationAccepted = false },
		"existing preserved":   func(value *Snapshot) { value.ExistingObjectPreserved = true },
		"outcome":              func(value *Snapshot) { value.ValidationOutcome = dryrunreconciler.OutcomeRejected },
		"persisted":            func(value *Snapshot) { value.Persisted = true },
		"delete":               func(value *Snapshot) { value.DeleteAllowed = true },
		"observation":          func(value *Snapshot) { value.ObservationOnly = false },
		"execution":            func(value *Snapshot) { value.ExecutionAllowed = true },
		"production":           func(value *Snapshot) { value.ProductionMutationAllowed = true },
		"failure code":         func(value *Snapshot) { value.FailureCode = "private" },
		"current removed":      func(value *Snapshot) { value.CurrentStatus = nil },
		"last valid removed":   func(value *Snapshot) { value.LastValidStatus = nil },
		"evaluation removed":   func(value *Snapshot) { value.LastEvaluationAt = nil },
		"current evidence":     func(value *Snapshot) { value.CurrentStatus.ValidationAccepted = false },
		"last evidence":        func(value *Snapshot) { value.LastValidStatus.ValidationAccepted = false },
	}
	for name, mutate := range mutations {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			value := cloneSnapshot(base)
			mutate(&value)
			value.Digest = DigestSnapshot(value)
			if err := ValidateSnapshot(value); !errors.Is(err, ErrService) {
				t.Fatalf("mutated snapshot accepted: %#v", value)
			}
		})
	}
	badDigest := cloneSnapshot(base)
	badDigest.Digest = "sha256:" + strings.Repeat("0", 64)
	if err := ValidateSnapshot(badDigest); !errors.Is(err, ErrService) {
		t.Fatalf("bad digest accepted: %v", err)
	}
}

func TestValidationAgentDependencyBoundary(t *testing.T) {
	output, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("list validation agent dependencies: %v", err)
	}
	var local []string
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{"database/sql", "os/exec", "fugue/internal/backupmaterializer"} {
			if dependency == forbidden {
				t.Fatalf("validation agent gained forbidden dependency %q", dependency)
			}
		}
		for _, prefix := range []string{
			"k8s.io/", "fugue/internal/api", "fugue/internal/auth", "fugue/internal/store", "fugue/internal/model",
			"fugue/internal/backupmaterializer/agent", "fugue/internal/backupmaterializer/client",
			"fugue/internal/backupmaterializer/composition", "fugue/internal/backupmaterializer/httpapi",
			"fugue/internal/backupmaterializer/legacysource", "fugue/internal/backupmaterializer/localissuer",
			"fugue/internal/backupmaterializer/secretreader", "fugue/internal/backupmaterializer/secretwriter/projected",
			"fugue/internal/backupmaterializer/storesource", "fugue/internal/backupmaterializeridentity",
			"fugue/internal/backupmaterializerreview",
		} {
			if strings.HasPrefix(dependency, prefix) {
				t.Fatalf("validation agent crossed component boundary through %q", dependency)
			}
		}
	}
	sort.Strings(local)
	wantLocal := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/dryrunreconciler",
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconcile",
		"fugue/internal/backupmaterializer/reconciler",
		"fugue/internal/backupmaterializer/secretwriter",
		"fugue/internal/backupmaterializer/validationagent",
		"fugue/internal/backupmaterializer/validationcycle",
	}
	if !reflect.DeepEqual(local, wantLocal) {
		t.Fatalf("validation agent local closure drifted: got=%v want=%v", local, wantLocal)
	}
	directOutput, err := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".").Output()
	if err != nil {
		t.Fatalf("list direct validation agent imports: %v", err)
	}
	gotDirect := strings.Fields(string(directOutput))
	sort.Strings(gotDirect)
	wantDirect := []string{
		"context", "crypto/sha256", "encoding/hex", "encoding/json", "errors", "fmt", "io", "log", "net/http",
		"fugue/internal/backupmaterializer/dryrunreconciler", "fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconciler", "fugue/internal/backupmaterializer/validationcycle",
		"reflect", "sync", "time",
	}
	sort.Strings(wantDirect)
	if !reflect.DeepEqual(gotDirect, wantDirect) {
		t.Fatalf("validation agent direct imports drifted: got=%v want=%v", gotDirect, wantDirect)
	}
}

type statusFixture struct {
	cellKey      string
	accepted     validationcycle.Status
	conflict     validationcycle.Status
	noop         validationcycle.Status
	privateSpec  string
	privateToken string
}

func testStatusFixture(t *testing.T, target backupcontrol.BackupTarget, suffix string) statusFixture {
	t.Helper()
	issuedAt := testTime()
	evaluatedAt := issuedAt.Add(30 * time.Second)
	bundle, plan := testGeneration(t, "run-"+suffix, target, issuedAt, evaluatedAt)
	createPrepared := testPrepared(
		t, plan.CellKey, evaluatedAt, desiredStub{bundle: bundle}, currentStub{observation: testAbsent(t, plan.CellKey)},
	)
	accepted := testValidationStatus(t, plan, createPrepared, nil, evaluatedAt.Add(time.Second))
	conflict := testValidationStatus(t, plan, createPrepared, secretwriter.ErrConflict, evaluatedAt.Add(time.Second))
	noopPrepared := testPrepared(
		t, plan.CellKey, evaluatedAt, desiredStub{bundle: bundle}, currentStub{observation: testManaged(t, plan, evaluatedAt)},
	)
	noop := testValidationStatus(t, plan, noopPrepared, nil, evaluatedAt.Add(time.Second))
	data, err := plan.Data(evaluatedAt)
	if err != nil {
		t.Fatalf("read private plan data: %v", err)
	}
	return statusFixture{
		cellKey: plan.CellKey, accepted: accepted, conflict: conflict, noop: noop,
		privateSpec: string(data.SpecDocument), privateToken: string(data.ObserverToken),
	}
}

type desiredStub struct {
	bundle materializercontract.ObserverInputBundle
	err    error
}

func (stub desiredStub) Fetch(context.Context) (materializercontract.ObserverInputBundle, error) {
	return stub.bundle, stub.err
}

type currentStub struct {
	observation reconcile.Observation
	err         error
}

func (stub currentStub) Observe(context.Context) (reconcile.Observation, error) {
	return stub.observation, stub.err
}

type preparedSource struct {
	prepared reconciler.PreparedCycle
}

func (*preparedSource) Enabled() bool { return true }

func (source *preparedSource) PrepareOnce(context.Context) (reconciler.PreparedCycle, error) {
	return source.prepared, nil
}

type writerStub struct {
	result secretwriter.Result
	err    error
}

func (*writerStub) Enabled() bool { return true }

func (writer *writerStub) DryRun(
	context.Context,
	materialization.Plan,
	reconcile.Decision,
) (secretwriter.Result, error) {
	return writer.result, writer.err
}

func testValidationStatus(
	t *testing.T,
	plan materialization.Plan,
	prepared reconciler.PreparedCycle,
	writerErr error,
	validatedAt time.Time,
) validationcycle.Status {
	t.Helper()
	writer := &writerStub{err: writerErr}
	if writerErr == nil && prepared.CandidateAvailable {
		writer.result = acceptedReceipt(plan, prepared.Status, validatedAt)
	}
	validator, err := dryrunreconciler.New(dryrunreconciler.Config{
		Enabled: true, CellKey: plan.CellKey, Validator: writer, Now: func() time.Time { return validatedAt },
	})
	if err != nil {
		t.Fatalf("construct dry-run controller: %v", err)
	}
	cycle, err := validationcycle.New(validationcycle.Config{
		Enabled: true, CellKey: plan.CellKey, Source: &preparedSource{prepared: prepared}, Validator: validator,
	})
	if err != nil {
		t.Fatalf("construct validation cycle: %v", err)
	}
	status, err := cycle.ReconcileOnce(context.Background())
	if err != nil || validationcycle.ValidateStatus(status) != nil {
		t.Fatalf("produce validation status: status=%#v err=%v validation=%v", status, err, validationcycle.ValidateStatus(status))
	}
	return status
}

func testPrepared(
	t *testing.T,
	cellKey string,
	now time.Time,
	desired reconciler.DesiredSource,
	current reconciler.CurrentSource,
) reconciler.PreparedCycle {
	t.Helper()
	controller, err := reconciler.New(reconciler.Config{
		Enabled: true, CellKey: cellKey, DesiredSource: desired, CurrentSource: current, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("construct source reconciler: %v", err)
	}
	prepared, err := controller.PrepareOnce(context.Background())
	if err != nil || reconciler.ValidatePreparedCycle(prepared) != nil {
		t.Fatalf("prepare source cycle: prepared=%#v err=%v validation=%v", prepared, err, reconciler.ValidatePreparedCycle(prepared))
	}
	return prepared
}

func acceptedReceipt(plan materialization.Plan, status reconciler.Status, validatedAt time.Time) secretwriter.Result {
	result := secretwriter.Result{
		APIVersion: secretwriter.APIVersion, Kind: secretwriter.Kind, Policy: secretwriter.Policy,
		Namespace: plan.Namespace, SecretName: plan.SecretName, CellKey: plan.CellKey, CellID: plan.CellID,
		Action: status.Action, PlanDigest: plan.Digest, DecisionDigest: status.DecisionDigest,
		RequestDigest:  "sha256:" + strings.Repeat("b", 64),
		IdempotencyKey: "backup-materializer-secret-dry-run/" + plan.CellID + "/" + strings.TrimPrefix(status.DecisionDigest, "sha256:"),
		ValidatedAt:    validatedAt, Accepted: true, ServerSideDryRun: true,
	}
	result.Digest = secretwriter.DigestResult(result)
	return result
}

func testGeneration(
	t *testing.T,
	runID string,
	target backupcontrol.BackupTarget,
	issuedAt time.Time,
	buildAt time.Time,
) (materializercontract.ObserverInputBundle, materialization.Plan) {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		runID, runID, target, "backend-1", "sha256:"+strings.Repeat("a", 64), 4, 120, 1800,
	)
	if err != nil {
		t.Fatalf("build backup spec: %v", err)
	}
	keyring := backupidentity.DeriveKeyring(strings.Repeat("k", 32), "backup-key-1", "", "", nil)
	bundle, err := backupmaterializer.IssueObserverInputBundle(keyring, spec, "tenant-1", issuedAt)
	if err != nil {
		t.Fatalf("issue observer bundle: %v", err)
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
		Namespace: plan.Namespace, SecretName: plan.SecretName, UID: "01234567-89ab-cdef-0123-456789abcdef",
		ResourceVersion: "42", SecretType: manifest.SecretType, Labels: cloneMap(manifest.Labels),
		Annotations: cloneMap(manifest.Annotations), Data: map[string][]byte{
			data.SpecKey: append([]byte(nil), data.SpecDocument...), data.TokenKey: append([]byte(nil), data.ObserverToken...),
		},
	})
	if err != nil {
		t.Fatalf("seal current generation: %v", err)
	}
	observation, err := reconcile.ObserveManaged(snapshot)
	if err != nil {
		t.Fatalf("observe managed generation: %v", err)
	}
	return observation
}

func testAbsent(t *testing.T, cellKey string) reconcile.Observation {
	t.Helper()
	observation, err := reconcile.ObserveAbsent(cellKey)
	if err != nil {
		t.Fatalf("observe absent: %v", err)
	}
	return observation
}

func testService(t *testing.T, cellKey string, cycle Cycle, logger *log.Logger) *Service {
	t.Helper()
	service, err := New(Config{
		Enabled: true, CellKey: cellKey, Cycle: cycle, Interval: time.Second,
		AttemptTimeout: time.Second, Now: func() time.Time { return testTime().Add(5 * time.Minute) },
	}, logger)
	if err != nil {
		t.Fatalf("construct validation agent: %v", err)
	}
	return service
}

func assertSnapshotSecretFree(t *testing.T, snapshot Snapshot, fixture statusFixture) {
	t.Helper()
	document, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	rendered := string(document) + fmt.Sprintf("%#v %v", snapshot, snapshot)
	for _, sensitive := range []string{fixture.privateSpec, fixture.privateToken, "tenant-1"} {
		if sensitive != "" && strings.Contains(rendered, sensitive) {
			t.Fatalf("snapshot leaked private input %q", sensitive)
		}
	}
}

func cloneMap(value map[string]string) map[string]string {
	result := make(map[string]string, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func testAppTarget() backupcontrol.BackupTarget {
	return backupcontrol.BackupTarget{Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database"}
}

func testTime() time.Time { return time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC) }
