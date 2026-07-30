package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"fugue/internal/backupmaterializer/client"
	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
	"fugue/internal/backupmaterializer/secretreader"
)

var (
	_ DesiredSource = (*client.Client)(nil)
	_ CurrentSource = (*secretreader.Reader)(nil)
)

func TestReconcileOnceComposesCellLocalCASAndLKGMatrix(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 2, 1, 0, 0, 0, time.UTC)
	applyAt := issuedAt.Add(30 * time.Second)
	bundle, oldPlan := testGeneration(t, "run-1", testAppTarget(), issuedAt, applyAt)
	absent := testAbsent(t, oldPlan.CellKey)
	managed := testManaged(t, oldPlan, applyAt)

	t.Run("absent valid desired is create candidate only", func(t *testing.T) {
		desired := &desiredStub{bundle: bundle}
		current := &currentStub{observation: absent}
		status := testReconcileOnce(t, oldPlan.CellKey, applyAt, desired, current)
		if status.CurrentState != string(reconcile.StateAbsent) || status.DesiredState != DesiredAvailable ||
			status.Action != reconcile.ActionCreateIfAbsent || status.Reason != string(reconcile.ReasonDesiredGenerationReady) ||
			!status.MutationCandidate || status.Ready || status.Converged || status.LastKnownGoodServing ||
			status.Stable || status.Blocked || status.Retryable || status.DeleteAllowed || !status.ObservationOnly ||
			status.ExecutionAllowed || status.ProductionMutationAllowed || desired.Calls() != 1 || current.Calls() != 1 {
			t.Fatalf("create status drifted: %#v", status)
		}
	})

	t.Run("matching managed generation is converged", func(t *testing.T) {
		status := testReconcileOnce(t, oldPlan.CellKey, applyAt, &desiredStub{bundle: bundle}, &currentStub{observation: managed})
		if status.Action != reconcile.ActionNoop || status.Reason != string(reconcile.ReasonCurrentGenerationMatches) ||
			!status.Ready || !status.Converged || status.LastKnownGoodServing || !status.Stable || status.Blocked ||
			status.MutationCandidate || status.Retryable {
			t.Fatalf("noop status drifted: %#v", status)
		}
	})

	newIssuedAt := issuedAt.Add(2 * time.Minute)
	newApplyAt := newIssuedAt.Add(30 * time.Second)
	newBundle, newPlan := testGeneration(t, "run-1", testAppTarget(), newIssuedAt, newApplyAt)
	t.Run("changed managed generation is fenced replace candidate", func(t *testing.T) {
		status := testReconcileOnce(t, oldPlan.CellKey, newApplyAt, &desiredStub{bundle: newBundle}, &currentStub{observation: managed})
		if status.Action != reconcile.ActionReplaceResourceVersionCAS ||
			status.Reason != string(reconcile.ReasonDesiredGenerationChanged) || !status.MutationCandidate ||
			status.Ready || status.Converged || status.Stable || status.Blocked || status.Retryable ||
			status.DesiredPlanDigest != newPlan.Digest || status.Decision == nil ||
			!status.Decision.RequireUIDMatch || !status.Decision.RequireResourceVersionCAS ||
			!status.Decision.RetainExisting || status.Decision.ExpectedUID == "" ||
			status.Decision.ExpectedResourceVersion == "" {
			t.Fatalf("replace status drifted: %#v", status)
		}
	})

	t.Run("desired source loss retains unexpired LKG", func(t *testing.T) {
		now := oldPlan.RenewAfter.Add(time.Minute)
		status := testReconcileOnce(t, oldPlan.CellKey, now, &desiredStub{err: errors.New("source unavailable")}, &currentStub{observation: managed})
		if status.DesiredState != DesiredUnavailable || status.Action != reconcile.ActionRetainLastKnownGood ||
			status.Reason != string(reconcile.ReasonSourceUnavailableRetainLKG) || !status.Ready || status.Converged ||
			!status.LastKnownGoodServing || !status.Stable || status.Blocked || !status.Retryable ||
			status.MutationCandidate || status.DesiredPlanDigest != "" {
			t.Fatalf("LKG status drifted: %#v", status)
		}
	})

	t.Run("desired source loss at expiry blocks without delete", func(t *testing.T) {
		status := testReconcileOnce(t, oldPlan.CellKey, oldPlan.ExpiresAt, &desiredStub{err: errors.New("source unavailable")}, &currentStub{observation: managed})
		if status.Action != reconcile.ActionBlock || status.Reason != string(reconcile.ReasonLastKnownGoodExpired) ||
			status.Ready || status.Converged || status.LastKnownGoodServing || status.Stable || !status.Blocked ||
			!status.Retryable || status.MutationCandidate || status.DeleteAllowed || status.ExecutionAllowed {
			t.Fatalf("expired status drifted: %#v", status)
		}
	})

	t.Run("invalid desired is retryable and retains LKG", func(t *testing.T) {
		invalid := bundle
		invalid.Digest = "sha256:" + strings.Repeat("0", 64)
		status := testReconcileOnce(t, oldPlan.CellKey, applyAt, &desiredStub{bundle: invalid}, &currentStub{observation: managed})
		if status.DesiredState != DesiredInvalid || status.Action != reconcile.ActionRetainLastKnownGood ||
			!status.Ready || !status.LastKnownGoodServing || !status.Retryable || status.DesiredPlanDigest != "" {
			t.Fatalf("invalid desired status drifted: %#v", status)
		}
	})

	t.Run("cross-cell desired is invalid and cannot replace", func(t *testing.T) {
		otherBundle, _ := testGeneration(t, "run-1", testRegistryTarget(), issuedAt, applyAt)
		status := testReconcileOnce(t, oldPlan.CellKey, applyAt, &desiredStub{bundle: otherBundle}, &currentStub{observation: managed})
		if status.DesiredState != DesiredInvalid || status.Action != reconcile.ActionRetainLastKnownGood ||
			status.MutationCandidate || !status.Retryable || status.DesiredPlanDigest != "" {
			t.Fatalf("cross-cell desired status drifted: %#v", status)
		}
	})

	for _, state := range []reconcile.CurrentState{reconcile.StateForeign, reconcile.StateMalformed} {
		state := state
		t.Run("obstruction "+string(state)+" short-circuits desired", func(t *testing.T) {
			observation, err := reconcile.ObserveObstruction(oldPlan.CellKey, state, "uid-obstruction", "71")
			if err != nil {
				t.Fatalf("observe obstruction: %v", err)
			}
			desired := &desiredStub{bundle: bundle}
			status := testReconcileOnce(t, oldPlan.CellKey, applyAt, desired, &currentStub{observation: observation})
			wantReason := string(reconcile.ReasonCurrentObjectForeign)
			if state == reconcile.StateMalformed {
				wantReason = string(reconcile.ReasonCurrentObjectMalformed)
			}
			if status.DesiredState != DesiredNotRead || status.Action != reconcile.ActionBlock ||
				status.Reason != wantReason || !status.Blocked || status.Retryable || status.MutationCandidate ||
				status.Ready || status.DeleteAllowed || desired.Calls() != 0 {
				t.Fatalf("obstruction status drifted: %#v calls=%d", status, desired.Calls())
			}
		})
	}
}

func TestReconcileOnceContainsCurrentFailuresInsideOneCell(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 2, 2, 0, 0, 0, time.UTC)
	now := issuedAt.Add(30 * time.Second)
	bundle, plan := testGeneration(t, "run-1", testAppTarget(), issuedAt, now)
	otherObservation := testAbsent(t, testCellKey(t, testRegistryTarget()))
	tests := map[string]struct {
		observation reconcile.Observation
		err         error
	}{
		"source error":           {err: errors.New("Kubernetes API unavailable")},
		"invalid observation":    {observation: reconcile.Observation{}},
		"cross-cell observation": {observation: otherObservation},
	}
	for name, fixture := range tests {
		fixture := fixture
		t.Run(name, func(t *testing.T) {
			desired := &desiredStub{bundle: bundle}
			source := &currentStub{observation: fixture.observation, err: fixture.err}
			status := testReconcileOnce(t, plan.CellKey, now, desired, source)
			if status.CurrentState != CurrentUnavailable || status.DesiredState != DesiredNotRead ||
				status.Action != reconcile.ActionBlock || status.Reason != ReasonCurrentObservationUnavailable ||
				status.CurrentObservationDigest != "" || status.DesiredPlanDigest != "" || status.DecisionDigest != "" ||
				status.Decision != nil || status.Ready || status.Converged || status.LastKnownGoodServing ||
				status.Stable || !status.Blocked || !status.Retryable || status.MutationCandidate ||
				status.DeleteAllowed || !status.ObservationOnly || status.ExecutionAllowed ||
				status.ProductionMutationAllowed || desired.Calls() != 0 || source.Calls() != 1 {
				t.Fatalf("current-unavailable status drifted: %#v desiredCalls=%d currentCalls=%d", status, desired.Calls(), source.Calls())
			}
		})
	}
}

func TestReconcileOncePropagatesCancellationAndConstructionFailsClosed(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 2, 3, 0, 0, 0, time.UTC)
	now := issuedAt.Add(30 * time.Second)
	bundle, plan := testGeneration(t, "run-1", testAppTarget(), issuedAt, now)
	absent := testAbsent(t, plan.CellKey)

	t.Run("disabled ignores every supplied capability", func(t *testing.T) {
		var clockCalls atomic.Int32
		desired := &desiredStub{bundle: bundle}
		current := &currentStub{observation: absent}
		reconciler, err := New(Config{
			Enabled: false, CellKey: "private-invalid-cell", DesiredSource: desired, CurrentSource: current,
			Now: func() time.Time { clockCalls.Add(1); return now },
		})
		if err != nil || reconciler.Enabled() || reconciler.cellKey != "" || reconciler.cellID != "" ||
			reconciler.desired != nil || reconciler.current != nil || reconciler.now != nil {
			t.Fatalf("disabled construction retained capability: reconciler=%#v err=%v", reconciler, err)
		}
		if _, err := reconciler.ReconcileOnce(context.Background()); !errors.Is(err, ErrDisabled) {
			t.Fatalf("disabled reconcile error = %v", err)
		}
		if desired.Calls() != 0 || current.Calls() != 0 || clockCalls.Load() != 0 {
			t.Fatalf("disabled reconciler invoked capability: desired=%d current=%d clock=%d", desired.Calls(), current.Calls(), clockCalls.Load())
		}
	})

	t.Run("invalid and typed nil configuration", func(t *testing.T) {
		var nilDesired *desiredStub
		var nilCurrent *currentStub
		configs := []Config{
			{Enabled: true, CellKey: "invalid", DesiredSource: &desiredStub{}, CurrentSource: &currentStub{}},
			{Enabled: true, CellKey: plan.CellKey, CurrentSource: &currentStub{}},
			{Enabled: true, CellKey: plan.CellKey, DesiredSource: &desiredStub{}},
			{Enabled: true, CellKey: plan.CellKey, DesiredSource: nilDesired, CurrentSource: &currentStub{}},
			{Enabled: true, CellKey: plan.CellKey, DesiredSource: &desiredStub{}, CurrentSource: nilCurrent},
		}
		for index, config := range configs {
			if reconciler, err := New(config); !errors.Is(err, ErrConfig) || reconciler != nil {
				t.Fatalf("config %d result = %#v, %v; want nil ErrConfig", index, reconciler, err)
			}
		}
		var nilReconciler *Reconciler
		if _, err := nilReconciler.ReconcileOnce(context.Background()); !errors.Is(err, ErrConfig) {
			t.Fatalf("nil reconciler error = %v", err)
		}
	})

	t.Run("pre-canceled context invokes nothing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		desired := &desiredStub{bundle: bundle}
		current := &currentStub{observation: absent}
		reconciler := testReconciler(t, plan.CellKey, now, desired, current)
		if _, err := reconciler.ReconcileOnce(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-canceled error = %v", err)
		}
		if desired.Calls() != 0 || current.Calls() != 0 {
			t.Fatalf("pre-canceled reconcile invoked source: desired=%d current=%d", desired.Calls(), current.Calls())
		}
	})

	t.Run("current cancellation stops before desired", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		desired := &desiredStub{bundle: bundle}
		current := &currentStub{observation: absent, hook: cancel}
		reconciler := testReconciler(t, plan.CellKey, now, desired, current)
		if _, err := reconciler.ReconcileOnce(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("current cancellation error = %v", err)
		}
		if current.Calls() != 1 || desired.Calls() != 0 {
			t.Fatalf("current cancellation calls: current=%d desired=%d", current.Calls(), desired.Calls())
		}
	})

	t.Run("desired cancellation propagates", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		desired := &desiredStub{bundle: bundle, hook: cancel}
		current := &currentStub{observation: absent}
		reconciler := testReconciler(t, plan.CellKey, now, desired, current)
		if _, err := reconciler.ReconcileOnce(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("desired cancellation error = %v", err)
		}
		if current.Calls() != 1 || desired.Calls() != 1 {
			t.Fatalf("desired cancellation calls: current=%d desired=%d", current.Calls(), desired.Calls())
		}
	})

	t.Run("source context error without canceled context is cell local", func(t *testing.T) {
		status := testReconcileOnce(t, plan.CellKey, now, &desiredStub{err: context.DeadlineExceeded}, &currentStub{observation: absent})
		if status.DesiredState != DesiredUnavailable || !status.Retryable || status.Action != reconcile.ActionBlock {
			t.Fatalf("source timeout escaped cell-local status: %#v", status)
		}
	})

	t.Run("clock is sampled once and canonicalized", func(t *testing.T) {
		var calls atomic.Int32
		nonCanonical := now.In(time.FixedZone("test", 8*60*60)).Add(987 * time.Nanosecond)
		reconciler, err := New(Config{
			Enabled: true, CellKey: plan.CellKey, DesiredSource: &desiredStub{bundle: bundle},
			CurrentSource: &currentStub{observation: absent},
			Now:           func() time.Time { calls.Add(1); return nonCanonical },
		})
		if err != nil {
			t.Fatalf("construct reconciler: %v", err)
		}
		status, err := reconciler.ReconcileOnce(context.Background())
		if err != nil || calls.Load() != 1 || status.EvaluatedAt != now || status.Decision == nil || status.Decision.DecidedAt != now {
			t.Fatalf("clock result drifted: status=%#v calls=%d err=%v", status, calls.Load(), err)
		}
	})

	t.Run("zero clock fails before sources", func(t *testing.T) {
		desired := &desiredStub{bundle: bundle}
		current := &currentStub{observation: absent}
		reconciler := testReconciler(t, plan.CellKey, time.Time{}, desired, current)
		if _, err := reconciler.ReconcileOnce(context.Background()); !errors.Is(err, ErrConfig) {
			t.Fatalf("zero clock error = %v", err)
		}
		if desired.Calls() != 0 || current.Calls() != 0 {
			t.Fatalf("zero clock invoked sources: desired=%d current=%d", desired.Calls(), current.Calls())
		}
	})

	if _, err := testReconciler(t, plan.CellKey, now, &desiredStub{bundle: bundle}, &currentStub{observation: absent}).ReconcileOnce(nil); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil context error = %v", err)
	}
}

func TestStatusValidationRejectsSemanticAndExecutionDrift(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 2, 4, 0, 0, 0, time.UTC)
	now := issuedAt.Add(30 * time.Second)
	bundle, plan := testGeneration(t, "run-1", testAppTarget(), issuedAt, now)
	status := testReconcileOnce(t, plan.CellKey, now, &desiredStub{bundle: bundle}, &currentStub{observation: testAbsent(t, plan.CellKey)})

	mutations := map[string]func(*Status){
		"API version":          func(value *Status) { value.APIVersion = "v2" },
		"kind":                 func(value *Status) { value.Kind = "Other" },
		"policy":               func(value *Status) { value.Policy = "upsert" },
		"cell":                 func(value *Status) { value.CellKey = testCellKey(t, testRegistryTarget()) },
		"cell ID":              func(value *Status) { value.CellID = "registry-0000000000000000" },
		"current state":        func(value *Status) { value.CurrentState = string(reconcile.StateManaged) },
		"desired state":        func(value *Status) { value.DesiredState = DesiredNotRead },
		"action":               func(value *Status) { value.Action = reconcile.ActionBlock },
		"reason":               func(value *Status) { value.Reason = string(reconcile.ReasonCurrentObjectForeign) },
		"current digest":       func(value *Status) { value.CurrentObservationDigest = "not-a-digest" },
		"desired digest":       func(value *Status) { value.DesiredPlanDigest = "" },
		"decision digest":      func(value *Status) { value.DecisionDigest = "sha256:" + strings.Repeat("0", 64) },
		"decision missing":     func(value *Status) { value.Decision = nil },
		"ready":                func(value *Status) { value.Ready = true },
		"converged":            func(value *Status) { value.Converged = true },
		"LKG":                  func(value *Status) { value.LastKnownGoodServing = true },
		"stable":               func(value *Status) { value.Stable = true },
		"blocked":              func(value *Status) { value.Blocked = true },
		"retryable":            func(value *Status) { value.Retryable = true },
		"mutation candidate":   func(value *Status) { value.MutationCandidate = false },
		"delete":               func(value *Status) { value.DeleteAllowed = true },
		"observation disabled": func(value *Status) { value.ObservationOnly = false },
		"execution":            func(value *Status) { value.ExecutionAllowed = true },
		"production mutation":  func(value *Status) { value.ProductionMutationAllowed = true },
		"noncanonical time":    func(value *Status) { value.EvaluatedAt = value.EvaluatedAt.Add(time.Nanosecond) },
		"nested decision": func(value *Status) {
			value.Decision.Action = reconcile.ActionBlock
			value.Decision.Digest = reconcile.DigestDecision(*value.Decision)
		},
	}
	for name, mutate := range mutations {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			candidate := cloneStatus(status)
			mutate(&candidate)
			candidate.IdempotencyKey = idempotencyKey(candidate)
			candidate.Digest = DigestStatus(candidate)
			if err := ValidateStatus(candidate); !errors.Is(err, ErrInvariant) {
				t.Fatalf("mutated status error = %v, want ErrInvariant: %#v", err, candidate)
			}
		})
	}

	badKey := cloneStatus(status)
	badKey.IdempotencyKey += "-mutated"
	badKey.Digest = DigestStatus(badKey)
	if err := ValidateStatus(badKey); !errors.Is(err, ErrInvariant) {
		t.Fatalf("idempotency drift error = %v", err)
	}
	badDigest := cloneStatus(status)
	badDigest.Digest = "sha256:" + strings.Repeat("0", 64)
	if err := ValidateStatus(badDigest); !errors.Is(err, ErrInvariant) {
		t.Fatalf("digest drift error = %v", err)
	}

	unavailable := newCurrentUnavailableStatus(status.CellKey, status.CellID, now)
	if err := ValidateStatus(unavailable); err != nil {
		t.Fatalf("valid current-unavailable status: %v", err)
	}
	badUnavailable := unavailable
	badUnavailable.Retryable = false
	badUnavailable.IdempotencyKey = idempotencyKey(badUnavailable)
	badUnavailable.Digest = DigestStatus(badUnavailable)
	if err := ValidateStatus(badUnavailable); !errors.Is(err, ErrInvariant) {
		t.Fatalf("current-unavailable drift error = %v", err)
	}
}

func TestReconcileStatusIsSecretFreeAndIdempotentAcrossEvaluationTime(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 2, 5, 0, 0, 0, time.UTC)
	firstAt := issuedAt.Add(20 * time.Second)
	secondAt := firstAt.Add(time.Second)
	bundle, plan := testGeneration(t, "run-1", testAppTarget(), issuedAt, firstAt)
	absent := testAbsent(t, plan.CellKey)
	first := testReconcileOnce(t, plan.CellKey, firstAt, &desiredStub{bundle: bundle}, &currentStub{observation: absent})
	second := testReconcileOnce(t, plan.CellKey, secondAt, &desiredStub{bundle: bundle}, &currentStub{observation: absent})
	if first.IdempotencyKey == "" || first.IdempotencyKey != second.IdempotencyKey || first.Digest == second.Digest ||
		first.DecisionDigest == second.DecisionDigest || first.EvaluatedAt == second.EvaluatedAt {
		t.Fatalf("time/idempotency binding drifted: first=%#v second=%#v", first, second)
	}
	document, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	rendered := strings.Join([]string{string(document), fmt.Sprint(first), fmt.Sprintf("%#v", first)}, "\n")
	privateSpec, err := json.Marshal(bundle.DesiredSpec)
	if err != nil {
		t.Fatalf("marshal private spec fixture: %v", err)
	}
	if strings.Contains(rendered, bundle.ObserverToken) || strings.Contains(rendered, string(privateSpec)) ||
		strings.Contains(rendered, "tenant-1") {
		t.Fatalf("public status exposed private observer input: %s", rendered)
	}
	if !strings.Contains(rendered, "executionAllowed=false") {
		t.Fatalf("diagnostic output omitted execution gate: %s", rendered)
	}

	desired := &desiredStub{bundle: bundle}
	current := &currentStub{observation: absent}
	config := Config{
		Enabled: true, CellKey: plan.CellKey, DesiredSource: desired, CurrentSource: current,
	}
	reconciler, err := New(config)
	if err != nil || !reconciler.Enabled() {
		t.Fatalf("construct default-clock reconciler: reconciler=%#v err=%v", reconciler, err)
	}
	diagnostics := strings.Join([]string{
		fmt.Sprint(config), fmt.Sprintf("%#v", config), fmt.Sprint(reconciler), fmt.Sprintf("%#v", reconciler),
	}, "\n")
	if !strings.Contains(diagnostics, "[REDACTED]") || strings.Contains(diagnostics, bundle.ObserverToken) ||
		strings.Contains(diagnostics, plan.CellKey) {
		t.Fatalf("configuration diagnostics exposed private inputs: %s", diagnostics)
	}
	var nilReconciler *Reconciler
	if got := nilReconciler.String(); got != "backup materializer reconciler <nil>" || nilReconciler.GoString() != got {
		t.Fatalf("nil reconciler diagnostic drifted: %q", got)
	}
}

func TestReconcileOnceIsDeterministicUnderConcurrentEvaluation(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 2, 6, 0, 0, 0, time.UTC)
	now := issuedAt.Add(30 * time.Second)
	bundle, plan := testGeneration(t, "run-1", testAppTarget(), issuedAt, now)
	desired := &desiredStub{bundle: bundle}
	current := &currentStub{observation: testAbsent(t, plan.CellKey)}
	reconciler := testReconciler(t, plan.CellKey, now, desired, current)
	want, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("baseline reconcile: %v", err)
	}
	const readers = 32
	results := make(chan Status, readers)
	errorsFound := make(chan error, readers)
	var wait sync.WaitGroup
	for range readers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			status, reconcileErr := reconciler.ReconcileOnce(context.Background())
			results <- status
			errorsFound <- reconcileErr
		}()
	}
	wait.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatalf("concurrent reconcile error: %v", err)
		}
	}
	for got := range results {
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("concurrent status drifted: got=%#v want=%#v", got, want)
		}
	}
	if desired.Calls() != readers+1 || current.Calls() != readers+1 {
		t.Fatalf("source call count drifted: desired=%d current=%d", desired.Calls(), current.Calls())
	}
}

func TestReconcilerProductionDependencyBoundaryIsPureAndNonExecutable(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-deps", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list reconciler dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{
			"database/sql", "net", "net/http", "os/exec",
			"fugue/internal/backupidentity", "fugue/internal/backupmaterializer",
		} {
			if dependency == forbidden {
				t.Fatalf("reconciler dependency widened to %q", dependency)
			}
		}
		for _, prefix := range []string{"k8s.io/", "fugue/internal/api", "fugue/internal/store", "fugue/internal/model"} {
			if strings.HasPrefix(dependency, prefix) {
				t.Fatalf("reconciler dependency widened to %q", dependency)
			}
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconcile",
		"fugue/internal/backupmaterializer/reconciler",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("reconciler local dependency closure drifted: got=%v want=%v", local, want)
	}
	directCommand := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	directOutput, err := directCommand.Output()
	if err != nil {
		t.Fatalf("list direct reconciler dependencies: %v", err)
	}
	direct := strings.Fields(string(directOutput))
	sort.Strings(direct)
	wantDirect := []string{
		"context",
		"crypto/sha256",
		"encoding/hex",
		"encoding/json",
		"errors",
		"fmt",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconcile",
		"reflect",
		"strings",
		"time",
	}
	if !reflect.DeepEqual(direct, wantDirect) {
		t.Fatalf("reconciler direct dependency boundary widened: got=%v want=%v", direct, wantDirect)
	}
}

type desiredStub struct {
	mu     sync.Mutex
	bundle materializercontract.ObserverInputBundle
	err    error
	hook   func()
	calls  int
}

func (stub *desiredStub) Fetch(context.Context) (materializercontract.ObserverInputBundle, error) {
	stub.mu.Lock()
	stub.calls++
	bundle, err, hook := stub.bundle, stub.err, stub.hook
	stub.mu.Unlock()
	if hook != nil {
		hook()
	}
	return bundle, err
}

func (stub *desiredStub) Calls() int {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	return stub.calls
}

type currentStub struct {
	mu          sync.Mutex
	observation reconcile.Observation
	err         error
	hook        func()
	calls       int
}

func (stub *currentStub) Observe(context.Context) (reconcile.Observation, error) {
	stub.mu.Lock()
	stub.calls++
	observation, err, hook := stub.observation, stub.err, stub.hook
	stub.mu.Unlock()
	if hook != nil {
		hook()
	}
	return observation, err
}

func (stub *currentStub) Calls() int {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	return stub.calls
}

func testReconciler(t *testing.T, cellKey string, now time.Time, desired DesiredSource, current CurrentSource) *Reconciler {
	t.Helper()
	reconciler, err := New(Config{
		Enabled: true, CellKey: cellKey, DesiredSource: desired, CurrentSource: current,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("construct reconciler: %v", err)
	}
	return reconciler
}

func testReconcileOnce(t *testing.T, cellKey string, now time.Time, desired DesiredSource, current CurrentSource) Status {
	t.Helper()
	status, err := testReconciler(t, cellKey, now, desired, current).ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconcile once: %v", err)
	}
	if err := ValidateStatus(status); err != nil || status.Digest == "" || status.IdempotencyKey == "" {
		t.Fatalf("status invalid: %#v err=%v", status, err)
	}
	return status
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
		runID,
		runID,
		target,
		"backend-1",
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		4,
		120,
		1800,
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
		Namespace:       manifest.Namespace,
		SecretName:      manifest.SecretName,
		UID:             "01234567-89ab-cdef-0123-456789abcdef",
		ResourceVersion: "42",
		SecretType:      manifest.SecretType,
		Labels:          cloneStringMap(manifest.Labels),
		Annotations:     cloneStringMap(manifest.Annotations),
		Data: map[string][]byte{
			data.SpecKey:  append([]byte(nil), data.SpecDocument...),
			data.TokenKey: append([]byte(nil), data.ObserverToken...),
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

func testAbsent(t *testing.T, cellKey string) reconcile.Observation {
	t.Helper()
	observation, err := reconcile.ObserveAbsent(cellKey)
	if err != nil {
		t.Fatalf("observe absent: %v", err)
	}
	return observation
}

func testCellKey(t *testing.T, target backupcontrol.BackupTarget) string {
	t.Helper()
	cellKey := backupcontrol.BackupCellKey(target)
	if cellKey == "" {
		t.Fatalf("derive cell key for %#v", target)
	}
	return cellKey
}

func testAppTarget() backupcontrol.BackupTarget {
	return backupcontrol.BackupTarget{Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database"}
}

func testRegistryTarget() backupcontrol.BackupTarget {
	return backupcontrol.BackupTarget{Type: backupcontrol.TargetRegistry, ScopeKey: "platform/registry"}
}

func cloneStatus(status Status) Status {
	if status.Decision != nil {
		decision := *status.Decision
		status.Decision = &decision
	}
	return status
}

func cloneStringMap(value map[string]string) map[string]string {
	cloned := make(map[string]string, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}
