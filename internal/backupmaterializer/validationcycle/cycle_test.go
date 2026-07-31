package validationcycle

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
	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/dryrunreconciler"
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
	"fugue/internal/backupmaterializer/reconciler"
	"fugue/internal/backupmaterializer/secretwriter"
)

var (
	_ Source             = (*reconciler.Reconciler)(nil)
	_ CandidateValidator = (*dryrunreconciler.Reconciler)(nil)
)

type sourceStub struct {
	enabled  bool
	prepared reconciler.PreparedCycle
	err      error
	fn       func(context.Context) (reconciler.PreparedCycle, error)
	calls    atomic.Int64
}

func (stub *sourceStub) Enabled() bool { return stub != nil && stub.enabled }

func (stub *sourceStub) PrepareOnce(ctx context.Context) (reconciler.PreparedCycle, error) {
	stub.calls.Add(1)
	if stub.fn != nil {
		return stub.fn(ctx)
	}
	return stub.prepared, stub.err
}

type writerStub struct {
	enabled  bool
	result   secretwriter.Result
	err      error
	fn       func(context.Context, materialization.Plan, reconcile.Decision) (secretwriter.Result, error)
	calls    atomic.Int64
	mu       sync.Mutex
	plan     materialization.Plan
	decision reconcile.Decision
}

func (stub *writerStub) Enabled() bool { return stub != nil && stub.enabled }

func (stub *writerStub) DryRun(
	ctx context.Context,
	plan materialization.Plan,
	decision reconcile.Decision,
) (secretwriter.Result, error) {
	stub.calls.Add(1)
	stub.mu.Lock()
	stub.plan = plan
	stub.decision = decision
	stub.mu.Unlock()
	if stub.fn != nil {
		return stub.fn(ctx, plan, decision)
	}
	return stub.result, stub.err
}

type candidateValidatorStub struct {
	enabled bool
	status  dryrunreconciler.Status
	err     error
	fn      func(context.Context, materialization.Plan, reconciler.Status) (dryrunreconciler.Status, error)
	calls   atomic.Int64
}

func (stub *candidateValidatorStub) Enabled() bool { return stub != nil && stub.enabled }

func (stub *candidateValidatorStub) ValidateCandidateOnce(
	ctx context.Context,
	plan materialization.Plan,
	status reconciler.Status,
) (dryrunreconciler.Status, error) {
	stub.calls.Add(1)
	if stub.fn != nil {
		return stub.fn(ctx, plan, status)
	}
	return stub.status, stub.err
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

func TestValidationCycleAcceptsExactCreateAndReplaceCandidates(t *testing.T) {
	t.Parallel()
	base := testTime()
	createBundle, createPlan := testGeneration(t, "run-cycle-create", testAppTarget(), base, base.Add(30*time.Second))
	createPrepared := testPrepared(
		t, createPlan.CellKey, base.Add(30*time.Second), desiredStub{bundle: createBundle},
		currentStub{observation: testAbsent(t, createPlan.CellKey)},
	)

	oldAt := base.Add(time.Minute)
	_, oldPlan := testGeneration(t, "run-cycle-old", testAppTarget(), oldAt, oldAt.Add(30*time.Second))
	replaceIssuedAt := base.Add(3 * time.Minute)
	replaceAt := replaceIssuedAt.Add(30 * time.Second)
	replaceBundle, replacePlan := testGeneration(t, "run-cycle-replace", testAppTarget(), replaceIssuedAt, replaceAt)
	replacePrepared := testPrepared(
		t, replacePlan.CellKey, replaceAt, desiredStub{bundle: replaceBundle},
		currentStub{observation: testManaged(t, oldPlan, oldAt.Add(30*time.Second))},
	)

	for _, fixture := range []struct {
		name      string
		plan      materialization.Plan
		prepared  reconciler.PreparedCycle
		preserved bool
	}{
		{name: "create", plan: createPlan, prepared: createPrepared},
		{name: "replace", plan: replacePlan, prepared: replacePrepared, preserved: true},
	} {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			validatedAt := fixture.prepared.EvaluatedAt.Add(time.Second)
			writer := &writerStub{enabled: true, result: acceptedReceipt(fixture.plan, fixture.prepared.Status, validatedAt)}
			validator := testDryRunController(t, fixture.plan.CellKey, writer, validatedAt)
			source := &sourceStub{enabled: true, prepared: fixture.prepared}
			cycle := testCycle(t, fixture.plan.CellKey, source, validator)
			status, err := cycle.ReconcileOnce(context.Background())
			if err != nil || ValidateStatus(status) != nil || !status.ValidationRequired || !status.ValidationAttempted ||
				!status.ValidationAccepted || status.ValidationOutcome != dryrunreconciler.OutcomeAccepted ||
				status.Ready || status.Converged || status.LastKnownGoodServing || status.Blocked || status.Retryable ||
				!status.MutationCandidate || status.ExistingObjectPreserved != fixture.preserved ||
				status.Persisted || status.DeleteAllowed || !status.ObservationOnly || status.ExecutionAllowed ||
				status.ProductionMutationAllowed || status.PreparedCycle == nil || status.ValidationStatus == nil ||
				status.ValidationAttemptedAt == nil || *status.ValidationAttemptedAt != validatedAt ||
				source.calls.Load() != 1 || writer.calls.Load() != 1 {
				t.Fatalf("accepted validation cycle drifted: status=%#v err=%v validation=%v source=%d writer=%d", status, err, ValidateStatus(status), source.calls.Load(), writer.calls.Load())
			}
			if status.PreparedCycle.Digest != fixture.prepared.Digest ||
				status.PreparedCycle.CandidatePlanDigest != fixture.plan.Digest ||
				status.ValidationStatus.ReconcileStatusDigest != fixture.prepared.StatusDigest {
				t.Fatalf("nested evidence detached: status=%#v", status)
			}
			writer.mu.Lock()
			gotPlanDigest := writer.plan.Digest
			gotDecisionDigest := writer.decision.Digest
			writer.mu.Unlock()
			if gotPlanDigest != fixture.plan.Digest || gotDecisionDigest != fixture.prepared.Status.DecisionDigest {
				t.Fatalf("private validation input detached: plan=%s decision=%s", gotPlanDigest, gotDecisionDigest)
			}
			originalDigest := status.Digest
			fixture.prepared.Status.Decision.Digest = "invalid"
			if status.Digest != originalDigest || ValidateStatus(status) != nil {
				t.Fatal("validation status retained source decision pointer")
			}
			assertStatusSecretFree(t, status, fixture.plan, fixture.prepared.EvaluatedAt, "")
		})
	}
}

func TestValidationCycleSkipsValidatorForNonCandidateStates(t *testing.T) {
	t.Parallel()
	now := testTime()
	bundle, plan := testGeneration(t, "run-cycle-skip", testAppTarget(), now, now.Add(30*time.Second))
	evaluatedAt := now.Add(30 * time.Second)
	managed := testManaged(t, plan, evaluatedAt)
	fixtures := []struct {
		name      string
		prepared  reconciler.PreparedCycle
		ready     bool
		converged bool
		lkg       bool
		blocked   bool
		retryable bool
		private   string
	}{
		{
			name: "no-op", ready: true, converged: true,
			prepared: testPrepared(t, plan.CellKey, evaluatedAt, desiredStub{bundle: bundle}, currentStub{observation: managed}),
		},
		{
			name: "last-known-good", ready: true, lkg: true, retryable: true, private: "private desired failure",
			prepared: testPrepared(t, plan.CellKey, plan.RenewAfter.Add(time.Minute),
				desiredStub{err: errors.New("private desired failure")}, currentStub{observation: managed}),
		},
		{
			name: "current-unavailable", blocked: true, retryable: true, private: "private Kubernetes failure",
			prepared: testPrepared(t, plan.CellKey, evaluatedAt, desiredStub{bundle: bundle},
				currentStub{err: errors.New("private Kubernetes failure")}),
		},
	}
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			validator := &candidateValidatorStub{enabled: true, fn: func(context.Context, materialization.Plan, reconciler.Status) (dryrunreconciler.Status, error) {
				panic("validator called for non-candidate")
			}}
			source := &sourceStub{enabled: true, prepared: fixture.prepared}
			status, err := testCycle(t, plan.CellKey, source, validator).ReconcileOnce(context.Background())
			if err != nil || ValidateStatus(status) != nil || status.ValidationRequired || status.ValidationAttempted ||
				status.ValidationAccepted || status.ValidationOutcome != "" || status.ValidationStatus != nil ||
				status.CandidatePlanDigest != "" || status.Ready != fixture.ready || status.Converged != fixture.converged ||
				status.LastKnownGoodServing != fixture.lkg || status.Blocked != fixture.blocked ||
				status.Retryable != fixture.retryable || status.MutationCandidate ||
				source.calls.Load() != 1 || validator.calls.Load() != 0 {
				t.Fatalf("non-candidate cycle drifted: status=%#v err=%v validation=%v source=%d validator=%d", status, err, ValidateStatus(status), source.calls.Load(), validator.calls.Load())
			}
			assertStatusSecretFree(t, status, plan, evaluatedAt, fixture.private)
		})
	}
}

func TestValidationCycleCarriesFixedCellLocalFailureOutcomes(t *testing.T) {
	t.Parallel()
	now := testTime()
	bundle, plan := testGeneration(t, "run-cycle-failure", testAppTarget(), now, now.Add(30*time.Second))
	prepared := testPrepared(
		t, plan.CellKey, now.Add(30*time.Second), desiredStub{bundle: bundle},
		currentStub{observation: testAbsent(t, plan.CellKey)},
	)
	attemptedAt := prepared.EvaluatedAt.Add(time.Second)
	tests := []struct {
		name      string
		err       error
		outcome   dryrunreconciler.Outcome
		retryable bool
	}{
		{name: "conflict", err: secretwriter.ErrConflict, outcome: dryrunreconciler.OutcomeConflict, retryable: true},
		{name: "rejected", err: secretwriter.ErrRejected, outcome: dryrunreconciler.OutcomeRejected, retryable: false},
		{name: "credential", err: secretwriter.ErrCredentialUnavailable, outcome: dryrunreconciler.OutcomeCredentialUnavailable, retryable: true},
		{name: "unavailable", err: secretwriter.ErrUnavailable, outcome: dryrunreconciler.OutcomeUnavailable, retryable: true},
		{name: "response", err: secretwriter.ErrResponse, outcome: dryrunreconciler.OutcomeResponseInvalid, retryable: true},
		{name: "reconcile", err: secretwriter.ErrIntent, outcome: dryrunreconciler.OutcomeReconcileRequired, retryable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer := &writerStub{enabled: true, err: fmt.Errorf("private writer failure: %w", test.err)}
			validator := testDryRunController(t, plan.CellKey, writer, attemptedAt)
			status, err := testCycle(t, plan.CellKey, &sourceStub{enabled: true, prepared: prepared}, validator).
				ReconcileOnce(context.Background())
			if err != nil || ValidateStatus(status) != nil || status.ValidationOutcome != test.outcome ||
				!status.ValidationRequired || !status.ValidationAttempted || status.ValidationAccepted ||
				!status.Blocked || status.Retryable != test.retryable || status.ValidationStatus == nil ||
				status.ValidationStatus.Outcome != test.outcome || writer.calls.Load() != 1 {
				t.Fatalf("failure outcome drifted: status=%#v err=%v validation=%v", status, err, ValidateStatus(status))
			}
			assertStatusSecretFree(t, status, plan, prepared.EvaluatedAt, "private writer failure")
		})
	}
}

func TestValidationCycleFailsClosedBeforeOrAfterCapabilityBoundaries(t *testing.T) {
	t.Parallel()
	now := testTime()
	bundle, plan := testGeneration(t, "run-cycle-invalid", testAppTarget(), now, now.Add(30*time.Second))
	prepared := testPrepared(
		t, plan.CellKey, now.Add(30*time.Second), desiredStub{bundle: bundle},
		currentStub{observation: testAbsent(t, plan.CellKey)},
	)
	validator := &candidateValidatorStub{enabled: true}

	t.Run("source error is normalized", func(t *testing.T) {
		source := &sourceStub{enabled: true, err: errors.New("private source error")}
		_, err := testCycle(t, plan.CellKey, source, validator).ReconcileOnce(context.Background())
		if !errors.Is(err, ErrInvariant) || strings.Contains(err.Error(), "private") || validator.calls.Load() != 0 {
			t.Fatalf("source error = %v validator=%d", err, validator.calls.Load())
		}
	})

	t.Run("invalid prepared cycle", func(t *testing.T) {
		invalid := prepared
		invalid.Digest = "invalid"
		localValidator := &candidateValidatorStub{enabled: true}
		_, err := testCycle(t, plan.CellKey, &sourceStub{enabled: true, prepared: invalid}, localValidator).
			ReconcileOnce(context.Background())
		if !errors.Is(err, ErrInvariant) || localValidator.calls.Load() != 0 {
			t.Fatalf("invalid prepared error=%v validator=%d", err, localValidator.calls.Load())
		}
	})

	t.Run("validator error is normalized", func(t *testing.T) {
		localValidator := &candidateValidatorStub{enabled: true, err: errors.New("private validator error")}
		_, err := testCycle(t, plan.CellKey, &sourceStub{enabled: true, prepared: prepared}, localValidator).
			ReconcileOnce(context.Background())
		if !errors.Is(err, ErrInvariant) || strings.Contains(err.Error(), "private") || localValidator.calls.Load() != 1 {
			t.Fatalf("validator error=%v calls=%d", err, localValidator.calls.Load())
		}
	})

	t.Run("invalid validator status", func(t *testing.T) {
		localValidator := &candidateValidatorStub{enabled: true, status: dryrunreconciler.Status{}}
		_, err := testCycle(t, plan.CellKey, &sourceStub{enabled: true, prepared: prepared}, localValidator).
			ReconcileOnce(context.Background())
		if !errors.Is(err, ErrInvariant) || localValidator.calls.Load() != 1 {
			t.Fatalf("invalid validator status error=%v calls=%d", err, localValidator.calls.Load())
		}
	})
}

func TestValidationCycleCancellationAndConstructionAreFailClosed(t *testing.T) {
	t.Parallel()
	var typedSource *sourceStub
	var typedValidator *candidateValidatorStub
	disabled, err := New(Config{
		Enabled: false, CellKey: "private invalid cell", Source: typedSource, Validator: typedValidator,
	})
	if err != nil || disabled.Enabled() || disabled.source != nil || disabled.validator != nil || disabled.cellKey != "" {
		t.Fatalf("disabled construction retained capability: cycle=%#v err=%v", disabled, err)
	}
	if _, err := disabled.ReconcileOnce(context.Background()); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled cycle error = %v", err)
	}
	if strings.Contains(fmt.Sprintf("%#v", Config{CellKey: "private-cell"}), "private-cell") {
		t.Fatal("configuration formatting leaked cell")
	}

	cellKey := testCellKey(t, testAppTarget())
	for name, config := range map[string]Config{
		"invalid cell":        {Enabled: true, CellKey: "backup/all/invalid", Source: &sourceStub{enabled: true}, Validator: &candidateValidatorStub{enabled: true}},
		"nil source":          {Enabled: true, CellKey: cellKey, Validator: &candidateValidatorStub{enabled: true}},
		"typed nil source":    {Enabled: true, CellKey: cellKey, Source: typedSource, Validator: &candidateValidatorStub{enabled: true}},
		"disabled source":     {Enabled: true, CellKey: cellKey, Source: &sourceStub{}, Validator: &candidateValidatorStub{enabled: true}},
		"nil validator":       {Enabled: true, CellKey: cellKey, Source: &sourceStub{enabled: true}},
		"typed nil validator": {Enabled: true, CellKey: cellKey, Source: &sourceStub{enabled: true}, Validator: typedValidator},
		"disabled validator":  {Enabled: true, CellKey: cellKey, Source: &sourceStub{enabled: true}, Validator: &candidateValidatorStub{}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(config); !errors.Is(err, ErrConfig) {
				t.Fatalf("construction error = %v, want ErrConfig", err)
			}
		})
	}
	if (*Cycle)(nil).Enabled() || !strings.Contains((*Cycle)(nil).String(), "<nil>") {
		t.Fatal("nil cycle enablement or formatting drifted")
	}
	if _, err := (*Cycle)(nil).ReconcileOnce(context.Background()); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil cycle error = %v", err)
	}

	now := testTime()
	bundle, plan := testGeneration(t, "run-cycle-cancel", testAppTarget(), now, now.Add(30*time.Second))
	prepared := testPrepared(t, plan.CellKey, now.Add(30*time.Second), desiredStub{bundle: bundle}, currentStub{observation: testAbsent(t, plan.CellKey)})
	source := &sourceStub{enabled: true, prepared: prepared}
	validator := &candidateValidatorStub{enabled: true}
	cycle := testCycle(t, plan.CellKey, source, validator)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cycle.ReconcileOnce(canceled); !errors.Is(err, context.Canceled) || source.calls.Load() != 0 {
		t.Fatalf("pre-canceled cycle: err=%v source=%d", err, source.calls.Load())
	}

	during, cancelDuring := context.WithCancel(context.Background())
	validator.fn = func(context.Context, materialization.Plan, reconciler.Status) (dryrunreconciler.Status, error) {
		cancelDuring()
		return dryrunreconciler.Status{}, errors.New("private canceled validator")
	}
	if _, err := cycle.ReconcileOnce(during); !errors.Is(err, context.Canceled) || validator.calls.Load() != 1 {
		t.Fatalf("mid-validation cancellation: err=%v validator=%d", err, validator.calls.Load())
	}
}

func TestValidationCycleStatusRejectsEveryBindingAndCapabilityDrift(t *testing.T) {
	t.Parallel()
	now := testTime()
	bundle, plan := testGeneration(t, "run-cycle-status", testAppTarget(), now, now.Add(30*time.Second))
	prepared := testPrepared(t, plan.CellKey, now.Add(30*time.Second), desiredStub{bundle: bundle}, currentStub{observation: testAbsent(t, plan.CellKey)})
	validatedAt := prepared.EvaluatedAt.Add(time.Second)
	writer := &writerStub{enabled: true, result: acceptedReceipt(plan, prepared.Status, validatedAt)}
	valid, err := testCycle(
		t, plan.CellKey, &sourceStub{enabled: true, prepared: prepared}, testDryRunController(t, plan.CellKey, writer, validatedAt),
	).ReconcileOnce(context.Background())
	if err != nil || ValidateStatus(valid) != nil {
		t.Fatalf("build valid validation status: err=%v validation=%v", err, ValidateStatus(valid))
	}
	tests := map[string]func(*Status){
		"API":                func(value *Status) { value.APIVersion = "v2" },
		"cell":               func(value *Status) { value.CellKey = "backup/all/invalid" },
		"action":             func(value *Status) { value.Action = reconcile.ActionNoop },
		"prepared digest":    func(value *Status) { value.PreparedCycleDigest = strings.Repeat("0", 64) },
		"missing prepared":   func(value *Status) { value.PreparedCycle = nil },
		"prepared evidence":  func(value *Status) { value.PreparedCycle.Digest = "invalid" },
		"candidate digest":   func(value *Status) { value.CandidatePlanDigest = strings.Repeat("0", 64) },
		"validation outcome": func(value *Status) { value.ValidationOutcome = dryrunreconciler.OutcomeRejected },
		"validation digest":  func(value *Status) { value.ValidationStatusDigest = strings.Repeat("0", 64) },
		"missing validation": func(value *Status) { value.ValidationStatus = nil },
		"nested validation":  func(value *Status) { value.ValidationStatus.Digest = "invalid" },
		"attempt time":       func(value *Status) { value.ValidationAttemptedAt = timePointer(value.EvaluatedAt) },
		"ready":              func(value *Status) { value.Ready = true },
		"converged":          func(value *Status) { value.Converged = true },
		"LKG":                func(value *Status) { value.LastKnownGoodServing = true },
		"blocked":            func(value *Status) { value.Blocked = true },
		"retryable":          func(value *Status) { value.Retryable = true },
		"not candidate":      func(value *Status) { value.MutationCandidate = false },
		"not required":       func(value *Status) { value.ValidationRequired = false },
		"not attempted":      func(value *Status) { value.ValidationAttempted = false },
		"not accepted":       func(value *Status) { value.ValidationAccepted = false },
		"preserved":          func(value *Status) { value.ExistingObjectPreserved = true },
		"persisted":          func(value *Status) { value.Persisted = true },
		"delete":             func(value *Status) { value.DeleteAllowed = true },
		"not observation":    func(value *Status) { value.ObservationOnly = false },
		"execution":          func(value *Status) { value.ExecutionAllowed = true },
		"production":         func(value *Status) { value.ProductionMutationAllowed = true },
		"idempotency":        func(value *Status) { value.IdempotencyKey = "other" },
		"digest":             func(value *Status) { value.Digest = "invalid" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := cloneStatus(valid)
			mutate(&candidate)
			if name != "digest" {
				candidate.Digest = DigestStatus(candidate)
			}
			if err := ValidateStatus(candidate); !errors.Is(err, ErrInvariant) {
				t.Fatalf("status drift error = %v, want ErrInvariant", err)
			}
		})
	}
}

func TestValidationCycleIsDeterministicUnderConcurrentEvaluation(t *testing.T) {
	t.Parallel()
	now := testTime()
	bundle, plan := testGeneration(t, "run-cycle-concurrent", testAppTarget(), now, now.Add(30*time.Second))
	prepared := testPrepared(t, plan.CellKey, now.Add(30*time.Second), desiredStub{bundle: bundle}, currentStub{observation: testAbsent(t, plan.CellKey)})
	validatedAt := prepared.EvaluatedAt.Add(time.Second)
	writer := &writerStub{enabled: true, result: acceptedReceipt(plan, prepared.Status, validatedAt)}
	source := &sourceStub{enabled: true, prepared: prepared}
	cycle := testCycle(t, plan.CellKey, source, testDryRunController(t, plan.CellKey, writer, validatedAt))
	const workers = 64
	results := make(chan Status, workers)
	errorsFound := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			status, err := cycle.ReconcileOnce(context.Background())
			results <- status
			errorsFound <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatalf("concurrent validation cycle error: %v", err)
		}
	}
	var first Status
	for status := range results {
		if first.Digest == "" {
			first = status
			continue
		}
		if !reflect.DeepEqual(status, first) {
			t.Fatalf("concurrent status drifted: got=%#v want=%#v", status, first)
		}
	}
	if source.calls.Load() != workers || writer.calls.Load() != workers || ValidateStatus(first) != nil {
		t.Fatalf("concurrent call count/status drifted: source=%d writer=%d status=%#v", source.calls.Load(), writer.calls.Load(), first)
	}
}

func TestValidationCycleProductionDependencyBoundary(t *testing.T) {
	t.Parallel()
	output, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("list validation cycle dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{"database/sql", "os/exec", "fugue/internal/backupmaterializer"} {
			if dependency == forbidden {
				t.Fatalf("validation cycle gained forbidden dependency %q", dependency)
			}
		}
		for _, prefix := range []string{
			"k8s.io/", "fugue/internal/api", "fugue/internal/auth", "fugue/internal/store", "fugue/internal/model",
			"fugue/internal/backupmaterializer/agent", "fugue/internal/backupmaterializer/client",
			"fugue/internal/backupmaterializer/composition", "fugue/internal/backupmaterializer/httpapi",
			"fugue/internal/backupmaterializer/secretreader", "fugue/internal/backupmaterializer/secretwriter/projected",
			"fugue/internal/backupmaterializeridentity", "fugue/internal/backupmaterializerreview",
		} {
			if strings.HasPrefix(dependency, prefix) {
				t.Fatalf("validation cycle crossed component boundary through %q", dependency)
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
		"fugue/internal/backupmaterializer/validationcycle",
	}
	if !reflect.DeepEqual(local, wantLocal) {
		t.Fatalf("validation cycle local closure drifted: got=%v want=%v", local, wantLocal)
	}
	directOutput, err := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".").Output()
	if err != nil {
		t.Fatalf("list direct validation cycle imports: %v", err)
	}
	gotDirect := strings.Fields(string(directOutput))
	sort.Strings(gotDirect)
	wantDirect := []string{
		"context", "crypto/sha256", "encoding/hex", "encoding/json", "errors", "fmt",
		"fugue/internal/backupmaterializer/dryrunreconciler", "fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconcile", "fugue/internal/backupmaterializer/reconciler",
		"reflect", "strings", "time",
	}
	sort.Strings(wantDirect)
	if !reflect.DeepEqual(gotDirect, wantDirect) {
		t.Fatalf("validation cycle direct imports drifted: got=%v want=%v", gotDirect, wantDirect)
	}
}

func testCycle(t *testing.T, cellKey string, source Source, validator CandidateValidator) *Cycle {
	t.Helper()
	cycle, err := New(Config{Enabled: true, CellKey: cellKey, Source: source, Validator: validator})
	if err != nil {
		t.Fatalf("construct validation cycle: %v", err)
	}
	return cycle
}

func testDryRunController(t *testing.T, cellKey string, writer dryrunreconciler.Validator, now time.Time) *dryrunreconciler.Reconciler {
	t.Helper()
	controller, err := dryrunreconciler.New(dryrunreconciler.Config{
		Enabled: true, CellKey: cellKey, Validator: writer, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("construct dry-run controller: %v", err)
	}
	return controller
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

func testTime() time.Time { return time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC) }

func cloneMap(value map[string]string) map[string]string {
	result := make(map[string]string, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func cloneStatus(value Status) Status {
	cloned := value
	if value.PreparedCycle != nil {
		prepared := *value.PreparedCycle
		prepared.Status = cloneReconcileStatus(prepared.Status)
		cloned.PreparedCycle = &prepared
	}
	if value.ValidationStatus != nil {
		validation := cloneDryRunStatus(*value.ValidationStatus)
		cloned.ValidationStatus = &validation
	}
	if value.ValidationAttemptedAt != nil {
		attemptedAt := *value.ValidationAttemptedAt
		cloned.ValidationAttemptedAt = &attemptedAt
	}
	return cloned
}

func cloneReconcileStatus(value reconciler.Status) reconciler.Status {
	cloned := value
	if value.Decision != nil {
		decision := *value.Decision
		cloned.Decision = &decision
	}
	return cloned
}

func cloneDryRunStatus(value dryrunreconciler.Status) dryrunreconciler.Status {
	cloned := value
	if value.ReconcileStatus != nil {
		status := cloneReconcileStatus(*value.ReconcileStatus)
		cloned.ReconcileStatus = &status
	}
	if value.Receipt != nil {
		receipt := *value.Receipt
		cloned.Receipt = &receipt
	}
	return cloned
}

func assertStatusSecretFree(t *testing.T, status Status, plan materialization.Plan, now time.Time, extra string) {
	t.Helper()
	document, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal validation status: %v", err)
	}
	data, err := plan.Data(now)
	if err != nil {
		t.Fatalf("read private fixture: %v", err)
	}
	rendered := string(document) + fmt.Sprintf("%#v %v", status, status)
	for _, sensitive := range []string{string(data.SpecDocument), string(data.ObserverToken), "tenant-1", extra} {
		if sensitive != "" && strings.Contains(rendered, sensitive) {
			t.Fatalf("validation status leaked private input %q", sensitive)
		}
	}
}
