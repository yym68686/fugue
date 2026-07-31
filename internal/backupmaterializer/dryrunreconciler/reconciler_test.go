package dryrunreconciler

import (
	"bytes"
	"context"
	"encoding/base64"
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
	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
	"fugue/internal/backupmaterializer/reconciler"
	"fugue/internal/backupmaterializer/secretwriter"
)

var _ Validator = (*secretwriter.Writer)(nil)

type validatorStub struct {
	enabled  bool
	result   secretwriter.Result
	err      error
	fn       func(context.Context, materialization.Plan, reconcile.Decision) (secretwriter.Result, error)
	calls    atomic.Int64
	mu       sync.Mutex
	plan     materialization.Plan
	decision reconcile.Decision
}

func (stub *validatorStub) Enabled() bool { return stub != nil && stub.enabled }

func (stub *validatorStub) DryRun(
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

type desiredStub struct {
	bundle materializercontract.ObserverInputBundle
}

func (stub desiredStub) Fetch(context.Context) (materializercontract.ObserverInputBundle, error) {
	return stub.bundle, nil
}

type currentStub struct {
	observation reconcile.Observation
}

func (stub currentStub) Observe(context.Context) (reconcile.Observation, error) {
	return stub.observation, nil
}

func TestDisabledDryRunReconcilerIgnoresEveryCapability(t *testing.T) {
	t.Parallel()
	var typedNil *validatorStub
	controller, err := New(Config{
		Enabled: false, CellKey: "private invalid cell", Validator: typedNil,
		Now: func() time.Time { panic("disabled clock used") },
	})
	if err != nil || controller.Enabled() || controller.cellKey != "" || controller.cellID != "" ||
		controller.validator != nil || controller.now != nil {
		t.Fatalf("disabled construction retained capability: controller=%#v err=%v", controller, err)
	}
	if _, err := controller.ValidateCandidateOnce(context.Background(), materialization.Plan{}, reconciler.Status{}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled validation error = %v, want ErrDisabled", err)
	}
	config := Config{CellKey: "private-cell"}
	if strings.Contains(fmt.Sprintf("%#v", config), "private-cell") || !strings.Contains(fmt.Sprintf("%#v", config), "[REDACTED]") {
		t.Fatalf("configuration formatting leaked: %#v", config)
	}
}

func TestValidateCandidateOnceBindsAcceptedCreateAndReplaceReceipts(t *testing.T) {
	t.Parallel()
	base := testTime()
	createBundle, createPlan := testGeneration(t, "run-create", testAppTarget(), base)
	createCycle := testCycle(t, createBundle, createPlan, testAbsent(t, createPlan.CellKey), base.Add(30*time.Second))

	oldBundle, oldPlan := testGeneration(t, "run-old", testAppTarget(), base)
	_ = oldBundle
	replaceAt := base.Add(2*time.Minute + 30*time.Second)
	replaceBundle, replacePlan := testGeneration(t, "run-new", testAppTarget(), base.Add(2*time.Minute))
	replaceCycle := testCycle(t, replaceBundle, replacePlan, testManaged(t, oldPlan, base.Add(30*time.Second)), replaceAt)

	for _, fixture := range []struct {
		name      string
		plan      materialization.Plan
		cycle     reconciler.Status
		preserved bool
	}{
		{name: "create", plan: createPlan, cycle: createCycle, preserved: false},
		{name: "replace", plan: replacePlan, cycle: replaceCycle, preserved: true},
	} {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			validatedAt := fixture.cycle.EvaluatedAt.Add(time.Second)
			validator := &validatorStub{enabled: true, result: acceptedReceipt(fixture.plan, fixture.cycle, validatedAt)}
			controller := testController(t, fixture.plan.CellKey, validator, validatedAt)
			status, err := controller.ValidateCandidateOnce(context.Background(), fixture.plan, fixture.cycle)
			if err != nil || ValidateStatus(status) != nil || status.Outcome != OutcomeAccepted ||
				!status.ValidationAccepted || !status.ServerSideDryRunAccepted || status.Blocked || status.Retryable ||
				status.Persisted || status.DeleteAllowed || !status.ObservationOnly || status.ExecutionAllowed ||
				status.ProductionMutationAllowed || status.ExistingObjectPreserved != fixture.preserved ||
				status.Receipt == nil || status.ReceiptDigest != validator.result.Digest || status.AttemptedAt != validatedAt ||
				status.ReconcileStatus == nil || status.ReconcileStatus.Digest != fixture.cycle.Digest || validator.calls.Load() != 1 {
				t.Fatalf("accepted status drifted: status=%#v err=%v validation=%v calls=%d", status, err, ValidateStatus(status), validator.calls.Load())
			}
			validator.mu.Lock()
			gotPlanDigest := validator.plan.Digest
			gotDecisionDigest := validator.decision.Digest
			validator.mu.Unlock()
			if gotPlanDigest != fixture.plan.Digest || gotDecisionDigest != fixture.cycle.DecisionDigest {
				t.Fatalf("validator input detached: plan=%s decision=%s", gotPlanDigest, gotDecisionDigest)
			}
			originalDigest := status.Digest
			fixture.cycle.Decision.Digest = strings.Repeat("0", 64)
			if status.Digest != originalDigest || ValidateStatus(status) != nil {
				t.Fatal("status retained caller-owned decision pointer")
			}
			assertStatusSecretFree(t, status, fixture.plan, "")
		})
	}
}

func TestValidateCandidateOnceTranslatesFailuresIntoCellLocalStatus(t *testing.T) {
	t.Parallel()
	now := testTime()
	bundle, plan := testGeneration(t, "run-failures", testAppTarget(), now)
	cycle := testCycle(t, bundle, plan, testAbsent(t, plan.CellKey), now.Add(30*time.Second))
	attemptedAt := cycle.EvaluatedAt.Add(2 * time.Second)
	tests := []struct {
		name      string
		err       error
		outcome   Outcome
		retryable bool
	}{
		{name: "conflict", err: fmt.Errorf("private conflict: %w", secretwriter.ErrConflict), outcome: OutcomeConflict, retryable: true},
		{name: "rejected", err: fmt.Errorf("private rejection: %w", secretwriter.ErrRejected), outcome: OutcomeRejected, retryable: false},
		{name: "credential", err: fmt.Errorf("private credential: %w", secretwriter.ErrCredentialUnavailable), outcome: OutcomeCredentialUnavailable, retryable: true},
		{name: "unavailable", err: fmt.Errorf("private network: %w", secretwriter.ErrUnavailable), outcome: OutcomeUnavailable, retryable: true},
		{name: "response", err: fmt.Errorf("private response: %w", secretwriter.ErrResponse), outcome: OutcomeResponseInvalid, retryable: true},
		{name: "fresh reconcile", err: fmt.Errorf("private stale intent: %w", secretwriter.ErrIntent), outcome: OutcomeReconcileRequired, retryable: true},
		{name: "unknown", err: errors.New("private unknown validator failure"), outcome: OutcomeUnavailable, retryable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validator := &validatorStub{enabled: true, err: test.err}
			status, err := testController(t, plan.CellKey, validator, attemptedAt).
				ValidateCandidateOnce(context.Background(), plan, cycle)
			if err != nil || ValidateStatus(status) != nil || status.Outcome != test.outcome ||
				status.ValidationAccepted || status.ServerSideDryRunAccepted || !status.Blocked ||
				status.Retryable != test.retryable || status.Receipt != nil || status.ReceiptDigest != "" ||
				status.AttemptedAt != attemptedAt || validator.calls.Load() != 1 {
				t.Fatalf("failure status drifted: status=%#v err=%v validation=%v", status, err, ValidateStatus(status))
			}
			assertStatusSecretFree(t, status, plan, "private")
		})
	}
}

func TestValidateCandidateOnceRejectsInvalidInputBeforeClockAndValidator(t *testing.T) {
	t.Parallel()
	now := testTime()
	bundle, plan := testGeneration(t, "run-input", testAppTarget(), now)
	cycle := testCycle(t, bundle, plan, testAbsent(t, plan.CellKey), now.Add(30*time.Second))
	otherBundle, otherPlan := testGeneration(t, "run-other", testRegistryTarget(), now)
	otherCycle := testCycle(t, otherBundle, otherPlan, testAbsent(t, otherPlan.CellKey), now.Add(30*time.Second))
	managed := testManaged(t, plan, cycle.EvaluatedAt)
	noopCycle := testCycle(t, bundle, plan, managed, cycle.EvaluatedAt)
	invalidCycle := cloneCycle(cycle)
	invalidCycle.Digest = "invalid"

	tests := map[string]struct {
		plan  materialization.Plan
		cycle reconciler.Status
	}{
		"empty plan":       {plan: materialization.Plan{}, cycle: cycle},
		"cross-cell plan":  {plan: otherPlan, cycle: cycle},
		"cross-cell cycle": {plan: plan, cycle: otherCycle},
		"no-op cycle":      {plan: plan, cycle: noopCycle},
		"invalid cycle":    {plan: plan, cycle: invalidCycle},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var clockCalls atomic.Int64
			validator := &validatorStub{enabled: true, result: acceptedReceipt(plan, cycle, cycle.EvaluatedAt)}
			controller, err := New(Config{
				Enabled: true, CellKey: plan.CellKey, Validator: validator,
				Now: func() time.Time { clockCalls.Add(1); return cycle.EvaluatedAt },
			})
			if err != nil {
				t.Fatalf("construct controller: %v", err)
			}
			if _, err := controller.ValidateCandidateOnce(context.Background(), test.plan, test.cycle); !errors.Is(err, ErrInvariant) {
				t.Fatalf("invalid input error = %v, want ErrInvariant", err)
			}
			if validator.calls.Load() != 0 || clockCalls.Load() != 0 {
				t.Fatalf("invalid input crossed capability boundary: validator=%d clock=%d", validator.calls.Load(), clockCalls.Load())
			}
		})
	}

	validator := &validatorStub{enabled: true, result: acceptedReceipt(plan, cycle, cycle.EvaluatedAt)}
	controller := testController(t, plan.CellKey, validator, cycle.EvaluatedAt.Add(-time.Second))
	if _, err := controller.ValidateCandidateOnce(context.Background(), plan, cycle); !errors.Is(err, ErrInvariant) || validator.calls.Load() != 0 {
		t.Fatalf("backward clock crossed validator: err=%v calls=%d", err, validator.calls.Load())
	}
}

func TestValidateCandidateOncePropagatesCancellation(t *testing.T) {
	t.Parallel()
	now := testTime()
	bundle, plan := testGeneration(t, "run-cancel", testAppTarget(), now)
	cycle := testCycle(t, bundle, plan, testAbsent(t, plan.CellKey), now.Add(30*time.Second))
	validator := &validatorStub{enabled: true, result: acceptedReceipt(plan, cycle, cycle.EvaluatedAt)}
	controller := testController(t, plan.CellKey, validator, cycle.EvaluatedAt)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := controller.ValidateCandidateOnce(canceled, plan, cycle); !errors.Is(err, context.Canceled) || validator.calls.Load() != 0 {
		t.Fatalf("pre-canceled call: err=%v calls=%d", err, validator.calls.Load())
	}

	during, cancelDuring := context.WithCancel(context.Background())
	validator.fn = func(context.Context, materialization.Plan, reconcile.Decision) (secretwriter.Result, error) {
		cancelDuring()
		return secretwriter.Result{}, secretwriter.ErrUnavailable
	}
	if _, err := controller.ValidateCandidateOnce(during, plan, cycle); !errors.Is(err, context.Canceled) || validator.calls.Load() != 1 {
		t.Fatalf("mid-call cancellation: err=%v calls=%d", err, validator.calls.Load())
	}
}

func TestValidateCandidateOnceRejectsInvalidReceiptBinding(t *testing.T) {
	t.Parallel()
	now := testTime()
	bundle, plan := testGeneration(t, "run-receipt", testAppTarget(), now)
	cycle := testCycle(t, bundle, plan, testAbsent(t, plan.CellKey), now.Add(30*time.Second))
	valid := acceptedReceipt(plan, cycle, cycle.EvaluatedAt.Add(time.Second))
	tests := map[string]func(*secretwriter.Result){
		"cell":            func(value *secretwriter.Result) { value.CellKey = "backup/all/invalid" },
		"action":          func(value *secretwriter.Result) { value.Action = reconcile.ActionNoop },
		"plan":            func(value *secretwriter.Result) { value.PlanDigest = strings.Repeat("0", 64) },
		"decision":        func(value *secretwriter.Result) { value.DecisionDigest = strings.Repeat("0", 64) },
		"before decision": func(value *secretwriter.Result) { value.ValidatedAt = cycle.EvaluatedAt.Add(-time.Second) },
		"before attempt":  func(value *secretwriter.Result) { value.ValidatedAt = cycle.EvaluatedAt },
		"stale": func(value *secretwriter.Result) {
			value.ValidatedAt = cycle.EvaluatedAt.Add(secretwriter.MaximumDecisionAge + time.Second)
		},
		"accepted": func(value *secretwriter.Result) { value.Accepted = false },
		"digest":   func(value *secretwriter.Result) { value.Digest = "invalid" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			receipt := valid
			mutate(&receipt)
			if name != "digest" {
				receipt.Digest = secretwriter.DigestResult(receipt)
			}
			validator := &validatorStub{enabled: true, result: receipt}
			if _, err := testController(t, plan.CellKey, validator, cycle.EvaluatedAt.Add(time.Second)).
				ValidateCandidateOnce(context.Background(), plan, cycle); !errors.Is(err, ErrInvariant) {
				t.Fatalf("invalid receipt error = %v, want ErrInvariant", err)
			}
			if validator.calls.Load() != 1 {
				t.Fatalf("receipt was not tested through validator: calls=%d", validator.calls.Load())
			}
		})
	}
}

func TestStatusContractRejectsEveryCapabilityAndBindingDrift(t *testing.T) {
	t.Parallel()
	now := testTime()
	bundle, plan := testGeneration(t, "run-status", testAppTarget(), now)
	cycle := testCycle(t, bundle, plan, testAbsent(t, plan.CellKey), now.Add(30*time.Second))
	validator := &validatorStub{enabled: true, result: acceptedReceipt(plan, cycle, cycle.EvaluatedAt.Add(time.Second))}
	valid, err := testController(t, plan.CellKey, validator, cycle.EvaluatedAt.Add(time.Second)).
		ValidateCandidateOnce(context.Background(), plan, cycle)
	if err != nil || ValidateStatus(valid) != nil {
		t.Fatalf("build valid status: err=%v validation=%v", err, ValidateStatus(valid))
	}
	tests := map[string]func(*Status){
		"API":             func(value *Status) { value.APIVersion = "v2" },
		"cell":            func(value *Status) { value.CellKey = "backup/all/invalid" },
		"namespace":       func(value *Status) { value.Namespace = "default" },
		"action":          func(value *Status) { value.Action = reconcile.ActionNoop },
		"cycle digest":    func(value *Status) { value.ReconcileStatusDigest = strings.Repeat("0", 64) },
		"plan digest":     func(value *Status) { value.DesiredPlanDigest = strings.Repeat("0", 64) },
		"decision digest": func(value *Status) { value.DecisionDigest = strings.Repeat("0", 64) },
		"missing cycle":   func(value *Status) { value.ReconcileStatus = nil },
		"nested cycle":    func(value *Status) { value.ReconcileStatus.Digest = "invalid" },
		"time":            func(value *Status) { value.AttemptedAt = value.EvaluatedAt.Add(-time.Second) },
		"not candidate":   func(value *Status) { value.MutationCandidate = false },
		"not accepted":    func(value *Status) { value.ValidationAccepted = false },
		"not dry-run":     func(value *Status) { value.ServerSideDryRunAccepted = false },
		"preserved":       func(value *Status) { value.ExistingObjectPreserved = true },
		"blocked":         func(value *Status) { value.Blocked = true },
		"retryable":       func(value *Status) { value.Retryable = true },
		"persisted":       func(value *Status) { value.Persisted = true },
		"delete":          func(value *Status) { value.DeleteAllowed = true },
		"not observation": func(value *Status) { value.ObservationOnly = false },
		"execution":       func(value *Status) { value.ExecutionAllowed = true },
		"production":      func(value *Status) { value.ProductionMutationAllowed = true },
		"missing receipt": func(value *Status) { value.Receipt = nil },
		"receipt digest":  func(value *Status) { value.ReceiptDigest = strings.Repeat("0", 64) },
		"idempotency":     func(value *Status) { value.IdempotencyKey = "other" },
		"digest":          func(value *Status) { value.Digest = "invalid" },
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

	failureValidator := &validatorStub{enabled: true, err: secretwriter.ErrRejected}
	failure, err := testController(t, plan.CellKey, failureValidator, cycle.EvaluatedAt.Add(time.Second)).
		ValidateCandidateOnce(context.Background(), plan, cycle)
	if err != nil || ValidateStatus(failure) != nil {
		t.Fatalf("build rejected status: err=%v validation=%v", err, ValidateStatus(failure))
	}
	failure.Retryable = true
	failure.Digest = DigestStatus(failure)
	if err := ValidateStatus(failure); !errors.Is(err, ErrInvariant) {
		t.Fatalf("rejected status became retryable: %v", err)
	}
}

func TestDryRunCycleIsDeterministicUnderConcurrentEvaluation(t *testing.T) {
	t.Parallel()
	now := testTime()
	bundle, plan := testGeneration(t, "run-concurrent", testAppTarget(), now)
	cycle := testCycle(t, bundle, plan, testAbsent(t, plan.CellKey), now.Add(30*time.Second))
	validatedAt := cycle.EvaluatedAt.Add(time.Second)
	validator := &validatorStub{enabled: true, result: acceptedReceipt(plan, cycle, validatedAt)}
	controller := testController(t, plan.CellKey, validator, validatedAt)
	const workers = 64
	results := make(chan Status, workers)
	errorsCh := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			status, err := controller.ValidateCandidateOnce(context.Background(), plan, cycle)
			results <- status
			errorsCh <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent validation error: %v", err)
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
	if validator.calls.Load() != workers || ValidateStatus(first) != nil {
		t.Fatalf("concurrent call count/status drifted: calls=%d status=%#v", validator.calls.Load(), first)
	}
}

func TestDryRunReconcilerRejectsInvalidConstructionAndNilReceiver(t *testing.T) {
	t.Parallel()
	cellKey := testCellKey(t, testAppTarget())
	var typedNil *validatorStub
	tests := map[string]Config{
		"invalid cell":        {Enabled: true, CellKey: "backup/all/invalid", Validator: &validatorStub{enabled: true}},
		"nil validator":       {Enabled: true, CellKey: cellKey},
		"typed nil validator": {Enabled: true, CellKey: cellKey, Validator: typedNil},
		"disabled validator":  {Enabled: true, CellKey: cellKey, Validator: &validatorStub{}},
	}
	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := New(config); !errors.Is(err, ErrConfig) {
				t.Fatalf("construction error = %v, want ErrConfig", err)
			}
		})
	}
	if (*Reconciler)(nil).Enabled() || !strings.Contains((*Reconciler)(nil).String(), "<nil>") {
		t.Fatal("nil reconciler enablement or formatting drifted")
	}
	if _, err := (*Reconciler)(nil).ValidateCandidateOnce(context.Background(), materialization.Plan{}, reconciler.Status{}); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil receiver error = %v, want ErrConfig", err)
	}
}

func TestDryRunReconcilerProductionDependencyBoundary(t *testing.T) {
	t.Parallel()
	output, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("list dry-run reconciler dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{"database/sql", "os/exec", "fugue/internal/backupmaterializer"} {
			if dependency == forbidden {
				t.Fatalf("dry-run reconciler gained forbidden dependency %q", dependency)
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
				t.Fatalf("dry-run reconciler crossed component boundary through %q", dependency)
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
	}
	if !reflect.DeepEqual(local, wantLocal) {
		t.Fatalf("dry-run reconciler local closure drifted: got=%v want=%v", local, wantLocal)
	}
	directOutput, err := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".").Output()
	if err != nil {
		t.Fatalf("list direct dry-run reconciler imports: %v", err)
	}
	gotDirect := strings.Fields(string(directOutput))
	sort.Strings(gotDirect)
	wantDirect := []string{
		"context", "crypto/sha256", "encoding/hex", "encoding/json", "errors", "fmt",
		"fugue/internal/backupmaterializer/materialization", "fugue/internal/backupmaterializer/reconcile",
		"fugue/internal/backupmaterializer/reconciler", "fugue/internal/backupmaterializer/secretwriter",
		"reflect", "strings", "time",
	}
	sort.Strings(wantDirect)
	if !reflect.DeepEqual(gotDirect, wantDirect) {
		t.Fatalf("dry-run reconciler direct imports drifted: got=%v want=%v", gotDirect, wantDirect)
	}
}

func testController(t *testing.T, cellKey string, validator Validator, now time.Time) *Reconciler {
	t.Helper()
	controller, err := New(Config{Enabled: true, CellKey: cellKey, Validator: validator, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("construct dry-run reconciler: %v", err)
	}
	return controller
}

func testCycle(
	t *testing.T,
	bundle materializercontract.ObserverInputBundle,
	plan materialization.Plan,
	current reconcile.Observation,
	now time.Time,
) reconciler.Status {
	t.Helper()
	controller, err := reconciler.New(reconciler.Config{
		Enabled: true, CellKey: plan.CellKey, DesiredSource: desiredStub{bundle: bundle}, CurrentSource: currentStub{observation: current},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("construct source reconciler: %v", err)
	}
	status, err := controller.ReconcileOnce(context.Background())
	if err != nil || reconciler.ValidateStatus(status) != nil {
		t.Fatalf("evaluate source cycle: status=%#v err=%v validation=%v", status, err, reconciler.ValidateStatus(status))
	}
	return status
}

func acceptedReceipt(plan materialization.Plan, cycle reconciler.Status, validatedAt time.Time) secretwriter.Result {
	result := secretwriter.Result{
		APIVersion: secretwriter.APIVersion, Kind: secretwriter.Kind, Policy: secretwriter.Policy,
		Namespace: plan.Namespace, SecretName: plan.SecretName, CellKey: plan.CellKey, CellID: plan.CellID,
		Action: cycle.Action, PlanDigest: plan.Digest, DecisionDigest: cycle.DecisionDigest,
		RequestDigest:  "sha256:" + strings.Repeat("b", 64),
		IdempotencyKey: "backup-materializer-secret-dry-run/" + plan.CellID + "/" + strings.TrimPrefix(cycle.DecisionDigest, "sha256:"),
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
) (materializercontract.ObserverInputBundle, materialization.Plan) {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		runID, runID, target, "backend-1", "sha256:"+strings.Repeat("a", 64), 4, 120, 1800,
	)
	if err != nil {
		t.Fatalf("build backup spec: %v", err)
	}
	tokenID := "AAAAAAAAAAAAAAAAAAAAAA"
	claims := struct {
		Version       string `json:"v"`
		CredentialID  string `json:"credential_id"`
		TokenID       string `json:"token_id"`
		RunID         string `json:"run_id"`
		TenantID      string `json:"tenant_id"`
		CellKey       string `json:"cell_key"`
		SpecDigest    string `json:"spec_digest"`
		Permission    string `json:"permission"`
		IssuedAtUnix  int64  `json:"issued_at"`
		ExpiresAtUnix int64  `json:"expires_at"`
	}{
		Version: "v1", CredentialID: materializercontract.CredentialIDForCell(spec.CellKey), TokenID: tokenID,
		RunID: spec.RunID, TenantID: "tenant-1", CellKey: spec.CellKey, SpecDigest: spec.Digest,
		Permission: materializercontract.ObserverIdentityPermission, IssuedAtUnix: issuedAt.Unix(),
		ExpiresAtUnix: issuedAt.Add(materializercontract.ObserverIdentityTTL).Unix(),
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal observer claims: %v", err)
	}
	observerToken := "fugue_bo_v1." + base64.RawURLEncoding.EncodeToString([]byte("backup-key-1")) + "." +
		base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{'s'}, 32))
	bundle := materializercontract.ObserverInputBundle{
		APIVersion: materializercontract.ObserverInputBundleAPIVersion, Kind: materializercontract.ObserverInputBundleKind,
		Policy: materializercontract.ObserverInputBundlePolicy, CellKey: spec.CellKey, RunID: spec.RunID,
		SpecDigest: spec.Digest, CredentialID: claims.CredentialID, TokenID: tokenID, DesiredSpec: spec,
		ObserverToken: observerToken, IssuedAt: issuedAt,
		RenewAfter: issuedAt.Add(materializercontract.ObserverIdentityRenewAfter),
		ExpiresAt:  issuedAt.Add(materializercontract.ObserverIdentityTTL), ObservationOnly: true,
	}
	bundle.Digest = materializercontract.DigestObserverInputBundle(bundle)
	plan, err := materialization.Build(bundle, issuedAt.Add(30*time.Second))
	if err != nil {
		t.Fatalf("build materialization plan: %v", err)
	}
	return bundle, plan
}

func testAbsent(t *testing.T, cellKey string) reconcile.Observation {
	t.Helper()
	observation, err := reconcile.ObserveAbsent(cellKey)
	if err != nil {
		t.Fatalf("observe absent: %v", err)
	}
	return observation
}

func testManaged(t *testing.T, plan materialization.Plan, now time.Time) reconcile.Observation {
	t.Helper()
	manifest, err := reconcile.BuildManifest(plan, now)
	if err != nil {
		t.Fatalf("build current manifest: %v", err)
	}
	data, err := plan.Data(now)
	if err != nil {
		t.Fatalf("read current plan data: %v", err)
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

func testCellKey(t *testing.T, target backupcontrol.BackupTarget) string {
	t.Helper()
	key := backupcontrol.BackupCellKey(target)
	if key == "" {
		t.Fatalf("derive cell key for %+v", target)
	}
	return key
}

func testAppTarget() backupcontrol.BackupTarget {
	return backupcontrol.BackupTarget{Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database"}
}

func testRegistryTarget() backupcontrol.BackupTarget {
	return backupcontrol.BackupTarget{Type: backupcontrol.TargetRegistry, ScopeKey: "platform/registry"}
}

func testTime() time.Time { return time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC) }

func cloneMap(value map[string]string) map[string]string {
	result := make(map[string]string, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func cloneCycle(value reconciler.Status) reconciler.Status {
	cloned := value
	if value.Decision != nil {
		decision := *value.Decision
		cloned.Decision = &decision
	}
	return cloned
}

func cloneStatus(value Status) Status {
	cloned := value
	if value.ReconcileStatus != nil {
		cycle := cloneCycle(*value.ReconcileStatus)
		cloned.ReconcileStatus = &cycle
	}
	if value.Receipt != nil {
		receipt := *value.Receipt
		cloned.Receipt = &receipt
	}
	return cloned
}

func assertStatusSecretFree(t *testing.T, status Status, plan materialization.Plan, extra string) {
	t.Helper()
	document, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal dry-run status: %v", err)
	}
	data, err := plan.Data(status.EvaluatedAt)
	if err != nil {
		t.Fatalf("read secret fixture: %v", err)
	}
	rendered := string(document) + fmt.Sprintf("%#v %v", status, status)
	for _, sensitive := range []string{string(data.SpecDocument), string(data.ObserverToken), extra} {
		if sensitive != "" && strings.Contains(rendered, sensitive) {
			t.Fatalf("dry-run status leaked private input %q", sensitive)
		}
	}
}
