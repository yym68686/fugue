package releaseadapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/releasedomain"
)

type phaseLog struct {
	mutex  sync.Mutex
	phases []Phase
}

func (log *phaseLog) append(phase Phase) {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	log.phases = append(log.phases, phase)
}

func (log *phaseLog) snapshot() []Phase {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	return append([]Phase(nil), log.phases...)
}

type fakeAdapter struct {
	domain       releasedomain.Domain
	commands     map[Phase]Command
	panicDomain  bool
	panicCommand bool
}

type adversarialContext struct {
	context.Context
	panicOnErr atomic.Bool
}

func (ctx *adversarialContext) Err() error {
	if ctx.panicOnErr.Load() {
		panic("private context panic")
	}
	return ctx.Context.Err()
}

func (adapter *fakeAdapter) Domain() releasedomain.Domain {
	if adapter.panicDomain {
		panic("private adapter metadata")
	}
	return adapter.domain
}

func (adapter *fakeAdapter) CommandFor(phase Phase) Command {
	if adapter.panicCommand {
		panic("private command metadata")
	}
	return adapter.commands[phase]
}

type memoryTrace struct {
	mutex        sync.Mutex
	events       []TraceEvent
	failAt       map[uint64]error
	panicAt      map[uint64]bool
	onRecord     map[uint64]func()
	barrierErr   error
	barrierPanic bool
	barrierCalls int
	onBarrier    func()
}

func (trace *memoryTrace) Record(event TraceEvent) error {
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	if trace.panicAt[event.Sequence] {
		panic("private trace panic")
	}
	if callback := trace.onRecord[event.Sequence]; callback != nil {
		callback()
	}
	if err := trace.failAt[event.Sequence]; err != nil {
		return err
	}
	trace.events = append(trace.events, event)
	return nil
}

func (trace *memoryTrace) Barrier() error {
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	trace.barrierCalls++
	if trace.onBarrier != nil {
		trace.onBarrier()
	}
	if trace.barrierPanic {
		panic("private trace barrier panic")
	}
	return trace.barrierErr
}

func (trace *memoryTrace) snapshot() []TraceEvent {
	trace.mutex.Lock()
	defer trace.mutex.Unlock()
	return append([]TraceEvent(nil), trace.events...)
}

func testAuthorization(t *testing.T, domain releasedomain.Domain) releasedomain.ExecutionAuthorization {
	t.Helper()
	if _, err := releasedomain.ParseDomain(string(domain)); err != nil {
		t.Fatalf("fixture domain: %v", err)
	}
	objectRules := make([]releasedomain.ObjectRule, 0, len(releasedomain.KnownDomains()))
	for _, knownDomain := range releasedomain.KnownDomains() {
		objectRules = append(objectRules, releasedomain.ObjectRule{
			ID:        "fixture-" + string(knownDomain),
			Domain:    knownDomain,
			Version:   "v1",
			Kind:      "ConfigMap",
			Scope:     releasedomain.ScopeNamespaced,
			Namespace: "${releaseNamespace}",
			Name:      "fixture-" + string(knownDomain),
			RequiredLabels: map[string]string{
				"test.fugue.dev/domain": string(knownDomain),
			},
		})
	}
	spec := &releasedomain.OwnershipSpec{
		APIVersion:       releasedomain.OwnershipAPIVersion,
		Kind:             releasedomain.OwnershipKind,
		Domains:          releasedomain.KnownDomains(),
		RequiredBindings: []string{"releaseName", "releaseNamespace"},
		ObjectRules:      objectRules,
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("fixture ownership: %v", err)
	}
	ownership, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal fixture ownership: %v", err)
	}
	manifestForVersion := func(version string) []byte {
		raw := []byte(fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: fixture-%s
  namespace: fugue-system
  labels:
    test.fugue.dev/domain: %s
data:
  version: %s
`, domain, domain, version))
		canonical, canonicalErr := releasedomain.CanonicalizeRenderedManifest(raw, spec, "fugue-system")
		if canonicalErr != nil {
			t.Fatalf("canonicalize fixture manifest: %v", canonicalErr)
		}
		return canonical
	}
	baseManifest := manifestForVersion("base")
	targetManifest := manifestForVersion("target")
	classificationContext, err := releasedomain.NewClassificationContextEvidence(
		"fugue-system",
		map[string]string{"releaseName": "fugue", "releaseNamespace": "fugue-system"},
		false,
	)
	if err != nil {
		t.Fatalf("classification context: %v", err)
	}
	rendered := releasedomain.ClassifyRendered(baseManifest, targetManifest, spec, releasedomain.RenderedOptions{
		DefaultNamespace: "fugue-system",
		Bindings: map[string]string{
			"releaseName":      "fugue",
			"releaseNamespace": "fugue-system",
		},
	})
	plan := releasedomain.BuildPlan(releasedomain.PlanInput{
		Files: releasedomain.FileClassification{
			Domains:  []releasedomain.Domain{domain},
			Evidence: []releasedomain.Evidence{{Source: "changed-file", Subject: "fixture", Domains: []releasedomain.Domain{domain}}},
		},
		Rendered: rendered,
		Digests: releasedomain.DigestEvidence{
			Base:                   "41",
			Target:                 "42",
			Live:                   "41",
			BaseManifest:           testDigest(baseManifest),
			TargetManifest:         testDigest(targetManifest),
			RepeatedTargetManifest: testDigest(targetManifest),
			Ownership:              testDigest(ownership),
			ChangedFiles:           "sha256:changed-files",
			ClassificationContext:  classificationContext,
		},
	})
	if plan.Result != releasedomain.OutcomeSingle {
		t.Fatalf("fixture plan result = %q, want single: %#v", plan.Result, plan.Unknown)
	}
	envelope, err := releasedomain.NewTransactionEnvelope(plan, plan.PlanDigest, domain)
	if err != nil {
		t.Fatalf("new transaction envelope: %v", err)
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal transaction envelope: %v", err)
	}
	transactionAuthorization, err := releasedomain.DecodeAndVerifyTransactionEnvelope(
		bytes.NewReader(encoded),
		plan.PlanDigest,
		domain,
	)
	if err != nil {
		t.Fatalf("decode transaction envelope: %v", err)
	}
	authorization, err := releasedomain.VerifyRollbackOwnership(releasedomain.RollbackOwnershipInput{
		Transaction: transactionAuthorization,
		Binding: releasedomain.ExecutionBinding{
			ReleaseName:       "fugue",
			ReleaseNamespace:  "fugue-system",
			BaseRevision:      plan.Digests.Base,
			TargetRevision:    plan.Digests.Target,
			UpgradeArgvDigest: testDigest([]byte("helm\x00upgrade\x00fugue\x00")),
			HooksPolicy:       releasedomain.HooksPolicyNoHooks,
		},
		Ownership:              ownership,
		BaseManifest:           baseManifest,
		TargetManifest:         targetManifest,
		RepeatedTargetManifest: targetManifest,
	})
	if err != nil {
		t.Fatalf("verify rollback ownership: %v", err)
	}
	return authorization
}

func testDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", digest)
}

func TestRunAndCommandsRequireExecutionAuthorizationType(t *testing.T) {
	executionType := reflect.TypeOf(releasedomain.ExecutionAuthorization{})
	transactionType := reflect.TypeOf(releasedomain.TransactionAuthorization{})
	runType := reflect.TypeOf(Run)
	if got := runType.In(2); got != executionType || got == transactionType {
		t.Fatalf("Run authorization type = %s, want %s", got, executionType)
	}
	commandRun, ok := reflect.TypeOf((*Command)(nil)).Elem().MethodByName("Run")
	if !ok {
		t.Fatal("Command.Run method is missing")
	}
	if commandRun.Type.NumIn() != 2 {
		t.Fatalf("Command.Run input count = %d, want 2", commandRun.Type.NumIn())
	}
	if got := commandRun.Type.In(1); got != executionType || got == transactionType {
		t.Fatalf("Command.Run authorization type = %s, want %s", got, executionType)
	}
}

func completeFakeAdapter(
	domain releasedomain.Domain,
	log *phaseLog,
	errorsByPhase map[Phase]error,
	panics map[Phase]bool,
) *fakeAdapter {
	commands := make(map[Phase]Command, len(orderedPhases))
	for _, currentPhase := range orderedPhases {
		phase := currentPhase
		commands[phase] = CommandFunc(func(context.Context, releasedomain.ExecutionAuthorization) error {
			log.append(phase)
			if panics[phase] {
				panic("private phase panic")
			}
			return errorsByPhase[phase]
		})
	}
	return &fakeAdapter{domain: domain, commands: commands}
}

func TestRunTransactionPhaseMatrix(t *testing.T) {
	domain := releasedomain.DomainNodeLocal
	authorization := testAuthorization(t, domain)
	prepareErr := errors.New("prepare fixture failure")
	applyErr := errors.New("apply fixture failure")
	verifyErr := errors.New("verify fixture failure")
	rollbackErr := errors.New("rollback fixture failure")
	tests := []struct {
		name          string
		errorsByPhase map[Phase]error
		wantPhases    []Phase
		wantErrors    []error
	}{
		{name: "success", errorsByPhase: map[Phase]error{}, wantPhases: []Phase{PhasePrepare, PhaseApply, PhaseVerify}},
		{name: "prepare failure", errorsByPhase: map[Phase]error{PhasePrepare: prepareErr}, wantPhases: []Phase{PhasePrepare}, wantErrors: []error{prepareErr}},
		{name: "apply failure", errorsByPhase: map[Phase]error{PhaseApply: applyErr}, wantPhases: []Phase{PhasePrepare, PhaseApply, PhaseRollback}, wantErrors: []error{applyErr}},
		{name: "verify failure", errorsByPhase: map[Phase]error{PhaseVerify: verifyErr}, wantPhases: []Phase{PhasePrepare, PhaseApply, PhaseVerify, PhaseRollback}, wantErrors: []error{verifyErr}},
		{
			name:          "rollback failure is not retried",
			errorsByPhase: map[Phase]error{PhaseApply: applyErr, PhaseRollback: rollbackErr},
			wantPhases:    []Phase{PhasePrepare, PhaseApply, PhaseRollback},
			wantErrors:    []error{applyErr, rollbackErr},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			log := &phaseLog{}
			trace := &memoryTrace{}
			adapter := completeFakeAdapter(domain, log, test.errorsByPhase, nil)
			err := Run(context.Background(), time.Second, authorization, adapter, trace)
			if len(test.wantErrors) == 0 && err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if len(test.wantErrors) > 0 && err == nil {
				t.Fatal("Run() unexpectedly succeeded")
			}
			for _, wantErr := range test.wantErrors {
				if !errors.Is(err, wantErr) {
					t.Fatalf("Run() error %v does not preserve %v", err, wantErr)
				}
			}
			if got := fmt.Sprint(log.snapshot()); got != fmt.Sprint(test.wantPhases) {
				t.Fatalf("phase trace = %s, want %s", got, fmt.Sprint(test.wantPhases))
			}
			rollbackCount := 0
			for _, phase := range log.snapshot() {
				if phase == PhaseRollback {
					rollbackCount++
				}
			}
			if rollbackCount > 1 {
				t.Fatalf("rollback count = %d, want at most one", rollbackCount)
			}
		})
	}
}

func TestRunValidatesCompleteAdapterBeforeTraceOrCommands(t *testing.T) {
	domain := releasedomain.DomainNodeLocal
	authorization := testAuthorization(t, domain)
	tests := []struct {
		name    string
		adapter DomainAdapter
	}{
		{name: "nil adapter", adapter: nil},
		{name: "typed nil adapter", adapter: (*fakeAdapter)(nil)},
		{name: "unknown domain", adapter: &fakeAdapter{domain: "other", commands: map[Phase]Command{}}},
		{name: "domain mismatch", adapter: completeFakeAdapter(releasedomain.DomainBackup, &phaseLog{}, nil, nil)},
		{name: "domain panic", adapter: &fakeAdapter{panicDomain: true}},
		{name: "command lookup panic", adapter: &fakeAdapter{domain: domain, panicCommand: true}},
	}
	for _, missing := range orderedPhases {
		commands := completeFakeAdapter(domain, &phaseLog{}, nil, nil).commands
		delete(commands, missing)
		tests = append(tests, struct {
			name    string
			adapter DomainAdapter
		}{name: "missing " + string(missing), adapter: &fakeAdapter{domain: domain, commands: commands}})
	}
	var typedNil CommandFunc
	commands := completeFakeAdapter(domain, &phaseLog{}, nil, nil).commands
	commands[PhaseVerify] = typedNil
	tests = append(tests, struct {
		name    string
		adapter DomainAdapter
	}{name: "typed nil command", adapter: &fakeAdapter{domain: domain, commands: commands}})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace := &memoryTrace{}
			if err := Run(context.Background(), time.Second, authorization, test.adapter, trace); err == nil {
				t.Fatal("Run() unexpectedly succeeded")
			}
			if events := trace.snapshot(); len(events) != 0 {
				t.Fatalf("invalid adapter wrote %d trace events", len(events))
			}
		})
	}

	valid := completeFakeAdapter(domain, &phaseLog{}, nil, nil)
	if err := Run(context.Background(), time.Second, releasedomain.ExecutionAuthorization{}, valid, &memoryTrace{}); err == nil {
		t.Fatal("zero authorization unexpectedly succeeded")
	}
	if err := Run(nil, time.Second, authorization, valid, &memoryTrace{}); err == nil {
		t.Fatal("nil context unexpectedly succeeded")
	}
	var typedNilContext *adversarialContext
	if err := Run(typedNilContext, time.Second, authorization, valid, &memoryTrace{}); err == nil {
		t.Fatal("typed nil context unexpectedly succeeded")
	}
	if err := Run(context.Background(), 0, authorization, valid, &memoryTrace{}); err == nil {
		t.Fatal("zero rollback timeout unexpectedly succeeded")
	}
	if err := Run(context.Background(), time.Second, authorization, valid, nil); err == nil {
		t.Fatal("nil trace unexpectedly succeeded")
	}
	var typedNilTrace *memoryTrace
	if err := Run(context.Background(), time.Second, authorization, valid, typedNilTrace); err == nil {
		t.Fatal("typed nil trace unexpectedly succeeded")
	}
}

func TestRunTraceFailuresRespectWriteBoundary(t *testing.T) {
	domain := releasedomain.DomainNodeLocal
	authorization := testAuthorization(t, domain)
	traceErr := errors.New("trace fixture failure")
	tests := []struct {
		name       string
		failAt     uint64
		phaseError map[Phase]error
		wantPhases []Phase
	}{
		{name: "transaction start", failAt: 1, wantPhases: nil},
		{name: "prepare success", failAt: 3, wantPhases: []Phase{PhasePrepare}},
		{name: "apply start", failAt: 4, wantPhases: []Phase{PhasePrepare}},
		{name: "apply success", failAt: 5, wantPhases: []Phase{PhasePrepare, PhaseApply, PhaseRollback}},
		{name: "verify start", failAt: 6, wantPhases: []Phase{PhasePrepare, PhaseApply, PhaseRollback}},
		{name: "verify success", failAt: 7, wantPhases: []Phase{PhasePrepare, PhaseApply, PhaseVerify, PhaseRollback}},
		{name: "transaction success", failAt: 8, wantPhases: []Phase{PhasePrepare, PhaseApply, PhaseVerify, PhaseRollback}},
		{
			name:       "rollback start",
			failAt:     6,
			phaseError: map[Phase]error{PhaseApply: errors.New("apply failure")},
			wantPhases: []Phase{PhasePrepare, PhaseApply, PhaseRollback},
		},
		{
			name:       "rollback finish",
			failAt:     7,
			phaseError: map[Phase]error{PhaseApply: errors.New("apply failure")},
			wantPhases: []Phase{PhasePrepare, PhaseApply, PhaseRollback},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			log := &phaseLog{}
			adapter := completeFakeAdapter(domain, log, test.phaseError, nil)
			trace := &memoryTrace{failAt: map[uint64]error{test.failAt: traceErr}}
			err := Run(context.Background(), time.Second, authorization, adapter, trace)
			if err == nil || !errors.Is(err, traceErr) {
				t.Fatalf("Run() error = %v, want trace failure", err)
			}
			if got := fmt.Sprint(log.snapshot()); got != fmt.Sprint(test.wantPhases) {
				t.Fatalf("phase trace = %s, want %s", got, fmt.Sprint(test.wantPhases))
			}
			rollbackCount := 0
			for _, phase := range log.snapshot() {
				if phase == PhaseRollback {
					rollbackCount++
				}
			}
			if rollbackCount > 1 {
				t.Fatalf("rollback count = %d", rollbackCount)
			}
		})
	}
}

func TestRunTracePanicAfterApplyStillRollsBackOnce(t *testing.T) {
	domain := releasedomain.DomainNodeLocal
	authorization := testAuthorization(t, domain)
	log := &phaseLog{}
	err := Run(
		context.Background(),
		time.Second,
		authorization,
		completeFakeAdapter(domain, log, nil, nil),
		&memoryTrace{panicAt: map[uint64]bool{5: true}},
	)
	if !errors.Is(err, ErrTracePanic) {
		t.Fatalf("Run() error = %v, want trace panic sentinel", err)
	}
	if got := fmt.Sprint(log.snapshot()); got != fmt.Sprint([]Phase{PhasePrepare, PhaseApply, PhaseRollback}) {
		t.Fatalf("phase trace = %s", got)
	}
}

func TestRunPanicsAndCancellationUseExactlyOneRollbackAfterApply(t *testing.T) {
	domain := releasedomain.DomainNodeLocal
	authorization := testAuthorization(t, domain)
	for _, phase := range []Phase{PhasePrepare, PhaseApply, PhaseVerify, PhaseRollback} {
		t.Run("panic "+string(phase), func(t *testing.T) {
			log := &phaseLog{}
			errorsByPhase := map[Phase]error{}
			panics := map[Phase]bool{phase: true}
			if phase == PhaseRollback {
				errorsByPhase[PhaseApply] = errors.New("apply before rollback panic")
			}
			err := Run(
				context.Background(),
				time.Second,
				authorization,
				completeFakeAdapter(domain, log, errorsByPhase, panics),
				&memoryTrace{},
			)
			if err == nil || !errors.Is(err, ErrCommandPanic) {
				t.Fatalf("Run() error = %v, want panic sentinel", err)
			}
			rollbackCount := 0
			for _, called := range log.snapshot() {
				if called == PhaseRollback {
					rollbackCount++
				}
			}
			wantRollback := 0
			if phase != PhasePrepare {
				wantRollback = 1
			}
			if rollbackCount != wantRollback {
				t.Fatalf("rollback count = %d, want %d; phases=%v", rollbackCount, wantRollback, log.snapshot())
			}
		})
	}

	t.Run("apply cancellation uses independent rollback context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		log := &phaseLog{}
		adapter := completeFakeAdapter(domain, log, nil, nil)
		adapter.commands[PhaseApply] = CommandFunc(func(context.Context, releasedomain.ExecutionAuthorization) error {
			log.append(PhaseApply)
			cancel()
			return nil
		})
		adapter.commands[PhaseRollback] = CommandFunc(func(ctx context.Context, _ releasedomain.ExecutionAuthorization) error {
			log.append(PhaseRollback)
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("rollback inherited canceled context: %w", err)
			}
			return nil
		})
		err := Run(ctx, time.Second, authorization, adapter, &memoryTrace{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context canceled", err)
		}
		if got := fmt.Sprint(log.snapshot()); got != fmt.Sprint([]Phase{PhasePrepare, PhaseApply, PhaseRollback}) {
			t.Fatalf("phase trace = %s", got)
		}
	})

	t.Run("prepare cancellation has no rollback", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		log := &phaseLog{}
		adapter := completeFakeAdapter(domain, log, nil, nil)
		adapter.commands[PhasePrepare] = CommandFunc(func(context.Context, releasedomain.ExecutionAuthorization) error {
			log.append(PhasePrepare)
			cancel()
			return nil
		})
		err := Run(ctx, time.Second, authorization, adapter, &memoryTrace{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context canceled", err)
		}
		if got := fmt.Sprint(log.snapshot()); got != fmt.Sprint([]Phase{PhasePrepare}) {
			t.Fatalf("phase trace = %s", got)
		}
	})

	t.Run("context Err panic after apply rolls back", func(t *testing.T) {
		ctx := &adversarialContext{Context: context.Background()}
		log := &phaseLog{}
		adapter := completeFakeAdapter(domain, log, nil, nil)
		adapter.commands[PhaseApply] = CommandFunc(func(context.Context, releasedomain.ExecutionAuthorization) error {
			log.append(PhaseApply)
			ctx.panicOnErr.Store(true)
			return nil
		})
		err := Run(ctx, time.Second, authorization, adapter, &memoryTrace{})
		if !errors.Is(err, ErrContextPanic) {
			t.Fatalf("Run() error = %v, want context panic sentinel", err)
		}
		if got := fmt.Sprint(log.snapshot()); got != fmt.Sprint([]Phase{PhasePrepare, PhaseApply, PhaseRollback}) {
			t.Fatalf("phase trace = %s", got)
		}
	})

	t.Run("cancellation while recording verify success rolls back", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		log := &phaseLog{}
		trace := &memoryTrace{onRecord: map[uint64]func(){7: cancel}}
		err := Run(ctx, time.Second, authorization, completeFakeAdapter(domain, log, nil, nil), trace)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context canceled", err)
		}
		if got := fmt.Sprint(log.snapshot()); got != fmt.Sprint([]Phase{PhasePrepare, PhaseApply, PhaseVerify, PhaseRollback}) {
			t.Fatalf("phase trace = %s", got)
		}
	})

	t.Run("cancellation during pre-commit barrier rolls back", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		log := &phaseLog{}
		trace := &memoryTrace{onBarrier: cancel}
		err := Run(ctx, time.Second, authorization, completeFakeAdapter(domain, log, nil, nil), trace)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context canceled", err)
		}
		if got := fmt.Sprint(log.snapshot()); got != fmt.Sprint([]Phase{PhasePrepare, PhaseApply, PhaseVerify, PhaseRollback}) {
			t.Fatalf("phase trace = %s", got)
		}
	})

	t.Run("cancellation during success record loses commit race", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		log := &phaseLog{}
		trace := &memoryTrace{onRecord: map[uint64]func(){8: cancel}}
		if err := Run(ctx, time.Second, authorization, completeFakeAdapter(domain, log, nil, nil), trace); err != nil {
			t.Fatalf("Run() error after linearized success = %v", err)
		}
		if got := fmt.Sprint(log.snapshot()); got != fmt.Sprint([]Phase{PhasePrepare, PhaseApply, PhaseVerify}) {
			t.Fatalf("phase trace = %s", got)
		}
		events := trace.snapshot()
		if len(events) != 8 || events[7].Phase != TracePhaseTransaction || events[7].State != TraceStateSucceeded {
			t.Fatalf("terminal trace events = %#v", events)
		}
	})
}

func TestRunTraceBarrierFailureAfterApplyRollsBackOnce(t *testing.T) {
	domain := releasedomain.DomainNodeLocal
	authorization := testAuthorization(t, domain)
	for _, panicOnBarrier := range []bool{false, true} {
		name := "error"
		fixtureErr := errors.New("trace barrier fixture failure")
		traceErr := fixtureErr
		if panicOnBarrier {
			name = "panic"
			traceErr = ErrTracePanic
		}
		t.Run(name, func(t *testing.T) {
			log := &phaseLog{}
			trace := &memoryTrace{
				barrierErr:   fixtureErr,
				barrierPanic: panicOnBarrier,
			}
			err := Run(context.Background(), time.Second, authorization, completeFakeAdapter(domain, log, nil, nil), trace)
			if !errors.Is(err, traceErr) {
				t.Fatalf("Run() error = %v, want %v", err, traceErr)
			}
			if got := fmt.Sprint(log.snapshot()); got != fmt.Sprint([]Phase{PhasePrepare, PhaseApply, PhaseVerify, PhaseRollback}) {
				t.Fatalf("phase trace = %s", got)
			}
		})
	}
}

func TestRunRollbackTimeoutIsBoundedAndNotRetried(t *testing.T) {
	domain := releasedomain.DomainNodeLocal
	authorization := testAuthorization(t, domain)
	log := &phaseLog{}
	applyErr := errors.New("apply failure")
	adapter := completeFakeAdapter(domain, log, map[Phase]error{PhaseApply: applyErr}, nil)
	adapter.commands[PhaseRollback] = CommandFunc(func(ctx context.Context, _ releasedomain.ExecutionAuthorization) error {
		log.append(PhaseRollback)
		<-ctx.Done()
		return ctx.Err()
	})
	started := time.Now()
	err := Run(context.Background(), 20*time.Millisecond, authorization, adapter, &memoryTrace{})
	if !errors.Is(err, applyErr) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded rollback took %s", elapsed)
	}
	if got := fmt.Sprint(log.snapshot()); got != fmt.Sprint([]Phase{PhasePrepare, PhaseApply, PhaseRollback}) {
		t.Fatalf("phase trace = %s", got)
	}
}

func TestRunConcurrentTransactionsDoNotShareState(t *testing.T) {
	domain := releasedomain.DomainNodeLocal
	authorization := testAuthorization(t, domain)
	const count = 24
	var wait sync.WaitGroup
	errorsChannel := make(chan error, count)
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			log := &phaseLog{}
			trace := &memoryTrace{}
			err := Run(
				context.Background(),
				time.Second,
				authorization,
				completeFakeAdapter(domain, log, nil, nil),
				trace,
			)
			if err != nil {
				errorsChannel <- err
				return
			}
			if got := fmt.Sprint(log.snapshot()); got != fmt.Sprint([]Phase{PhasePrepare, PhaseApply, PhaseVerify}) {
				errorsChannel <- fmt.Errorf("phase trace = %s", got)
				return
			}
			if events := trace.snapshot(); len(events) != 8 || events[0].Sequence != 1 || events[7].Sequence != 8 {
				errorsChannel <- fmt.Errorf("event trace = %#v", events)
			}
		}()
	}
	wait.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		t.Error(err)
	}
}
