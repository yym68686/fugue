package releaseadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"fugue/internal/releasedomain"
)

type dispatcherProbe struct {
	mutex sync.Mutex

	domain        releasedomain.Domain
	adapterDomain releasedomain.Domain
	factoryErr    error
	factoryPanic  bool
	phaseErrors   map[Phase]error
	phasePanics   map[Phase]bool

	factoryCalls    int
	domainCalls     int
	commandForCalls map[Phase]int
	phaseCalls      map[Phase]int
	factoryAuth     releasedomain.ExecutionAuthorization
}

type dispatcherProbeAdapter struct {
	probe *dispatcherProbe
}

func newDispatcherProbe(domain releasedomain.Domain) *dispatcherProbe {
	return &dispatcherProbe{
		domain:          domain,
		adapterDomain:   domain,
		phaseErrors:     make(map[Phase]error),
		phasePanics:     make(map[Phase]bool),
		commandForCalls: make(map[Phase]int),
		phaseCalls:      make(map[Phase]int),
	}
}

func (probe *dispatcherProbe) factory(authorization releasedomain.ExecutionAuthorization) (DomainAdapter, error) {
	probe.mutex.Lock()
	probe.factoryCalls++
	probe.factoryAuth = authorization
	factoryErr := probe.factoryErr
	factoryPanic := probe.factoryPanic
	probe.mutex.Unlock()
	if factoryPanic {
		panic("private factory panic")
	}
	if factoryErr != nil {
		return nil, factoryErr
	}
	return &dispatcherProbeAdapter{probe: probe}, nil
}

func (adapter *dispatcherProbeAdapter) Domain() releasedomain.Domain {
	adapter.probe.mutex.Lock()
	defer adapter.probe.mutex.Unlock()
	adapter.probe.domainCalls++
	return adapter.probe.adapterDomain
}

func (adapter *dispatcherProbeAdapter) CommandFor(phase Phase) Command {
	adapter.probe.mutex.Lock()
	adapter.probe.commandForCalls[phase]++
	adapter.probe.mutex.Unlock()
	return CommandFunc(func(context.Context, releasedomain.ExecutionAuthorization) error {
		adapter.probe.mutex.Lock()
		adapter.probe.phaseCalls[phase]++
		phaseErr := adapter.probe.phaseErrors[phase]
		phasePanic := adapter.probe.phasePanics[phase]
		adapter.probe.mutex.Unlock()
		if phasePanic {
			panic("private phase panic")
		}
		return phaseErr
	})
}

type dispatcherProbeSnapshot struct {
	factoryCalls    int
	domainCalls     int
	commandForCalls map[Phase]int
	phaseCalls      map[Phase]int
	factoryAuth     releasedomain.ExecutionAuthorization
}

func (probe *dispatcherProbe) snapshot() dispatcherProbeSnapshot {
	probe.mutex.Lock()
	defer probe.mutex.Unlock()
	commandForCalls := make(map[Phase]int, len(probe.commandForCalls))
	for phase, count := range probe.commandForCalls {
		commandForCalls[phase] = count
	}
	phaseCalls := make(map[Phase]int, len(probe.phaseCalls))
	for phase, count := range probe.phaseCalls {
		phaseCalls[phase] = count
	}
	return dispatcherProbeSnapshot{
		factoryCalls:    probe.factoryCalls,
		domainCalls:     probe.domainCalls,
		commandForCalls: commandForCalls,
		phaseCalls:      phaseCalls,
		factoryAuth:     probe.factoryAuth,
	}
}

func dispatcherProbes() map[releasedomain.Domain]*dispatcherProbe {
	return map[releasedomain.Domain]*dispatcherProbe{
		releasedomain.DomainNodeLocal:        newDispatcherProbe(releasedomain.DomainNodeLocal),
		releasedomain.DomainAuthoritativeDNS: newDispatcherProbe(releasedomain.DomainAuthoritativeDNS),
		releasedomain.DomainControlPlane:     newDispatcherProbe(releasedomain.DomainControlPlane),
		releasedomain.DomainImageCache:       newDispatcherProbe(releasedomain.DomainImageCache),
		releasedomain.DomainBackup:           newDispatcherProbe(releasedomain.DomainBackup),
	}
}

func newTestDispatcher(t *testing.T, probes map[releasedomain.Domain]*dispatcherProbe) Dispatcher {
	t.Helper()
	dispatcher, err := NewDispatcher(
		probes[releasedomain.DomainNodeLocal].factory,
		probes[releasedomain.DomainAuthoritativeDNS].factory,
		probes[releasedomain.DomainControlPlane].factory,
		probes[releasedomain.DomainImageCache].factory,
		probes[releasedomain.DomainBackup].factory,
	)
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}
	return dispatcher
}

func TestDispatcherRunsOnlySelectedFactoryMetadataAndPhases(t *testing.T) {
	for _, selectedDomain := range releasedomain.KnownDomains() {
		selectedDomain := selectedDomain
		t.Run(string(selectedDomain), func(t *testing.T) {
			probes := dispatcherProbes()
			dispatcher := newTestDispatcher(t, probes)
			authorization := testAuthorization(t, selectedDomain)
			trace := &memoryTrace{}
			if err := dispatcher.Dispatch(context.Background(), time.Second, authorization, trace); err != nil {
				t.Fatalf("Dispatch() error = %v", err)
			}

			for _, domain := range releasedomain.KnownDomains() {
				snapshot := probes[domain].snapshot()
				if domain != selectedDomain {
					assertDispatcherProbeUnused(t, domain, snapshot)
					continue
				}
				if snapshot.factoryCalls != 1 || snapshot.domainCalls != 1 {
					t.Fatalf("selected %q factory/domain calls = %d/%d, want 1/1", domain, snapshot.factoryCalls, snapshot.domainCalls)
				}
				if err := snapshot.factoryAuth.Verify(); err != nil {
					t.Fatalf("selected %q factory received invalid authorization: %v", domain, err)
				}
				if snapshot.factoryAuth.Domain() != selectedDomain ||
					snapshot.factoryAuth.PlanDigest() != authorization.PlanDigest() ||
					snapshot.factoryAuth.Binding() != authorization.Binding() {
					t.Fatalf("selected %q factory authorization differs from dispatch authorization", domain)
				}
				for _, phase := range orderedPhases {
					if snapshot.commandForCalls[phase] != 1 {
						t.Errorf("selected %q CommandFor(%s) calls = %d, want 1", domain, phase, snapshot.commandForCalls[phase])
					}
				}
				if snapshot.phaseCalls[PhasePrepare] != 1 || snapshot.phaseCalls[PhaseApply] != 1 ||
					snapshot.phaseCalls[PhaseVerify] != 1 || snapshot.phaseCalls[PhaseRollback] != 0 {
					t.Errorf("selected %q phase calls = %#v", domain, snapshot.phaseCalls)
				}
			}
			if events := trace.snapshot(); len(events) != 8 || events[0].Domain != selectedDomain || events[7].State != TraceStateSucceeded {
				t.Fatalf("selected %q trace = %#v", selectedDomain, events)
			}
		})
	}
}

func TestDispatcherFiveDomainApplyFailuresRollbackSelectedExactlyOnce(t *testing.T) {
	for _, selectedDomain := range releasedomain.KnownDomains() {
		selectedDomain := selectedDomain
		t.Run(string(selectedDomain), func(t *testing.T) {
			applyErr := fmt.Errorf("%s apply fixture failure", selectedDomain)
			probes := dispatcherProbes()
			probes[selectedDomain].phaseErrors[PhaseApply] = applyErr
			dispatcher := newTestDispatcher(t, probes)
			err := dispatcher.Dispatch(
				context.Background(),
				time.Second,
				testAuthorization(t, selectedDomain),
				&memoryTrace{},
			)
			if !errors.Is(err, applyErr) {
				t.Fatalf("Dispatch() error = %v, want apply error", err)
			}

			for _, domain := range releasedomain.KnownDomains() {
				snapshot := probes[domain].snapshot()
				if domain != selectedDomain {
					assertDispatcherProbeUnused(t, domain, snapshot)
					continue
				}
				if snapshot.factoryCalls != 1 || snapshot.phaseCalls[PhasePrepare] != 1 ||
					snapshot.phaseCalls[PhaseApply] != 1 || snapshot.phaseCalls[PhaseVerify] != 0 ||
					snapshot.phaseCalls[PhaseRollback] != 1 {
					t.Fatalf("selected %q apply-failure calls = %#v", domain, snapshot)
				}
			}
		})
	}
}

func TestDispatcherFactoryFailureAndPanicProduceNoTraceOrCommand(t *testing.T) {
	factoryErr := errors.New("factory fixture failure")
	tests := []struct {
		name      string
		configure func(*dispatcherProbe)
		wantErr   error
	}{
		{
			name: "error",
			configure: func(probe *dispatcherProbe) {
				probe.factoryErr = factoryErr
			},
			wantErr: factoryErr,
		},
		{
			name: "panic",
			configure: func(probe *dispatcherProbe) {
				probe.factoryPanic = true
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probes := dispatcherProbes()
			selected := probes[releasedomain.DomainNodeLocal]
			test.configure(selected)
			dispatcher := newTestDispatcher(t, probes)
			trace := &memoryTrace{}
			err := dispatcher.Dispatch(
				context.Background(),
				time.Second,
				testAuthorization(t, releasedomain.DomainNodeLocal),
				trace,
			)
			if err == nil {
				t.Fatal("Dispatch() unexpectedly succeeded")
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("Dispatch() error = %v, want %v", err, test.wantErr)
			}
			if len(trace.snapshot()) != 0 || trace.barrierCalls != 0 {
				t.Fatalf("factory failure trace calls = events %d, barriers %d", len(trace.snapshot()), trace.barrierCalls)
			}
			snapshot := selected.snapshot()
			if snapshot.factoryCalls != 1 || snapshot.domainCalls != 0 || len(snapshot.commandForCalls) != 0 || len(snapshot.phaseCalls) != 0 {
				t.Fatalf("factory failure selected calls = %#v", snapshot)
			}
			for _, domain := range releasedomain.KnownDomains()[1:] {
				assertDispatcherProbeUnused(t, domain, probes[domain].snapshot())
			}
		})
	}
}

func TestDispatcherRejectsAdapterDomainMismatchBeforeTraceOrCommands(t *testing.T) {
	probes := dispatcherProbes()
	selected := probes[releasedomain.DomainNodeLocal]
	selected.adapterDomain = releasedomain.DomainBackup
	dispatcher := newTestDispatcher(t, probes)
	trace := &memoryTrace{}
	err := dispatcher.Dispatch(
		context.Background(),
		time.Second,
		testAuthorization(t, releasedomain.DomainNodeLocal),
		trace,
	)
	if err == nil {
		t.Fatal("Dispatch() unexpectedly accepted adapter domain mismatch")
	}
	if len(trace.snapshot()) != 0 || trace.barrierCalls != 0 {
		t.Fatalf("domain mismatch trace calls = events %d, barriers %d", len(trace.snapshot()), trace.barrierCalls)
	}
	snapshot := selected.snapshot()
	if snapshot.factoryCalls != 1 || snapshot.domainCalls != 1 || len(snapshot.phaseCalls) != 0 {
		t.Fatalf("domain mismatch selected calls = %#v", snapshot)
	}
	for _, phase := range orderedPhases {
		if snapshot.commandForCalls[phase] != 1 {
			t.Errorf("domain mismatch CommandFor(%s) calls = %d, want 1", phase, snapshot.commandForCalls[phase])
		}
	}
	for _, domain := range releasedomain.KnownDomains()[1:] {
		assertDispatcherProbeUnused(t, domain, probes[domain].snapshot())
	}
}

func TestDispatcherPhaseFailureMatrixRollbackExactlyOnce(t *testing.T) {
	prepareErr := errors.New("prepare fixture failure")
	applyErr := errors.New("apply fixture failure")
	verifyErr := errors.New("verify fixture failure")
	rollbackErr := errors.New("rollback fixture failure")
	tests := []struct {
		name          string
		errorsByPhase map[Phase]error
		wantCalls     map[Phase]int
		wantErrors    []error
	}{
		{
			name:      "success",
			wantCalls: map[Phase]int{PhasePrepare: 1, PhaseApply: 1, PhaseVerify: 1},
		},
		{
			name:          "prepare failure has no rollback",
			errorsByPhase: map[Phase]error{PhasePrepare: prepareErr},
			wantCalls:     map[Phase]int{PhasePrepare: 1},
			wantErrors:    []error{prepareErr},
		},
		{
			name:          "apply failure",
			errorsByPhase: map[Phase]error{PhaseApply: applyErr},
			wantCalls:     map[Phase]int{PhasePrepare: 1, PhaseApply: 1, PhaseRollback: 1},
			wantErrors:    []error{applyErr},
		},
		{
			name:          "verify failure",
			errorsByPhase: map[Phase]error{PhaseVerify: verifyErr},
			wantCalls:     map[Phase]int{PhasePrepare: 1, PhaseApply: 1, PhaseVerify: 1, PhaseRollback: 1},
			wantErrors:    []error{verifyErr},
		},
		{
			name:          "rollback failure is not retried",
			errorsByPhase: map[Phase]error{PhaseApply: applyErr, PhaseRollback: rollbackErr},
			wantCalls:     map[Phase]int{PhasePrepare: 1, PhaseApply: 1, PhaseRollback: 1},
			wantErrors:    []error{applyErr, rollbackErr},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probes := dispatcherProbes()
			selected := probes[releasedomain.DomainNodeLocal]
			for phase, phaseErr := range test.errorsByPhase {
				selected.phaseErrors[phase] = phaseErr
			}
			dispatcher := newTestDispatcher(t, probes)
			err := dispatcher.Dispatch(
				context.Background(),
				time.Second,
				testAuthorization(t, releasedomain.DomainNodeLocal),
				&memoryTrace{},
			)
			if len(test.wantErrors) == 0 && err != nil {
				t.Fatalf("Dispatch() error = %v", err)
			}
			if len(test.wantErrors) > 0 && err == nil {
				t.Fatal("Dispatch() unexpectedly succeeded")
			}
			for _, wantErr := range test.wantErrors {
				if !errors.Is(err, wantErr) {
					t.Fatalf("Dispatch() error = %v, want %v", err, wantErr)
				}
			}
			snapshot := selected.snapshot()
			for _, phase := range orderedPhases {
				if snapshot.phaseCalls[phase] != test.wantCalls[phase] {
					t.Errorf("%s calls = %d, want %d", phase, snapshot.phaseCalls[phase], test.wantCalls[phase])
				}
			}
			if snapshot.phaseCalls[PhaseRollback] > 1 {
				t.Fatalf("rollback calls = %d, want at most 1", snapshot.phaseCalls[PhaseRollback])
			}
			for _, domain := range releasedomain.KnownDomains()[1:] {
				assertDispatcherProbeUnused(t, domain, probes[domain].snapshot())
			}
		})
	}
}

func TestDispatcherRejectsInvalidAuthorizationBeforeAnyFactory(t *testing.T) {
	probes := dispatcherProbes()
	dispatcher := newTestDispatcher(t, probes)
	trace := &memoryTrace{}
	if err := dispatcher.Dispatch(
		context.Background(),
		time.Second,
		releasedomain.ExecutionAuthorization{},
		trace,
	); err == nil {
		t.Fatal("Dispatch() unexpectedly accepted zero authorization")
	}
	if len(trace.snapshot()) != 0 || trace.barrierCalls != 0 {
		t.Fatalf("invalid authorization trace calls = events %d, barriers %d", len(trace.snapshot()), trace.barrierCalls)
	}
	for _, domain := range releasedomain.KnownDomains() {
		assertDispatcherProbeUnused(t, domain, probes[domain].snapshot())
	}
}

func TestNewDispatcherRequiresEveryFixedFactory(t *testing.T) {
	valid := func(releasedomain.ExecutionAuthorization) (DomainAdapter, error) {
		return nil, errors.New("not called")
	}
	tests := []struct {
		name      string
		factories [5]AdapterFactory
	}{
		{name: "node-local", factories: [5]AdapterFactory{nil, valid, valid, valid, valid}},
		{name: "authoritative-dns", factories: [5]AdapterFactory{valid, nil, valid, valid, valid}},
		{name: "control-plane", factories: [5]AdapterFactory{valid, valid, nil, valid, valid}},
		{name: "image-cache", factories: [5]AdapterFactory{valid, valid, valid, nil, valid}},
		{name: "backup", factories: [5]AdapterFactory{valid, valid, valid, valid, nil}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewDispatcher(
				test.factories[0],
				test.factories[1],
				test.factories[2],
				test.factories[3],
				test.factories[4],
			); err == nil {
				t.Fatal("NewDispatcher() unexpectedly accepted nil factory")
			}
		})
	}
}

func assertDispatcherProbeUnused(t *testing.T, domain releasedomain.Domain, snapshot dispatcherProbeSnapshot) {
	t.Helper()
	if snapshot.factoryCalls != 0 || snapshot.domainCalls != 0 || len(snapshot.commandForCalls) != 0 || len(snapshot.phaseCalls) != 0 {
		t.Fatalf("unselected %q probe was used: %#v", domain, snapshot)
	}
}
