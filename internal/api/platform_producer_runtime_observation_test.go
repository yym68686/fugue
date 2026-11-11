package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

func producerRuntimeSnapshot(t *testing.T, s *Server) producerRuntimeObservation {
	t.Helper()
	value, err := s.ProducerRuntimeObservation(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	out := value.(producerRuntimeObservation)
	if out.SchemaVersion != producerRuntimeObservationSchema || out.BootID == "" || out.PID <= 0 || out.StartedAt.IsZero() || out.FleetCoverageProven {
		t.Fatal("invalid process identity or self-certified fleet coverage", out)
	}
	for _, calls := range []producerRuntimeCalls{out.Managers, out.Workers, out.NewInput} {
		if calls.Started != calls.Active+calls.Returned {
			t.Fatal("inconsistent function lifetime counters", calls)
		}
	}
	return out
}

func TestProducerRuntimeCanceledWorkerRemainsUntilReturn(t *testing.T) {
	s := &Server{producerCaptureRuntime: newProducerCaptureRuntime()}
	r := s.platformProducerRuntime()
	ctx, cancel := context.WithCancel(t.Context())
	managerReturned := r.manager()
	workerReturned := r.worker(ctx, platformproducer.Scope)
	inputReturned := r.input(platformproducer.Scope)
	before := producerRuntimeSnapshot(t, s)
	if before.Workers.Active != 1 || before.NewInput.Active != 1 || len(before.ActiveWorkers) != 1 || before.ActiveWorkers[0].CancellationSeen {
		t.Fatal("live worker is absent", before)
	}
	cancel()
	during := producerRuntimeSnapshot(t, s)
	if during.Workers.Active != 1 || during.NewInput.Active != 1 || !during.ActiveWorkers[0].CancellationSeen {
		t.Fatal("cancellation was mistaken for exit", during)
	}
	secondReturned := r.worker(t.Context(), platformproducer.Scope)
	if got := producerRuntimeSnapshot(t, s); got.Workers.Active != 2 || len(got.ActiveWorkers) != 2 || got.Scopes[0].Workers.Active != 2 {
		t.Fatal("overlapping lifetimes collapsed", got)
	}
	inputReturned()
	workerReturned()
	workerReturned()
	secondReturned()
	managerReturned()
	after := producerRuntimeSnapshot(t, s)
	if !after.TrackingComplete || after.Workers.Active != 0 || after.Workers.Returned != 2 || after.NewInput.Returned != 1 || after.Managers.Returned != 1 || len(after.ActiveWorkers) != 0 {
		t.Fatal("returned lifetimes were not joined", after)
	}
	after.Scopes[0].Scope = "mutated-copy"
	if producerRuntimeSnapshot(t, s).Scopes[0].Scope != platformproducer.Scope {
		t.Fatal("caller mutated retained observation state")
	}
}

func TestProducerRuntimeRealCaptureCancellationAndPanic(t *testing.T) {
	state, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	activateTestProducer(t, server, "runtime-observation", "shadow")
	initial := producerRuntimeSnapshot(t, server)
	if initial.InputDeferralProtocol != platformproducer.InputDeferralSchema {
		t.Fatal("finite control capability not identified", initial)
	}
	if initial.AdmissionInstalled || initial.Participant != nil || initial.ProducerStarted {
		t.Fatal("observation installed a production gate", initial)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := server.reconcilePlatformConfigurationWithCapture(ctx, func(captureCtx context.Context, _ model.Principal) (platformIntentProjectionResponse, error) {
			close(entered)
			<-release // intentionally remain alive after the caller cancels
			return platformIntentProjectionResponse{}, captureCtx.Err()
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("real producer did not enter capture")
	}
	cancel()
	active := producerRuntimeSnapshot(t, server)
	if active.NewInput.Active != 1 || active.NewInput.Returned != 0 || !active.ProducerStarted || active.AdmissionInstalled {
		close(release)
		t.Fatal("canceled real capture was declared returned", active)
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("capture cancellation changed", err)
	}
	func() {
		defer func() {
			if recover() != "synthetic-capture-panic" {
				t.Error("producer panic was changed or swallowed")
			}
		}()
		_, _ = server.reconcilePlatformConfigurationWithCapture(t.Context(), func(context.Context, model.Principal) (platformIntentProjectionResponse, error) {
			panic("synthetic-capture-panic")
		})
	}()
	returned := producerRuntimeSnapshot(t, server)
	if returned.NewInput != (producerRuntimeCalls{Started: 2, Returned: 2}) || !returned.TrackingComplete {
		t.Fatal("real capture error/panic lost its lifetime", returned)
	}
	restarted := NewServer(state, auth.New(state, ""), nil, ServerConfig{})
	fresh := producerRuntimeSnapshot(t, restarted)
	if fresh.BootID == returned.BootID || fresh.NewInput.Started != 0 {
		t.Fatal("new boot inherited another process's evidence", fresh)
	}
}

func TestProducerRuntimeBoundedOverflowNeverBecomesCompleteZero(t *testing.T) {
	s := &Server{producerCaptureRuntime: newProducerCaptureRuntime()}
	r := s.platformProducerRuntime()
	for i := range 33 {
		r.input(fmt.Sprintf("%s:cell-%d", platformproducer.Scope, i))()
	}
	returns := []func(){}
	for range 129 {
		returns = append(returns, r.worker(t.Context(), platformproducer.Scope+":cell-0"))
	}
	active := producerRuntimeSnapshot(t, s)
	if active.TrackingComplete || len(active.Scopes) != 32 || len(active.ActiveWorkers) != 128 || active.Workers.Active != 129 {
		t.Fatal("observation exceeded bounds or hid omitted work", active)
	}
	for _, returned := range returns {
		returned()
	}
	zero := producerRuntimeSnapshot(t, s)
	raw, err := json.Marshal(zero)
	if err != nil || len(raw) > 65536 || zero.TrackingComplete || zero.NewInput.Active != 0 || zero.Workers.Active != 0 {
		t.Fatal("overflow became a complete empty roster or unbounded response", len(raw), err, zero)
	}
}

func TestProducerRuntimeConcurrentSnapshotsAndEnrollmentBinding(t *testing.T) {
	s := &Server{producerCaptureRuntime: newProducerCaptureRuntime()}
	r := s.platformProducerRuntime()
	db := &captureParticipantTestStore{}
	p, err := newPlatformProducerCaptureParticipantForBoot(t.Context(), db, r.bootID, []string{platformproducer.Scope})
	if err != nil {
		t.Fatal(err)
	}
	s.producerCaptureParticipant = p
	s.producerCaptureAdmission = s.admitPlatformProducerCapture
	finish, _, err := p.admit(t.Context(), platformproducer.Scope, model.PlatformPublicationPrecondition{})
	if err != nil {
		t.Fatal(err)
	}
	observed := producerRuntimeSnapshot(t, s)
	if observed.Participant.ID != observed.BootID || observed.Participant.LocalAttempts != 1 || observed.Participant.Closed || !observed.AdmissionInstalled {
		t.Fatal("enrollment is not bound to the observed boot", observed)
	}
	observed.Participant.Scopes[0] = "mutated-copy"
	if producerRuntimeSnapshot(t, s).Participant.Scopes[0] != platformproducer.Scope {
		t.Fatal("caller mutated participant scopes")
	}
	var wg sync.WaitGroup
	for range 24 {
		wg.Go(func() {
			returned := r.input(platformproducer.Scope)
			defer returned()
			// Concurrent readers may receive the documented bounded refusal;
			// refusal must not leak a function lifetime or stop business work.
			_, _ = s.ProducerRuntimeObservation(t.Context())
		})
	}
	wg.Wait()
	if err := finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := p.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	closed := producerRuntimeSnapshot(t, s)
	if closed.NewInput != (producerRuntimeCalls{Started: 24, Returned: 24}) || !closed.Participant.Closed || closed.Participant.LocalAttempts != 0 {
		t.Fatal("concurrent lifetimes or close observation differ", closed)
	}
	s.producerCaptureMu.Lock()
	_, err = s.ProducerRuntimeObservation(t.Context())
	s.producerCaptureMu.Unlock()
	if err == nil {
		t.Fatal("observation waited through private setup contention")
	}
}

func TestProducerRuntimeRealBackgroundJoinsWorkers(t *testing.T) {
	_, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	started := make(chan struct{}, 1)
	server.log = log.New(producerLeadershipLog{started}, "", 0)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		server.StartBackgroundPlatformConfiguration(ctx)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("background worker did not start")
	}
	live := producerRuntimeSnapshot(t, server)
	if live.Managers.Active != 1 || live.Workers.Active != 1 || len(live.ActiveWorkers) != 1 || live.ActiveWorkers[0].Scope != platformproducer.Scope {
		t.Fatal("background lifetime was not wired", live)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("background worker did not return")
	}
	exited := producerRuntimeSnapshot(t, server)
	if exited.Managers != (producerRuntimeCalls{Started: 1, Returned: 1}) || exited.Workers != (producerRuntimeCalls{Started: 1, Returned: 1}) || len(exited.ActiveWorkers) != 0 || !exited.TrackingComplete {
		t.Fatal("background return did not join its workers", exited)
	}
}
