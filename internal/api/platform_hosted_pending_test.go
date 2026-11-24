package api

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func TestHostedCaptureGatePrioritizesGlobalWithoutOverlappingOrWaiting(t *testing.T) {
	var g hostedCaptureGate
	host, leaveHost, ok := g.enter(t.Context(), true)
	if !ok {
		t.Fatal("first host rejected")
	}
	if _, _, ok = g.enter(t.Context(), true); ok || host.Err() != nil {
		t.Fatal("host job displaced another")
	}
	if _, _, ok = g.enter(t.Context(), false); ok || host.Err() == nil {
		t.Fatal("ordinary must cancel, not overlap")
	}
	if _, _, ok = g.enter(t.Context(), false); ok {
		t.Fatal("cancellation is not completion")
	}
	leaveHost()
	ordinary, leaveOrdinary, ok := g.enter(t.Context(), false)
	if !ok {
		t.Fatal("ordinary could not resume after actual return")
	}
	if _, _, ok = g.enter(t.Context(), true); ok || ordinary.Err() != nil {
		t.Fatal("host job preempted ordinary")
	}
	leaveOrdinary()
	_, finish, ok := g.enter(t.Context(), true)
	if !ok {
		t.Fatal("new pending observation could not start")
	}
	finish()
}

func TestPendingHostQueueCoalescesRolloutWaitsAndJoinsOnLoss(t *testing.T) {
	entered, done := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	schedule, stop := pendingHostQueue(t.Context(), func(ctx context.Context, j pendingHostJob) {
		if calls.Add(1) != 1 || j.authority.ID != "selected" {
			t.Error("duplicate or wrong work")
		}
		close(entered)
		<-ctx.Done()
		close(done)
	})
	a := model.PlatformArtifactRelease{ID: "selected"}
	schedule(platformproducer.Policy{}, a)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	for i := 0; i < 100; i++ {
		schedule(platformproducer.Policy{}, a)
	}
	stop()
	select {
	case <-done:
	default:
		t.Fatal("capture survived the elected worker")
	}
	schedule(platformproducer.Policy{}, a)
	if calls.Load() != 1 {
		t.Fatal("queued old global ticks after shutdown")
	}
}

func TestPendingHostCaptureRequiresOrdinaryContinuousGlobalPolicy(t *testing.T) {
	p := platformproducer.Policy{Mode: "serving", InputSource: "business-static-intent", TargetScope: platformconfig.GlobalScopeKey, DNSPolicyArtifactID: "dns", Serving: &platformproducer.ServingPolicy{}}
	if !canCapturePendingHosts(platformproducer.Scope, p) || canCapturePendingHosts("foreign", p) {
		t.Fatal("wrong scheduler scope")
	}
	for _, change := range []func(*platformproducer.Policy){
		func(p *platformproducer.Policy) { p.Serving = &platformproducer.ServingPolicy{SinglePublication: true} },
		func(p *platformproducer.Policy) { p.Mode = "paused" }, func(p *platformproducer.Policy) { p.Serving = nil },
		func(p *platformproducer.Policy) { p.TargetScope = "authority-cell:other" }, func(p *platformproducer.Policy) { p.PublicationRole = platformconfig.PublicationRoleCellDNS },
		func(p *platformproducer.Policy) { p.InputSource = platformproducer.DNSProducerInputSource }, func(p *platformproducer.Policy) { p.DNSPolicyArtifactID = "" },
	} {
		q := p
		change(&q)
		if canCapturePendingHosts(platformproducer.Scope, q) {
			t.Fatalf("borrowed a non-host policy: %+v", q)
		}
	}
}
