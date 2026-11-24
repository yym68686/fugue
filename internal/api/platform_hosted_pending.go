package api

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/store"
)

func canCapturePendingHosts(scope string, p platformproducer.Policy) bool {
	return scope == platformproducer.Scope && p.Mode == "serving" && p.Serving != nil && !p.Serving.SinglePublication && p.InputSource == "business-static-intent" && p.TargetScope == pc.GlobalScopeKey && p.PublicationRole == "" && p.DNSPolicyArtifactID != ""
}

type pendingHostJob struct {
	policy    platformproducer.Policy
	authority model.PlatformArtifactRelease
}

// Full input capture can take minutes. Keep at most one per global leader:
// a returning ordinary publisher supersedes a host-only capture, since it
// also compiles the host offers. Do not wait in the leader loop, which must
// remain free to observe and roll back a gray release. This gate never
// substitutes for producer election, durable participation or publication CAS.
type hostedCaptureGate struct {
	mu         sync.Mutex
	busy       bool
	cancelHost context.CancelFunc
}

func (g *hostedCaptureGate) enter(ctx context.Context, hostOnly bool) (context.Context, func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if ctx.Err() != nil {
		return ctx, nil, false
	}
	if g.busy {
		if !hostOnly && g.cancelHost != nil {
			g.cancelHost()
		}
		return ctx, nil, false
	}
	g.busy = true
	if hostOnly {
		ctx, g.cancelHost = context.WithCancel(ctx)
	}
	return ctx, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.cancelHost != nil {
			g.cancelHost()
		}
		g.cancelHost = nil
		g.busy = false
	}, true
}

// One coalescing worker per elected global producer, not one per hostname or
// tick. A slow physical input read cannot delay ordinary rollback/timeout.
// Never leave work running after returning the producer's leadership.
func pendingHostQueue(parent context.Context, run func(context.Context, pendingHostJob)) (func(platformproducer.Policy, model.PlatformArtifactRelease), func()) {
	ctx, cancel := context.WithCancel(parent)
	jobs := make(chan pendingHostJob, 1)
	var busy atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case job := <-jobs:
				if ctx.Err() != nil {
					return
				}
				run(ctx, job)
				busy.Store(false)
			}
		}
	}()
	return func(p platformproducer.Policy, a model.PlatformArtifactRelease) {
		if ctx.Err() != nil || !busy.CompareAndSwap(false, true) {
			return
		}
		select {
		case jobs <- pendingHostJob{p, a}:
		case <-ctx.Done():
			busy.Store(false)
		}
	}, func() { cancel(); wg.Wait() }
}

func (s *Server) pendingHostWorker(ctx context.Context, scope string) (func(platformproducer.Policy, model.PlatformArtifactRelease), func()) {
	if scope != platformproducer.Scope {
		return nil, func() {}
	}
	var last string
	var retry time.Time // accessed only by this queue's single consumer
	return pendingHostQueue(ctx, func(ctx context.Context, j pendingHostJob) {
		if !canCapturePendingHosts(scope, j.policy) {
			return
		}
		defer s.platformProducerRuntime().worker(ctx, scope)()
		read, cancel := context.WithTimeout(ctx, 5*time.Second)
		hint, e := s.store.HostedPathWakeHint(read)
		cancel()
		if e != nil {
			if ctx.Err() == nil && s.log != nil {
				s.log.Printf("independent host scheduling input unavailable: %v", e)
			}
			return
		}
		if hint == "" {
			return
		}
		key := j.authority.ID + "|" + hint
		if key == last && time.Now().Before(retry) {
			return
		}
		input, releaseInput, acquired := s.hostedCaptureGate.enter(ctx, true)
		if !acquired {
			return
		}
		defer releaseInput()
		e = s.captureHostOffersWhilePending(input, scope, j)
		// Ordinary capture now owns reconciliation. Do not record a
		// canceled attempt as successfully processed or impose retry backoff.
		if input.Err() != nil {
			return
		}
		last = key
		wait := 5 * time.Minute
		if e != nil {
			wait = time.Minute
		}
		retry = time.Now().Add(wait)
		if e != nil && ctx.Err() == nil && s.log != nil {
			s.log.Printf("independent host capture pending policy_release=%s error=%v", j.authority.ID, e)
		}
	})
}

// This lane only writes the existing host draft/selection protocol. It has
// no call to materialize, stage, release, or verify a global publication.
// The pending gray is never its compiler input or evidence.
func (s *Server) captureHostOffersWhilePending(parent context.Context, scope string, j pendingHostJob) (resultErr error) {
	ctx, cancel := context.WithTimeout(parent, 7*time.Minute)
	defer cancel()
	if !canCapturePendingHosts(scope, j.policy) {
		return nil
	}
	a, release, ok, e := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, scope, "shadow")
	if e != nil || !ok {
		return e
	}
	if release.ID != j.authority.ID || release.FencingToken != j.authority.FencingToken || release.ArtifactID != a.ID || a.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(a) != nil {
		return store.ErrConflict
	}
	p, e := platformproducer.Decode(a)
	if e != nil || !canCapturePendingHosts(scope, p) {
		return e
	}
	pin := model.PlatformPublicationPrecondition{ArtifactID: a.ID, ContentHash: a.ContentHash, ReleaseID: release.ID, FencingToken: release.FencingToken}
	deferred, e := s.store.ProducerInputDeferred(ctx, scope, pin)
	if e != nil || deferred {
		return e
	}
	defer s.platformProducerRuntime().input(scope)()
	if s.producerCaptureAdmission != nil {
		finish, deferred, e := s.producerCaptureAdmission(ctx, scope, pin)
		if e != nil || deferred {
			return e
		}
		if finish == nil {
			return errors.New("independent host capture lacks its admission handle")
		}
		defer func() {
			done, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer stop()
			resultErr = errors.Join(resultErr, finish(done))
		}()
	}
	capture, cancelCapture := context.WithTimeout(ctx, 5*time.Minute)
	projection, e := s.capturePlatformIntentForProducer(capture, platformProducerPrincipal(), p)
	captureErr := capture.Err()
	cancelCapture()
	if e != nil {
		return e
	}
	if captureErr != nil {
		return captureErr
	}
	if projection.Intent.Scope != p.TargetScope || projection.Policy.Scope != p.TargetScope {
		return store.ErrConflict
	}
	s.reconcileHostedWithdrawals(ctx, p, release, projection)
	h, e := s.discoverHostedCapture(p, projection)
	if e != nil || h.phase == "" {
		return e
	}
	source, e := s.readHostedAddressSourceWith(ctx, p, release, projection.hostedAddressHolds, s.observeHostedDraftBase)
	if e != nil {
		return e
	}
	if source.Publication.ReleaseID != h.selected.ID || source.Publication.FencingToken != h.selected.FencingToken || source.Publication.ReleaseSetID != h.selected.ArtifactID {
		return store.ErrConflict
	}
	req := pc.CompileRequest{OmitOriginalInput: s.compilerInputOmitOriginal, Intent: projection.Intent, Policy: projection.Policy, RuntimeSnapshot: projection.RuntimeSnapshot}
	compiled, scoped, e := pc.CompileHostedPathWithCapture(req, source)
	if e != nil || scoped == nil {
		return e
	}
	return s.recordPathOffers(ctx, release, source, compiled, scoped)
}

func (s *Server) recordPathOffers(ctx context.Context, authority model.PlatformArtifactRelease, source pc.HostedAddressSource, compiled pc.CompileResult, scoped *pc.HostedTransitionCapture) error {
	proposalCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	ids, result := s.store.EnsureCompiledHostedPathProposals(proposalCtx, source, compiled, scoped)
	cancel()
	if s.log != nil {
		s.log.Printf("hosted path compiler proposals policy_release=%s base_release=%s ids=%v error=%v", authority.ID, source.Publication.ReleaseID, ids, result)
	}
	offerCtx, done := context.WithTimeout(ctx, 30*time.Second)
	defer done()
	for _, id := range ids {
		if e := offerCtx.Err(); e != nil {
			return errors.Join(result, e)
		}
		sequence, e := s.store.OfferCompiledHostedPreparation(offerCtx, source, id, func(in store.ProjectHostRouteInputs, p pc.HostedScopeProposal) (string, string, error) {
			b, err := matchHostedProjectIntent(in, p)
			if err != nil || b.Direction != "forward" {
				return "", "", errors.Join(store.ErrConflict, err)
			}
			return b.OwnerDigest, b.PathDigest, nil
		})
		if s.log != nil {
			s.log.Printf("hosted path preparation offer draft=%s sequence=%d error=%v", id, sequence, e)
		}
		result = errors.Join(result, e)
	}
	return result
}
