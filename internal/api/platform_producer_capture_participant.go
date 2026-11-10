package api

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

type producerCaptureParticipantStore interface {
	RegisterProducerCaptureParticipant(context.Context, store.ProducerCaptureParticipant, model.Principal) error
	AddProducerCaptureParticipantScopes(context.Context, store.ProducerCaptureParticipant, model.Principal) (store.ProducerCaptureParticipant, error)
	GetProducerCaptureParticipant(context.Context, string, model.Principal) (store.ProducerCaptureParticipant, error)
	BeginProducerCapture(context.Context, string, string, model.PlatformPublicationPrecondition, model.Principal) (store.ProducerCaptureWork, bool, error)
	FinishProducerCapture(context.Context, store.ProducerCaptureWork, model.Principal) error
	CloseProducerCaptureParticipant(context.Context, store.ProducerCaptureParticipant, model.Principal) error
}

// Local join state never replaces durable tickets. An unsuccessful completion
// can join locally while its persisted ticket still refuses participant close.
type platformProducerCaptureParticipant struct {
	store    producerCaptureParticipantStore
	identity store.ProducerCaptureParticipant
	mu       sync.Mutex
	closing  bool
	closed   bool
	active   int
	drained  chan struct{}
}

func newPlatformProducerCaptureParticipant(ctx context.Context, db producerCaptureParticipantStore, scopes []string) (*platformProducerCaptureParticipant, error) {
	return newPlatformProducerCaptureParticipantForBoot(ctx, db, model.NewID("producer-boot"), scopes)
}

func newPlatformProducerCaptureParticipantForBoot(ctx context.Context, db producerCaptureParticipantStore, bootID string, scopes []string) (*platformProducerCaptureParticipant, error) {
	if db == nil {
		return nil, fmt.Errorf("capture participant store unavailable")
	}
	p := &platformProducerCaptureParticipant{store: db, identity: store.ProducerCaptureParticipant{ID: bootID, Scopes: append([]string(nil), scopes...)}, drained: make(chan struct{})}
	if err := db.RegisterProducerCaptureParticipant(ctx, p.identity, platformProducerPrincipal()); err != nil {
		return nil, err // uncertain enrollment never grants capture
	}
	return p, nil
}

func (p *platformProducerCaptureParticipant) admit(ctx context.Context, scope string, policy model.PlatformPublicationPrecondition) (func(context.Context) error, bool, error) {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return nil, false, fmt.Errorf("capture participant is closing")
	}
	// Reserve the local attempt before I/O, so close can join it without
	// holding a non-cancelable mutex behind a slow/uncertain admission commit.
	p.active++
	known := slices.Contains(p.identity.Scopes, scope)
	p.mu.Unlock()
	if !known {
		registered, err := p.store.AddProducerCaptureParticipantScopes(ctx, store.ProducerCaptureParticipant{ID: p.identity.ID, Scopes: []string{scope}}, platformProducerPrincipal())
		if err != nil {
			p.finishLocal()
			return nil, false, err // uncertain enrollment never grants capture
		}
		if registered.ID != p.identity.ID || !slices.Contains(registered.Scopes, scope) {
			p.finishLocal()
			return nil, false, fmt.Errorf("capture scope enrollment returned an unbound identity")
		}
		p.mu.Lock()
		// Concurrent extensions can return different serialized snapshots. A
		// late older reply must not erase a scope learned by another admission.
		for _, registeredScope := range registered.Scopes {
			if !slices.Contains(p.identity.Scopes, registeredScope) {
				p.identity.Scopes = append(p.identity.Scopes, registeredScope)
			}
		}
		slices.Sort(p.identity.Scopes)
		p.mu.Unlock()
	}
	work, deferred, err := p.store.BeginProducerCapture(ctx, p.identity.ID, scope, policy, platformProducerPrincipal())
	if err != nil || deferred {
		p.finishLocal()
		return nil, deferred, err
	}
	if work.ID == "" || work.ParticipantID != p.identity.ID || work.Scope != scope || work.Policy != policy {
		p.finishLocal()
		return nil, false, fmt.Errorf("capture admission returned an unbound ticket")
	}
	var once sync.Once
	return func(exitCtx context.Context) error {
		result := fmt.Errorf("capture completion handle already consumed")
		once.Do(func() {
			result = p.store.FinishProducerCapture(exitCtx, work, platformProducerPrincipal())
			p.finishLocal()
		})
		return result
	}, false, nil
}

func (p *platformProducerCaptureParticipant) finishLocal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active--
	if p.closing && p.active == 0 {
		close(p.drained)
	}
}

func (p *platformProducerCaptureParticipant) close(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	if !p.closing {
		p.closing = true
		if p.active == 0 {
			close(p.drained)
		}
	}
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err() // no cancellation of the already admitted work
	case <-p.drained:
	}
	// An enrollment may have committed while its response was lost. Refresh
	// the complete durable scope set after local attempts join; cached scopes
	// cannot prove an exact exit. No new admission can start once closing.
	registered, err := p.store.GetProducerCaptureParticipant(ctx, p.identity.ID, platformProducerPrincipal())
	if err != nil {
		return err
	}
	if registered.ID != p.identity.ID {
		return fmt.Errorf("capture participant exit returned an unbound identity")
	}
	if err := p.store.CloseProducerCaptureParticipant(ctx, registered, platformProducerPrincipal()); err != nil {
		return err
	}
	p.mu.Lock()
	p.identity.Scopes = slices.Clone(registered.Scopes)
	p.closed = true
	p.mu.Unlock()
	return nil
}

// Private preparation only: no HTTP, config, environment or startup caller
// supplies this enrollment. The complete rollout/old-work/resource proofs and
// separate finite enable protocol remain mandatory before production use.
func (s *Server) configurePlatformProducerCaptureParticipant(ctx context.Context, scopes []string) error {
	s.producerCaptureMu.Lock()
	defer s.producerCaptureMu.Unlock()
	if s.producerCaptureStarted || s.producerCaptureAdmission != nil || s.producerCaptureParticipant != nil {
		return fmt.Errorf("capture enrollment must precede producer start")
	}
	if s.producerCaptureRuntime == nil {
		s.producerCaptureRuntime = newProducerCaptureRuntime()
	}
	p, err := newPlatformProducerCaptureParticipantForBoot(ctx, s.store, s.producerCaptureRuntime.bootID, scopes)
	if err != nil {
		return err
	}
	s.producerCaptureParticipant = p
	s.producerCaptureAdmission = s.admitPlatformProducerCapture
	return nil
}

func (s *Server) markPlatformProducerCaptureStarted() {
	s.producerCaptureMu.Lock()
	s.producerCaptureStarted = true
	s.producerCaptureMu.Unlock()
}

func (s *Server) closePlatformProducerCaptureParticipant(ctx context.Context) {
	if p := s.producerCaptureParticipant; p != nil {
		exitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := p.close(exitCtx); err != nil && s.log != nil {
			s.log.Printf("platform producer participant remains unclosed: %v", err)
		}
	}
}
