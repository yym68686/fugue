package dnsserver

import (
	"context"
	"errors"
	"sync"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"

	"golang.org/x/sync/semaphore"
)

type dnsContentBudget struct {
	once sync.Once
	gate *semaphore.Weighted
}

var errDNSContentBusy = errors.New("DNS content work deferred for serving")

func (s *Service) contentBudget() *dnsContentBudget {
	if s.sharedContentBudget != nil {
		return s.sharedContentBudget
	}
	return &s.dnsContentBudget
}

// Limit the transient compiler working set across serving, renewal and shadow.
// The permit covers pure CPU work only, never network probes or authority reads.
func (s *Service) buildDNSRouteSources(ctx context.Context, artifact model.PlatformArtifact, snapshot model.PlatformDNSRouteSourceSnapshot, now time.Time) (*dnsroutesource.Plans, error) {
	release, err := s.acquireDNSContent(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.routeSourceCompiler.Build(artifact, snapshot, s.platformDNSKeys(), now)
}

func (s *Service) acquireDNSContent(ctx context.Context) (func(), error) {
	budget := s.contentBudget()
	budget.once.Do(func() { budget.gate = semaphore.NewWeighted(1) })
	if s.shadowValidationParent != nil {
		if !budget.gate.TryAcquire(1) {
			return nil, errDNSContentBusy
		}
		return func() { budget.gate.Release(1) }, nil
	}
	if err := budget.gate.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	return func() { budget.gate.Release(1) }, nil
}
