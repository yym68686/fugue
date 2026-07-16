package releaseadapter

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/releasedomain"
)

// AdapterFactory constructs the implementation for one already-authorized
// domain. A factory must not perform a release mutation; Apply remains the
// transaction's sole forward write boundary.
type AdapterFactory func(releasedomain.ExecutionAuthorization) (DomainAdapter, error)

// Dispatcher is the closed, fixed-domain adapter registry. Keeping one
// explicit field per known domain prevents configuration or untrusted input
// from turning a domain string into a function name or dynamic lookup key.
type Dispatcher struct {
	nodeLocalFactory        AdapterFactory
	authoritativeDNSFactory AdapterFactory
	controlPlaneFactory     AdapterFactory
	imageCacheFactory       AdapterFactory
	backupFactory           AdapterFactory
}

// NewDispatcher registers exactly one factory for each of the five release
// domains. It invokes no factory and performs no adapter metadata lookup.
func NewDispatcher(
	nodeLocalFactory AdapterFactory,
	authoritativeDNSFactory AdapterFactory,
	controlPlaneFactory AdapterFactory,
	imageCacheFactory AdapterFactory,
	backupFactory AdapterFactory,
) (Dispatcher, error) {
	factories := []struct {
		domain  releasedomain.Domain
		factory AdapterFactory
	}{
		{domain: releasedomain.DomainNodeLocal, factory: nodeLocalFactory},
		{domain: releasedomain.DomainAuthoritativeDNS, factory: authoritativeDNSFactory},
		{domain: releasedomain.DomainControlPlane, factory: controlPlaneFactory},
		{domain: releasedomain.DomainImageCache, factory: imageCacheFactory},
		{domain: releasedomain.DomainBackup, factory: backupFactory},
	}
	for _, registered := range factories {
		if registered.factory == nil {
			return Dispatcher{}, fmt.Errorf("release domain adapter factory %q is nil", registered.domain)
		}
	}
	return Dispatcher{
		nodeLocalFactory:        nodeLocalFactory,
		authoritativeDNSFactory: authoritativeDNSFactory,
		controlPlaneFactory:     controlPlaneFactory,
		imageCacheFactory:       imageCacheFactory,
		backupFactory:           backupFactory,
	}, nil
}

// Dispatch constructs and runs only the adapter selected by a sealed
// ExecutionAuthorization. Zero-, multiple-, and unknown-domain plans cannot
// produce that authorization type; a zero or corrupted value is rejected
// before any factory, metadata, trace, or phase call.
func (dispatcher Dispatcher) Dispatch(
	ctx context.Context,
	rollbackTimeout time.Duration,
	authorization releasedomain.ExecutionAuthorization,
	trace Trace,
) error {
	if err := authorization.Verify(); err != nil {
		return fmt.Errorf("release domain dispatch authorization: %w", err)
	}

	var factory AdapterFactory
	switch authorization.Domain() {
	case releasedomain.DomainNodeLocal:
		factory = dispatcher.nodeLocalFactory
	case releasedomain.DomainAuthoritativeDNS:
		factory = dispatcher.authoritativeDNSFactory
	case releasedomain.DomainControlPlane:
		factory = dispatcher.controlPlaneFactory
	case releasedomain.DomainImageCache:
		factory = dispatcher.imageCacheFactory
	case releasedomain.DomainBackup:
		factory = dispatcher.backupFactory
	default:
		// ExecutionAuthorization.Verify currently makes this unreachable. Keep
		// the explicit default so a future domain cannot silently dispatch.
		return fmt.Errorf("release domain dispatch does not support domain %q", authorization.Domain())
	}
	if factory == nil {
		return fmt.Errorf("release domain adapter factory %q is nil", authorization.Domain())
	}
	adapter, err := runAdapterFactory(factory, authorization)
	if err != nil {
		return fmt.Errorf("construct release domain adapter %q: %w", authorization.Domain(), err)
	}
	return Run(ctx, rollbackTimeout, authorization, adapter, trace)
}

func runAdapterFactory(
	factory AdapterFactory,
	authorization releasedomain.ExecutionAuthorization,
) (adapter DomainAdapter, resultErr error) {
	defer func() {
		if recover() != nil {
			adapter = nil
			resultErr = fmt.Errorf("release domain adapter factory panicked")
		}
	}()
	return factory(authorization)
}
