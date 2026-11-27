package controller

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"fugue/internal/imagebilling"
)

const postOperationMaintenanceTimeout = 15 * time.Second

func (s *Service) syncTenantBillingImageStorage(ctx context.Context, tenantID string) error {
	if s == nil || !s.syncBillingImageStorage || s.Store == nil || s.inspectManagedImage == nil || strings.TrimSpace(s.registryPushBase) == "" {
		return nil
	}
	if !s.imageStoreRegistryFallbackEnabled() {
		return nil
	}

	return imagebilling.Refresh(ctx, s.Store.ImageStorageBilling(tenantID), s.inspectManagedImage, s.registryPushBase, s.registryPullBase)
}

func postOperationMaintenanceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, postOperationMaintenanceTimeout)
}

func runPostOperationMaintenance(ctx context.Context, logger *log.Logger, label string, fn func(context.Context) error) {
	if fn == nil {
		return
	}
	// Maintenance stays asynchronous after operation completion, but belongs
	// to the worker epoch. An operation's own timeout must not cancel it;
	// leadership shutdown must cancel and join it before the worker exits.
	lifetime, tracked := ctx.Value(operationWorkerLifetimeKey{}).(operationWorkerLifetime)
	parent := context.Background()
	if tracked {
		parent = lifetime.ctx
		lifetime.tasks.Add(1)
	}
	go func() {
		if tracked {
			defer lifetime.tasks.Done()
		}
		ctx, cancel := postOperationMaintenanceContext(parent)
		defer cancel()
		if ctx.Err() != nil {
			return
		}
		if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) && logger != nil {
			logger.Printf("%s failed: %v", strings.TrimSpace(label), err)
		}
	}()
}
