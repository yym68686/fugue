package controller

import (
	"context"
	"log"
	"strings"
	"time"

	"fugue/internal/appimages"
)

const postOperationMaintenanceTimeout = 15 * time.Second

func (s *Service) syncTenantBillingImageStorage(ctx context.Context, tenantID string) error {
	if s == nil || !s.syncBillingImageStorage || s.Store == nil || s.inspectManagedImage == nil || strings.TrimSpace(s.registryPushBase) == "" {
		return nil
	}

	apps, err := s.Store.ListAppsMetadata(tenantID, false)
	if err != nil {
		return err
	}
	ops, err := s.Store.ListOperations(tenantID, false)
	if err != nil {
		return err
	}

	storageBytes, err := appimages.MeasureTenantStorageBytes(
		ctx,
		s.inspectManagedImage,
		apps,
		ops,
		s.registryPushBase,
		s.registryPullBase,
	)
	if err != nil {
		return err
	}
	_, err = s.Store.SyncTenantBillingImageStorage(tenantID, appimages.StorageBytesToGibibytes(storageBytes))
	return err
}

func postOperationMaintenanceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, postOperationMaintenanceTimeout)
}

func runPostOperationMaintenance(logger *log.Logger, label string, fn func(context.Context) error) {
	if fn == nil {
		return
	}
	go func() {
		ctx, cancel := postOperationMaintenanceContext(context.Background())
		defer cancel()
		if err := fn(ctx); err != nil && logger != nil {
			logger.Printf("%s failed: %v", strings.TrimSpace(label), err)
		}
	}()
}
