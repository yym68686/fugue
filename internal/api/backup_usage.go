package api

import (
	"context"
	"time"

	"fugue/internal/backupusage"
	"fugue/internal/model"
	"fugue/internal/store"
)

type backupUsageSource struct {
	store *store.Store
}

// Cleanup compares identities across backend records for the same physical key.
func backupUsagePhysicalObjectID(backend model.DataBackend, key string) string {
	return backupusage.NamespaceOf(backend).ObjectID(key)
}

func newBackupUsageService(db *store.Store, inventory BackupInventoryConfig, ttl time.Duration) *backupusage.Service {
	return backupusage.New(backupUsageSource{store: db}, backupArtifactObjectKeysForDeletion, backupusage.Config{
		CacheTTL: ttl, InventoryRefreshInterval: inventory.normalized().RefreshInterval,
		ArtifactGCGrace: backupArtifactGCGrace, FailedRunGCGrace: backupFailedRunGCGrace,
	})
}

func (s *Server) loadBackupUsage(ctx context.Context, tenantID string, platformAdmin bool) (backupusage.Usage, error) {
	return s.backupUsage.Load(ctx, tenantID, platformAdmin)
}

func (a backupUsageSource) BackupUsage(tenantID string, platformAdmin bool) (model.BackupUsage, error) {
	return a.store.BackupUsage(tenantID, platformAdmin)
}

func (a backupUsageSource) ListBackupBackends(tenantID string, platformAdmin bool) ([]model.BackupBackend, error) {
	return a.store.ListBackupBackends(tenantID, platformAdmin)
}

func (a backupUsageSource) ListUsageArtifacts(tenantID string, platformAdmin bool) ([]backupusage.UsageArtifact, error) {
	records, err := a.store.ListBackupUsageArtifacts(tenantID, platformAdmin)
	if err != nil {
		return nil, err
	}
	artifacts := make([]backupusage.UsageArtifact, len(records))
	for i, record := range records {
		artifacts[i] = backupusage.UsageArtifact{Artifact: record.Artifact, PhysicalDeletedAt: record.PhysicalDeletedAt}
	}
	return artifacts, nil
}

func (a backupUsageSource) ResolveNamespace(backendID, tenantID string, platformAdmin bool) (backupusage.Namespace, error) {
	record, err := a.store.GetBackupBackendForUse(backendID, tenantID, platformAdmin)
	if err != nil {
		return backupusage.Namespace{}, err
	}
	// Retain the existing backend configuration checks without passing the
	// credentials or the constructed object-storage client to reconciliation.
	backend, err := newDataObjectBackend(model.BackupBackendAsDataBackend(record))
	if err != nil {
		return backupusage.Namespace{}, err
	}
	return backupusage.NamespaceOf(backend.backend), nil
}

func (a backupUsageSource) ReadInventory(ctx context.Context, namespaceKey string) (backupusage.Inventory, error) {
	cp, err := readBackupInventory(ctx, a.store, namespaceKey)
	if err != nil {
		return backupusage.Inventory{}, err
	}
	progress := func(scan *backupInventoryScan) backupusage.InventoryProgress {
		return backupusage.InventoryProgress{Generation: scan.Generation, Pages: scan.Pages, Objects: scan.Objects}
	}
	view := backupusage.Inventory{LastAttempt: cp.LastAttempt, Error: cp.Error}
	if cp.Current != nil {
		p := progress(cp.Current)
		view.Current = &p
	}
	if scan := cp.Complete; scan != nil {
		view.Complete = &backupusage.InventorySnapshot{
			InventoryProgress: progress(scan),
			NamespaceOwner:    scan.NamespaceOwner, StartedAt: scan.StartedAt, FinishedAt: scan.FinishedAt,
			Wanted: scan.Wanted, References: scan.References, Totals: scan.Totals,
		}
	}
	return view, nil
}
