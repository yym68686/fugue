package api

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/backupusage"
	"fugue/internal/model"
)

const (
	defaultBackupUsageReconciliationCacheTTL = 30 * time.Second
	backupUsageReconciliationTimeout         = 15 * time.Second
)

type backupUsageBackendGroup struct {
	backends []model.BackupBackend
}

type backupUsageObjectReference struct {
	artifact          model.BackupArtifact
	physicalDeletedAt *time.Time
	expectedSize      *int64
	namespaceKey      string
}

type backupUsageObservedObject struct {
	size         int64
	lastModified time.Time
	observedAt   time.Time
}

type backupUsageObjectInventory struct {
	objects    []dataObjectInfo
	observedAt time.Time
}

func (s *Server) loadBackupUsage(ctx context.Context, tenantID string, platformAdmin bool) (backupusage.Usage, error) {
	recordedUsage, err := s.store.BackupUsage(tenantID, platformAdmin)
	if err != nil {
		return backupusage.Usage{}, err
	}
	usage := backupusage.FromModel(recordedUsage)
	cacheKey := "platform"
	if !platformAdmin {
		cacheKey = "tenant:" + strings.TrimSpace(tenantID)
	}
	reconciliation, err := s.backupUsageReconciliationCache.do(cacheKey, func() (backupusage.Reconciliation, error) {
		return s.buildBackupUsageReconciliation(ctx, tenantID, platformAdmin)
	})
	if err != nil {
		return backupusage.Usage{}, err
	}
	usage.Reconciliation = &reconciliation
	if reconciliationMeasuredCompletely(reconciliation) {
		physicalBytes := reconciliation.ReferencedBytes + reconciliation.UnreferencedBytes
		physicalObjects := reconciliation.ReferencedObjectCount + reconciliation.UnreferencedObjectCount
		usage.PhysicalBytes = &physicalBytes
		usage.PhysicalObjectCount = &physicalObjects
	}
	return usage, nil
}

func (s *Server) buildBackupUsageReconciliation(ctx context.Context, tenantID string, platformAdmin bool) (backupusage.Reconciliation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, backupUsageReconciliationTimeout)
	defer cancel()

	backends, err := s.store.ListBackupBackends(tenantID, platformAdmin)
	if err != nil {
		return backupusage.Reconciliation{}, err
	}
	artifacts, err := s.store.ListBackupUsageArtifacts(tenantID, platformAdmin)
	if err != nil {
		return backupusage.Reconciliation{}, err
	}

	now := time.Now().UTC()
	reconciliation := backupusage.Reconciliation{}
	backendByID := make(map[string]model.BackupBackend, len(backends))
	groupsByKey := map[string]*backupUsageBackendGroup{}
	groupByBackendID := map[string]*backupUsageBackendGroup{}
	for _, backend := range backends {
		backend = model.NormalizeBackupBackend(backend)
		backendByID[backend.ID] = backend
		if backend.Provider != model.DataBackendProviderCloudflareR2 {
			continue
		}
		groupKey := backupUsageStorageNamespaceKey(model.BackupBackendAsDataBackend(backend))
		group := groupsByKey[groupKey]
		if group == nil {
			group = &backupUsageBackendGroup{}
			groupsByKey[groupKey] = group
		}
		group.backends = append(group.backends, backend)
		groupByBackendID[backend.ID] = group
		reconciliation.BackendCount++
	}

	references := map[string]backupUsageObjectReference{}
	unresolvedBackends := map[string]struct{}{}
	for _, usageArtifact := range artifacts {
		artifact := usageArtifact.Artifact
		backendID := strings.TrimSpace(artifact.BackendID)
		backend, ok := backendByID[backendID]
		if !ok {
			if usageArtifact.PhysicalDeletedAt == nil {
				unresolvedBackends[firstNonEmptyString(backendID, "missing-backend")] = struct{}{}
			}
			continue
		}
		if backend.Provider != model.DataBackendProviderCloudflareR2 {
			continue
		}
		if groupByBackendID[backend.ID] == nil {
			unresolvedBackends[backend.ID] = struct{}{}
			continue
		}
		keyBackend := &dataObjectBackend{backend: model.BackupBackendAsDataBackend(backend)}
		logicalKeys, keyErr := backupArtifactObjectKeysForDeletion(artifact)
		if keyErr != nil {
			if usageArtifact.PhysicalDeletedAt == nil {
				reconciliation.InvalidReferenceCount++
			}
			continue
		}
		for _, logicalKey := range logicalKeys {
			if !platformAdmin && backend.TenantID == "" && !backupUsageTenantOwnsLogicalObject(tenantID, logicalKey) {
				if usageArtifact.PhysicalDeletedAt == nil {
					reconciliation.InvalidReferenceCount++
				}
				continue
			}
			physicalKey := keyBackend.objectKey(logicalKey)
			objectID := backupUsagePhysicalObjectID(keyBackend.backend, physicalKey)
			manifest := logicalKey == strings.Trim(strings.TrimSpace(artifact.ManifestObjectKey), "/")
			var expectedSize *int64
			if !manifest || artifact.Kind == model.BackupArtifactKindDataSnapshot || strings.TrimSpace(artifact.ObjectKey) == "" {
				size := artifact.SizeBytes
				expectedSize = &size
			}
			if existing, exists := references[objectID]; exists {
				if existing.physicalDeletedAt != nil && usageArtifact.PhysicalDeletedAt != nil {
					continue
				}
				reconciliation.DuplicateReferenceCount++
				if existing.physicalDeletedAt == nil || usageArtifact.PhysicalDeletedAt != nil {
					continue
				}
			}
			references[objectID] = backupUsageObjectReference{
				artifact:          artifact,
				physicalDeletedAt: usageArtifact.PhysicalDeletedAt,
				expectedSize:      expectedSize,
				namespaceKey:      backupUsageStorageNamespaceKey(keyBackend.backend),
			}
			if usageArtifact.PhysicalDeletedAt == nil {
				reconciliation.ExpectedObjectCount++
			}
		}
	}

	groupKeys := make([]string, 0, len(groupsByKey))
	for key := range groupsByKey {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)
	observed := map[string]backupUsageObservedObject{}
	measuredNamespaces := map[string]time.Time{}
	for _, groupKey := range groupKeys {
		group := groupsByKey[groupKey]
		sort.Slice(group.backends, func(i, j int) bool { return group.backends[i].ID < group.backends[j].ID })
		var (
			objectBackend       *dataObjectBackend
			objects             []dataObjectInfo
			inventoryObservedAt time.Time
			lastErr             error
		)
		for _, backend := range group.backends {
			fullBackend, getErr := s.store.GetBackupBackendForUse(backend.ID, tenantID, platformAdmin)
			if getErr != nil {
				lastErr = getErr
				continue
			}
			candidate, openErr := newDataObjectBackend(model.BackupBackendAsDataBackend(fullBackend))
			if openErr != nil {
				lastErr = openErr
				continue
			}
			if !backupUsageR2NamespaceIsMeasurable(candidate) {
				lastErr = fmt.Errorf("backup backend %s has no exact measurable R2 namespace", backend.ID)
				continue
			}
			inventory, listErr := s.backupUsageObjectInventoryCache.do(groupKey, func() (backupUsageObjectInventory, error) {
				listed, err := candidate.listObjects(ctx, "")
				if err != nil {
					return backupUsageObjectInventory{}, err
				}
				return backupUsageObjectInventory{objects: listed, observedAt: time.Now().UTC()}, nil
			})
			if listErr != nil {
				lastErr = listErr
				continue
			}
			objectBackend = candidate
			objects = inventory.objects
			inventoryObservedAt = inventory.observedAt
			if reconciliation.ObservedAt.IsZero() || inventory.observedAt.Before(reconciliation.ObservedAt) {
				reconciliation.ObservedAt = inventory.observedAt
			}
			break
		}
		if objectBackend == nil {
			for _, backend := range group.backends {
				unresolvedBackends[backend.ID] = struct{}{}
			}
			if s.log != nil {
				s.log.Printf("backup usage reconciliation R2 namespace unavailable backends=%d: %v", len(group.backends), lastErr)
			}
			continue
		}
		reconciliation.MeasuredBackendCount += len(group.backends)
		measuredNamespaces[groupKey] = inventoryObservedAt
		tenantOwnsNamespace := platformAdmin
		if !platformAdmin && len(group.backends) > 0 {
			tenantOwnsNamespace = true
			for _, backend := range group.backends {
				// A shared record for the same physical namespace makes that
				// namespace multi-tenant even when a duplicate tenant-owned
				// record also exists. Require every record to be tenant-owned
				// before attributing arbitrary keys to the tenant.
				if strings.TrimSpace(backend.TenantID) != strings.TrimSpace(tenantID) {
					tenantOwnsNamespace = false
					break
				}
			}
		}
		for _, object := range objects {
			logicalKey, logical := objectBackend.logicalObjectKey(object.Key)
			// ListObjects uses an S3 string prefix. A sibling such as
			// "backup-root-old" can therefore be returned while it is not part
			// of the configured "backup-root/" logical namespace.
			if !logical && !backupUsagePhysicalKeyInNamespace(objectBackend.backend, object.Key) {
				continue
			}
			// Unsafe/non-canonical keys still occupy the configured physical
			// namespace and must be included in platform (or wholly tenant-owned
			// namespace) totals. They are never converted into logical keys or
			// passed to a delete path. A shared namespace cannot safely attribute
			// such a key to one tenant, so tenant views omit it.
			if !platformAdmin && !tenantOwnsNamespace && (!logical || !backupUsageTenantOwnsLogicalObject(tenantID, logicalKey)) {
				continue
			}
			objectID := backupUsagePhysicalObjectID(objectBackend.backend, object.Key)
			if existing, exists := observed[objectID]; exists {
				if existing.size != object.Size {
					reconciliation.SizeMismatchCount++
				}
				if inventoryObservedAt.After(existing.observedAt) {
					existing.size = object.Size
					existing.lastModified = object.LastModified
					existing.observedAt = inventoryObservedAt
					observed[objectID] = existing
				}
				continue
			}
			observed[objectID] = backupUsageObservedObject{size: object.Size, lastModified: object.LastModified, observedAt: inventoryObservedAt}
		}
	}

	for objectID, object := range observed {
		reference, referenced := references[objectID]
		if !referenced {
			reconciliation.UnreferencedObjectCount++
			reconciliation.UnreferencedBytes += object.size
			classificationTime := object.observedAt
			if classificationTime.IsZero() {
				classificationTime = now
			}
			if backupUsageObjectWithinCleanupGrace(object.lastModified, classificationTime) {
				reconciliation.ProvisionalObjectCount++
				reconciliation.ProvisionalBytes += object.size
			} else {
				reconciliation.OrphanedObjectCount++
				reconciliation.OrphanedBytes += object.size
			}
			continue
		}
		reconciliation.ReferencedObjectCount++
		reconciliation.ReferencedBytes += object.size
		if reference.expectedSize != nil && *reference.expectedSize != object.size {
			reconciliation.SizeMismatchCount++
		}
		artifact := reference.artifact
		if reference.physicalDeletedAt != nil {
			if observedAt := measuredNamespaces[reference.namespaceKey]; !observedAt.IsZero() && reference.physicalDeletedAt.After(observedAt) {
				// The cached inventory predates the durable success marker. The
				// object was legitimately present in that physical snapshot, but
				// current metadata proves cleanup completed afterward; do not emit
				// a false lingering-object alert until the inventory cache refreshes.
				reconciliation.PendingDeletionObjectCount++
				reconciliation.PendingDeletionBytes += object.size
				continue
			}
			reconciliation.LingeringDeletedObjectCount++
			reconciliation.LingeringDeletedBytes += object.size
			continue
		}
		switch artifact.Status {
		case model.BackupArtifactStatusActive:
			reconciliation.ActiveObjectCount++
			reconciliation.ActiveBytes += object.size
		case model.BackupArtifactStatusDeleted, model.BackupArtifactStatusExpired:
			if artifact.Protected {
				continue
			}
			if artifact.DeletedAt == nil {
				reconciliation.InvalidReferenceCount++
				continue
			}
			if now.Before(artifact.DeletedAt.UTC().Add(backupArtifactGCGrace)) {
				reconciliation.PendingDeletionObjectCount++
				reconciliation.PendingDeletionBytes += object.size
			} else {
				reconciliation.OverdueDeletionObjectCount++
				reconciliation.OverdueDeletionBytes += object.size
			}
		default:
			reconciliation.InvalidReferenceCount++
		}
	}
	for objectID, reference := range references {
		if _, exists := observed[objectID]; exists {
			continue
		}
		observedAt, measured := measuredNamespaces[reference.namespaceKey]
		if !measured {
			continue
		}
		artifact := reference.artifact
		if reference.physicalDeletedAt == nil && artifact.Status == model.BackupArtifactStatusActive {
			// A shared namespace inventory can be cached just before another
			// request creates durable metadata for a newly uploaded object. Such
			// an artifact did not exist at the physical snapshot boundary and is
			// not evidence that R2 lost an active object.
			if !artifact.CreatedAt.IsZero() && artifact.CreatedAt.After(observedAt) {
				continue
			}
			reconciliation.MissingActiveObjectCount++
		}
	}

	reconciliation.UnresolvedBackendCount = len(unresolvedBackends)
	if reconciliation.ObservedAt.IsZero() {
		reconciliation.ObservedAt = now
	}
	reconciliation.Status, reconciliation.Message = summarizeBackupUsageReconciliation(reconciliation)
	if platformAdmin && s.log != nil && reconciliation.Status != backupusage.ReconciliationStatusComplete {
		s.log.Printf("backup usage reconciliation status=%s physical_backends=%d/%d orphaned_objects=%d orphaned_bytes=%d missing_active=%d overdue_deletion=%d lingering_deleted=%d invalid_references=%d size_mismatches=%d unresolved_backends=%d",
			reconciliation.Status,
			reconciliation.MeasuredBackendCount,
			reconciliation.BackendCount,
			reconciliation.OrphanedObjectCount,
			reconciliation.OrphanedBytes,
			reconciliation.MissingActiveObjectCount,
			reconciliation.OverdueDeletionObjectCount,
			reconciliation.LingeringDeletedObjectCount,
			reconciliation.InvalidReferenceCount,
			reconciliation.SizeMismatchCount,
			reconciliation.UnresolvedBackendCount,
		)
	}
	return reconciliation, nil
}

func summarizeBackupUsageReconciliation(reconciliation backupusage.Reconciliation) (string, string) {
	if reconciliation.MeasuredBackendCount < reconciliation.BackendCount || reconciliation.UnresolvedBackendCount > 0 {
		if reconciliation.MeasuredBackendCount == 0 && (reconciliation.BackendCount > 0 || reconciliation.UnresolvedBackendCount > 0) {
			return backupusage.ReconciliationStatusUnavailable, "R2 physical inventory is unavailable; billable bytes remain database-recorded only"
		}
		return backupusage.ReconciliationStatusPartial, "R2 physical inventory is incomplete; physical totals are omitted"
	}
	if reconciliation.OrphanedObjectCount > 0 ||
		reconciliation.MissingActiveObjectCount > 0 ||
		reconciliation.OverdueDeletionObjectCount > 0 ||
		reconciliation.LingeringDeletedObjectCount > 0 ||
		reconciliation.DuplicateReferenceCount > 0 ||
		reconciliation.InvalidReferenceCount > 0 ||
		reconciliation.SizeMismatchCount > 0 {
		return backupusage.ReconciliationStatusDrift, "R2 physical inventory differs from durable backup metadata"
	}
	if reconciliation.ProvisionalObjectCount > 0 {
		return backupusage.ReconciliationStatusReconciling, "recent unreferenced objects remain within the failed-upload cleanup grace period"
	}
	return backupusage.ReconciliationStatusComplete, "R2 physical inventory matches durable backup metadata"
}

func reconciliationMeasuredCompletely(reconciliation backupusage.Reconciliation) bool {
	return reconciliation.MeasuredBackendCount == reconciliation.BackendCount && reconciliation.UnresolvedBackendCount == 0
}

func backupUsageStorageNamespaceKey(backend model.DataBackend) string {
	return strings.Join([]string{
		model.NormalizeDataBackendProvider(backend.Provider),
		strings.ToLower(strings.TrimRight(strings.TrimSpace(backend.Endpoint), "/")),
		strings.TrimSpace(backend.Bucket),
		strings.Trim(strings.TrimSpace(backend.Prefix), "/"),
	}, "\x00")
}

func backupUsagePhysicalObjectID(backend model.DataBackend, physicalKey string) string {
	return strings.Join([]string{
		model.NormalizeDataBackendProvider(backend.Provider),
		strings.ToLower(strings.TrimRight(strings.TrimSpace(backend.Endpoint), "/")),
		strings.TrimSpace(backend.Bucket),
		physicalKey,
	}, "\x00")
}

func backupUsagePhysicalKeyInNamespace(backend model.DataBackend, physicalKey string) bool {
	prefix := strings.Trim(strings.TrimSpace(backend.Prefix), "/")
	if prefix == "" {
		return physicalKey != ""
	}
	return strings.HasPrefix(physicalKey, prefix+"/")
}

func backupUsageR2NamespaceIsMeasurable(objectBackend *dataObjectBackend) bool {
	if objectBackend == nil || strings.TrimSpace(objectBackend.backend.Endpoint) == "" {
		return false
	}
	const probe = "fugue-usage-namespace-probe"
	logical, ok := objectBackend.logicalObjectKey(objectBackend.objectKey(probe))
	return ok && logical == probe
}

func backupUsageTenantOwnsLogicalObject(tenantID, logicalKey string) bool {
	tenantID = strings.TrimSpace(tenantID)
	parts := strings.Split(strings.Trim(strings.TrimSpace(logicalKey), "/"), "/")
	if tenantID == "" || len(parts) < 2 || parts[1] != tenantID {
		return false
	}
	return parts[0] == "apps" || parts[0] == "data-workspaces"
}

func backupUsageObjectWithinCleanupGrace(lastModified, now time.Time) bool {
	if lastModified.IsZero() {
		return false
	}
	lastModified = lastModified.UTC()
	return !lastModified.After(now.Add(time.Minute)) && now.Sub(lastModified) <= backupFailedRunGCGrace
}
