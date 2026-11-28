package backupusage

import (
	"context"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/objectstorage"
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

func (s *Service) reconcile(ctx context.Context, tenantID string, platformAdmin bool) (Reconciliation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, reconciliationTimeout)
	defer cancel()

	backends, err := s.source.ListBackupBackends(tenantID, platformAdmin)
	if err != nil {
		return Reconciliation{}, err
	}
	artifacts, err := s.source.ListUsageArtifacts(tenantID, platformAdmin)
	if err != nil {
		return Reconciliation{}, err
	}

	now := s.now().UTC()
	reconciliation := Reconciliation{}
	backendByID := make(map[string]model.BackupBackend, len(backends))
	groupsByKey := map[string]*backupUsageBackendGroup{}
	groupByBackendID := map[string]*backupUsageBackendGroup{}
	for _, backend := range backends {
		backend = model.NormalizeBackupBackend(backend)
		backendByID[backend.ID] = backend
		if backend.Provider != model.DataBackendProviderCloudflareR2 {
			continue
		}
		groupKey := NamespaceOf(model.BackupBackendAsDataBackend(backend)).Key()
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
				if backendID == "" {
					backendID = "missing-backend"
				}
				unresolvedBackends[backendID] = struct{}{}
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
		namespace := NamespaceOf(model.BackupBackendAsDataBackend(backend))
		logicalKeys, keyErr := OwnedObjectKeys(artifact)
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
			physicalKey := objectstorage.PhysicalKey(namespace.Prefix, logicalKey)
			objectID := namespace.ObjectID(physicalKey)
			manifest := logicalKey == strings.Trim(strings.TrimSpace(artifact.ManifestObjectKey), "/")
			var expectedSize *int64
			if artifact.Kind != model.BackupArtifactKindLonghornSnapshot && (!manifest || artifact.Kind == model.BackupArtifactKindDataSnapshot || strings.TrimSpace(artifact.ObjectKey) == "") {
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
				namespaceKey:      namespace.Key(),
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
	observedWanted := map[string]bool{}
	for _, groupKey := range groupKeys {
		group := groupsByKey[groupKey]
		sort.Slice(group.backends, func(i, j int) bool { return group.backends[i].ID < group.backends[j].ID })
		var (
			namespace           Namespace
			measured            bool
			objects             []InventoryObject
			inventoryObservedAt time.Time
		)
		for _, backend := range group.backends {
			candidate, err := s.source.ResolveNamespace(backend.ID, tenantID, platformAdmin)
			if err != nil || !candidate.Measurable() {
				continue
			}
			cp, err := s.source.ReadInventory(ctx, groupKey)
			if err != nil {
				continue
			}
			reconciliation.Refreshing = reconciliation.Refreshing || cp.Current != nil
			if !cp.LastAttempt.IsZero() {
				t := cp.LastAttempt
				reconciliation.LastAttemptAt = &t
			}
			reconciliation.ScanError = cp.Error
			progress := cp.Current
			if progress == nil && cp.Complete != nil {
				progress = &cp.Complete.InventoryProgress
			}
			if progress != nil {
				reconciliation.ScannedPages += progress.Pages
				reconciliation.ScannedObjects += progress.Objects
				reconciliation.ScanGeneration = progress.Generation
			}
			if cp.Complete == nil || cp.Complete.FinishedAt == nil {
				continue
			}
			complete := cp.Complete
			for key := range complete.Wanted {
				observedWanted[candidate.ObjectID(key)] = true
			}
			namespace, measured = candidate, true
			inventoryObservedAt = complete.StartedAt
			reconciliation.ScanStartedAt = &complete.StartedAt
			reconciliation.ScanFinishedAt = complete.FinishedAt
			reconciliation.LastSuccessAt = complete.FinishedAt
			reconciliation.Stale = reconciliation.Stale || cp.Error != "" || s.now().Sub(*complete.FinishedAt) > 2*s.cfg.InventoryRefreshInterval
			owns := platformAdmin || (tenantID != "" && complete.NamespaceOwner == tenantID)
			totalKey := "all"
			if !owns {
				totalKey = "tenant:" + tenantID
			}
			totals := complete.Totals[totalKey]
			reconciliation.RepositoryManagedObjectCount += totals.RepositoryCount
			reconciliation.RepositoryManagedBytes += totals.RepositoryBytes
			totals.Count -= totals.RepositoryCount
			totals.Bytes -= totals.RepositoryBytes
			for _, object := range complete.References {
				logical, ok := objectstorage.LogicalKey(candidate.Prefix, object.Key)
				if !owns && (!ok || !backupUsageTenantOwnsLogicalObject(tenantID, logical)) {
					continue
				}
				objects = append(objects, object)
				totals.Count--
				totals.Bytes -= object.Size
				if ObjectWithinCleanupGrace(object.LastModified, object.ObservedAt, s.cfg.FailedRunGCGrace) {
					totals.ProvisionalCount--
					totals.ProvisionalBytes -= object.Size
				}
			}
			reconciliation.UnreferencedObjectCount += totals.Count
			reconciliation.UnreferencedBytes += totals.Bytes
			reconciliation.ProvisionalObjectCount += totals.ProvisionalCount
			reconciliation.ProvisionalBytes += totals.ProvisionalBytes
			reconciliation.OrphanedObjectCount += totals.Count - totals.ProvisionalCount
			reconciliation.OrphanedBytes += totals.Bytes - totals.ProvisionalBytes
			if reconciliation.ObservedAt.IsZero() || inventoryObservedAt.Before(reconciliation.ObservedAt) {
				reconciliation.ObservedAt = inventoryObservedAt
			}
			break
		}
		if !measured {
			for _, backend := range group.backends {
				unresolvedBackends[backend.ID] = struct{}{}
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
			logicalKey, logical := objectstorage.LogicalKey(namespace.Prefix, object.Key)
			// ListObjects uses an S3 string prefix. A sibling such as
			// "backup-root-old" can therefore be returned while it is not part
			// of the configured "backup-root/" logical namespace.
			if !logical && !namespace.ContainsPhysicalKey(object.Key) {
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
			objectID := namespace.ObjectID(object.Key)
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
			observed[objectID] = backupUsageObservedObject{size: object.Size, lastModified: object.LastModified, observedAt: object.ObservedAt}
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
			if ObjectWithinCleanupGrace(object.lastModified, classificationTime, s.cfg.FailedRunGCGrace) {
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
			if now.Before(artifact.DeletedAt.UTC().Add(s.cfg.ArtifactGCGrace)) {
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
		if !measured || !observedWanted[objectID] {
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

	return reconciliation, nil
}

func summarizeBackupUsageReconciliation(reconciliation Reconciliation) (string, string) {
	if reconciliation.MeasuredBackendCount < reconciliation.BackendCount || reconciliation.UnresolvedBackendCount > 0 {
		if reconciliation.MeasuredBackendCount == 0 && (reconciliation.BackendCount > 0 || reconciliation.UnresolvedBackendCount > 0) {
			return ReconciliationStatusUnavailable, "R2 physical inventory is unavailable; billable bytes remain database-recorded only"
		}
		return ReconciliationStatusPartial, "R2 physical inventory is incomplete; physical totals are omitted"
	}
	if reconciliation.OrphanedObjectCount > 0 ||
		reconciliation.MissingActiveObjectCount > 0 ||
		reconciliation.OverdueDeletionObjectCount > 0 ||
		reconciliation.LingeringDeletedObjectCount > 0 ||
		reconciliation.DuplicateReferenceCount > 0 ||
		reconciliation.InvalidReferenceCount > 0 ||
		reconciliation.SizeMismatchCount > 0 {
		return ReconciliationStatusDrift, "R2 physical inventory differs from durable backup metadata"
	}
	if reconciliation.ProvisionalObjectCount > 0 {
		return ReconciliationStatusReconciling, "recent unreferenced objects remain within the failed-upload cleanup grace period"
	}
	return ReconciliationStatusComplete, "R2 physical inventory matches durable backup metadata"
}

func reconciliationMeasuredCompletely(reconciliation Reconciliation) bool {
	return reconciliation.MeasuredBackendCount == reconciliation.BackendCount && reconciliation.UnresolvedBackendCount == 0
}

func backupUsageTenantOwnsLogicalObject(tenantID, logicalKey string) bool {
	tenantID = strings.TrimSpace(tenantID)
	parts := strings.Split(strings.Trim(strings.TrimSpace(logicalKey), "/"), "/")
	if tenantID == "" || len(parts) < 2 || parts[1] != tenantID {
		return false
	}
	return parts[0] == "apps" || parts[0] == "data-workspaces"
}
