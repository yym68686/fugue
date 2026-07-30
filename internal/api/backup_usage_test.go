package api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/backupusage"
	"fugue/internal/model"
	"fugue/internal/store"
)

type backupUsageS3Object struct {
	body         []byte
	lastModified time.Time
}

type backupUsageS3 struct {
	*httptest.Server
	mu        sync.Mutex
	objects   map[string]backupUsageS3Object
	failList  bool
	listCalls int
}

type backupUsageS3ListObject struct {
	Key          string `xml:"Key"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}

type backupUsageS3ListResult struct {
	XMLName     xml.Name                  `xml:"ListBucketResult"`
	Xmlns       string                    `xml:"xmlns,attr,omitempty"`
	IsTruncated bool                      `xml:"IsTruncated"`
	Contents    []backupUsageS3ListObject `xml:"Contents"`
}

func newBackupUsageS3(t *testing.T) *backupUsageS3 {
	t.Helper()
	fake := &backupUsageS3{objects: map[string]backupUsageS3Object{}}
	fake.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		prefix := r.URL.Query().Get("prefix")
		fake.mu.Lock()
		fake.listCalls++
		failList := fake.failList
		contents := make([]backupUsageS3ListObject, 0, len(fake.objects))
		for key, object := range fake.objects {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			contents = append(contents, backupUsageS3ListObject{
				Key:          key,
				Size:         int64(len(object.body)),
				LastModified: object.lastModified.UTC().Format(time.RFC3339),
			})
		}
		fake.mu.Unlock()
		if failList {
			http.Error(w, "synthetic list failure", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(backupUsageS3ListResult{IsTruncated: false, Contents: contents})
	}))
	t.Cleanup(fake.Close)
	return fake
}

func (f *backupUsageS3) put(key string, size int, lastModified time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = backupUsageS3Object{body: bytes.Repeat([]byte{'x'}, size), lastModified: lastModified}
}

func (f *backupUsageS3) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls
}

func newBackupUsageTestServer(t *testing.T, fake *backupUsageS3) (*store.Store, model.BackupBackend, *Server) {
	t.Helper()
	clearBackupUsageSeedEnv(t)
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		ID:           "backup_backend_usage_r2",
		Name:         "usage-r2",
		Provider:     model.DataBackendProviderCloudflareR2,
		Bucket:       "bucket",
		Endpoint:     fake.URL,
		Region:       "auto",
		Prefix:       "backup-root",
		Status:       "active",
		Billable:     true,
		Capabilities: model.DataBackendCapabilitiesForProvider(model.DataBackendProviderCloudflareR2),
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backup backend: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	return stateStore, backend, server
}

func createBackupUsageArtifact(t *testing.T, stateStore *store.Store, artifact model.BackupArtifact) model.BackupArtifact {
	t.Helper()
	created, err := stateStore.CreateBackupArtifact(artifact)
	if err != nil {
		t.Fatalf("create backup artifact %s: %v", artifact.ID, err)
	}
	return created
}

func TestBackupUsageReconciliationReportsExactTenantPhysicalBytes(t *testing.T) {
	fake := newBackupUsageS3(t)
	stateStore, backend, server := newBackupUsageTestServer(t, fake)
	server.backupUsageReconciliationCache = newExpiringResponseCache[backupusage.Reconciliation](0)
	now := time.Now().UTC()

	active := createBackupUsageArtifact(t, stateStore, model.BackupArtifact{
		ID:                "artifact-active",
		RunID:             "backup_run_active",
		TenantID:          "tenant_a",
		Target:            model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", ProjectID: "project_a", AppID: "app_a"},
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindAppPGDump,
		ObjectKey:         "apps/tenant_a/project_a/app_a/backup_run_active/database.dump",
		ManifestObjectKey: "apps/tenant_a/project_a/app_a/backup_run_active/manifest.json",
		SizeBytes:         100,
		Status:            model.BackupArtifactStatusActive,
		Billable:          true,
	})
	deletedAt := now.Add(-30 * time.Minute)
	deleted := createBackupUsageArtifact(t, stateStore, model.BackupArtifact{
		ID:                "artifact-deleted",
		RunID:             "backup_run_deleted",
		TenantID:          "tenant_a",
		Target:            active.Target,
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindAppPGDump,
		ObjectKey:         "apps/tenant_a/project_a/app_a/backup_run_deleted/database.dump",
		ManifestObjectKey: "apps/tenant_a/project_a/app_a/backup_run_deleted/manifest.json",
		SizeBytes:         50,
		Status:            model.BackupArtifactStatusDeleted,
		Billable:          true,
		DeletedAt:         &deletedAt,
	})

	fake.put("backup-root/"+active.ObjectKey, 100, now.Add(-2*time.Hour))
	fake.put("backup-root/"+active.ManifestObjectKey, 17, now.Add(-2*time.Hour))
	fake.put("backup-root/"+deleted.ObjectKey, 50, now.Add(-30*time.Minute))
	fake.put("backup-root/"+deleted.ManifestObjectKey, 11, now.Add(-30*time.Minute))
	fake.put("backup-root/apps/tenant_a/project_a/app_a/backup_run_orphan/database.dump", 70, now.Add(-2*time.Hour))
	fake.put("backup-root/apps/tenant_b/project_b/app_b/backup_run_other/database.dump", 999, now.Add(-2*time.Hour))

	usage, err := server.loadBackupUsage(t.Context(), "tenant_a", false)
	if err != nil {
		t.Fatalf("load tenant usage: %v", err)
	}
	if usage.BillableBytes != 100 {
		t.Fatalf("billable bytes = %d, want 100", usage.BillableBytes)
	}
	if usage.PhysicalBytes == nil || *usage.PhysicalBytes != 248 {
		t.Fatalf("physical bytes = %v, want 248", usage.PhysicalBytes)
	}
	if usage.PhysicalObjectCount == nil || *usage.PhysicalObjectCount != 5 {
		t.Fatalf("physical objects = %v, want 5", usage.PhysicalObjectCount)
	}
	reconciliation := usage.Reconciliation
	if reconciliation == nil || reconciliation.Status != backupusage.ReconciliationStatusDrift {
		t.Fatalf("unexpected reconciliation: %+v", reconciliation)
	}
	if reconciliation.ActiveBytes != 117 || reconciliation.PendingDeletionBytes != 61 || reconciliation.OrphanedBytes != 70 || reconciliation.OrphanedObjectCount != 1 {
		t.Fatalf("unexpected physical composition: %+v", reconciliation)
	}
	if reconciliation.MissingActiveObjectCount != 0 || reconciliation.OverdueDeletionObjectCount != 0 {
		t.Fatalf("unexpected reconciliation anomalies: %+v", reconciliation)
	}
	otherTenantUsage, err := server.loadBackupUsage(t.Context(), "tenant_b", false)
	if err != nil {
		t.Fatalf("load other tenant usage: %v", err)
	}
	if otherTenantUsage.PhysicalBytes == nil || *otherTenantUsage.PhysicalBytes != 999 || otherTenantUsage.PhysicalObjectCount == nil || *otherTenantUsage.PhysicalObjectCount != 1 {
		t.Fatalf("shared R2 inventory leaked or omitted tenant attribution: %+v", otherTenantUsage)
	}
	if fake.calls() != 1 {
		t.Fatalf("tenant reconciliations did not share the same namespace inventory cache: LIST calls=%d", fake.calls())
	}
}

func TestBackupUsageReconciliationTreatsRecentUnreferencedUploadAsReconciling(t *testing.T) {
	fake := newBackupUsageS3(t)
	_, _, server := newBackupUsageTestServer(t, fake)
	server.backupUsageReconciliationCache = newExpiringResponseCache[backupusage.Reconciliation](0)
	fake.put("backup-root/apps/tenant_a/project_a/app_a/backup_run_upload/database.dump", 42, time.Now().UTC().Add(-time.Minute))

	usage, err := server.loadBackupUsage(t.Context(), "tenant_a", false)
	if err != nil {
		t.Fatalf("load tenant usage: %v", err)
	}
	if usage.Reconciliation == nil || usage.Reconciliation.Status != backupusage.ReconciliationStatusReconciling || usage.Reconciliation.ProvisionalObjectCount != 1 || usage.Reconciliation.OrphanedObjectCount != 0 {
		t.Fatalf("unexpected reconciliation: %+v", usage.Reconciliation)
	}
	if usage.PhysicalBytes == nil || *usage.PhysicalBytes != 42 {
		t.Fatalf("physical bytes = %v, want 42", usage.PhysicalBytes)
	}
}

func TestBackupUsageReconciliationCountsUnsafeKeysInsidePhysicalNamespace(t *testing.T) {
	fake := newBackupUsageS3(t)
	_, _, server := newBackupUsageTestServer(t, fake)
	server.backupUsageReconciliationCache = newExpiringResponseCache[backupusage.Reconciliation](0)
	now := time.Now().UTC().Add(-2 * backupFailedRunGCGrace)
	fake.put("backup-root//unsafe-object", 41, now)
	fake.put("backup-root-old/not-in-fugue-namespace", 99, now)

	usage, err := server.loadBackupUsage(t.Context(), "", true)
	if err != nil {
		t.Fatalf("load platform usage: %v", err)
	}
	if usage.PhysicalBytes == nil || *usage.PhysicalBytes != 41 || usage.PhysicalObjectCount == nil || *usage.PhysicalObjectCount != 1 {
		t.Fatalf("physical namespace was undercounted or sibling prefix leaked: %+v", usage)
	}
	if usage.Reconciliation == nil || usage.Reconciliation.Status != backupusage.ReconciliationStatusDrift || usage.Reconciliation.OrphanedObjectCount != 1 || usage.Reconciliation.OrphanedBytes != 41 {
		t.Fatalf("unsafe physical key was not reported as orphaned drift: %+v", usage.Reconciliation)
	}
	tenantUsage, err := server.loadBackupUsage(t.Context(), "tenant_a", false)
	if err != nil {
		t.Fatalf("load tenant usage: %v", err)
	}
	if tenantUsage.PhysicalBytes == nil || *tenantUsage.PhysicalBytes != 0 || tenantUsage.PhysicalObjectCount == nil || *tenantUsage.PhysicalObjectCount != 0 {
		t.Fatalf("unattributable unsafe key leaked into shared tenant usage: %+v", tenantUsage)
	}
	if fake.calls() != 1 {
		t.Fatalf("platform and tenant views did not share one raw namespace inventory: LIST calls=%d", fake.calls())
	}
}

func TestBackupUsageReconciliationDoesNotCompareNewMetadataToOlderInventory(t *testing.T) {
	fake := newBackupUsageS3(t)
	stateStore, backend, server := newBackupUsageTestServer(t, fake)
	server.backupUsageReconciliationCache = newExpiringResponseCache[backupusage.Reconciliation](0)

	// Prime only the shared physical inventory. A later tenant reconciliation
	// must not claim an artifact created after this snapshot is missing.
	if _, err := server.loadBackupUsage(t.Context(), "", true); err != nil {
		t.Fatalf("prime platform inventory: %v", err)
	}
	createBackupUsageArtifact(t, stateStore, model.BackupArtifact{
		ID:                "artifact-after-inventory",
		RunID:             "backup_run_after_inventory",
		TenantID:          "tenant_a",
		Target:            model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", ProjectID: "project_a", AppID: "app_a"},
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindAppPGDump,
		ObjectKey:         "apps/tenant_a/project_a/app_a/backup_run_after_inventory/database.dump",
		ManifestObjectKey: "apps/tenant_a/project_a/app_a/backup_run_after_inventory/manifest.json",
		SizeBytes:         100,
		Status:            model.BackupArtifactStatusActive,
	})

	usage, err := server.loadBackupUsage(t.Context(), "tenant_a", false)
	if err != nil {
		t.Fatalf("load tenant usage: %v", err)
	}
	if usage.Reconciliation == nil || usage.Reconciliation.Status != backupusage.ReconciliationStatusComplete || usage.Reconciliation.MissingActiveObjectCount != 0 {
		t.Fatalf("new metadata was compared to an older physical snapshot: %+v", usage.Reconciliation)
	}
}

func TestBackupUsageReconciliationDoesNotReportCleanupNewerThanCachedInventoryAsLingering(t *testing.T) {
	fake := newBackupUsageS3(t)
	stateStore, backend, server := newBackupUsageTestServer(t, fake)
	server.backupUsageReconciliationCache = newExpiringResponseCache[backupusage.Reconciliation](0)
	now := time.Now().UTC()
	deletedAt := now.Add(-2 * backupArtifactGCGrace)
	artifact := createBackupUsageArtifact(t, stateStore, model.BackupArtifact{
		ID:                "artifact-cleaned-after-inventory",
		RunID:             "backup_run_cleaned_after_inventory",
		TenantID:          "tenant_a",
		Target:            model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", ProjectID: "project_a", AppID: "app_a"},
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindAppPGDump,
		ObjectKey:         "apps/tenant_a/project_a/app_a/backup_run_cleaned_after_inventory/database.dump",
		ManifestObjectKey: "apps/tenant_a/project_a/app_a/backup_run_cleaned_after_inventory/manifest.json",
		SizeBytes:         100,
		Status:            model.BackupArtifactStatusDeleted,
		DeletedAt:         &deletedAt,
	})
	fake.put("backup-root/"+artifact.ObjectKey, 100, now.Add(-time.Hour))
	fake.put("backup-root/"+artifact.ManifestObjectKey, 17, now.Add(-time.Hour))
	if _, err := server.loadBackupUsage(t.Context(), "", true); err != nil {
		t.Fatalf("prime platform inventory: %v", err)
	}
	if err := stateStore.MarkBackupArtifactPhysicalDeleted(artifact.ID, time.Now().UTC().Add(time.Millisecond)); err != nil {
		t.Fatalf("mark physical cleanup: %v", err)
	}

	usage, err := server.loadBackupUsage(t.Context(), "tenant_a", false)
	if err != nil {
		t.Fatalf("load tenant usage: %v", err)
	}
	if usage.Reconciliation == nil || usage.Reconciliation.Status != backupusage.ReconciliationStatusComplete || usage.Reconciliation.LingeringDeletedObjectCount != 0 || usage.Reconciliation.PendingDeletionObjectCount != 2 {
		t.Fatalf("cleanup newer than cached inventory caused false drift: %+v", usage.Reconciliation)
	}
}

func TestBackupUsageReconciliationOmitsPhysicalTotalsWhenR2IsUnavailable(t *testing.T) {
	fake := newBackupUsageS3(t)
	fake.failList = true
	stateStore, backend, server := newBackupUsageTestServer(t, fake)
	server.backupUsageReconciliationCache = newExpiringResponseCache[backupusage.Reconciliation](0)
	createBackupUsageArtifact(t, stateStore, model.BackupArtifact{
		ID:                "artifact-unmeasured",
		RunID:             "backup_run_unmeasured",
		TenantID:          "tenant_a",
		Target:            model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", ProjectID: "project_a", AppID: "app_a"},
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindAppPGDump,
		ObjectKey:         "apps/tenant_a/project_a/app_a/backup_run_unmeasured/database.dump",
		ManifestObjectKey: "apps/tenant_a/project_a/app_a/backup_run_unmeasured/manifest.json",
		SizeBytes:         100,
		Status:            model.BackupArtifactStatusActive,
	})

	usage, err := server.loadBackupUsage(t.Context(), "", true)
	if err != nil {
		t.Fatalf("load platform usage: %v", err)
	}
	if usage.PhysicalBytes != nil || usage.PhysicalObjectCount != nil {
		t.Fatalf("unavailable measurement returned physical totals: %+v", usage)
	}
	if usage.Reconciliation == nil || usage.Reconciliation.Status != backupusage.ReconciliationStatusUnavailable || usage.Reconciliation.MeasuredBackendCount != 0 || usage.Reconciliation.BackendCount != 1 {
		t.Fatalf("unexpected unavailable reconciliation: %+v", usage.Reconciliation)
	}
	if usage.Reconciliation.MissingActiveObjectCount != 0 {
		t.Fatalf("unmeasured backend was guessed to have missing objects: %+v", usage.Reconciliation)
	}
}

func TestBackupUsageReconciliationFailsClosedForArtifactWithoutBackend(t *testing.T) {
	fake := newBackupUsageS3(t)
	stateStore, _, server := newBackupUsageTestServer(t, fake)
	server.backupUsageReconciliationCache = newExpiringResponseCache[backupusage.Reconciliation](0)
	createBackupUsageArtifact(t, stateStore, model.BackupArtifact{
		ID:                "artifact-without-backend",
		RunID:             "backup_run_without_backend",
		TenantID:          "tenant_a",
		Target:            model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", ProjectID: "project_a", AppID: "app_a"},
		Kind:              model.BackupArtifactKindAppPGDump,
		ObjectKey:         "apps/tenant_a/project_a/app_a/backup_run_without_backend/database.dump",
		ManifestObjectKey: "apps/tenant_a/project_a/app_a/backup_run_without_backend/manifest.json",
		Status:            model.BackupArtifactStatusActive,
	})

	usage, err := server.loadBackupUsage(t.Context(), "tenant_a", false)
	if err != nil {
		t.Fatalf("load tenant usage: %v", err)
	}
	if usage.PhysicalBytes != nil || usage.PhysicalObjectCount != nil || usage.Reconciliation == nil || usage.Reconciliation.Status != backupusage.ReconciliationStatusPartial || usage.Reconciliation.UnresolvedBackendCount != 1 {
		t.Fatalf("artifact without a resolvable backend did not fail closed: %+v", usage)
	}
}

func TestBackupUsageReconciliationMarksMixedBackendMeasurementPartial(t *testing.T) {
	measured := newBackupUsageS3(t)
	unavailable := newBackupUsageS3(t)
	unavailable.failList = true
	stateStore, _, server := newBackupUsageTestServer(t, measured)
	server.backupUsageReconciliationCache = newExpiringResponseCache[backupusage.Reconciliation](0)
	server.backupUsageObjectInventoryCache = newExpiringResponseCache[backupUsageObjectInventory](0)
	_, err := stateStore.CreateBackupBackend(model.BackupBackend{
		ID:           "backup_backend_usage_unavailable",
		Name:         "usage-r2-unavailable",
		Provider:     model.DataBackendProviderCloudflareR2,
		Bucket:       "other-bucket",
		Endpoint:     unavailable.URL,
		Region:       "auto",
		Prefix:       "other-root",
		Status:       "active",
		Billable:     true,
		Capabilities: model.DataBackendCapabilitiesForProvider(model.DataBackendProviderCloudflareR2),
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create unavailable backend: %v", err)
	}
	measured.put("backup-root/platform/registry/backup_run_orphan/registry.tar.gz", 64, time.Now().UTC().Add(-2*time.Hour))

	usage, err := server.loadBackupUsage(t.Context(), "", true)
	if err != nil {
		t.Fatalf("load platform usage: %v", err)
	}
	if usage.PhysicalBytes != nil || usage.PhysicalObjectCount != nil {
		t.Fatalf("partial measurement returned exact physical totals: %+v", usage)
	}
	if usage.Reconciliation == nil || usage.Reconciliation.Status != backupusage.ReconciliationStatusPartial || usage.Reconciliation.BackendCount != 2 || usage.Reconciliation.MeasuredBackendCount != 1 || usage.Reconciliation.OrphanedBytes != 64 {
		t.Fatalf("unexpected partial reconciliation: %+v", usage.Reconciliation)
	}
}

func TestBackupUsageReconciliationDetectsMissingOverdueAndLingeringObjects(t *testing.T) {
	fake := newBackupUsageS3(t)
	stateStore, backend, server := newBackupUsageTestServer(t, fake)
	server.backupUsageReconciliationCache = newExpiringResponseCache[backupusage.Reconciliation](0)
	now := time.Now().UTC()
	target := model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", ProjectID: "project_a", AppID: "app_a"}

	active := createBackupUsageArtifact(t, stateStore, model.BackupArtifact{ID: "artifact-missing", RunID: "backup_run_missing", TenantID: "tenant_a", Target: target, BackendID: backend.ID, Kind: model.BackupArtifactKindAppPGDump, ObjectKey: "apps/tenant_a/project_a/app_a/backup_run_missing/database.dump", ManifestObjectKey: "apps/tenant_a/project_a/app_a/backup_run_missing/manifest.json", SizeBytes: 100, Status: model.BackupArtifactStatusActive})
	oldDeletedAt := now.Add(-2 * backupArtifactGCGrace)
	overdue := createBackupUsageArtifact(t, stateStore, model.BackupArtifact{ID: "artifact-overdue", RunID: "backup_run_overdue", TenantID: "tenant_a", Target: target, BackendID: backend.ID, Kind: model.BackupArtifactKindAppPGDump, ObjectKey: "apps/tenant_a/project_a/app_a/backup_run_overdue/database.dump", ManifestObjectKey: "apps/tenant_a/project_a/app_a/backup_run_overdue/manifest.json", SizeBytes: 50, Status: model.BackupArtifactStatusDeleted, DeletedAt: &oldDeletedAt})
	lingering := createBackupUsageArtifact(t, stateStore, model.BackupArtifact{ID: "artifact-lingering", RunID: "backup_run_lingering", TenantID: "tenant_a", Target: target, BackendID: backend.ID, Kind: model.BackupArtifactKindAppPGDump, ObjectKey: "apps/tenant_a/project_a/app_a/backup_run_lingering/database.dump", ManifestObjectKey: "apps/tenant_a/project_a/app_a/backup_run_lingering/manifest.json", SizeBytes: 25, Status: model.BackupArtifactStatusDeleted, DeletedAt: &oldDeletedAt})
	if err := stateStore.MarkBackupArtifactPhysicalDeleted(lingering.ID, now.Add(-time.Minute)); err != nil {
		t.Fatalf("mark lingering artifact physically deleted: %v", err)
	}

	fake.put("backup-root/"+active.ObjectKey, 99, now.Add(-2*time.Hour))
	fake.put("backup-root/"+overdue.ObjectKey, 50, now.Add(-2*time.Hour))
	fake.put("backup-root/"+overdue.ManifestObjectKey, 10, now.Add(-2*time.Hour))
	fake.put("backup-root/"+lingering.ObjectKey, 25, now.Add(-2*time.Hour))
	fake.put("backup-root/"+lingering.ManifestObjectKey, 9, now.Add(-2*time.Hour))

	usage, err := server.loadBackupUsage(t.Context(), "tenant_a", false)
	if err != nil {
		t.Fatalf("load tenant usage: %v", err)
	}
	reconciliation := usage.Reconciliation
	if reconciliation == nil || reconciliation.Status != backupusage.ReconciliationStatusDrift {
		t.Fatalf("unexpected reconciliation: %+v", reconciliation)
	}
	if reconciliation.SizeMismatchCount != 1 || reconciliation.MissingActiveObjectCount != 1 || reconciliation.OverdueDeletionObjectCount != 2 || reconciliation.LingeringDeletedObjectCount != 2 {
		t.Fatalf("unexpected drift counters: %+v", reconciliation)
	}
}

func TestBackupUsageReconciliationIgnoresInvalidMetadataAlreadyPhysicallyDeleted(t *testing.T) {
	fake := newBackupUsageS3(t)
	stateStore, backend, server := newBackupUsageTestServer(t, fake)
	server.backupUsageReconciliationCache = newExpiringResponseCache[backupusage.Reconciliation](0)
	deletedAt := time.Now().UTC().Add(-2 * backupArtifactGCGrace)
	artifact := createBackupUsageArtifact(t, stateStore, model.BackupArtifact{
		ID:                "artifact-legacy-invalid",
		RunID:             "backup_run_legacy_invalid",
		TenantID:          "tenant_a",
		Target:            model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", AppID: "app_a"},
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindAppPGDump,
		ObjectKey:         "legacy/object-without-run/database.dump",
		ManifestObjectKey: "legacy/object-without-run/manifest.json",
		Status:            model.BackupArtifactStatusDeleted,
		DeletedAt:         &deletedAt,
	})
	if err := stateStore.MarkBackupArtifactPhysicalDeleted(artifact.ID, time.Now().UTC().Add(-time.Hour)); err != nil {
		t.Fatalf("mark legacy artifact physically deleted: %v", err)
	}

	usage, err := server.loadBackupUsage(t.Context(), "tenant_a", false)
	if err != nil {
		t.Fatalf("load tenant usage: %v", err)
	}
	if usage.Reconciliation == nil || usage.Reconciliation.Status != backupusage.ReconciliationStatusComplete || usage.Reconciliation.InvalidReferenceCount != 0 {
		t.Fatalf("fully deleted legacy metadata caused current physical drift: %+v", usage.Reconciliation)
	}
}

func TestBackupUsageEndpointAndMetricsExposeCompletePhysicalInventory(t *testing.T) {
	fake := newBackupUsageS3(t)
	stateStore, backend, server := newBackupUsageTestServer(t, fake)
	now := time.Now().UTC()
	artifact := createBackupUsageArtifact(t, stateStore, model.BackupArtifact{
		ID:                "artifact-endpoint",
		RunID:             "backup_run_endpoint",
		TenantID:          "tenant_a",
		Target:            model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", ProjectID: "project_a", AppID: "app_a"},
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindAppPGDump,
		ObjectKey:         "apps/tenant_a/project_a/app_a/backup_run_endpoint/database.dump",
		ManifestObjectKey: "apps/tenant_a/project_a/app_a/backup_run_endpoint/manifest.json",
		SizeBytes:         100,
		Status:            model.BackupArtifactStatusActive,
		Billable:          true,
	})
	fake.put("backup-root/"+artifact.ObjectKey, 100, now.Add(-time.Hour))
	fake.put("backup-root/"+artifact.ManifestObjectKey, 17, now.Add(-time.Hour))
	fake.put("backup-root-old/not-in-fugue-namespace", 999, now.Add(-time.Hour))

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/backups/usage", "bootstrap-secret", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("backup usage status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Usage backupusage.Usage `json:"usage"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Usage.PhysicalBytes == nil || *response.Usage.PhysicalBytes != 117 || response.Usage.Reconciliation == nil || response.Usage.Reconciliation.Status != backupusage.ReconciliationStatusComplete {
		t.Fatalf("unexpected usage response: %+v", response.Usage)
	}

	metrics := httptest.NewRecorder()
	server.MetricsHandler().ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		"fugue_backup_physical_bytes 117.000000",
		"fugue_backup_physical_objects 2.000000",
		`fugue_backup_reconciliation_status{status="complete"} 1.000000`,
		"fugue_backup_lingering_deleted_objects 0.000000",
		"fugue_backup_size_mismatches 0.000000",
		"fugue_backup_unresolved_backends 0.000000",
		"fugue_backup_reconciliation_drift 0.000000",
	} {
		if !strings.Contains(metrics.Body.String(), want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.Body.String())
		}
	}
	if fake.calls() != 1 {
		t.Fatalf("usage response and metrics should share one reconciliation cache load, got %d LIST calls", fake.calls())
	}
	checks, err := server.robustnessBackupChecks(t.Context())
	if err != nil {
		t.Fatalf("build backup robustness checks: %v", err)
	}
	foundReconciliation := false
	for _, check := range checks {
		if check.Name != "backup_storage_reconciliation" {
			continue
		}
		foundReconciliation = true
		if !check.Pass || check.Observed != "status=complete measured_backends=1/1 orphaned_objects=0 missing_active=0 overdue_deletion=0 lingering_deleted=0 invalid_references=0 size_mismatches=0 unresolved_backends=0" {
			t.Fatalf("unexpected backup reconciliation robustness check: %+v", check)
		}
	}
	if !foundReconciliation {
		t.Fatal("backup reconciliation robustness check was not emitted")
	}
}

func clearBackupUsageSeedEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"FUGUE_DATA_BACKEND_PROVIDER",
		"FUGUE_DATA_BACKEND_BUCKET",
		"FUGUE_DATA_BACKEND_ACCESS_KEY_ID",
		"FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY",
		"FUGUE_DATA_BACKEND_SESSION_TOKEN",
		"FUGUE_DATA_BACKEND_ENDPOINT",
		"FUGUE_DATA_R2_ACCOUNT_ID",
		"FUGUE_DATA_BACKEND_REGION",
		"FUGUE_DATA_BACKEND_PREFIX",
		"FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY",
	} {
		t.Setenv(key, "")
	}
}

func TestBackupUsageTenantObjectOwnershipIsCanonical(t *testing.T) {
	tests := map[string]bool{
		"apps/tenant_a/project/app/run/database.dump":                  true,
		"data-workspaces/tenant_a/project/workspace/run/manifest.json": true,
		"apps/tenant_b/project/app/run/database.dump":                  false,
		"control-plane/2026/07/30/12/run/control-plane.dump":           false,
		"tenant_a/arbitrary": false,
	}
	for key, want := range tests {
		t.Run(fmt.Sprintf("%t-%s", want, strings.ReplaceAll(key, "/", "-")), func(t *testing.T) {
			if got := backupUsageTenantOwnsLogicalObject("tenant_a", key); got != want {
				t.Fatalf("ownership for %q = %t, want %t", key, got, want)
			}
		})
	}
}

func TestBackupUsageR2NamespaceMustRoundTripExactly(t *testing.T) {
	tests := []struct {
		name    string
		backend model.DataBackend
		want    bool
	}{
		{name: "safe prefix", backend: model.DataBackend{Endpoint: "https://r2.example.test", Prefix: "backup-root"}, want: true},
		{name: "empty prefix", backend: model.DataBackend{Endpoint: "https://r2.example.test"}, want: true},
		{name: "missing endpoint", backend: model.DataBackend{Prefix: "backup-root"}, want: false},
		{name: "cleaned prefix", backend: model.DataBackend{Endpoint: "https://r2.example.test", Prefix: "backup-root/../other-root"}, want: false},
		{name: "dot prefix", backend: model.DataBackend{Endpoint: "https://r2.example.test", Prefix: "."}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objectBackend := &dataObjectBackend{backend: tt.backend}
			if got := backupUsageR2NamespaceIsMeasurable(objectBackend); got != tt.want {
				t.Fatalf("measurable = %t, want %t", got, tt.want)
			}
		})
	}
}
