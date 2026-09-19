package api

import (
	"context"
	"encoding/json"
	"fmt"
	"fugue/internal/backupusage"
	"fugue/internal/model"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestBackupInventoryResumesAcrossReplicasAndPreservesComplete(t *testing.T) {
	fake := newBackupUsageS3(t)
	state, _, a := newBackupUsageTestServer(t, fake)
	var requests atomic.Int32
	fail := false
	fake.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if fail {
			http.Error(w, "denied", 403)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("continuation-token") == "" {
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>page2</NextContinuationToken><Contents><Key>backup-root/apps/tenant_a/p/a/old</Key><Size>10</Size></Contents></ListBucketResult>`)
		} else {
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>backup-root/apps/tenant_b/p/a/old</Key><Size>20</Size></Contents></ListBucketResult>`)
		}
	})
	if err := a.advanceBackupInventories(t.Context()); err != nil {
		t.Fatal(err)
	}
	usage, err := a.loadBackupUsage(t.Context(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	if usage.PhysicalBytes != nil || !usage.Reconciliation.Refreshing {
		t.Fatalf("partial scan became complete: %+v", usage)
	}
	b := &Server{store: state, backupInventoryConfig: BackupInventoryConfig{RefreshInterval: time.Nanosecond}}
	b.backupUsageReconciliationCache = newExpiringResponseCache[backupusage.Reconciliation](0)
	if err = b.advanceBackupInventories(t.Context()); err != nil {
		t.Fatal(err)
	}
	usage, err = b.loadBackupUsage(t.Context(), "tenant_a", false)
	if err != nil || usage.PhysicalBytes == nil || *usage.PhysicalBytes != 10 {
		t.Fatalf("tenant boundary: %+v %v", usage, err)
	}
	if requests.Load() != 2 {
		t.Fatalf("restarted first page: %d", requests.Load())
	}
	before := *usage.Reconciliation.LastSuccessAt
	fail = true
	if err = b.advanceBackupInventories(t.Context()); err != nil {
		t.Fatal(err)
	}
	usage, err = b.loadBackupUsage(t.Context(), "tenant_a", false)
	if err != nil || usage.PhysicalBytes == nil || *usage.PhysicalBytes != 10 || !usage.Reconciliation.Stale || !usage.Reconciliation.LastSuccessAt.Equal(before) {
		t.Fatalf("failed scan replaced success: %+v %v", usage, err)
	}
}
func TestMetricsReadCannotStartR2Work(t *testing.T) {
	fake := newBackupUsageS3(t)
	_, _, s := newBackupUsageTestServer(t, fake)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	rr := httptest.NewRecorder()
	s.MetricsHandler().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil).WithContext(ctx))
	if fake.calls() != 0 || rr.Code != 200 {
		t.Fatalf("scrape performed external work: %d %d", fake.calls(), rr.Code)
	}
}
func TestBackupInventoryRejectsBrokenPagination(t *testing.T) {
	fake := newBackupUsageS3(t)
	_, _, s := newBackupUsageTestServer(t, fake)
	fake.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>`)
	})
	if err := s.advanceBackupInventories(t.Context()); err != nil {
		t.Fatal(err)
	}
	groups, _ := s.backupInventoryGroups()
	for _, g := range groups {
		raw, _ := s.store.ReadObservationCheckpoint(t.Context(), g.key)
		var cp backupInventoryCheckpoint
		_ = json.Unmarshal(raw, &cp)
		if cp.Complete != nil || cp.Error != "invalid_pagination" {
			t.Fatalf("bad pagination accepted: %+v", cp)
		}
	}
}

func TestInventorySeparatesDeclaredRepositoryBlocksFromArtifactObjects(t *testing.T) {
	fake := newBackupUsageS3(t)
	state, backend, s := newBackupUsageTestServer(t, fake)
	artifact := createBackupUsageArtifact(t, state, model.BackupArtifact{
		ID: "snapshot_a", RunID: "run_a", BackendID: backend.ID, Kind: model.BackupArtifactKindLonghornSnapshot,
		ManifestObjectKey: "control-plane/run_a/manifest.json", SizeBytes: 1000000,
		Status:   model.BackupArtifactStatusActive,
		Manifest: model.BackupManifest{Metadata: map[string]string{"backup_target_url": "s3://bucket@auto/backup-root/shared-repository"}},
	})
	old := time.Now().Add(-48 * time.Hour)
	fake.put("backup-root/"+artifact.ManifestObjectKey, 123, old)
	fake.put("backup-root/shared-repository/blocks/chunk", 456, old)
	fake.put("backup-root/shared-repository-old/not-owned", 7, old)
	groups, err := s.backupInventoryGroups()
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range groups {
		due, err := s.advanceBackupInventoryScheduled(t.Context(), g)
		if err != nil || time.Until(due) < 14*time.Minute {
			t.Fatalf("completed scan should sleep until refresh: %v %v", due, err)
		}
	}
	usage, err := s.loadBackupUsage(t.Context(), "", true)
	if err != nil {
		t.Fatal(err)
	}
	r := usage.Reconciliation
	if usage.PhysicalBytes == nil || *usage.PhysicalBytes != 586 || r.RepositoryManagedBytes != 456 || r.RepositoryManagedObjectCount != 1 || r.OrphanedObjectCount != 1 || r.OrphanedBytes != 7 || r.SizeMismatchCount != 0 {
		t.Fatalf("repository blocks or snapshot upload size misclassified: %+v", usage)
	}
	for _, raw := range []string{"s3://foreign@auto/backup-root/shared", "s3://bucket@auto/other", "s3://bucket@auto/backup-root", "s3://bucket@auto/backup-root/a/../b"} {
		artifact.Manifest.Metadata["backup_target_url"] = raw
		if prefix := backupRepositoryPrefix(artifact, model.BackupBackendAsDataBackend(backend)); prefix != "" {
			t.Fatalf("unsafe repository boundary accepted: %q", raw)
		}
	}
	tenant, err := s.loadBackupUsage(t.Context(), "tenant_a", false)
	if err != nil || tenant.Reconciliation.RepositoryManagedBytes != 0 {
		t.Fatalf("shared blocks attributed to tenant: %+v %v", tenant, err)
	}
}
