package api

import (
	"context"
	"encoding/json"
	"fmt"
	"fugue/internal/backupusage"
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
