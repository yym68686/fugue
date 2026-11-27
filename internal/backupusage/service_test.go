package backupusage

import (
	"context"
	"errors"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

type readScope struct {
	tenant string
	admin  bool
}

type testSource struct {
	ledger         map[readScope]int64
	backendReads   map[readScope]int
	resolvedScopes []readScope
	backend        model.BackupBackend
	inventory      Inventory
	ledgerError    error
	backendError   error
}

func (f *testSource) BackupUsage(tenant string, admin bool) (model.BackupUsage, error) {
	return model.BackupUsage{TenantID: tenant, BillableBytes: f.ledger[readScope{tenant, admin}]}, f.ledgerError
}

func (f *testSource) ListBackupBackends(tenant string, admin bool) ([]model.BackupBackend, error) {
	f.backendReads[readScope{tenant, admin}]++
	return []model.BackupBackend{f.backend}, f.backendError
}

func (f *testSource) ListUsageArtifacts(string, bool) ([]UsageArtifact, error) { return nil, nil }

func (f *testSource) ResolveNamespace(_ string, tenant string, admin bool) (Namespace, error) {
	f.resolvedScopes = append(f.resolvedScopes, readScope{tenant, admin})
	return NamespaceOf(model.BackupBackendAsDataBackend(f.backend)), nil
}

func (f *testSource) ReadInventory(_ context.Context, key string) (Inventory, error) {
	if key != NamespaceOf(model.BackupBackendAsDataBackend(f.backend)).Key() {
		return Inventory{}, errors.New("unexpected checkpoint namespace")
	}
	return f.inventory, nil
}

func newTestService(now *time.Time) (*Service, *testSource) {
	started := now.Add(-time.Second)
	finished := *now
	f := &testSource{
		ledger:       map[readScope]int64{},
		backendReads: map[readScope]int{},
		backend: model.BackupBackend{
			ID: "backend", Provider: model.DataBackendProviderCloudflareR2,
			Bucket: "bucket", Endpoint: "https://storage.example", Prefix: "backups",
		},
		inventory: Inventory{Complete: &InventorySnapshot{
			StartedAt: started, FinishedAt: &finished,
			Totals: map[string]InventoryTotals{
				"all":      {Count: 3, Bytes: 90},
				"tenant:a": {Count: 1, Bytes: 10},
				"tenant:b": {Count: 1, Bytes: 30},
			},
		}},
	}
	artifactKeys := func(model.BackupArtifact) ([]string, error) {
		return nil, errors.New("fixture should not contain artifacts")
	}
	svc := New(f, artifactKeys, Config{
		CacheTTL: 30 * time.Second, InventoryRefreshInterval: 15 * time.Minute,
		ArtifactGCGrace: time.Hour, FailedRunGCGrace: time.Hour,
	})
	svc.now = func() time.Time { return *now }
	return svc, f
}

func TestBackupUsageServiceScopesAndExpiresOnlyPhysicalCache(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc, source := newTestService(&now)
	tenantA, tenantB, admin := readScope{"a", false}, readScope{"b", false}, readScope{"", true}

	check := func(scope readScope, wantBillable, wantPhysical int64, wantReads int) {
		t.Helper()
		got, err := svc.Load(t.Context(), scope.tenant, scope.admin)
		if err != nil || got.BillableBytes != wantBillable || got.PhysicalBytes == nil || *got.PhysicalBytes != wantPhysical {
			t.Fatalf("scope %+v: usage=%+v, err=%v, want ledger=%d physical=%d", scope, got, err, wantBillable, wantPhysical)
		}
		if got := source.backendReads[scope]; got != wantReads {
			t.Fatalf("scope %+v: reconciliation reads=%d, want %d", scope, got, wantReads)
		}
	}

	source.ledger[tenantA], source.ledger[tenantB], source.ledger[admin] = 11, 31, 91
	check(tenantA, 11, 10, 1)
	check(tenantB, 31, 30, 1)
	check(admin, 91, 90, 1)

	source.ledger[tenantA] = 12
	refreshed := *source.inventory.Complete
	refreshed.Totals = map[string]InventoryTotals{"tenant:a": {Count: 1, Bytes: 13}}
	source.inventory.Complete = &refreshed
	check(tenantA, 12, 10, 1)
	now = now.Add(31 * time.Second)
	check(tenantA, 12, 13, 2)
	wantScopes := []readScope{tenantA, tenantB, admin, tenantA}
	if len(source.resolvedScopes) != len(wantScopes) {
		t.Fatalf("namespace resolutions: %v", source.resolvedScopes)
	}
	for i, want := range wantScopes {
		if source.resolvedScopes[i] != want {
			t.Fatalf("namespace resolution %d: got %+v, want %+v", i, source.resolvedScopes[i], want)
		}
	}
}

func TestBackupUsageServiceRejectsUnfinishedSnapshotWithoutLosingLedger(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc, source := newTestService(&now)
	source.ledger[readScope{"a", false}] = 17
	source.inventory.Complete.FinishedAt = nil
	source.inventory.Current = &InventoryProgress{Generation: "in-flight", Pages: 2, Objects: 7}

	got, err := svc.Load(t.Context(), "a", false)
	if err != nil || got.BillableBytes != 17 || got.PhysicalBytes != nil || got.PhysicalObjectCount != nil {
		t.Fatalf("unfinished inventory produced %+v, err=%v", got, err)
	}
	if r := got.Reconciliation; r == nil || r.Status != ReconciliationStatusUnavailable || !r.Refreshing || r.ScannedPages != 2 {
		t.Fatalf("unfinished inventory status = %+v", r)
	}
}

func TestBackupUsageServiceDoesNotCacheReadErrors(t *testing.T) {
	now := time.Now()
	svc, source := newTestService(&now)
	failure := errors.New("source read failed")
	source.ledgerError = failure
	if _, err := svc.Load(t.Context(), "", true); !errors.Is(err, failure) || len(source.backendReads) != 0 {
		t.Fatalf("ledger failure did not stop reconciliation: %v, reads=%v", err, source.backendReads)
	}
	source.ledgerError, source.backendError = nil, failure
	if _, err := svc.Load(t.Context(), "", true); !errors.Is(err, failure) {
		t.Fatalf("expected exact backend error, got %v", err)
	}
	source.backendError = nil
	if got, err := svc.Load(t.Context(), "", true); err != nil || got.PhysicalBytes == nil || *got.PhysicalBytes != 90 {
		t.Fatalf("backend recovery: usage=%+v, err=%v", got, err)
	}
}

func TestBackupUsagePackageDoesNotImportConcreteApplicationLayers(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file.Name(), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Imports {
			path, err := strconv.Unquote(decl.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			switch path {
			case "fugue/internal/backupusage/contract", "fugue/internal/model", "fugue/internal/objectstorage":
				continue
			}
			if strings.HasPrefix(path, "fugue/internal/") {
				t.Errorf("%s depends on %s; adapt dependencies outside backupusage", file.Name(), path)
			}
		}
	}
}
