package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/runtimeobservation"
	"fugue/internal/schemamigrate"
	"fugue/internal/store"
)

func TestMonitoringReadsOnlyAppIdentityPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for disposable PostgreSQL")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback fugue_test database")
	}
	state := store.New("", address)
	// Bootstrap the disposable base tables before running the independent
	// schema lane. A fresh database has no edge-instance marker yet.
	if err := state.Init(); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateEdgeInstanceFencing(t.Context(), address); err != nil {
		t.Fatal(err)
	}
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	apps := []model.App{}
	for i := 0; i < 2; i++ {
		tenant, err := state.CreateTenant(model.NewID("monitor"))
		if err != nil {
			t.Fatal(err)
		}
		defer func(id string) {
			if _, err := db.Exec(`DELETE FROM fugue_tenants WHERE id=$1`, id); err != nil {
				t.Error(err)
			}
		}(tenant.ID)
		raiseManagedTestCap(t, state, tenant.ID)
		project, err := state.CreateProject(tenant.ID, "apps", "")
		if err != nil {
			t.Fatal(err)
		}
		app, err := state.CreateApp(tenant.ID, project.ID, "demo", "unneeded description", model.AppSpec{Image: "registry.example/demo:v1", Ports: []int{8080}, Replicas: 2, RuntimeID: model.DefaultManagedRuntimeID, Env: map[string]string{"UNNEEDED": strings.Repeat("private-input-", 20000)}, Files: []model.AppFile{{Path: "/config", Content: strings.Repeat("synthetic-config-", 20000), Secret: true}}})
		if err != nil {
			t.Fatal(err)
		}
		apps = append(apps, app)
	}
	for i, entry := range []struct {
		app               int
		mode, status, dns string
	}{
		{0, model.AppDomainDNSModeManaged, model.AppDomainStatusPending, model.AppDomainDNSStatusPending},
		{0, model.AppDomainDNSModeManaged, model.AppDomainStatusVerified, model.AppDomainDNSStatusReady},
		{0, model.AppDomainDNSModeExternal, model.AppDomainStatusPending, model.AppDomainDNSStatusPending},
		{1, model.AppDomainDNSModeManaged, model.AppDomainStatusPending, model.AppDomainDNSStatusReady},
	} {
		app := apps[entry.app]
		_, err := state.PutAppDomain(model.AppDomain{AppID: app.ID, TenantID: app.TenantID, Hostname: strings.ToLower(model.NewID("monitor")) + ".example.test", DNSMode: entry.mode, Status: entry.status, DNSStatus: entry.dns})
		if err != nil {
			t.Fatal(i, err)
		}
	}
	facts := func() []runtimeobservation.DatabaseQueryFact {
		value, err := runtimeobservation.DatabaseObservations.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot struct {
			Queries []runtimeobservation.DatabaseQueryFact `json:"queries"`
		}
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		return snapshot.Queries
	}
	// Derive the allowed query fingerprint from the real projection, avoiding a
	// test that only mirrors SQL construction or checks the method's name.
	began := time.Now()
	identities, _, err := state.ListAppWorkloadIdentitiesWithTiming("", true)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := ""
	for _, f := range facts() {
		if !f.Started.Before(began) && strings.HasSuffix(f.Caller, ".pgReadAppRows") {
			fingerprint = f.Fingerprint
		}
	}
	if fingerprint == "" {
		t.Fatal("identity query was not observed")
	}
	for _, app := range identities {
		if len(app.Spec.Env) > 0 || len(app.Spec.Files) > 0 || app.Source != nil || len(app.BackingServices) > 0 {
			t.Fatal("identity query transferred executable payload")
		}
	}
	assertIdentityRead := func(start time.Time) {
		t.Helper()
		count := 0
		for _, f := range facts() {
			if f.Started.Before(start) {
				continue
			}
			if strings.HasSuffix(f.Caller, ".pgReadAppRows") {
				count++
				if f.Fingerprint != fingerprint {
					t.Fatal("monitor read full application payload instead of attribution identity")
				}
			}
		}
		if count != 1 {
			t.Fatalf("expected one identity inventory, observed%d", count)
		}
	}
	server := NewServer(state, auth.New(state, ""), nil, ServerConfig{})
	var metrics bytes.Buffer
	began = time.Now()
	if count := server.countManagedAppDomainDNSPendingMetric(&metrics); count != 2 {
		t.Fatal("managed DNS pending count changed", count, metrics.String())
	}
	assertIdentityRead(began)
	snapshots := []clusterNodeSnapshot{}
	for _, app := range apps {
		pod := rightSizingTestPod("demo-a", "Running", app.ID, "demo")
		pod.Metadata.Namespace = runtime.NamespaceForTenant(app.TenantID)
		fallback := rightSizingTestPod("demo-b", "Running", "", "demo")
		fallback.Metadata.Namespace = pod.Metadata.Namespace
		fallback.Metadata.Labels = map[string]string{runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue, runtime.FugueLabelName: "demo"}
		first, second := rightSizingTestPodUsage("demo-a", 100, 256, 80, 240, 1), rightSizingTestPodUsage("demo-b", 220, 384, 180, 320, 2)
		first.PodRef.Namespace = pod.Metadata.Namespace
		second.PodRef.Namespace = pod.Metadata.Namespace
		snapshots = append(snapshots, clusterNodeSnapshot{pods: []clusterNodePod{pod, fallback}, summary: &kubeNodeSummary{Pods: []kubeNodeSummaryPod{first, second}}})
	}
	server.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, snapshots)
	began = time.Now()
	if err := server.recordCurrentResourceUsageSamples(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertIdentityRead(began)
	for _, app := range apps {
		samples, err := state.ListResourceUsageSamples(app.TenantID, rightSizingSampleTargetKindAppV1, app.ID, began.Add(-time.Second))
		if err != nil || len(samples) != 1 {
			t.Fatal("tenant workload sample missing", len(samples), err)
		}
		s := samples[0]
		if s.TargetID != app.ID || s.TargetName != app.Name || s.ProjectID != app.ProjectID || s.CPUMilliCores == nil || *s.CPUMilliCores != 180 || s.MemoryBytes == nil || *s.MemoryBytes != 320*1024*1024 {
			t.Fatalf("identity projection changed resource attribution:%+v", s)
		}
		full, err := state.GetApp(app.ID)
		if err != nil || len(full.Spec.Env) == 0 || len(full.Spec.Files) == 0 {
			t.Fatal("monitor mutated stored configuration", err)
		}
	}
}
