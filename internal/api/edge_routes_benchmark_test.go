package api

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func BenchmarkDeriveEdgeRouteBundle(b *testing.B) {
	stateStore := store.New(filepath.Join(b.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		b.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Edge Bundle Benchmark Tenant")
	if err != nil {
		b.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "edge-bundle-bench", "")
	if err != nil {
		b.Fatalf("create project: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{
		AppBaseDomain:          "bench.fugue.pro",
		APIPublicDomain:        "api.bench.fugue.pro",
		EdgeQualityRankingMode: "active",
		EdgeTLSAskToken:        "edge-secret",
		AllowLegacyEdgeToken:   true,
	})
	now := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	for edgeIndex := 0; edgeIndex < 3; edgeIndex++ {
		if _, _, err := stateStore.UpdateEdgeHeartbeat(model.EdgeNode{
			ID:          fmt.Sprintf("edge-bench-%d", edgeIndex),
			EdgeGroupID: defaultEdgeGroupID,
			PublicIPv4:  fmt.Sprintf("203.0.113.%d", 20+edgeIndex),
			Status:      model.EdgeHealthHealthy,
			Healthy:     true,
		}); err != nil {
			b.Fatalf("record healthy edge node: %v", err)
		}
	}
	for appIndex := 0; appIndex < 100; appIndex++ {
		hostname := fmt.Sprintf("app-%03d.bench.fugue.pro", appIndex)
		app, err := stateStore.CreateAppWithRoute(tenant.ID, project.ID, fmt.Sprintf("app-%03d", appIndex), "", model.AppSpec{
			Image:     fmt.Sprintf("ghcr.io/example/app-%03d:latest", appIndex),
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_managed_shared",
		}, model.AppRoute{
			Hostname:    hostname,
			BaseDomain:  "bench.fugue.pro",
			PublicURL:   "https://" + hostname,
			ServicePort: 8080,
		})
		if err != nil {
			b.Fatalf("create app %d: %v", appIndex, err)
		}
		specCopy := app.Spec
		op, err := stateStore.CreateOperation(model.Operation{
			TenantID:      tenant.ID,
			Type:          model.OperationTypeDeploy,
			AppID:         app.ID,
			DesiredSpec:   &specCopy,
			ExecutionMode: model.ExecutionModeManaged,
		})
		if err != nil {
			b.Fatalf("create operation %d: %v", appIndex, err)
		}
		if _, err := stateStore.CompleteManagedOperationWithResult(op.ID, "", "deployed", &specCopy, nil); err != nil {
			b.Fatalf("complete operation %d: %v", appIndex, err)
		}
		if appIndex%2 == 0 {
			if _, err := stateStore.PutAppDomain(model.AppDomain{
				Hostname:    fmt.Sprintf("custom-%03d.example.com", appIndex),
				AppID:       app.ID,
				TenantID:    app.TenantID,
				Status:      model.AppDomainStatusVerified,
				TLSStatus:   model.AppDomainTLSStatusReady,
				RouteTarget: server.primaryCustomDomainTarget(app),
				CreatedAt:   now,
				UpdatedAt:   now,
			}); err != nil {
				b.Fatalf("put app domain %d: %v", appIndex, err)
			}
		}
	}
	req := httptest.NewRequest("GET", "/v1/edge/routes?edge_group_id="+defaultEdgeGroupID, nil).WithContext(context.Background())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bundle, err := server.deriveEdgeRouteBundle(req, edgeRouteBundleOptions{EdgeGroupID: defaultEdgeGroupID})
		if err != nil {
			b.Fatalf("derive edge route bundle: %v", err)
		}
		if len(bundle.Routes) == 0 {
			b.Fatal("expected benchmark bundle to include routes")
		}
	}
}
