package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestEdgePodLiveServingStateSuppressesRecentCaddyRestart(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 28, 9, 37, 0, 0, time.UTC)
	finishedAt := now.Add(-30 * time.Second)
	pod := kubePodInfo{}
	pod.Spec.Containers = []struct {
		Name  string `json:"name"`
		Image string `json:"image,omitempty"`
	}{{Name: "edge"}, {Name: "caddy"}}
	pod.Status.Phase = "Running"
	pod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name:  "edge",
		Ready: true,
		State: kubeRuntimeState{Running: &struct{}{}},
	}, {
		Name:      "caddy",
		Ready:     true,
		State:     kubeRuntimeState{Running: &struct{}{}},
		LastState: kubeRuntimeState{Terminated: &kubeStateDetail{Reason: "OOMKilled", ExitCode: 137, FinishedAt: &finishedAt}},
	}}

	state := edgePodLiveServingState(pod, now)
	if state.Serving || !strings.Contains(state.Reason, "restarted recently") {
		t.Fatalf("expected recent caddy restart to suppress serving, got %+v", state)
	}
}

func TestEdgePodLiveServingStateRequiresExpectedCaddyStatus(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 28, 9, 37, 0, 0, time.UTC)
	pod := kubePodInfo{}
	pod.Spec.Containers = []struct {
		Name  string `json:"name"`
		Image string `json:"image,omitempty"`
	}{{Name: "edge"}, {Name: "caddy"}}
	pod.Status.Phase = "Running"
	pod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name:  "edge",
		Ready: true,
		State: kubeRuntimeState{Running: &struct{}{}},
	}}

	state := edgePodLiveServingState(pod, now)
	if state.Serving || !strings.Contains(state.Reason, "status is missing") {
		t.Fatalf("expected missing caddy status to suppress serving, got %+v", state)
	}
}

func TestEdgeNodeRouteServingCapableUsesLiveCaddyState(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 28, 9, 37, 0, 0, time.UTC)
	node := model.EdgeNode{
		ID:                 "edge-node-a",
		Status:             model.EdgeHealthHealthy,
		Healthy:            true,
		RouteBundleVersion: "routegen_a",
		LastHeartbeatAt:    &now,
	}
	live := edgeLiveServingEvidence{State: edgeLiveServingEvidenceObserved, ByNode: map[string]edgeLiveServingState{
		"edge-node-a": {Serving: false, Reason: "caddy container restarted recently"},
	}}

	if edgeNodeRouteServingCapableWithLive(node, now, live) {
		t.Fatal("expected live caddy state to make the edge node non-serving")
	}

	unknown := edgeLiveServingEvidence{State: edgeLiveServingEvidenceUnknown}
	if !edgeNodeRouteServingCapableWithLive(node, now, unknown) {
		t.Fatal("expected unknown live evidence to preserve heartbeat-derived serving state")
	}
	observedEmpty := edgeLiveServingEvidence{State: edgeLiveServingEvidenceObserved, ByNode: map[string]edgeLiveServingState{}}
	if !edgeNodeRouteServingCapableWithLive(node, now, observedEmpty) {
		t.Fatal("expected an observed inventory without this node to preserve heartbeat-derived serving state")
	}
}

func TestEdgeLiveServingInventoryPropagatesCancellation(t *testing.T) {
	t.Parallel()

	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
		close(requestCanceled)
	}))
	defer api.Close()

	server := &Server{
		controlPlaneNamespace: "fugue-system",
		edgeLiveServingCache:  newExpiringResponseCache[map[string]edgeLiveServingState](5 * time.Second),
		newClusterNodeClient: func() (*clusterNodeClient, error) {
			return &clusterNodeClient{client: api.Client(), baseURL: api.URL}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan edgeLiveServingEvidence, 1)
	go func() { done <- server.edgeLiveServingByNode(ctx, time.Now().UTC()) }()
	<-requestStarted
	cancel()
	if evidence := <-done; evidence.State != edgeLiveServingEvidenceUnknown {
		t.Fatalf("canceled live inventory returned evidence: %+v", evidence)
	}
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("Kubernetes live inventory request did not observe caller cancellation")
	}
}

func TestDerivedEdgeGroupIDForRuntimeRequiresExplicitIdentity(t *testing.T) {
	t.Parallel()

	runtimeObj := model.Runtime{
		ID:   "runtime_us",
		Type: model.RuntimeTypeManagedOwned,
	}
	edgeGroupID := derivedEdgeGroupIDForRuntime(runtimeObj, true, map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "US",
	})
	if edgeGroupID != defaultEdgeGroupID {
		t.Fatalf("country label must not become edge identity, got %q", edgeGroupID)
	}

	runtimeObj.Labels = map[string]string{runtimepkg.LocationCountryCodeLabelKey: "HK", runtimepkg.EdgeGroupIDLabelKey: "edge-group-public-b"}
	edgeGroupID = derivedEdgeGroupIDForRuntime(runtimeObj, true, map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "US",
		runtimepkg.EdgeGroupIDLabelKey:         "edge-group-public-a",
	})
	if edgeGroupID != "edge-group-public-b" {
		t.Fatalf("expected explicit runtime group to take precedence over node labels, got %q", edgeGroupID)
	}
}

func TestEdgeRouteBindingDerivesNonActiveStatuses(t *testing.T) {
	t.Parallel()

	server := &Server{}
	ctx := context.Background()
	runtimes := map[string]model.Runtime{
		model.DefaultManagedRuntimeID: {
			ID:     model.DefaultManagedRuntimeID,
			Status: model.RuntimeStatusActive,
		},
	}

	disabledApp := model.App{
		ID:       "app_disabled",
		TenantID: "tenant_demo",
		Name:     "disabled",
		Route:    &model.AppRoute{Hostname: "disabled.fugue.pro", ServicePort: 8080},
		Spec: model.AppSpec{
			Replicas:  0,
			RuntimeID: model.DefaultManagedRuntimeID,
		},
	}
	disabled := server.compileTrafficEpochRouteBinding(ctx, disabledApp, "disabled.fugue.pro", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, time.Time{}, time.Time{}, runtimes, nil)
	if disabled.Status != model.EdgeRouteStatusDisabled || disabled.UpstreamURL != "" {
		t.Fatalf("expected disabled route without upstream, got %+v", disabled)
	}

	missingRuntimeApp := model.App{
		ID:       "app_missing_runtime",
		TenantID: "tenant_demo",
		Name:     "missing-runtime",
		Route:    &model.AppRoute{Hostname: "missing-runtime.fugue.pro", ServicePort: 8080},
		Spec: model.AppSpec{
			Replicas:  1,
			RuntimeID: "runtime_missing",
		},
		Status: model.AppStatus{CurrentReplicas: 1},
	}
	missingRuntime := server.compileTrafficEpochRouteBinding(ctx, missingRuntimeApp, "missing-runtime.fugue.pro", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, time.Time{}, time.Time{}, runtimes, nil)
	if missingRuntime.Status != model.EdgeRouteStatusRuntimeMissing || missingRuntime.UpstreamURL != "" {
		t.Fatalf("expected runtime-missing route without upstream, got %+v", missingRuntime)
	}

	unavailableApp := model.App{
		ID:       "app_unavailable",
		TenantID: "tenant_demo",
		Name:     "unavailable",
		Route:    &model.AppRoute{Hostname: "unavailable.fugue.pro", ServicePort: 8080},
		Spec: model.AppSpec{
			Replicas:  1,
			RuntimeID: model.DefaultManagedRuntimeID,
		},
		Status: model.AppStatus{CurrentReplicas: 0},
	}
	unavailable := server.compileTrafficEpochRouteBinding(ctx, unavailableApp, "unavailable.fugue.pro", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, time.Time{}, time.Time{}, runtimes, nil)
	if unavailable.Status != model.EdgeRouteStatusUnavailable || unavailable.UpstreamURL != "" {
		t.Fatalf("expected unavailable route without upstream, got %+v", unavailable)
	}

	nonHTTPApp := model.App{
		ID:       "app_redis",
		TenantID: "tenant_demo",
		Name:     "redis",
		Route:    &model.AppRoute{Hostname: "redis.fugue.pro", ServicePort: 6379},
		Source:   &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "redis:8-alpine"},
		Spec: model.AppSpec{
			Replicas:  1,
			RuntimeID: model.DefaultManagedRuntimeID,
		},
		Status: model.AppStatus{CurrentReplicas: 1},
	}
	nonHTTP := server.compileTrafficEpochRouteBinding(ctx, nonHTTPApp, "redis.fugue.pro", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, time.Time{}, time.Time{}, runtimes, nil)
	if nonHTTP.Status != model.EdgeRouteStatusUnavailable || nonHTTP.UpstreamURL != "" || !strings.Contains(nonHTTP.StatusReason, "non-HTTP") {
		t.Fatalf("expected known non-HTTP app route to be unavailable, got %+v", nonHTTP)
	}
}

func deployAppForEdgeRouteTest(t *testing.T, storeState edgeRouteTestStore, app model.App) model.App {
	t.Helper()
	specCopy := app.Spec
	deployOp, err := storeState.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		ExecutionMode:   model.ExecutionModeManaged,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := storeState.CompleteManagedOperationWithResult(deployOp.ID, "", "deployed", &specCopy, nil); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}
	reloaded, err := storeState.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload deployed app: %v", err)
	}
	return reloaded
}

type edgeRouteTestStore interface {
	CreateApp(string, string, string, string, model.AppSpec) (model.App, error)
	CreateOperation(model.Operation) (model.Operation, error)
	CompleteManagedOperationWithResult(string, string, string, *model.AppSpec, *model.AppSource) (model.Operation, error)
	GetApp(string) (model.App, error)
}

func recordHealthyEdgeForRouteTest(t *testing.T, storeState edgeRouteHeartbeatStore, id, groupID, publicIPv4 string) {
	t.Helper()
	if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, model.EdgeNode{
		ID:          id,
		EdgeGroupID: groupID,
		PublicIPv4:  publicIPv4,
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy edge node: %v", err)
	}
}
