package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestAppCPUResizeOutageDoesNotCreateOperationsAndPendingReplacementRecovers(t *testing.T) {
	st := store.New(t.TempDir() + "/state.json")
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := st.CreateTenant("Tenant")
	project, _ := st.CreateProject(tenant.ID, "services", "")
	app, err := st.CreateApp(tenant.ID, project.ID, "service", "", model.AppSpec{RuntimeID: "runtime_managed_shared", Image: "registry.example/service:one", Replicas: 1, Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 128, MemoryLimitMebibytes: 256}})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SyncManagedAppObservedStatus(app.ID, "ready", 1, "serving", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	app, _ = st.GetApp(app.ID)
	desired := app.Spec
	desired.Resources = model.CloneResourceSpec(desired.Resources)
	desired.Resources.CPUMilliCores = 75
	op, err := st.CreateAppCPUResizeOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeAppCPUResize, DesiredSpec: &desired}, app.Spec)
	if err != nil {
		t.Fatal(err)
	}
	op, _, err = st.TryClaimPendingOperation(op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CompleteManagedOperationGuarded(op, &app, "", "verified", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	app, _ = st.GetApp(app.ID)
	workload := runtime.RuntimeAppResourceName(app)
	pod := appResizeTestPod(t)
	pod.Metadata.Labels = map[string]string{runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue, runtime.FugueLabelTenantID: app.TenantID, runtime.FugueLabelAppID: app.ID, runtime.FugueLabelAppWorkload: workload}
	controller := true
	pod.Metadata.OwnerReferences = []kubeResizeOwnerReference{{Kind: "ReplicaSet", Name: "service-rs", UID: "rs-one", Controller: &controller}}
	old := pod
	old.Metadata.UID, old.Metadata.Name, old.Metadata.DeletionTimestamp = "old-uid", "old-pod", "2026-01-01T00:00:00Z"
	pods := []kubeResizePod{old, pod}
	mutations := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			mutations++
			t.Errorf("preflight mutated Kubernetes: %s", r.Method)
			http.Error(w, "unexpected", 500)
			return
		}
		switch {
		case strings.Contains(r.URL.Path, "/services/"):
			json.NewEncoder(w).Encode(map[string]any{"spec": map[string]any{"selector": pod.Metadata.Labels}})
		case strings.HasSuffix(r.URL.Path, "/deployments"):
			json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
		case strings.Contains(r.URL.Path, "/deployments/"):
			json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "deployment-one", "generation": 1}, "spec": map[string]any{"replicas": 1}, "status": map[string]any{"observedGeneration": 1}})
		case strings.HasSuffix(r.URL.Path, "/pods"):
			json.NewEncoder(w).Encode(map[string]any{"items": pods})
		case strings.Contains(r.URL.Path, "/replicasets/"):
			json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "rs-one", "ownerReferences": []map[string]any{{"kind": "Deployment", "name": workload, "uid": "deployment-one", "controller": true}}}})
		default:
			t.Errorf("unexpected read %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := &kubeClient{baseURL: server.URL, client: server.Client()}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc := &Service{Store: st, now: func() time.Time { return now }}
	// A full nine-hour outage at the normal 15-second reconciliation interval.
	for n := 0; n < 2160; n++ {
		queued, err := svc.reconcileAppCPUResizeTarget(t.Context(), c, app)
		var waiting *appCPUResizeWaiting
		if queued || !errors.As(err, &waiting) {
			t.Fatalf("outage attempt %d: queued=%v err=%v", n, queued, err)
		}
		now = now.Add(15 * time.Second)
	}
	ops, err := st.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil || len(ops) != 1 || mutations != 0 {
		t.Fatalf("outage generated operations/mutations: %d/%d %v", len(ops), mutations, err)
	}
	// Kubelet finally acknowledges the old deletion. The replacement does not
	// need to be Ready (or scheduled) to regain its verified CPU request.
	pod.Status.Phase, pod.Spec.NodeName = "Pending", ""
	pod.Status.Conditions, pod.Status.ContainerStatuses = nil, nil
	pods = []kubeResizePod{pod}
	_, fp, needed, err := svc.planAppCPUResize(t.Context(), c, app, 75)
	if err != nil || !needed {
		t.Fatal("Pending restore blocked", err)
	}
	pods[0].Metadata.ResourceVersion = "heartbeat-only-change"
	_, fp2, _, err := svc.planAppCPUResize(t.Context(), c, app, 75)
	if err != nil || fp != fp2 {
		t.Fatal("resourceVersion reset the retry budget", err)
	}
	queued, err := svc.reconcileAppCPUResizeTarget(t.Context(), c, app)
	if err != nil || !queued {
		t.Fatal("recovered cohort not queued", err)
	}
	ops, err = st.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil || len(ops) != 2 {
		t.Fatalf("expected exactly one restore operation: %d %v", len(ops), err)
	}
	for _, candidate := range ops {
		if candidate.Status == model.OperationStatusPending {
			if _, err := st.FailOperation(candidate.ID, "preflight raced with a rollout"); err != nil {
				t.Fatal(err)
			}
		}
	}
	queued, err = svc.reconcileAppCPUResizeTarget(t.Context(), c, app)
	var waiting *appCPUResizeWaiting
	if queued || !errors.As(err, &waiting) {
		t.Fatal("fast failure bypassed backoff", queued, err)
	}
}

func TestAppCPUResizeRetryBudgetPersistsAcrossLeadersAndBoundsFailures(t *testing.T) {
	path := t.TempDir() + "/state.json"
	st := store.New(path)
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	app := model.App{ID: "app_example", TenantID: "tenant_example"}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newLeader := func() *Service { return &Service{Store: store.New(path), now: func() time.Time { return now }} }
	admissions := 0
	for n := 0; n < 9*60*60; n++ {
		ok, err := newLeader().admitAppCPUResizeReconcile(context.Background(), app, "same-pod-and-intent")
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			admissions++
		}
		now = now.Add(time.Second)
	}
	if admissions > 120 || admissions < 100 {
		t.Fatalf("unbounded or permanently stalled retries: %d", admissions)
	}
	if ok, err := newLeader().admitAppCPUResizeReconcile(t.Context(), app, "new-pod-identity"); err != nil || !ok {
		t.Fatal("relevant change did not resume immediately", ok, err)
	}
	if ok, err := newLeader().admitAppCPUResizeReconcile(t.Context(), app, "new-pod-identity"); err != nil || ok {
		t.Fatal("another leader bypassed the reservation", ok, err)
	}
}

func TestAppCPUResizeRestoresUseDedicatedWorkerLane(t *testing.T) {
	op := model.Operation{Type: model.OperationTypeAppCPUResize, RequestedByType: model.ActorTypeSystem, RequestedByID: "app-cpu-resize-reconcile"}
	for _, lane := range []operationLane{operationLaneForegroundImport, operationLaneForegroundActivate, operationLaneGitHubSyncImport, operationLaneGitHubSyncActivate, operationLaneCPURequestRestore} {
		if pendingOperationMatchesLane(op, lane) != (lane == operationLaneCPURequestRestore) {
			t.Fatal("restore consumed an unrelated worker", lane)
		}
	}
	op.RequestedByType = model.ActorTypeAPIKey
	if pendingOperationMatchesLane(op, operationLaneCPURequestRestore) || !pendingOperationMatchesLane(op, operationLaneForegroundActivate) {
		t.Fatal("user resize routed as background restore")
	}
}
