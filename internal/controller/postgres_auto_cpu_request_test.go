package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestReconcileManagedPostgresAutoCPUQueuesGuardedOperation(t *testing.T) {
	st := store.New(t.TempDir() + "/state.json")
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := st.CreateTenant("CPU enrollment")
	if err != nil {
		t.Fatal(err)
	}
	project, err := st.CreateProject(tenant.ID, "database", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := st.CreateApp(tenant.ID, project.ID, "database-app", "", model.AppSpec{Image: "example/app:1", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID, Postgres: &model.AppPostgresSpec{Database: "app", User: "app", Password: "test", StorageSize: "1Gi", Resources: &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 512, CPULimitMilliCores: 250, MemoryLimitMebibytes: 768}}})
	if err != nil {
		t.Fatal(err)
	}
	service := app.BackingServices[0]
	namespace := runtime.NamespaceForTenant(tenant.ID)
	enrollment := config.ManagedPostgresCPURequestTarget{ServiceID: service.ID, Namespace: namespace, Cluster: service.Spec.Postgres.ServiceName, ClusterUID: "database-uid", SystemID: "123456"}
	now := time.Now().UTC()
	var samples []model.ResourceUsageSample
	cpu := int64(11)
	for i := 0; i < 12; i++ {
		samples = append(samples, model.ResourceUsageSample{TenantID: tenant.ID, ProjectID: project.ID, TargetKind: managedPostgresCPUUsageSampleKind, TargetID: service.ID, ObservedAt: now.Add(-time.Duration(i+1) * time.Minute), CPUMilliCores: &cpu})
	}
	if err := st.RecordResourceUsageSamples(samples, time.Time{}); err != nil {
		t.Fatal(err)
	}
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++
		if r.Method != http.MethodGet {
			t.Errorf("automatic scheduler mutated Kubernetes: %s", r.Method)
			w.WriteHeader(405)
			return
		}
		switch r.URL.Path {
		case "/apis/postgresql.cnpg.io/v1/namespaces/" + namespace + "/clusters/" + enrollment.Cluster:
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": enrollment.Cluster, "uid": enrollment.ClusterUID}, "status": map[string]any{"systemID": enrollment.SystemID, "currentPrimary": "database-1"}})
		case "/api/v1/namespaces/" + namespace + "/pods/database-1":
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "database-1", "namespace": namespace, "uid": "pod-uid"}, "spec": map[string]any{"containers": []any{map[string]any{"name": "postgres", "resources": map[string]any{"requests": map[string]string{"cpu": "100m", "memory": "512Mi"}, "limits": map[string]string{"cpu": "250m", "memory": "768Mi"}}}}}, "status": map[string]any{"containerStatuses": []any{map[string]any{"name": "postgres", "ready": true}}}})
		default:
			t.Errorf("unexpected read %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := &kubeClient{client: server.Client(), baseURL: server.URL}
	s := &Service{Store: st, Config: config.ControllerConfig{KubectlApply: true, ManagedPostgresInPlaceResize: config.ManagedPostgresInPlaceResizeConfig{Enabled: true}}}
	if queued, err := s.reconcileManagedPostgresAutoCPURequest(context.Background(), client, app); err != nil || queued || reads != 0 {
		t.Fatalf("unenrolled database processed: queued=%v reads=%d err=%v", queued, reads, err)
	}
	s.Config.ManagedPostgresInPlaceResize.AutoCPURequestDownscaleTargets = []config.ManagedPostgresCPURequestTarget{enrollment}
	if queued, err := s.reconcileManagedPostgresAutoCPURequest(context.Background(), client, app); err != nil || !queued {
		t.Fatalf("automatic resize not queued: queued=%v err=%v", queued, err)
	}
	ops, err := st.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil || len(ops) != 1 {
		t.Fatalf("operations=%d err=%v", len(ops), err)
	}
	op := ops[0]
	want := model.ResourceSpec{CPUMilliCores: 15, MemoryMebibytes: 512, CPULimitMilliCores: 250, MemoryLimitMebibytes: 768}
	if op.Type != model.OperationTypeDatabaseResize || op.RequestedByID != managedPostgresCPUActor(enrollment) || op.ServiceID != service.ID || op.DesiredSpec.Postgres.RuntimeResources == nil || *op.DesiredSpec.Postgres.RuntimeResources != want {
		t.Fatalf("unexpected queued intent %+v", op)
	}
	stored, err := st.GetBackingService(service.ID)
	if err != nil || stored.Spec.Postgres.RuntimeResources != nil {
		t.Fatalf("unverified runtime target persisted: err=%v", err)
	}
	if reads != 2 {
		t.Fatalf("expected only Cluster and Pod reads, got %d", reads)
	}
}

func TestManagedPostgresAutoCPURecommendationPreservesLiveEnvelope(t *testing.T) {
	now := time.Now().UTC()
	policy := model.NormalizeAppRightSizingSpec(model.AppRightSizingSpec{Mode: model.AppRightSizingModeAuto})
	current := kubeResourceRequirements{Requests: map[string]string{"cpu": "100m", "memory": "512Mi"}, Limits: map[string]string{"cpu": "250m", "memory": "768Mi"}}
	for _, test := range []struct {
		name  string
		cpu   int64
		age   time.Duration
		count int
		want  int64
	}{
		{"p75 rounded", 11, time.Minute, 12, 15},
		{"zero floor", 0, time.Minute, 12, 1},
		{"existing rounding", 1, time.Minute, 12, 5},
		{"no upscale", 101, time.Minute, 12, 0},
		{"below threshold", 99, time.Minute, 12, 0},
		{"stale", 4, time.Hour, 12, 0},
		{"insufficient", 4, time.Minute, 11, 0},
		{"future", 4, -time.Minute, 12, 0},
		{"negative", -1, time.Minute, 12, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			samples := make([]model.ResourceUsageSample, test.count)
			for i := range samples {
				samples[i] = model.ResourceUsageSample{CPUMilliCores: &test.cpu, ObservedAt: now.Add(-test.age)}
			}
			target, err := managedPostgresAutoCPURequestTarget(current, samples, policy, now)
			if err != nil {
				t.Fatal(err)
			}
			if test.want == 0 {
				if target != nil {
					t.Fatalf("unexpected target %+v", target)
				}
				return
			}
			want := model.ResourceSpec{CPUMilliCores: test.want, MemoryMebibytes: 512, CPULimitMilliCores: 250, MemoryLimitMebibytes: 768}
			if target == nil || *target != want {
				t.Fatalf("target=%+v want=%+v", target, want)
			}
		})
	}
	// P75 must not turn a single CPU spike into a request or use P50.
	values := []int64{1, 1, 1, 1, 1, 1, 11, 11, 11, 11, 11, 900}
	samples := make([]model.ResourceUsageSample, len(values))
	for i := range values {
		samples[i] = model.ResourceUsageSample{CPUMilliCores: &values[i], ObservedAt: now.Add(-time.Minute)}
	}
	target, err := managedPostgresAutoCPURequestTarget(current, samples, policy, now)
	if err != nil || target == nil || target.CPUMilliCores != 15 {
		t.Fatalf("P75 target=%+v err=%v", target, err)
	}
}

func TestManagedPostgresCPUEnrollmentRequiresExactDatabaseIdentity(t *testing.T) {
	f := newManagedPostgresResizeExecutorFixture(t)
	enrollment := config.ManagedPostgresCPURequestTarget{ServiceID: "service-db", Namespace: f.snapshot.Observation.Namespace, Cluster: f.snapshot.Cluster.Metadata.Name, ClusterUID: f.snapshot.Cluster.Metadata.UID, SystemID: f.snapshot.Cluster.Status.SystemID}
	cfg := config.ManagedPostgresInPlaceResizeConfig{AutoCPURequestDownscaleTargets: []config.ManagedPostgresCPURequestTarget{enrollment}}
	if managedPostgresCPUEnrollment(cfg, "other-service", enrollment.Namespace, enrollment.Cluster) != nil {
		t.Fatal("unenrolled service admitted")
	}
	if managedPostgresCPUEnrollment(cfg, enrollment.ServiceID, "other-tenant", enrollment.Cluster) != nil {
		t.Fatal("other tenant admitted")
	}
	if !managedPostgresCPUEnrollmentMatches(enrollment, f.snapshot) {
		t.Fatal("enrolled identity rejected")
	}
	f.snapshot.Cluster.Metadata.UID = "replacement-cluster"
	if managedPostgresCPUEnrollmentMatches(enrollment, f.snapshot) {
		t.Fatal("recreated database admitted")
	}
	f.snapshot.Cluster.Metadata.UID = enrollment.ClusterUID
	f.snapshot.Cluster.Status.SystemID = "replacement-system"
	if managedPostgresCPUEnrollmentMatches(enrollment, f.snapshot) {
		t.Fatal("reinitialized database admitted")
	}
}

func TestAutomaticPostgresCPUTargetRejectsNonCPUDrift(t *testing.T) {
	current := kubeResourceRequirements{Requests: map[string]string{"cpu": "100m", "memory": "512Mi"}, Limits: map[string]string{"cpu": "250m", "memory": "768Mi"}}
	target := cloneKubeResourceRequirements(current)
	target.Requests["cpu"] = "15m"
	if err := validateManagedPostgresAutomaticCPUTarget(current, target); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*kubeResourceRequirements){
		func(r *kubeResourceRequirements) { r.Requests["memory"] = "1Gi" },
		func(r *kubeResourceRequirements) { r.Limits["memory"] = "1Gi" },
		func(r *kubeResourceRequirements) { r.Limits["cpu"] = "500m" },
		func(r *kubeResourceRequirements) { r.Requests["cpu"] = "10m" },
	} {
		drifted := cloneKubeResourceRequirements(current)
		mutate(&drifted)
		if err := validateManagedPostgresAutomaticCPUTarget(drifted, target); err == nil {
			t.Fatal("live envelope drift accepted")
		}
	}
}

func TestExecuteManagedPostgresCPUDownscalePreservesPodAndLimits(t *testing.T) {
	f := newManagedPostgresResizeExecutorFixture(t)
	f.snapshot.Observation.InitContainers = []kubeResizeContainerSpec{{Name: "bootstrap-controller", Resources: kubeResourceRequirements{Requests: map[string]string{"cpu": "1m", "memory": "256Mi"}}}}
	target := cloneKubeResourceRequirements(f.snapshot.Observation.DesiredResources)
	target.Requests["cpu"] = "15m"
	final, err := executeManagedPostgresResizeStages(context.Background(), target, config.ManagedPostgresInPlaceResizeConfig{Enabled: true, CPURequestDownscaleEnabled: true}, 5*time.Second, time.Millisecond, f.hooks())
	if err != nil {
		t.Fatal(err)
	}
	if f.patchCalls != 1 || final.Observation.PodUID != f.initialPodUID || final.Observation.RestartCount != f.initialRestartCount {
		t.Fatalf("unexpected mutation/restart %+v", final.Observation)
	}
	if err := validateManagedPostgresAutomaticCPUTarget(final.Observation.DesiredResources, target); err != nil {
		t.Fatal(err)
	}
}
