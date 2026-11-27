package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

func postgresMemorySamples(now time.Time, mib int64, count int) []model.ResourceUsageSample {
	bytes := mib * 1024 * 1024
	out := make([]model.ResourceUsageSample, count)
	for i := range out {
		out[i] = model.ResourceUsageSample{ObservedAt: now.Add(-time.Duration(i+1) * 5 * time.Minute), MemoryBytes: &bytes}
	}
	return out
}

func TestManagedPostgresAutoMemoryRecommendation(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name                 string
		request, usage, want int64
		age                  time.Duration
		count                int
		wantErr              bool
	}{
		{"gradual downscale", 1024, 200, 768, 72 * time.Hour, 12, false},
		{"no extra 512Mi floor", 512, 100, 384, 72 * time.Hour, 12, false},
		{"upscale preserves limit", 512, 400, 608, 72 * time.Hour, 12, false},
		{"exact five percent", 320, 200, 304, 72 * time.Hour, 12, false},
		{"below five percent", 1000, 640, 0, 72 * time.Hour, 12, false},
		{"insufficient", 1024, 200, 0, 72 * time.Hour, 11, false},
		{"recent restart cannot downscale", 1024, 200, 0, 2 * time.Hour, 12, false},
		{"recent restart can upscale", 512, 400, 608, 2 * time.Hour, 12, false},
		{"unchanged", 768, 512, 0, 72 * time.Hour, 12, false},
		{"cannot raise limit", 1024, 1400, 0, 72 * time.Hour, 12, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := kubeResourceRequirements{Requests: map[string]string{"cpu": "100m", "memory": fmtMemory(tc.request)}, Limits: map[string]string{"cpu": "250m", "memory": "2Gi"}}
			target, err := managedPostgresAutoMemoryRequestTarget(current, postgresMemorySamples(now, tc.usage, tc.count), now.Add(-tc.age), now)
			if (err != nil) != tc.wantErr {
				t.Fatalf("target=%+v err=%v", target, err)
			}
			if tc.want == 0 {
				if target != nil {
					t.Fatalf("unexpected target %+v", target)
				}
				return
			}
			want := model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: tc.want, CPULimitMilliCores: 250, MemoryLimitMebibytes: 2048}
			if target == nil || *target != want {
				t.Fatalf("target=%+v want=%+v", target, want)
			}
		})
	}
}

func fmtMemory(mib int64) string { return fmt.Sprintf("%dMi", mib) }

func TestManagedPostgresAutoMemoryRejectsInvalidMetrics(t *testing.T) {
	now := time.Now().UTC()
	current := kubeResourceRequirements{Requests: map[string]string{"cpu": "100m", "memory": "1024Mi"}, Limits: map[string]string{"memory": "2Gi"}}
	for _, kind := range []string{"stale", "future", "duplicates", "negative", "missing", "previous process", "outside window", "overflow"} {
		t.Run(kind, func(t *testing.T) {
			samples := postgresMemorySamples(now, 100, 12)
			for i := range samples {
				switch kind {
				case "stale":
					samples[i].ObservedAt = samples[i].ObservedAt.Add(-time.Hour)
				case "future":
					samples[i].ObservedAt = now.Add(time.Duration(i+1) * time.Minute)
				case "duplicates":
					samples[i].ObservedAt = now.Add(-time.Minute)
				case "negative":
					v := int64(-1)
					samples[i].MemoryBytes = &v
				case "missing":
					samples[i].MemoryBytes = nil
				case "previous process":
					samples[i].ObservedAt = samples[i].ObservedAt.Add(-100 * time.Hour)
				case "outside window":
					samples[i].ObservedAt = samples[i].ObservedAt.Add(-200 * time.Hour)
				case "overflow":
					v := int64(math.MaxInt64)
					samples[i].MemoryBytes = &v
				}
			}
			target, err := managedPostgresAutoMemoryRequestTarget(current, samples, now.Add(-72*time.Hour), now)
			if target != nil || (kind == "overflow" && err == nil) {
				t.Fatalf("target=%+v err=%v", target, err)
			}
		})
	}
}

func TestManagedPostgresAutoMemoryFencesIdentityAndEnvelope(t *testing.T) {
	f := newManagedPostgresResizeExecutorFixture(t)
	f.snapshot.Cluster.Status.SystemID = "system-id"
	actor := managedPostgresMemoryActor(f.snapshot, 1024)
	target := cloneKubeResourceRequirements(f.snapshot.Observation.DesiredResources)
	target.Requests["memory"] = "768Mi"
	if err := validateManagedPostgresAutomaticMemoryTarget(actor, f.snapshot, target); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*managedPostgresResizeSnapshot){
		func(s *managedPostgresResizeSnapshot) { s.Cluster.Spec.Instances = 2 },
		func(s *managedPostgresResizeSnapshot) { s.Cluster.Metadata.UID = "replacement" },
		func(s *managedPostgresResizeSnapshot) { s.Cluster.Status.SystemID = "replacement" },
		func(s *managedPostgresResizeSnapshot) { s.Observation.PodUID = "replacement" },
		func(s *managedPostgresResizeSnapshot) { s.Observation.RestartCount++ },
		func(s *managedPostgresResizeSnapshot) { s.Observation.ContainerStartedAt = "changed" },
		func(s *managedPostgresResizeSnapshot) { s.Observation.DesiredResources.Requests["memory"] = "960Mi" },
		func(s *managedPostgresResizeSnapshot) { s.Observation.DesiredResources.Requests["cpu"] = "200m" },
		func(s *managedPostgresResizeSnapshot) {
			if s.Observation.DesiredResources.Limits == nil {
				s.Observation.DesiredResources.Limits = map[string]string{}
			}
			s.Observation.DesiredResources.Limits["cpu"] = "300m"
		},
		func(s *managedPostgresResizeSnapshot) { s.Observation.DesiredResources.Limits["memory"] = "2Gi" },
	} {
		changed := cloneManagedPostgresResizeSnapshot(f.snapshot)
		mutate(&changed)
		if err := validateManagedPostgresAutomaticMemoryTarget(actor, changed, target); err == nil {
			t.Fatal("identity or envelope drift accepted")
		}
	}
	for _, bad := range []string{"", managedPostgresAutoMemoryActor + "/hash/not-a-number", managedPostgresAutoMemoryActor + "/hash/0"} {
		if err := validateManagedPostgresAutomaticMemoryTarget(bad, f.snapshot, target); err == nil {
			t.Fatal("malformed actor accepted")
		}
	}
	final, err := executeManagedPostgresResizeStages(context.Background(), target, config.ManagedPostgresInPlaceResizeConfig{Enabled: true, MemoryRequestDownscaleEnabled: true}, time.Second, time.Millisecond, f.hooks())
	if err != nil {
		t.Fatal(err)
	}
	if err := validateManagedPostgresAutomaticMemoryTarget(actor, final, target); err != nil {
		t.Fatal(err)
	}
	if f.patchCalls != 1 || final.Observation.PodUID != f.initialPodUID || final.Observation.RestartCount != f.initialRestartCount || final.Observation.ContainerStartedAt != f.initialStartedAt {
		t.Fatal("memory resize changed Pod/process identity")
	}
}

func TestManagedPostgresAutoMemoryPreservesUnlimitedCPU(t *testing.T) {
	f := newManagedPostgresResizeExecutorFixture(t)
	f.snapshot.Cluster.Status.SystemID = "system-id"
	delete(f.snapshot.Observation.DesiredResources.Limits, "cpu")
	delete(f.snapshot.Observation.ActualResources.Limits, "cpu")
	f.snapshot.Observation.Containers[0].Resources = cloneKubeResourceRequirements(f.snapshot.Observation.DesiredResources)
	actor := managedPostgresMemoryActor(f.snapshot, 1024)
	target := cloneKubeResourceRequirements(f.snapshot.Observation.DesiredResources)
	target.Requests["memory"] = "768Mi"
	if err := validateManagedPostgresAutomaticMemoryTarget(actor, f.snapshot, target); err != nil {
		t.Fatalf("unchanged absent CPU limits must remain unlimited: %v", err)
	}
	final, err := executeManagedPostgresResizeStages(context.Background(), target, config.ManagedPostgresInPlaceResizeConfig{Enabled: true, MemoryRequestDownscaleEnabled: true}, time.Second, time.Millisecond, f.hooks())
	if err != nil {
		t.Fatal(err)
	}
	if _, set := final.Observation.DesiredResources.Limits["cpu"]; set {
		t.Fatal("memory resize synthesized a CPU limit")
	}
	if f.patchCalls != 1 || final.Observation.PodUID != f.initialPodUID || final.Observation.RestartCount != f.initialRestartCount {
		t.Fatal("unexpected mutation/restart")
	}
	changed := cloneKubeResourceRequirements(target)
	changed.Limits["cpu"] = "200m"
	if err := validateManagedPostgresAutomaticMemoryTarget(actor, f.snapshot, changed); err == nil {
		t.Fatal("introducing a CPU limit was accepted")
	}
	delete(changed.Limits, "cpu")
	changed.Requests["cpu"] = "200m"
	if err := validateManagedPostgresAutomaticMemoryTarget(actor, f.snapshot, changed); err == nil {
		t.Fatal("changing CPU request was accepted")
	}
}

func TestManagedPostgresAutoMemoryRecoveryWaitsWithoutRepatch(t *testing.T) {
	f := newManagedPostgresResizeExecutorFixture(t)
	target := cloneKubeResourceRequirements(f.snapshot.Observation.DesiredResources)
	target.Requests["memory"] = "1280Mi"
	f.snapshot.Observation.DesiredResources = cloneKubeResourceRequirements(target)
	f.snapshot.Observation.Containers[0].Resources = cloneKubeResourceRequirements(target)
	f.snapshot.Observation.Generation++
	f.snapshot.Observation.Conditions = []managedPostgresResizeCondition{{Type: "PodResizePending", Status: "True", Reason: "Deferred", Message: "node memory capacity unavailable"}}
	baseline, err := captureManagedPostgresResizeInvariantBaseline(f.snapshot.Observation.Namespace, f.snapshot.Cluster.Metadata.Name, f.snapshot.Cluster, func() managedPostgresResizeObservation {
		o := f.snapshot.Observation
		o.Conditions = nil
		o.ObservedGeneration = o.Generation
		return o
	}())
	if err != nil {
		t.Fatal(err)
	}
	hooks := f.hooks()
	hooks.AcceptedBaseline = &baseline
	final, err := executeManagedPostgresResizeStages(context.Background(), target, config.ManagedPostgresInPlaceResizeConfig{Enabled: true, RecoveryEnabled: true, MemoryRequestUpscaleEnabled: true}, time.Second, time.Millisecond, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if f.patchCalls != 0 || f.waitCalls == 0 || final.Observation.PodUID != f.initialPodUID || final.Observation.RestartCount != f.initialRestartCount {
		t.Fatal("recovery replayed resize or changed process identity")
	}
}

func TestManagedPostgresAutoMemoryRecoveryFencesAcceptedIdentity(t *testing.T) {
	for _, drift := range []string{"cluster generation", "pod generation", "pod uid", "restart", "target", "no accepted evidence"} {
		t.Run(drift, func(t *testing.T) {
			f := newManagedPostgresResizeExecutorFixture(t)
			target := cloneKubeResourceRequirements(f.snapshot.Observation.DesiredResources)
			target.Requests["memory"] = "1280Mi"
			baseline, err := captureManagedPostgresResizeInvariantBaseline(f.snapshot.Observation.Namespace, f.snapshot.Cluster.Metadata.Name, f.snapshot.Cluster, f.snapshot.Observation)
			if err != nil {
				t.Fatal(err)
			}
			f.snapshot.Observation.DesiredResources = cloneKubeResourceRequirements(target)
			f.snapshot.Observation.Conditions = []managedPostgresResizeCondition{{Type: "PodResizePending", Status: "True", Reason: "Deferred"}}
			hooks := f.hooks()
			hooks.AcceptedBaseline = &baseline
			switch drift {
			case "cluster generation":
				f.snapshot.Cluster.Metadata.Generation++
			case "pod generation":
				f.snapshot.Observation.Generation++
			case "pod uid":
				f.snapshot.Observation.PodUID = "replacement"
			case "restart":
				f.snapshot.Observation.RestartCount++
			case "target":
				f.snapshot.Observation.DesiredResources.Requests["memory"] = "1536Mi"
			case "no accepted evidence":
				hooks.AcceptedBaseline = nil
			}
			_, err = executeManagedPostgresResizeStages(context.Background(), target, config.ManagedPostgresInPlaceResizeConfig{Enabled: true, RecoveryEnabled: true, MemoryRequestUpscaleEnabled: true}, time.Second, time.Millisecond, hooks)
			if err == nil || f.patchCalls != 0 {
				t.Fatalf("recovery accepted drift or replayed mutation: patches=%d err=%v", f.patchCalls, err)
			}
		})
	}
}

func TestManagedPostgresResizeAcceptedBaseline(t *testing.T) {
	f := newManagedPostgresResizeExecutorFixture(t)
	o := f.snapshot.Observation
	evidence := model.OperationEvidence{
		Type: model.OperationEvidenceTypePostgresResizeAccepted, PayloadVersion: managedPostgresResizeEvidencePayloadVersion,
		SubjectNamespace: o.Namespace, SubjectUID: o.PodUID, PodName: o.PodName, NodeName: o.NodeName, ContainerName: o.ContainerName,
		Payload: map[string]any{"cluster_name": f.snapshot.Cluster.Metadata.Name, "cluster_uid": f.snapshot.Cluster.Metadata.UID,
			"cluster_generation": f.snapshot.Cluster.Metadata.Generation, "pod_uid": o.PodUID, "pod_generation": o.Generation,
			"restart_count": o.RestartCount, "container_started_at": o.ContainerStartedAt},
	}
	// Round-trip through generic JSON exactly as the evidence store does.
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &evidence); err != nil {
		t.Fatal(err)
	}
	baseline, err := managedPostgresResizeAcceptedBaseline(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateManagedPostgresResizeInvariantDuringResize(baseline, f.snapshot.Cluster, o); err != nil {
		t.Fatal(err)
	}
	delete(evidence.Payload, "pod_generation")
	if _, err := managedPostgresResizeAcceptedBaseline(evidence); err == nil {
		t.Fatal("incomplete evidence accepted")
	}
}

func TestManagedPostgresAutoMemoryWithoutEligibleServicesSkipsStore(t *testing.T) {
	s := &Service{Config: config.ControllerConfig{
		KubectlApply: true,
		ManagedPostgresInPlaceResize: config.ManagedPostgresInPlaceResizeConfig{
			Enabled: true, AutoMemoryRequestResizeEnabled: true, MemoryRequestUpscaleEnabled: true,
		},
	}}
	app := model.App{ID: "app-a", TenantID: "tenant-a", BackingServices: []model.BackingService{
		{Type: model.BackingServiceTypePostgres, Provisioner: model.BackingServiceProvisionerManaged},
		{Type: model.BackingServiceTypePostgres, Provisioner: model.BackingServiceProvisionerManaged, Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{Suspended: true}}},
		{Type: model.BackingServiceTypePostgres, Provisioner: model.BackingServiceProvisionerManaged, OwnerAppID: "app-b", Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{}}},
	}}
	// A nil store makes any unintended history or active operation read fail.
	if changed, err := s.reconcileManagedPostgresAutoMemoryRequest(context.Background(), nil, app); err != nil || changed {
		t.Fatalf("no candidates should be a no-op: changed=%t err=%v", changed, err)
	}
}

func TestManagedPostgresAutoMemoryNodePressure(t *testing.T) {
	var node kubeNode
	if validateManagedPostgresAutoMemoryNode(node, false, true) == nil {
		t.Fatal("missing node accepted")
	}
	node.Status.Conditions = []kubeNodeCondition{{Type: "Ready", Status: "True"}, {Type: "MemoryPressure", Status: "True"}}
	if validateManagedPostgresAutoMemoryNode(node, true, true) == nil || validateManagedPostgresAutoMemoryNode(node, true, false) != nil {
		t.Fatal("pressure guard did not distinguish downscale from upscale")
	}
	node.Status.Conditions[1].Status = "False"
	if err := validateManagedPostgresAutoMemoryNode(node, true, true); err != nil {
		t.Fatal(err)
	}
	node.Status.Conditions = node.Status.Conditions[:1]
	if validateManagedPostgresAutoMemoryNode(node, true, true) == nil {
		t.Fatal("unknown pressure admitted")
	}
}

func TestManagedPostgresAutoMemoryInitFloorNeverMutates(t *testing.T) {
	f := newManagedPostgresResizeExecutorFixture(t)
	f.snapshot.Observation.InitContainers = []kubeResizeContainerSpec{{Name: "bootstrap-controller", Resources: kubeResourceRequirements{Requests: map[string]string{"cpu": "1m", "memory": "1024Mi"}}}}
	target := cloneKubeResourceRequirements(f.snapshot.Observation.DesiredResources)
	target.Requests["memory"] = "768Mi"
	_, err := executeManagedPostgresResizeStages(context.Background(), target, config.ManagedPostgresInPlaceResizeConfig{Enabled: true, MemoryRequestDownscaleEnabled: true}, time.Second, time.Millisecond, f.hooks())
	if err == nil || f.patchCalls != 0 || f.lastEvent().Reason != "resize_policy_blocked" || !strings.Contains(f.lastEvent().Message, "ineffective_request_downscale") {
		t.Fatalf("init floor must remain a visible blocked operation without a mutation: err=%v patches=%d event=%+v", err, f.patchCalls, f.lastEvent())
	}
}

func TestReconcileManagedPostgresAutoMemoryQueuesIndependentlyOfAppMode(t *testing.T) {
	st := newStoreTestDatabase(t, "")
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := st.CreateTenant("memory policy")
	if err != nil {
		t.Fatal(err)
	}
	project, err := st.CreateProject(tenant.ID, "database", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{CPUMilliCores: 2000, MemoryMebibytes: 4096, StorageGibibytes: 20}); err != nil {
		t.Fatal(err)
	}
	app, err := st.CreateApp(tenant.ID, project.ID, "database-app", "", model.AppSpec{Image: "example/app:1", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID, RightSizing: &model.AppRightSizingSpec{Mode: model.AppRightSizingModeDisabled}, Postgres: &model.AppPostgresSpec{Database: "app", User: "app", Password: "test", StorageSize: "1Gi", Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 1024, CPULimitMilliCores: 200, MemoryLimitMebibytes: 1536}}})
	if err != nil {
		t.Fatal(err)
	}
	service := app.BackingServices[0]
	original := model.CloneAppPostgresSpec(service.Spec.Postgres)
	f := newManagedPostgresResizeExecutorFixture(t)
	namespace := runtime.NamespaceForTenant(tenant.ID)
	f.snapshot.Cluster.Metadata.Name = service.Spec.Postgres.ServiceName
	f.snapshot.Cluster.Status.SystemID = "system-id"
	f.snapshot.Observation.Namespace = namespace
	f.snapshot.Observation.ContainerStartedAt = time.Now().Add(-72 * time.Hour).UTC().Format(time.RFC3339)
	f.snapshot.Observation.Labels[cloudNativePGClusterLabel] = service.Spec.Postgres.ServiceName
	f.snapshot.Observation.OwnerReferences[0].Name = service.Spec.Postgres.ServiceName
	samples := postgresMemorySamples(time.Now().UTC(), 200, 12)
	for i := range samples {
		samples[i].TenantID = tenant.ID
		samples[i].ProjectID = project.ID
		samples[i].TargetKind = managedPostgresCPUUsageSampleKind
		samples[i].TargetID = service.ID
	}
	if err := st.RecordResourceUsageSamples(samples, time.Time{}); err != nil {
		t.Fatal(err)
	}
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++
		if r.Method != http.MethodGet {
			t.Errorf("scheduler mutated Kubernetes: %s", r.Method)
			w.WriteHeader(405)
			return
		}
		switch r.URL.Path {
		case "/apis/postgresql.cnpg.io/v1/namespaces/" + namespace + "/clusters/" + service.Spec.Postgres.ServiceName:
			_ = json.NewEncoder(w).Encode(f.snapshot.Cluster)
		case "/api/v1/namespaces/" + namespace + "/pods/" + f.snapshot.Observation.PodName:
			o := f.snapshot.Observation
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": o.PodName, "namespace": o.Namespace, "uid": o.PodUID, "resourceVersion": o.ResourceVersion, "generation": o.Generation, "labels": o.Labels, "ownerReferences": o.OwnerReferences}, "spec": map[string]any{"nodeName": o.NodeName, "containers": []kubeResizeContainerSpec{{Name: "postgres", Resources: o.DesiredResources}}}, "status": map[string]any{"phase": "Running", "observedGeneration": o.ObservedGeneration, "conditions": []map[string]string{{"type": "Ready", "status": "True"}}, "containerStatuses": []map[string]any{{"name": "postgres", "ready": true, "restartCount": o.RestartCount, "resources": o.ActualResources, "state": map[string]any{"running": map[string]string{"startedAt": o.ContainerStartedAt}}}}}})
		default:
			t.Errorf("unexpected read %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := &kubeClient{client: server.Client(), baseURL: server.URL}
	s := &Service{Store: st, Config: config.ControllerConfig{KubectlApply: true, ManagedPostgresInPlaceResize: config.ManagedPostgresInPlaceResizeConfig{Enabled: true, MemoryRequestDownscaleEnabled: true}}}
	if queued, err := s.reconcileManagedPostgresAutoMemoryRequest(context.Background(), client, app); err != nil || queued || reads != 0 {
		t.Fatalf("disabled policy processed database: queued=%v reads=%d err=%v", queued, reads, err)
	}
	s.Config.ManagedPostgresInPlaceResize.AutoMemoryRequestResizeEnabled = true
	if queued, err := s.reconcileManagedPostgresAutoMemoryRequest(context.Background(), client, app); err != nil || !queued {
		t.Fatalf("default database policy did not queue: queued=%v err=%v", queued, err)
	}
	ops, err := st.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil || len(ops) != 1 {
		t.Fatalf("ops=%+v err=%v", ops, err)
	}
	op := ops[0]
	if op.Type != model.OperationTypeDatabaseResize || op.ServiceID != service.ID || op.DesiredSpec.Postgres.RuntimeResources.MemoryMebibytes != 768 || op.RequestedByID != managedPostgresMemoryActor(f.snapshot, 1024) {
		t.Fatalf("unexpected operation %+v", op)
	}
	if queued, err := s.reconcileManagedPostgresAutoMemoryRequest(context.Background(), client, app); err != nil || queued || reads != 2 {
		t.Fatalf("active operation not deduplicated: queued=%v reads=%d err=%v", queued, reads, err)
	}
	stored, err := st.GetBackingService(service.ID)
	if err != nil || !reflect.DeepEqual(stored.Spec.Postgres, original) {
		t.Fatal("queueing changed unverified database resources")
	}
}
