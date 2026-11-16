package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	corev1 "k8s.io/api/core/v1"
)

func TestOperationPersistsExactStoragePlacementMemoryDecision(t *testing.T) {
	svc, app, op := recoveryFixture(t)
	f := newStoragePlacementFixture(t)
	svc.newKubeClient = func(string) (*kubeClient, error) { return f.client, nil }
	app.Spec.Replicas = 1
	app.Spec.Resources = &model.ResourceSpec{MemoryMebibytes: 1600, MemoryLimitMebibytes: 3200}
	app.Spec.PersistentStorage = &model.AppPersistentStorageSpec{Mode: model.AppPersistentStorageModeDedicatedPVC, StorageClassName: f.class.Name, StorageSize: "1Gi", Mounts: []model.AppPersistentStorageMount{{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"}}}
	op.DesiredSpec = &app.Spec
	claims, err := appStorageClaimRequests(svc.Renderer.BuildManagedAppChildObjects(app, runtimepkg.SchedulingConstraints{}, nil))
	if err != nil || len(claims) != 1 {
		t.Fatalf("render claims: %v %v", claims, err)
	}
	f.pvc.Name, f.pvc.Namespace = claims[0].Name, runtimepkg.NamespaceForTenant(app.TenantID)
	f.pv.Spec.ClaimRef.Name, f.pv.Spec.ClaimRef.Namespace = f.pvc.Name, f.pvc.Namespace
	f.pv.Spec.CSI = nil
	f.pv.Spec.HostPath = &corev1.HostPathVolumeSource{Path: "/private/storage-path-must-not-be-recorded"}
	f.pv.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: kubeHostnameLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{"storage-ready"}}}}}}}
	f.class.Provisioner = "example.test/local"
	var other kubePod
	other.Metadata.Name = "foreign-workload-must-not-be-recorded"
	other.Spec.NodeName = "storage-ready"
	other.Spec.Containers = []kubeContainerSpec{{Resources: kubeResourceRequirements{Requests: map[string]string{"memory": "32Gi"}}}}
	other.Status.Phase = "Running"
	f.pods = []kubePod{other}
	ctx := context.WithValue(context.Background(), schedulingOperationKey{}, op.ID)
	_, err = svc.managedSchedulingConstraintsForApp(ctx, app)
	if err == nil || !strings.Contains(err.Error(), "memory_request_budget_exceeded") || !strings.Contains(err.Error(), "ratio=0.9") {
		t.Fatalf("expected precise memory rejection, got %v", err)
	}
	if strings.Contains(err.Error(), "storage-ready") || strings.Contains(err.Error(), "compute-only") {
		t.Fatalf("error leaked physical identity: %v", err)
	}
	items, err := svc.Store.ListOperationEvidence(model.OperationEvidenceFilter{PlatformAdmin: true, OperationID: op.ID, Types: []string{model.OperationEvidenceTypeSchedulingDecision}})
	if err != nil || len(items) != 1 {
		t.Fatalf("missing durable decision: %v %v", items, err)
	}
	b, _ := json.Marshal(items[0].Payload)
	var payload struct {
		Decision model.SchedulingDecision `json:"scheduling_decision"`
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		t.Fatal(err)
	}
	d := payload.Decision
	if d.Outcome != "rejected" || d.MemoryRequestRatio != .9 || d.CPUCapacityEnforced || d.Demand.MemoryBytes != 1600*1024*1024 || len(d.Nodes) != 2 || !d.Volumes[0].NodeAffinityRequired {
		t.Fatalf("wrong snapshot: %+v", d)
	}
	n := d.Nodes[1]
	if n.Requested.MemoryBytes != 32*1024*1024*1024 || n.Budget.MemoryBytes != resourceCapacityWithRatio(n.Allocatable.MemoryBytes, .9) || n.Volumes[0].AffinityMatches == nil || !*n.Volumes[0].AffinityMatches {
		t.Fatalf("wrong actual capacity/affinity: %+v", n)
	}
	if strings.Contains(string(b), other.Metadata.Name) || strings.Contains(string(b), "private/storage-path") {
		t.Fatalf("recorded forbidden workload/storage fields: %s", b)
	}
	if !strings.Contains(string(b), "storage-ready") || !strings.Contains(string(b), "node_affinity") {
		t.Fatalf("missing administrator identity/affinity evidence: %s", b)
	}
}

func TestSchedulingNodeRecordsAllGatesWithoutAddingCPUGate(t *testing.T) {
	app := model.App{Spec: model.AppSpec{Replicas: 1, Resources: &model.ResourceSpec{CPUMilliCores: 9000, MemoryMebibytes: 1024}}}
	o := newSchedulingObservation(app, "app_placement")
	o.resourceInventoryObserved = true
	node := storageFixtureNode("private-node")
	node.Spec.Unschedulable = true
	node.Status.Conditions = []kubeNodeCondition{{Type: "Ready", Status: "False"}, {Type: "DiskPressure", Status: "True"}}
	node.Spec.Taints = []kubeTaint{{Key: "private-taint", Effect: "NoSchedule"}}
	n := o.observeNode(node.Metadata.Name, node, true, managedSharedNodeRequests{memoryBytes: 32 << 30, ephemeralBytes: 201 << 30}, managedSharedNodeRequests{}, map[string]string{"other-runtime": "required"}, nil, nil, nil)
	zero := int64(0)
	n.AvailablePodAddresses = &zero
	o.capacity(n)
	got := map[string]bool{}
	for _, r := range n.Rejections {
		got[r.Code] = true
	}
	for _, code := range []string{"runtime_selector_mismatch", "node_not_ready", "node_unschedulable", "node_disk_pressure", "node_taint_not_tolerated", "pod_addresses_insufficient", "memory_request_budget_exceeded", "ephemeral_storage_insufficient"} {
		if !got[code] {
			t.Errorf("missing %s: %+v", code, n.Rejections)
		}
	}
	if len(got) != 8 {
		t.Fatalf("unexpected admission gate: %+v", got)
	}
}

func TestSchedulingEvidenceChunksAndDoesNotExposeUnrelatedRuntimeCapacity(t *testing.T) {
	svc, app, op := recoveryFixture(t)
	o := newSchedulingObservation(app, "app_placement")
	o.resourceInventoryObserved = true
	o.decision.Outcome = "rejected"
	for i := 0; i < 49; i++ {
		node := storageFixtureNode(fmt.Sprintf("private-node-%03d", i))
		o.observeNode(node.Metadata.Name, node, true, managedSharedNodeRequests{memoryBytes: 1234567}, managedSharedNodeRequests{}, map[string]string{"unrelated-runtime": "required"}, nil, nil, nil)
	}
	ctx := context.WithValue(context.Background(), schedulingOperationKey{}, op.ID)
	svc.recordSchedulingObservation(ctx, app, o, nil)
	items, err := svc.Store.ListOperationEvidence(model.OperationEvidenceFilter{PlatformAdmin: true, OperationID: op.ID, Types: []string{model.OperationEvidenceTypeSchedulingDecision}})
	if err != nil || len(items) != 3 {
		t.Fatalf("missing chunks: count=%d err=%v", len(items), err)
	}
	count := 0
	for _, e := range items {
		b, _ := json.Marshal(e.Payload)
		if len(b) >= 128*1024 || e.Payload["payload_truncated"] != nil {
			t.Fatalf("snapshot was truncated: %s", b)
		}
		var p struct {
			Decision model.SchedulingDecision `json:"scheduling_decision"`
		}
		_ = json.Unmarshal(b, &p)
		count += len(p.Decision.Nodes)
		for _, n := range p.Decision.Nodes {
			if n.Requested.MemoryBytes != 0 || n.ResourceInventoryObserved {
				t.Fatalf("unrelated runtime capacity exposed: %+v", n)
			}
		}
		if !strings.Contains(string(b), "1234567") {
			t.Fatal("administrator snapshot lost actual resource values")
		}
	}
	if count != 49 {
		t.Fatalf("lost node decisions: %d", count)
	}
}

func TestSchedulingStopHasNoObservationDependencyAndIdleDoesNotInventOperation(t *testing.T) {
	svc, app, op := recoveryFixture(t)
	f := newStoragePlacementFixture(t)
	svc.newKubeClient = func(string) (*kubeClient, error) {
		t.Fatal("stop tried to initialize Kubernetes observations")
		return nil, nil
	}
	app.Spec.Replicas = 0
	app.Spec.PersistentStorage = &model.AppPersistentStorageSpec{StorageClassName: f.class.Name}
	ctx := context.WithValue(context.Background(), schedulingOperationKey{}, op.ID)
	if _, err := svc.managedSchedulingConstraintsForApp(ctx, app); err != nil {
		t.Fatal(err)
	}
	items, _ := svc.Store.ListOperationEvidence(model.OperationEvidenceFilter{PlatformAdmin: true, OperationID: op.ID, Types: []string{model.OperationEvidenceTypeSchedulingDecision}})
	if len(items) != 1 {
		t.Fatalf("stop evidence count: %d", len(items))
	}
	o := newSchedulingObservation(app, "app_placement")
	svc.recordSchedulingObservation(context.Background(), app, o, nil)
	after, _ := svc.Store.ListOperationEvidence(model.OperationEvidenceFilter{PlatformAdmin: true, OperationID: op.ID, Types: []string{model.OperationEvidenceTypeSchedulingDecision}})
	if len(after) != 1 {
		t.Fatal("idle reconcile invented operation evidence")
	}
}

func TestSchedulingObservationFailurePreservesCauseAndDoesNotClaimRejection(t *testing.T) {
	svc, app, op := recoveryFixture(t)
	app.Spec.Replicas = 1
	app.Spec.PersistentStorage = &model.AppPersistentStorageSpec{Mode: model.AppPersistentStorageModeDedicatedPVC, StorageClassName: "network-storage", StorageSize: "1Gi", Mounts: []model.AppPersistentStorageMount{{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"}}}
	svc.newKubeClient = func(string) (*kubeClient, error) { return nil, context.Canceled }
	ctx := context.WithValue(context.Background(), schedulingOperationKey{}, op.ID)
	_, err := svc.managedSchedulingConstraintsForApp(ctx, app)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "scheduling observation failed") {
		t.Fatalf("observation lost cancellation/error identity: %v", err)
	}
	items, err := svc.Store.ListOperationEvidence(model.OperationEvidenceFilter{PlatformAdmin: true, OperationID: op.ID, Types: []string{model.OperationEvidenceTypeSchedulingDecision}})
	if err != nil || len(items) != 1 || items[0].Confidence != model.OperationEvidenceConfidenceInsufficientEvidence || items[0].Severity != model.OperationEvidenceSeverityWarning {
		t.Fatalf("invented a capacity rejection from observation failure: %+v %v", items, err)
	}
}

func TestSurgeDecisionUsesRenderedDemandWithoutSubtractingServingPod(t *testing.T) {
	svc, app, op := recoveryFixture(t)
	f := newStoragePlacementFixture(t)
	app.Spec.Replicas = 1
	app.Spec.Resources = &model.ResourceSpec{MemoryMebibytes: 512}
	app = svc.Renderer.PrepareApp(app)
	deployment, found := expectedManagedAppDeployment(app, runtimepkg.SchedulingConstraints{NodeSelector: map[string]string{kubeHostnameLabelKey: "storage-ready"}})
	if !found {
		t.Fatal("no rendered deployment")
	}
	var serving kubePod
	serving.Metadata.Name, serving.Spec.NodeName = runtimepkg.RuntimeAppResourceName(app)+"-ready", "storage-ready"
	serving.Spec.Containers = []kubeContainerSpec{{Resources: kubeResourceRequirements{Requests: map[string]string{"memory": "32Gi"}}}}
	serving.Status.Phase = "Running"
	f.pods = []kubePod{serving}
	ctx := context.WithValue(context.Background(), schedulingOperationKey{}, op.ID)
	message, err := svc.observeZeroDowntimeCapacity(ctx, app, f.client, deployment)
	if err != nil || !strings.Contains(message, "memory_request_budget_exceeded") {
		t.Fatalf("missing surge rejection: %q %v", message, err)
	}
	items, _ := svc.Store.ListOperationEvidence(model.OperationEvidenceFilter{PlatformAdmin: true, OperationID: op.ID, Types: []string{model.OperationEvidenceTypeSchedulingDecision}})
	if len(items) != 1 {
		t.Fatalf("missing surge evidence: %+v", items)
	}
	b, _ := json.Marshal(items[0].Payload)
	var p struct {
		Decision model.SchedulingDecision `json:"scheduling_decision"`
	}
	_ = json.Unmarshal(b, &p)
	d := p.Decision
	if d.Stage != "surge_capacity" || d.MemoryRequestRatio != 1 || d.Demand.MemoryBytes != deploymentTemplateRequests(deployment).memoryBytes || d.RequiredPodAddresses != 1 || d.Nodes[1].EffectiveRequested.MemoryBytes != 32<<30 || d.Nodes[1].ExistingApp.MemoryBytes != 0 {
		t.Fatalf("surge policy/demand drifted: %+v", d)
	}
}
