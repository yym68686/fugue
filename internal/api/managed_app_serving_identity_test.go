package api

import (
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestManagedAppServingIdentityRequiresHealthyCurrentCohort(t *testing.T) {
	for _, test := range []struct {
		name                          string
		change                        func(*managedAppKubeSnapshot, string)
		fresh, complete, wantIdentity bool
	}{
		{"healthy", func(*managedAppKubeSnapshot, string) {}, true, true, true},
		{"stale cache", func(*managedAppKubeSnapshot, string) {}, false, true, false},
		{"incomplete query", func(*managedAppKubeSnapshot, string) {}, true, false, false},
		{"no namespace", func(s *managedAppKubeSnapshot, _ string) { s.namespaces = nil }, true, true, false},
		{"no service", func(s *managedAppKubeSnapshot, _ string) { s.services = nil }, true, true, false},
		{"no endpoint slice", func(s *managedAppKubeSnapshot, _ string) { s.endpointSlices = nil }, true, true, false},
		{"unready endpoint", func(s *managedAppKubeSnapshot, k string) {
			v := s.endpointSlices[k]
			v.ReadyAddresses = 0
			s.endpointSlices[k] = v
		}, true, true, false},
		{"no deployment", func(s *managedAppKubeSnapshot, _ string) { s.deployments = nil }, true, true, false},
		{"unobserved generation", func(s *managedAppKubeSnapshot, k string) {
			v := s.deployments[k]
			v.Status.ObservedGeneration = 1
			s.deployments[k] = v
		}, true, true, false},
		{"old cohort remains", func(s *managedAppKubeSnapshot, k string) {
			v := s.deployments[k]
			v.Status.Replicas = 2
			s.deployments[k] = v
		}, true, true, false},
		{"no updated replicas", func(s *managedAppKubeSnapshot, k string) {
			v := s.deployments[k]
			v.Status.UpdatedReplicas = 0
			s.deployments[k] = v
		}, true, true, false},
		{"wrong image", func(s *managedAppKubeSnapshot, k string) {
			v := s.deployments[k]
			v.Spec.Template.Spec.Containers[0].Image = "registry.example/worker:v2"
			s.deployments[k] = v
		}, true, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			app := model.App{ID: "app-a", TenantID: "tenant-a", Name: "worker", Spec: model.AppSpec{Image: "registry.example/worker:v1", Replicas: 1, Ports: []int{8080}, RuntimeID: "runtime-a"}}
			managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
			if err != nil {
				t.Fatal(err)
			}
			managed.Metadata.Generation = 2
			managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, DesiredReplicas: 1, ReadyReplicas: 1, ObservedGeneration: 2}
			ns := runtime.NamespaceForTenant(app.TenantID)
			// Both test resources share a neutral name so each snapshot mutation can
			// address the exact deployment/service without using production fixtures.
			name := "release-workload"
			key := kubeNamespacedKey(ns, name)
			var deployment kubeDeploymentRuntimeEvidence
			err = decodeKubeObject(map[string]any{
				"metadata": map[string]any{"name": name, "namespace": ns, "generation": 2},
				"spec":     map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"image": app.Spec.Image}}}}},
				"status":   map[string]any{"replicas": 1, "updatedReplicas": 1, "readyReplicas": 1, "availableReplicas": 1, "observedGeneration": 2},
			}, &deployment)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := managedAppKubeSnapshot{namespaces: map[string]struct{}{ns: {}}, deployments: map[string]kubeDeploymentRuntimeEvidence{key: deployment}, services: map[string]struct{}{key: {}}, endpointSlicesAvailable: true, endpointSlices: map[string]kubeEndpointRuntimeEvidence{key: {Present: true, ReadyAddresses: 1}}}
			release := model.AppRelease{ID: "release-a", AppID: app.ID, TenantID: app.TenantID, Role: model.AppReleaseRoleStable, Status: model.AppReleaseStatusServing, RuntimeID: app.Spec.RuntimeID, ResolvedImageRef: app.Spec.Image, ServiceName: name, DeploymentName: name}
			frozen := &managedAppStoreSnapshot{policies: map[string]model.AppTrafficPolicy{app.ID: {Mode: model.AppTrafficModeSingle, StableWeight: 100, StableReleaseID: release.ID}}, releases: map[string]model.AppRelease{release.ID: release}}
			server := &Server{store: store.New(filepath.Join(t.TempDir(), "unused.json"))}
			test.change(&snapshot, key)
			evidence, err := server.buildManagedAppRuntimeEvidenceWithStoreSnapshot(app, managed, true, snapshot, frozen)
			if err != nil {
				t.Fatal(err)
			}
			observed := server.applyManagedAppObservation(app, managedAppStatusCacheEntry{managed: managed, found: true, ok: test.complete, clusterID: "cluster-a", evidence: evidence, refreshedAt: time.Now().UTC()}, nil, test.fresh, "")
			if observed.ObservedStatus == nil {
				t.Fatal("observation missing")
			}
			got := observed.ObservedStatus.ServingReleaseID
			if (got == release.ID) != test.wantIdentity || (!test.wantIdentity && got != "") {
				t.Fatalf("serving identity=%q, want identity=%t; evidence=%+v status=%+v", got, test.wantIdentity, evidence, observed.ObservedStatus)
			}
		})
	}
}
