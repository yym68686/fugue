package managedappstatus

import (
	"slices"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestEvidencePrefersEmptyEndpointSlicesOverStaleEndpoints(t *testing.T) {
	app := model.App{ID: "app", TenantID: "tenant", Spec: model.AppSpec{Ports: []int{8080}, Replicas: 1}}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	key := namespacedKey(namespace, runtime.RuntimeAppServiceName(app))
	snapshot := RuntimeSnapshot{
		Namespaces:              map[string]struct{}{namespace: {}},
		Services:                map[string]struct{}{key: {}},
		Endpoints:               map[string]Endpoint{key: {Present: true, ReadyAddresses: 1}},
		EndpointSlicesAvailable: true,
	}
	evidence, err := EvaluateEvidence(app, runtime.ManagedAppObject{}, false, snapshot, EvidenceInputs{})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.EndpointPresent == nil || *evidence.EndpointPresent || evidence.EndpointReady == nil || *evidence.EndpointReady {
		t.Fatalf("legacy endpoint made empty slices appear ready: %+v", evidence)
	}
	snapshot.EndpointSlicesAvailable = false
	evidence, err = EvaluateEvidence(app, runtime.ManagedAppObject{}, false, snapshot, EvidenceInputs{})
	if err != nil || !boolPointerTrue(evidence.EndpointReady) {
		t.Fatalf("legacy endpoint was not used when EndpointSlice was unavailable: %+v, %v", evidence, err)
	}
}

func TestEvidenceRejectsForeignManagedAppIdentity(t *testing.T) {
	app := model.App{ID: "app", TenantID: "tenant"}
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{Name: runtime.ManagedAppResourceName(app), Namespace: runtime.NamespaceForTenant(app.TenantID)},
		Spec:     runtime.ManagedAppSpec{AppID: "foreign-app", TenantID: "foreign-tenant"},
	}
	evidence, err := EvaluateEvidence(app, managed, true, RuntimeSnapshot{}, EvidenceInputs{})
	if err != nil {
		t.Fatal(err)
	}
	for _, violation := range []string{"managed_app_identity_mismatch", "managed_app_tenant_mismatch"} {
		if !slices.Contains(evidence.InvariantViolations, violation) {
			t.Errorf("missing %s in %v", violation, evidence.InvariantViolations)
		}
	}
}
