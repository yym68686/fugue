package runtime

import (
	"testing"

	"fugue/internal/model"
)

func TestBuildManagedAppStateObjectsEncodesDesiredState(t *testing.T) {
	app := model.App{
		ID:        "app_demo_123",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  2,
			RuntimeID: "runtime_demo",
			Env: map[string]string{
				"APP_ENV": "prod",
			},
		},
		Bindings: []model.ServiceBinding{
			{
				ID:        "binding_demo",
				AppID:     "app_demo_123",
				ServiceID: "service_demo",
				Env: map[string]string{
					"DB_HOST": "postgres.demo.svc",
				},
			},
		},
	}

	objects := BuildManagedAppStateObjects(app, SchedulingConstraints{
		NodeSelector: map[string]string{
			RuntimeIDLabelKey: "runtime_demo",
		},
	})
	if len(objects) != 2 {
		t.Fatalf("expected namespace + managed app objects, got %d", len(objects))
	}

	managed := objects[1]
	if got := managed["apiVersion"]; got != ManagedAppAPIVersion {
		t.Fatalf("unexpected managed app apiVersion: %#v", got)
	}
	if got := managed["kind"]; got != ManagedAppKind {
		t.Fatalf("unexpected managed app kind: %#v", got)
	}

	metadata := managed["metadata"].(map[string]any)
	if got := metadata["name"]; got != "app-demo-123" {
		t.Fatalf("unexpected managed app resource name: %#v", got)
	}
	if got := metadata["namespace"]; got != "fg-tenant-demo" {
		t.Fatalf("unexpected managed app namespace: %#v", got)
	}

	spec := managed["spec"].(map[string]any)
	if got := spec["appID"]; got != "app_demo_123" {
		t.Fatalf("unexpected app id: %#v", got)
	}
	if got := spec["tenantID"]; got != "tenant_demo" {
		t.Fatalf("unexpected tenant id: %#v", got)
	}
	appSpec := spec["appSpec"].(map[string]any)
	if got := appSpec["image"]; got != "ghcr.io/example/demo:latest" {
		t.Fatalf("unexpected app image: %#v", got)
	}
	scheduling := spec["scheduling"].(map[string]any)
	nodeSelector := scheduling["nodeSelector"].(map[string]any)
	if got := nodeSelector[RuntimeIDLabelKey]; got != "runtime_demo" {
		t.Fatalf("unexpected node selector: %#v", got)
	}
}

func TestBuildManagedAppChildObjectsAddsOwnerReferences(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	managed, err := ManagedAppObjectFromMap(map[string]any{
		"apiVersion": ManagedAppAPIVersion,
		"kind":       ManagedAppKind,
		"metadata": map[string]any{
			"name":      "app-demo",
			"namespace": "fg-tenant-demo",
			"uid":       "uid-demo",
		},
	})
	if err != nil {
		t.Fatalf("decode managed app object: %v", err)
	}

	objects := BuildManagedAppChildObjects(app, SchedulingConstraints{}, ManagedAppOwnerReference(managed))
	if len(objects) != 2 {
		t.Fatalf("expected app deployment + service, got %d", len(objects))
	}

	for _, obj := range objects {
		if got := obj["kind"]; got == "Namespace" {
			t.Fatalf("managed child objects should not include namespace")
		}
		metadata := obj["metadata"].(map[string]any)
		ownerRefs := metadata["ownerReferences"].([]map[string]any)
		if len(ownerRefs) != 1 {
			t.Fatalf("expected one owner reference, got %#v", ownerRefs)
		}
		if got := ownerRefs[0]["uid"]; got != "uid-demo" {
			t.Fatalf("unexpected owner reference uid: %#v", got)
		}
	}
}

func TestOverlayAppStatusFromManagedAppUsesObservedReadyReplicas(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_demo",
		},
		Status: model.AppStatus{
			Phase:            "deployed",
			CurrentRuntimeID: "runtime_demo",
			CurrentReplicas:  2,
		},
	}

	updated := OverlayAppStatusFromManagedApp(app, ManagedAppObject{
		Status: ManagedAppStatus{
			Phase:         ManagedAppPhaseProgressing,
			ReadyReplicas: 1,
			Message:       "rollout in progress",
		},
	})

	if updated.Status.Phase != "deploying" {
		t.Fatalf("expected phase deploying, got %q", updated.Status.Phase)
	}
	if updated.Status.CurrentReplicas != 1 {
		t.Fatalf("expected current replicas 1, got %d", updated.Status.CurrentReplicas)
	}
	if updated.Status.LastMessage != "rollout in progress" {
		t.Fatalf("unexpected last message: %q", updated.Status.LastMessage)
	}
}
