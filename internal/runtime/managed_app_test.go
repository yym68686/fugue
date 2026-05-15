package runtime

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestBuildManagedAppStateObjectsEncodesDesiredState(t *testing.T) {
	app := model.App{
		ID:        "app_demo_123",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Source: &model.AppSource{
			Type:             model.AppSourceTypeDockerImage,
			ImageRef:         "ghcr.io/example/demo:latest",
			ComposeService:   "api",
			ComposeDependsOn: []string{"worker"},
		},
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
		Route: &model.AppRoute{
			Hostname:    "demo.example.com",
			PublicURL:   "https://demo.example.com",
			BaseDomain:  "example.com",
			ServicePort: 8080,
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
	source := spec["source"].(map[string]any)
	if got := source["compose_service"]; got != "api" {
		t.Fatalf("unexpected compose service: %#v", got)
	}
	route := spec["route"].(map[string]any)
	if got := route["public_url"]; got != "https://demo.example.com" {
		t.Fatalf("unexpected route public url: %#v", got)
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

func TestBuildManagedAppObjectOmitsInjectedFugueEnvFromSnapshot(t *testing.T) {
	app := model.App{
		ID:        "app_demo_123",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Env: map[string]string{
				"APP_ENV":          "prod",
				"FUGUE_TOKEN":      "injected-token",
				"FUGUE_PROJECT_ID": "project_demo",
				"FUGUE_ONLY":       "user-defined",
			},
		},
	}

	managed, err := ManagedAppObjectFromMap(BuildManagedAppObject(app, SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("decode managed app object: %v", err)
	}

	env := managed.Spec.AppSpec.Env
	if got := env["APP_ENV"]; got != "prod" {
		t.Fatalf("expected APP_ENV to be preserved, got %q", got)
	}
	if got := env["FUGUE_ONLY"]; got != "user-defined" {
		t.Fatalf("expected non-injected FUGUE_ONLY to be preserved, got %q", got)
	}
	for _, key := range []string{"FUGUE_TOKEN", "FUGUE_PROJECT_ID"} {
		if got := env[key]; got != "" {
			t.Fatalf("expected injected env %s to be omitted, got %q", key, got)
		}
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
		if got := ownerRefs[0]["controller"]; got != false {
			t.Fatalf("managed app owner reference should not claim controller ownership, got %#v", got)
		}
	}
}

func TestManagedAppRoundTripPreservesSourceForComposeAliases(t *testing.T) {
	app := model.App{
		ID:        "app_demo_123",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo-mongodb",
		Source: &model.AppSource{
			Type:           model.AppSourceTypeDockerImage,
			ImageRef:       "mongo:7.0",
			ComposeService: "mongodb",
		},
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/mongo:latest",
			Ports:     []int{27017},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
		Route: &model.AppRoute{
			Hostname:    "mongo.example.com",
			PublicURL:   "https://mongo.example.com",
			BaseDomain:  "example.com",
			ServicePort: 27017,
		},
	}

	managedMap := BuildManagedAppObject(app, SchedulingConstraints{})
	managed, err := ManagedAppObjectFromMap(managedMap)
	if err != nil {
		t.Fatalf("decode managed app object: %v", err)
	}

	roundTrip := AppFromManagedApp(managed)
	if roundTrip.Source == nil {
		t.Fatal("expected source to survive managed app round-trip")
	}
	if roundTrip.Source.ComposeService != "mongodb" {
		t.Fatalf("expected compose service mongodb, got %q", roundTrip.Source.ComposeService)
	}
	if roundTrip.Route == nil || roundTrip.Route.PublicURL != "https://mongo.example.com" {
		t.Fatalf("expected route to survive managed app round-trip, got %+v", roundTrip.Route)
	}

	objects := BuildManagedAppChildObjects(roundTrip, SchedulingConstraints{}, nil)
	if len(objects) != 4 {
		t.Fatalf("expected deployment + service + compose alias + legacy app-name alias, got %d", len(objects))
	}

	aliasService := objects[2]
	if got := aliasService["kind"]; got != "Service" {
		t.Fatalf("expected compose alias service, got %#v", got)
	}
	metadata := aliasService["metadata"].(map[string]any)
	if got := metadata["name"]; got != ComposeServiceAliasName(app.ProjectID, "mongodb") {
		t.Fatalf("expected alias name %q, got %#v", ComposeServiceAliasName(app.ProjectID, "mongodb"), got)
	}

	legacyAliasService := objects[3]
	legacyMetadata := legacyAliasService["metadata"].(map[string]any)
	if got := legacyMetadata["name"]; got != "demo-mongodb" {
		t.Fatalf("expected legacy app-name alias %q, got %#v", "demo-mongodb", got)
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

func TestOverlayAppStatusFromManagedAppOverlaysRuntimeTimestamps(t *testing.T) {
	releaseStartedAt := time.Date(2026, time.March, 26, 10, 0, 0, 0, time.UTC)
	releaseReadyAt := releaseStartedAt.Add(2 * time.Minute)
	serviceStartedAt := releaseStartedAt.Add(30 * time.Second)
	serviceReadyAt := releaseReadyAt

	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_demo",
			Replicas:  1,
		},
		Status: model.AppStatus{
			Phase:            "deployed",
			CurrentRuntimeID: "runtime_demo",
			CurrentReplicas:  1,
		},
		BackingServices: []model.BackingService{
			{ID: "service_demo"},
		},
	}

	updated := OverlayAppStatusFromManagedApp(app, ManagedAppObject{
		Status: ManagedAppStatus{
			Phase:                   ManagedAppPhaseReady,
			ReadyReplicas:           1,
			CurrentReleaseStartedAt: releaseStartedAt.Format(time.RFC3339Nano),
			CurrentReleaseReadyAt:   releaseReadyAt.Format(time.RFC3339Nano),
			BackingServices: []ManagedBackingServiceStatus{
				{
					ServiceID:               "service_demo",
					CurrentRuntimeStartedAt: serviceStartedAt.Format(time.RFC3339Nano),
					CurrentRuntimeReadyAt:   serviceReadyAt.Format(time.RFC3339Nano),
				},
			},
		},
	})

	if updated.Status.CurrentReleaseStartedAt == nil || !updated.Status.CurrentReleaseStartedAt.Equal(releaseStartedAt) {
		t.Fatalf("expected release started at %s, got %#v", releaseStartedAt.Format(time.RFC3339Nano), updated.Status.CurrentReleaseStartedAt)
	}
	if updated.Status.CurrentReleaseReadyAt == nil || !updated.Status.CurrentReleaseReadyAt.Equal(releaseReadyAt) {
		t.Fatalf("expected release ready at %s, got %#v", releaseReadyAt.Format(time.RFC3339Nano), updated.Status.CurrentReleaseReadyAt)
	}
	if updated.BackingServices[0].CurrentRuntimeStartedAt == nil || !updated.BackingServices[0].CurrentRuntimeStartedAt.Equal(serviceStartedAt) {
		t.Fatalf("expected service started at %s, got %#v", serviceStartedAt.Format(time.RFC3339Nano), updated.BackingServices[0].CurrentRuntimeStartedAt)
	}
	if updated.BackingServices[0].CurrentRuntimeReadyAt == nil || !updated.BackingServices[0].CurrentRuntimeReadyAt.Equal(serviceReadyAt) {
		t.Fatalf("expected service ready at %s, got %#v", serviceReadyAt.Format(time.RFC3339Nano), updated.BackingServices[0].CurrentRuntimeReadyAt)
	}
}
