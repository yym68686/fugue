package controller

import (
	"context"
	"errors"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"testing"

	"fugue/internal/appimages"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestSweepManagedAppImageRetentionPrunesStaleHistory(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Retention Sweep Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "gallery", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	const (
		pushBase = "registry.push.example"
		pullBase = "registry.pull.example"
	)

	currentSource := model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-current",
	}
	currentSpec := model.AppSpec{
		Image:            pullBase + "/fugue-apps/example-demo:git-current",
		ImageMirrorLimit: 1,
		Ports:            []int{80},
		Replicas:         1,
		RuntimeID:        "runtime_managed_shared",
	}
	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", currentSpec, currentSource, model.AppRoute{
		Hostname:    "demo.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo.apps.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	oldSource := currentSource
	oldSource.ResolvedImageRef = pushBase + "/fugue-apps/example-demo:git-old"
	oldSpec := currentSpec
	oldSpec.Image = pullBase + "/fugue-apps/example-demo:git-old"
	oldDeployOp, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &oldSpec,
		DesiredSource: &oldSource,
	})
	if err != nil {
		t.Fatalf("create old deploy operation: %v", err)
	}
	if _, err := stateStore.CompleteManagedOperationWithResult(oldDeployOp.ID, "/tmp/old.yaml", "old deployed", &oldSpec, &oldSource); err != nil {
		t.Fatalf("complete old deploy operation: %v", err)
	}
	currentDeployOp, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &currentSpec,
		DesiredSource: &currentSource,
	})
	if err != nil {
		t.Fatalf("create current deploy operation: %v", err)
	}
	if _, err := stateStore.CompleteManagedOperationWithResult(currentDeployOp.ID, "/tmp/current.yaml", "current deployed", &currentSpec, &currentSource); err != nil {
		t.Fatalf("complete current deploy operation: %v", err)
	}

	existingRefs := map[string]bool{
		pushBase + "/fugue-apps/example-demo:git-old":     true,
		pushBase + "/fugue-apps/example-demo:git-current": true,
	}
	deletedRefs := make([]string, 0, 1)
	gcRequests := 0
	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		registryPushBase: pushBase,
		registryPullBase: pullBase,
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			return existingRefs[imageRef], nil, nil
		},
		deleteManagedImage: func(_ context.Context, imageRef string) (appimages.DeleteResult, error) {
			deletedRefs = append(deletedRefs, imageRef)
			return appimages.DeleteResult{ImageRef: imageRef, Deleted: true}, nil
		},
		requestRegistryGC: func(_ context.Context, _ string) error {
			gcRequests++
			return nil
		},
		newKubeClient: func(string) (*kubeClient, error) {
			return nil, errors.New("kube disabled in test")
		},
	}

	if err := svc.sweepManagedAppImageRetention(context.Background()); err != nil {
		t.Fatalf("sweep image retention: %v", err)
	}

	wantDeletedRefs := []string{
		pushBase + "/fugue-apps/example-demo:git-old",
	}
	if !reflect.DeepEqual(deletedRefs, wantDeletedRefs) {
		t.Fatalf("expected deleted refs %v, got %v", wantDeletedRefs, deletedRefs)
	}
	if gcRequests != len(wantDeletedRefs) {
		t.Fatalf("expected %d registry GC requests, got %d", len(wantDeletedRefs), gcRequests)
	}
}

func TestSweepManagedAppImageRetentionStopsAfterContextCancellation(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Retention Cancel Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "gallery", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	const (
		pushBase = "registry.push.example"
		pullBase = "registry.pull.example"
	)

	createImportedAppWithHistory := func(name string) {
		t.Helper()
		currentSource := model.AppSource{
			Type:             model.AppSourceTypeGitHubPublic,
			RepoURL:          "https://github.com/example/" + name,
			ResolvedImageRef: pushBase + "/fugue-apps/" + name + ":git-current",
		}
		currentSpec := model.AppSpec{
			Image:            pullBase + "/fugue-apps/" + name + ":git-current",
			ImageMirrorLimit: 1,
			Ports:            []int{80},
			Replicas:         1,
			RuntimeID:        "runtime_managed_shared",
		}
		app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, name, "", currentSpec, currentSource, model.AppRoute{
			Hostname:    name + ".apps.example.com",
			BaseDomain:  "apps.example.com",
			PublicURL:   "https://" + name + ".apps.example.com",
			ServicePort: 80,
		})
		if err != nil {
			t.Fatalf("create app %s: %v", name, err)
		}
		oldSource := currentSource
		oldSource.ResolvedImageRef = pushBase + "/fugue-apps/" + name + ":git-old"
		oldSpec := currentSpec
		oldSpec.Image = pullBase + "/fugue-apps/" + name + ":git-old"
		oldDeployOp, err := stateStore.CreateOperation(model.Operation{
			TenantID:      tenant.ID,
			Type:          model.OperationTypeDeploy,
			AppID:         app.ID,
			DesiredSpec:   &oldSpec,
			DesiredSource: &oldSource,
		})
		if err != nil {
			t.Fatalf("create old deploy operation for %s: %v", name, err)
		}
		if _, err := stateStore.CompleteManagedOperationWithResult(oldDeployOp.ID, "/tmp/"+name+"-old.yaml", "old deployed", &oldSpec, &oldSource); err != nil {
			t.Fatalf("complete old deploy operation for %s: %v", name, err)
		}
	}

	createImportedAppWithHistory("demo-one")
	createImportedAppWithHistory("demo-two")

	ctx, cancel := context.WithCancel(context.Background())
	inspectCalls := 0
	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		registryPushBase: pushBase,
		registryPullBase: pullBase,
		inspectManagedImage: func(ctx context.Context, _ string) (bool, map[string]int64, error) {
			inspectCalls++
			cancel()
			return false, nil, ctx.Err()
		},
		deleteManagedImage: func(_ context.Context, imageRef string) (appimages.DeleteResult, error) {
			t.Fatalf("delete should not run after cancellation for %s", imageRef)
			return appimages.DeleteResult{}, nil
		},
		newKubeClient: func(string) (*kubeClient, error) {
			return nil, errors.New("kube disabled in test")
		},
	}

	err = svc.sweepManagedAppImageRetention(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if inspectCalls != 1 {
		t.Fatalf("expected sweep to stop after one inspect, got %d", inspectCalls)
	}
}
