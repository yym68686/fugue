package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

type recordingImporter struct {
	dockerImageOutput   *sourceimport.GitHubSourceImportOutput
	dockerImageReq      *sourceimport.DockerImageSourceImportRequest
	githubReq           *sourceimport.GitHubSourceImportRequest
	githubComposeEnvReq *sourceimport.GitHubComposeServiceEnvRequest
	githubComposeEnv    map[string]string
	githubOutput        *sourceimport.GitHubSourceImportOutput
	uploadReq           *sourceimport.UploadSourceImportRequest
	uploadOutput        *sourceimport.GitHubSourceImportOutput
	uploadErr           error
}

func (r *recordingImporter) ImportDockerImageSource(_ context.Context, req sourceimport.DockerImageSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
	reqCopy := req
	r.dockerImageReq = &reqCopy
	if r.dockerImageOutput != nil {
		output := *r.dockerImageOutput
		return output, nil
	}
	return sourceimport.GitHubSourceImportOutput{
		ImportResult: sourceimport.GitHubImportResult{
			DetectedProvider:     model.AppSourceTypeDockerImage,
			ImageRef:             "registry.push.example/fugue-apps/demo:image-abc123",
			DetectedPort:         9090,
			ExposesPublicService: true,
		},
		Source: model.AppSource{
			Type:             model.AppSourceTypeDockerImage,
			ImageRef:         req.ImageRef,
			ResolvedImageRef: "registry.push.example/fugue-apps/demo:image-abc123",
			DetectedProvider: model.AppSourceTypeDockerImage,
		},
	}, nil
}

func (r *recordingImporter) ImportGitHubSource(_ context.Context, req sourceimport.GitHubSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
	req.JobLabels = cloneStringMap(req.JobLabels)
	req.PlacementNodeSelector = cloneStringMap(req.PlacementNodeSelector)
	r.githubReq = &req
	if r.githubOutput != nil {
		output := *r.githubOutput
		return output, nil
	}
	return sourceimport.GitHubSourceImportOutput{
		ImportResult: sourceimport.GitHubImportResult{
			BuildStrategy:        model.AppBuildStrategyDockerfile,
			ImageRef:             "registry.push.example/fugue-apps/demo:git-abc123",
			DetectedPort:         8080,
			ExposesPublicService: true,
		},
		Source: model.AppSource{
			Type:          model.AppSourceTypeGitHubPublic,
			RepoURL:       req.RepoURL,
			RepoBranch:    "main",
			BuildStrategy: model.AppBuildStrategyDockerfile,
		},
	}, nil
}

func (r *recordingImporter) ImportUploadedArchiveSource(_ context.Context, req sourceimport.UploadSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
	req.JobLabels = cloneStringMap(req.JobLabels)
	req.PlacementNodeSelector = cloneStringMap(req.PlacementNodeSelector)
	reqCopy := req
	r.uploadReq = &reqCopy
	if r.uploadErr != nil {
		return sourceimport.GitHubSourceImportOutput{}, r.uploadErr
	}
	if r.uploadOutput != nil {
		output := *r.uploadOutput
		return output, nil
	}
	return sourceimport.GitHubSourceImportOutput{}, fmt.Errorf("unexpected upload import")
}

func (r *recordingImporter) SuggestGitHubComposeServiceEnv(_ context.Context, req sourceimport.GitHubComposeServiceEnvRequest) (map[string]string, error) {
	req.AppHosts = cloneStringMap(req.AppHosts)
	req.ManagedPostgresByOwner = clonePostgresSpecMap(req.ManagedPostgresByOwner)
	r.githubComposeEnvReq = &req
	return cloneStringMap(r.githubComposeEnv), nil
}

func (r *recordingImporter) SuggestUploadedComposeServiceEnv(context.Context, sourceimport.UploadComposeServiceEnvRequest) (map[string]string, error) {
	return nil, fmt.Errorf("unexpected upload compose env refresh")
}

func inspectManagedImageAlwaysExists(context.Context, string) (bool, map[string]int64, error) {
	return true, nil, nil
}

func TestStrictDistributedResolveImportedManagedImageRefUsesBuilderEvidenceWithoutRegistry(t *testing.T) {
	t.Parallel()

	imageRef := "registry.fugue.internal:5000/fugue-apps/app-1:git-abc"
	svc := &Service{
		Config:           config.ControllerConfig{ImageStoreMode: "distributed"},
		registryPushBase: "registry.fugue.internal:5000",
		registryPullBase: "registry.fugue.internal:5000",
		inspectManagedImage: func(context.Context, string) (bool, map[string]int64, error) {
			t.Fatal("strict distributed import must not inspect the central registry")
			return false, nil, nil
		},
		resolveManagedImageDigestRef: func(context.Context, string) (string, error) {
			t.Fatal("strict distributed import must not resolve digests through the central registry")
			return "", nil
		},
	}

	managedRef, runtimeRef, err := svc.resolveImportedManagedImageRef(
		context.Background(),
		model.App{ID: "app_1", TenantID: "tenant_1"},
		model.AppSource{Type: model.AppSourceTypeGitHubPublic, RepoURL: "https://example.com/app.git"},
		model.AppSource{Type: model.AppSourceTypeGitHubPublic, RepoURL: "https://example.com/app.git"},
		imageRef,
		true,
	)
	if err != nil {
		t.Fatalf("resolve imported managed image ref: %v", err)
	}
	if managedRef != imageRef || runtimeRef != imageRef {
		t.Fatalf("expected builder evidence image ref, got managed=%q runtime=%q", managedRef, runtimeRef)
	}
}

func resolveManagedImageDigestRefStub(digests map[string]string) func(context.Context, string) (string, error) {
	return func(_ context.Context, imageRef string) (string, error) {
		digest, ok := digests[strings.TrimSpace(imageRef)]
		if !ok {
			return "", fmt.Errorf("unexpected image ref %q", imageRef)
		}
		return sourceimport.DigestReferenceFromImageRef(imageRef, digest)
	}
}

func mustRewriteImportedDigestRef(t *testing.T, imageRef, pushBase, pullBase, digest string) string {
	t.Helper()
	digestRef, err := sourceimport.DigestReferenceFromImageRef(imageRef, digest)
	if err != nil {
		t.Fatalf("digest reference from image ref: %v", err)
	}
	runtimeImageRef, err := rewriteImportedImageRef(digestRef, pushBase, pullBase)
	if err != nil {
		t.Fatalf("rewrite imported image ref: %v", err)
	}
	return runtimeImageRef
}

func TestExecuteManagedImportOperationImportsDockerImageSource(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: "nginx:1.27",
	}, model.AppRoute{
		Hostname:   "demo.example.com",
		BaseDomain: "example.com",
		PublicURL:  "https://demo.example.com",
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}
	const managedImageDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	expectedRuntimeImageRef := mustRewriteImportedDigestRef(
		t,
		"registry.push.example/fugue-apps/demo:image-abc123",
		"registry.push.example",
		"registry.pull.example",
		managedImageDigest,
	)

	importer := &recordingImporter{}
	svc := &Service{
		Store:                        stateStore,
		Logger:                       log.New(io.Discard, "", 0),
		importer:                     importer,
		registryPushBase:             "registry.push.example",
		registryPullBase:             "registry.pull.example",
		inspectManagedImage:          inspectManagedImageAlwaysExists,
		resolveManagedImageDigestRef: resolveManagedImageDigestRefStub(map[string]string{"registry.push.example/fugue-apps/demo:image-abc123": managedImageDigest}),
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.dockerImageReq == nil {
		t.Fatal("expected importer to receive docker image request")
	}
	if importer.dockerImageReq.ImageRef != "nginx:1.27" {
		t.Fatalf("expected image ref nginx:1.27, got %q", importer.dockerImageReq.ImageRef)
	}

	ops, err := stateStore.ListOperations(tenant.ID, false)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil {
		t.Fatalf("expected deploy operation with desired spec, got %+v", deployOp)
	}
	if got := deployOp.DesiredSpec.Image; got != expectedRuntimeImageRef {
		t.Fatalf("expected runtime image rewrite, got %q", got)
	}
	if !reflect.DeepEqual(deployOp.DesiredSpec.Ports, []int{9090}) {
		t.Fatalf("expected detected port 9090, got %v", deployOp.DesiredSpec.Ports)
	}
	if !strings.HasPrefix(deployOp.DesiredSpec.RestartToken, "restart_") {
		t.Fatalf("expected deploy restart token to be refreshed, got %q", deployOp.DesiredSpec.RestartToken)
	}
	if deployOp.DesiredSource == nil {
		t.Fatal("expected desired source on deploy operation")
	}
	if deployOp.DesiredSource.Type != model.AppSourceTypeDockerImage {
		t.Fatalf("expected deploy source type %q, got %q", model.AppSourceTypeDockerImage, deployOp.DesiredSource.Type)
	}
	if deployOp.DesiredSource.ResolvedImageRef != "registry.push.example/fugue-apps/demo:image-abc123" {
		t.Fatalf("expected resolved image ref to be persisted, got %q", deployOp.DesiredSource.ResolvedImageRef)
	}

	completedImport, err := stateStore.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("get completed import operation: %v", err)
	}
	if completedImport.DesiredSpec == nil {
		t.Fatal("expected completed import operation to persist desired spec")
	}
	if completedImport.DesiredSpec.RestartToken != deployOp.DesiredSpec.RestartToken {
		t.Fatalf("expected completed import restart token %q, got %q", deployOp.DesiredSpec.RestartToken, completedImport.DesiredSpec.RestartToken)
	}
}

func TestExecuteManagedImportOperationImportsDockerImageSourceThroughRuntimeCacheWhenCentralRegistryUnavailable(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	updater, _, err := stateStore.EnrollNodeUpdater(
		nodeSecret,
		"worker-1",
		"https://203.0.113.10:9443",
		nil,
		"worker-1",
		"machine-1",
		"v2",
		"join-v2",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypePrepullAppImages},
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: updater.RuntimeID,
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: "nginx:1.27",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	const managedImageRef = "registry.push.example/fugue-apps/demo:image-abc123"
	const destinationImageRef = "203.0.113.10:5000/fugue-apps/demo:image-abc123"
	expectedRuntimeImageRef, err := rewriteImportedImageRef(managedImageRef, "registry.push.example", "registry.pull.example")
	if err != nil {
		t.Fatalf("rewrite imported image ref: %v", err)
	}

	importer := &recordingImporter{
		dockerImageOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				DetectedProvider:     model.AppSourceTypeDockerImage,
				ImageRef:             managedImageRef,
				DestinationImageRef:  destinationImageRef,
				DetectedPort:         9090,
				ExposesPublicService: true,
			},
			Source: model.AppSource{
				Type:             model.AppSourceTypeDockerImage,
				ImageRef:         "nginx:1.27",
				ResolvedImageRef: managedImageRef,
				DetectedProvider: model.AppSourceTypeDockerImage,
			},
		},
	}
	svc := &Service{
		Store:                   stateStore,
		Logger:                  log.New(io.Discard, "", 0),
		importer:                importer,
		registryPushBase:        "registry.push.example",
		registryPullBase:        "registry.pull.example",
		builderRegistryPushBase: "127.0.0.1:5000",
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			if imageRef != managedImageRef {
				t.Fatalf("unexpected managed image inspect ref %q", imageRef)
			}
			return false, nil, errors.New("central registry unavailable")
		},
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.dockerImageReq == nil {
		t.Fatal("expected importer to receive docker image request")
	}
	if got, want := importer.dockerImageReq.DestinationRegistryPushBase, "203.0.113.10:5000"; got != want {
		t.Fatalf("expected docker import to target runtime cache %q, got %q", want, got)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil || deployOp.DesiredSource == nil {
		t.Fatalf("expected deploy operation with desired spec/source, got %+v", deployOp)
	}
	if got := deployOp.DesiredSpec.Image; got != expectedRuntimeImageRef {
		t.Fatalf("expected runtime image ref %q, got %q", expectedRuntimeImageRef, got)
	}
	if got := deployOp.DesiredSource.ResolvedImageRef; got != managedImageRef {
		t.Fatalf("expected logical resolved image ref %q, got %q", managedImageRef, got)
	}

	locations, err := stateStore.ListImageLocations(model.ImageLocationFilter{
		TenantID:  tenant.ID,
		AppID:     app.ID,
		RuntimeID: updater.RuntimeID,
		Status:    model.ImageLocationStatusPresent,
	})
	if err != nil {
		t.Fatalf("list image locations: %v", err)
	}
	wantRefs := map[string]bool{
		managedImageRef:         false,
		expectedRuntimeImageRef: false,
	}
	for _, location := range locations {
		seen, ok := wantRefs[location.ImageRef]
		if !ok {
			t.Fatalf("unexpected target image location ref %q in %+v", location.ImageRef, locations)
		}
		if seen {
			t.Fatalf("duplicate target image location ref %q in %+v", location.ImageRef, locations)
		}
		wantRefs[location.ImageRef] = true
		if location.SourceOperationID != op.ID {
			t.Fatalf("expected source operation %q, got %q", op.ID, location.SourceOperationID)
		}
		if location.ClusterNodeName != updater.ClusterNodeName {
			t.Fatalf("expected cluster node %q, got %q", updater.ClusterNodeName, location.ClusterNodeName)
		}
		if location.CacheEndpoint != "http://203.0.113.10:5000" {
			t.Fatalf("expected cache endpoint http://203.0.113.10:5000, got %q", location.CacheEndpoint)
		}
	}
	for ref, seen := range wantRefs {
		if !seen {
			t.Fatalf("expected target image location for %q, got %+v", ref, locations)
		}
	}
}

func TestExecuteManagedImportOperationReusesManagedImageLocationForDockerImageSource(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	updater, _, err := stateStore.EnrollNodeUpdater(
		nodeSecret,
		"worker-1",
		"https://203.0.113.10:9443",
		nil,
		"worker-1",
		"machine-1",
		"v2",
		"join-v2",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypePrepullAppImages},
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}

	const managedImageRef = "registry.push.example/fugue-apps/runtime:upload-abc123"
	const runtimeImageRef = "registry.pull.example/fugue-apps/runtime:upload-abc123"
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        tenant.ID,
		AppID:           "app_template",
		ImageRef:        runtimeImageRef,
		RuntimeID:       updater.RuntimeID,
		ClusterNodeName: updater.ClusterNodeName,
		CacheEndpoint:   "http://203.0.113.10:5000",
		Status:          model.ImageLocationStatusPresent,
	}); err != nil {
		t.Fatalf("record image location: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: updater.RuntimeID,
		Ports:     []int{7777},
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: runtimeImageRef,
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{}
	svc := &Service{
		Store:                   stateStore,
		Logger:                  log.New(io.Discard, "", 0),
		importer:                importer,
		registryPushBase:        "registry.push.example",
		registryPullBase:        "registry.pull.example",
		builderRegistryPushBase: "127.0.0.1:5000",
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			if imageRef != managedImageRef {
				t.Fatalf("unexpected managed image inspect ref %q", imageRef)
			}
			return false, nil, errors.New("central registry manifest missing")
		},
		resolveManagedImageDigestRef: func(_ context.Context, imageRef string) (string, error) {
			if imageRef != managedImageRef {
				t.Fatalf("unexpected digest ref %q", imageRef)
			}
			return "", errors.New("central registry manifest missing")
		},
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.dockerImageReq != nil {
		t.Fatalf("expected managed image location reuse to skip docker importer, got request %+v", importer.dockerImageReq)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil || deployOp.DesiredSource == nil {
		t.Fatalf("expected deploy operation with desired spec/source, got %+v", deployOp)
	}
	if got := deployOp.DesiredSpec.Image; got != runtimeImageRef {
		t.Fatalf("expected runtime image ref %q, got %q", runtimeImageRef, got)
	}
	if got := deployOp.DesiredSource.ResolvedImageRef; got != managedImageRef {
		t.Fatalf("expected managed resolved image ref %q, got %q", managedImageRef, got)
	}
	if got := deployOp.DesiredSource.ImageRef; got != runtimeImageRef {
		t.Fatalf("expected source image ref %q, got %q", runtimeImageRef, got)
	}
}

func TestExecuteManagedImportOperationReusesDistributedImageLocationWithoutBuilderRegistryFlag(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	updater, _, err := stateStore.EnrollNodeUpdater(
		nodeSecret,
		"worker-1",
		"https://203.0.113.10:9443",
		nil,
		"worker-1",
		"machine-1",
		"v2",
		"join-v2",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypePrepullAppImages},
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}

	const imageRef = "registry.fugue.internal:5000/fugue-apps/runtime:git-abc123"
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        tenant.ID,
		AppID:           "app_template",
		ImageRef:        imageRef,
		RuntimeID:       updater.RuntimeID,
		ClusterNodeName: updater.ClusterNodeName,
		CacheEndpoint:   "http://203.0.113.10:5000",
		Status:          model.ImageLocationStatusPresent,
	}); err != nil {
		t.Fatalf("record image location: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: updater.RuntimeID,
		Ports:     []int{7777},
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: imageRef,
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{}
	svc := &Service{
		Config:           config.ControllerConfig{ImageStoreMode: "distributed"},
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		importer:         importer,
		registryPushBase: "registry.fugue.internal:5000",
		registryPullBase: "registry.fugue.internal:5000",
		inspectManagedImage: func(context.Context, string) (bool, map[string]int64, error) {
			t.Fatal("strict distributed reuse must not inspect the central registry")
			return false, nil, nil
		},
		resolveManagedImageDigestRef: func(context.Context, string) (string, error) {
			t.Fatal("strict distributed reuse must not resolve digests through the central registry")
			return "", nil
		},
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.dockerImageReq != nil {
		t.Fatalf("expected distributed image location reuse to skip docker importer, got request %+v", importer.dockerImageReq)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil || deployOp.DesiredSource == nil {
		t.Fatalf("expected deploy operation with desired spec/source, got %+v", deployOp)
	}
	if got := deployOp.DesiredSpec.Image; got != imageRef {
		t.Fatalf("expected runtime image ref %q, got %q", imageRef, got)
	}
	if got := deployOp.DesiredSource.ResolvedImageRef; got != imageRef {
		t.Fatalf("expected managed resolved image ref %q, got %q", imageRef, got)
	}
}

func TestExecuteManagedImportOperationReusesManagedImageLocationWithoutResolvedTarget(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	const managedImageRef = "registry.push.example/fugue-apps/runtime:git-abc123"
	const runtimeImageRef = "registry.pull.example/fugue-apps/runtime:git-abc123"
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        tenant.ID,
		AppID:           "app_runtime_template",
		ImageRef:        runtimeImageRef,
		RuntimeID:       "runtime_us_east",
		ClusterNodeName: "worker-1",
		CacheEndpoint:   "http://203.0.113.40:5000",
		Status:          model.ImageLocationStatusPresent,
	}); err != nil {
		t.Fatalf("record image location: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Ports:     []int{7777},
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: runtimeImageRef,
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{}
	svc := &Service{
		Store:                         stateStore,
		Logger:                        log.New(io.Discard, "", 0),
		importer:                      importer,
		registryPushBase:              "registry.push.example",
		registryPullBase:              "registry.pull.example",
		builderRegistryPushBase:       "127.0.0.1:5000",
		importImageInspectMaxAttempts: 1,
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			if imageRef != managedImageRef {
				t.Fatalf("unexpected managed image inspect ref %q", imageRef)
			}
			return false, nil, nil
		},
		resolveManagedImageDigestRef: func(_ context.Context, imageRef string) (string, error) {
			if imageRef != managedImageRef {
				t.Fatalf("unexpected digest ref %q", imageRef)
			}
			return "", errors.New("central registry manifest missing")
		},
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.dockerImageReq != nil {
		t.Fatalf("expected managed image location reuse to skip docker importer, got request %+v", importer.dockerImageReq)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil || deployOp.DesiredSource == nil {
		t.Fatalf("expected deploy operation with desired spec/source, got %+v", deployOp)
	}
	if got := deployOp.DesiredSpec.Image; got != runtimeImageRef {
		t.Fatalf("expected runtime image ref %q, got %q", runtimeImageRef, got)
	}
	if got := deployOp.DesiredSource.ResolvedImageRef; got != managedImageRef {
		t.Fatalf("expected managed resolved image ref %q, got %q", managedImageRef, got)
	}
	if got := deployOp.DesiredSource.ImageRef; got != runtimeImageRef {
		t.Fatalf("expected source image ref %q, got %q", runtimeImageRef, got)
	}
}

func TestExecuteManagedImportOperationImportsGitHubSourceThroughRuntimeCache(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	updater, _, err := stateStore.EnrollNodeUpdater(
		nodeSecret,
		"worker-1",
		"https://203.0.113.30:9443",
		nil,
		"worker-1",
		"machine-1",
		"v2",
		"join-v2",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypePrepullAppImages},
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: updater.RuntimeID,
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyDockerfile,
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	const managedImageRef = "registry.push.example/fugue-apps/demo:git-abc123"
	importer := &recordingImporter{
		githubOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				BuildStrategy:        model.AppBuildStrategyDockerfile,
				ImageRef:             managedImageRef,
				DestinationImageRef:  "203.0.113.30:5000/fugue-apps/demo:git-abc123",
				BuildJobName:         "fugue-build-demo",
				DetectedPort:         8080,
				ExposesPublicService: true,
			},
			Source: model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				RepoURL:          "https://github.com/example/demo",
				RepoBranch:       "main",
				BuildStrategy:    model.AppBuildStrategyDockerfile,
				ResolvedImageRef: managedImageRef,
			},
		},
	}
	svc := &Service{
		Store:                   stateStore,
		Logger:                  log.New(io.Discard, "", 0),
		importer:                importer,
		registryPushBase:        "registry.push.example",
		registryPullBase:        "registry.pull.example",
		builderRegistryPushBase: "127.0.0.1:5000",
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			if imageRef != managedImageRef {
				t.Fatalf("unexpected managed image inspect ref %q", imageRef)
			}
			return false, nil, errors.New("central registry unavailable")
		},
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.githubReq == nil {
		t.Fatal("expected importer to receive github request")
	}
	if got, want := importer.githubReq.DestinationRegistryPushBase, "203.0.113.30:5000"; got != want {
		t.Fatalf("expected github import to target runtime cache %q, got %q", want, got)
	}

	locations, err := stateStore.ListImageLocations(model.ImageLocationFilter{
		TenantID:  tenant.ID,
		AppID:     app.ID,
		RuntimeID: updater.RuntimeID,
		Status:    model.ImageLocationStatusPresent,
	})
	if err != nil {
		t.Fatalf("list image locations: %v", err)
	}
	if len(locations) == 0 {
		t.Fatal("expected target image location records")
	}
	for _, location := range locations {
		if location.CacheEndpoint != "http://203.0.113.30:5000" {
			t.Fatalf("expected cache endpoint http://203.0.113.30:5000, got %q", location.CacheEndpoint)
		}
	}
}

func TestExecuteManagedImportOperationImportsUploadSourceThroughRuntimeCache(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	updater, _, err := stateStore.EnrollNodeUpdater(
		nodeSecret,
		"worker-1",
		"https://203.0.113.40:9443",
		nil,
		"worker-1",
		"machine-1",
		"v2",
		"join-v2",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypePrepullAppImages},
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}
	upload, err := stateStore.CreateSourceUpload(tenant.ID, "demo.tgz", "application/gzip", []byte("archive-bytes"))
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: updater.RuntimeID,
	}, model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         upload.ID,
		UploadFilename:   upload.Filename,
		ArchiveSHA256:    upload.SHA256,
		ArchiveSizeBytes: upload.SizeBytes,
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DockerfilePath:   "Dockerfile",
		BuildContextDir:  ".",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	managedImageRef := "registry.push.example/fugue-apps/demo:upload-" + upload.SHA256[:12]
	importer := &recordingImporter{
		uploadOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				BuildStrategy:        model.AppBuildStrategyDockerfile,
				ImageRef:             managedImageRef,
				DestinationImageRef:  "203.0.113.40:5000/fugue-apps/demo:upload-" + upload.SHA256[:12],
				BuildJobName:         "fugue-build-demo",
				DetectedPort:         8080,
				ExposesPublicService: true,
			},
			Source: model.AppSource{
				Type:             model.AppSourceTypeUpload,
				UploadID:         upload.ID,
				UploadFilename:   upload.Filename,
				ArchiveSHA256:    upload.SHA256,
				ArchiveSizeBytes: upload.SizeBytes,
				BuildStrategy:    model.AppBuildStrategyDockerfile,
				ResolvedImageRef: managedImageRef,
			},
		},
	}
	svc := &Service{
		Store:                   stateStore,
		Config:                  config.ControllerConfig{SourceUploadBaseURL: "http://source.example"},
		Logger:                  log.New(io.Discard, "", 0),
		importer:                importer,
		registryPushBase:        "registry.push.example",
		registryPullBase:        "registry.pull.example",
		builderRegistryPushBase: "127.0.0.1:5000",
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			if imageRef != managedImageRef {
				t.Fatalf("unexpected managed image inspect ref %q", imageRef)
			}
			return false, nil, errors.New("central registry unavailable")
		},
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.uploadReq == nil {
		t.Fatal("expected importer to receive upload request")
	}
	if got, want := importer.uploadReq.DestinationRegistryPushBase, "203.0.113.40:5000"; got != want {
		t.Fatalf("expected upload import to target runtime cache %q, got %q", want, got)
	}
}

func TestExecuteManagedImportOperationResolvesManagedSharedPersistentImageImportToSelectedNodeCache(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	updater, _, err := stateStore.EnrollNodeUpdater(
		nodeSecret,
		"worker-1",
		"https://203.0.113.20:9443",
		nil,
		"worker-1",
		"machine-1",
		"v2",
		"join-v2",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypePrepullAppImages},
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}
	if _, err := stateStore.SetRuntimePoolMode(updater.RuntimeID, model.RuntimePoolModeInternalShared); err != nil {
		t.Fatalf("set runtime pool mode: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
		PersistentStorage: &model.AppPersistentStorageSpec{
			Mode:             model.AppPersistentStorageModeMovableRWO,
			StorageSize:      "1Gi",
			StorageClassName: "fugue-workspace-rwo",
			Mounts: []model.AppPersistentStorageMount{
				{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/workspace"},
			},
		},
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: "nginx:1.27",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	nodeName := updater.ClusterNodeName
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/pods":
			fmt.Fprint(w, `{"items":[]}`)
		case "/api/v1/nodes":
			fmt.Fprintf(w, `{"items":[{"metadata":{"name":%q,"labels":{"%s":%q}}}]}`, nodeName, runtime.RuntimeIDLabelKey, updater.RuntimeID)
		case "/api/v1/nodes/" + nodeName:
			encodeSharedPlacementNode(t, w, nodeName, "us", "4", "8Gi", "80Gi")
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	const managedImageRef = "registry.push.example/fugue-apps/demo:image-abc123"
	const destinationImageRef = "203.0.113.20:5000/fugue-apps/demo:image-abc123"
	expectedRuntimeImageRef, err := rewriteImportedImageRef(managedImageRef, "registry.push.example", "registry.pull.example")
	if err != nil {
		t.Fatalf("rewrite imported image ref: %v", err)
	}

	importer := &recordingImporter{
		dockerImageOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				DetectedProvider:     model.AppSourceTypeDockerImage,
				ImageRef:             managedImageRef,
				DestinationImageRef:  destinationImageRef,
				DetectedPort:         9090,
				ExposesPublicService: true,
			},
			Source: model.AppSource{
				Type:             model.AppSourceTypeDockerImage,
				ImageRef:         "nginx:1.27",
				ResolvedImageRef: managedImageRef,
				DetectedProvider: model.AppSourceTypeDockerImage,
			},
		},
	}
	svc := &Service{
		Store:                   stateStore,
		Logger:                  log.New(io.Discard, "", 0),
		importer:                importer,
		registryPushBase:        "registry.push.example",
		registryPullBase:        "registry.pull.example",
		builderRegistryPushBase: "127.0.0.1:5000",
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      kubeServer.Client(),
				baseURL:     kubeServer.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
		},
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			if imageRef != managedImageRef {
				t.Fatalf("unexpected managed image inspect ref %q", imageRef)
			}
			return false, nil, errors.New("central registry unavailable")
		},
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.dockerImageReq == nil {
		t.Fatal("expected importer to receive docker image request")
	}
	if got, want := importer.dockerImageReq.DestinationRegistryPushBase, "203.0.113.20:5000"; got != want {
		t.Fatalf("expected docker import to target selected shared node cache %q, got %q", want, got)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil || deployOp.DesiredSource == nil {
		t.Fatalf("expected deploy operation with desired spec/source, got %+v", deployOp)
	}
	if got := deployOp.DesiredSpec.Image; got != expectedRuntimeImageRef {
		t.Fatalf("expected runtime image ref %q, got %q", expectedRuntimeImageRef, got)
	}
	if got := deployOp.DesiredSource.ResolvedImageRef; got != managedImageRef {
		t.Fatalf("expected logical resolved image ref %q, got %q", managedImageRef, got)
	}

	locations, err := stateStore.ListImageLocations(model.ImageLocationFilter{
		TenantID:  tenant.ID,
		AppID:     app.ID,
		RuntimeID: updater.RuntimeID,
		Status:    model.ImageLocationStatusPresent,
	})
	if err != nil {
		t.Fatalf("list image locations: %v", err)
	}
	if len(locations) == 0 {
		t.Fatal("expected image locations on selected shared node cache")
	}
	for _, location := range locations {
		if location.ClusterNodeName != updater.ClusterNodeName {
			t.Fatalf("expected image location node %q, got %q", updater.ClusterNodeName, location.ClusterNodeName)
		}
		if location.CacheEndpoint != "http://203.0.113.20:5000" {
			t.Fatalf("expected cache endpoint http://203.0.113.20:5000, got %q", location.CacheEndpoint)
		}
	}
}

func TestControllerReachableImageCacheEndpointRejectsAbstractAndLoopbackHosts(t *testing.T) {
	t.Parallel()

	svc := &Service{builderRegistryPushBase: "127.0.0.1:5000"}
	for _, endpoint := range []string{"in-cluster", "127.0.0.1", "http://localhost:9443", "https://[::1]:9443"} {
		if registryBase, cacheEndpoint := svc.controllerReachableImageCacheEndpoint(model.Runtime{Endpoint: endpoint}); registryBase != "" || cacheEndpoint != "" {
			t.Fatalf("expected endpoint %q to be rejected, got registry=%q cache=%q", endpoint, registryBase, cacheEndpoint)
		}
	}
	registryBase, cacheEndpoint := svc.controllerReachableImageCacheEndpoint(model.Runtime{Endpoint: "https://203.0.113.20:9443"})
	if registryBase != "203.0.113.20:5000" || cacheEndpoint != "http://203.0.113.20:5000" {
		t.Fatalf("expected reachable node cache endpoint, got registry=%q cache=%q", registryBase, cacheEndpoint)
	}
}

func TestExecuteManagedImportOperationUsesPushBaseForPullBaseDockerImageSource(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	const originalRuntimeImageRef = "registry.pull.example/fugue-apps/template-runtime@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: originalRuntimeImageRef,
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	const managedImageDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	expectedRuntimeImageRef := mustRewriteImportedDigestRef(
		t,
		"registry.push.example/fugue-apps/demo:image-abc123",
		"registry.push.example",
		"registry.pull.example",
		managedImageDigest,
	)

	importer := &recordingImporter{}
	svc := &Service{
		Store:                        stateStore,
		Logger:                       log.New(io.Discard, "", 0),
		importer:                     importer,
		registryPushBase:             "registry.push.example",
		registryPullBase:             "registry.pull.example",
		inspectManagedImage:          inspectManagedImageAlwaysExists,
		resolveManagedImageDigestRef: resolveManagedImageDigestRefStub(map[string]string{"registry.push.example/fugue-apps/demo:image-abc123": managedImageDigest}),
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.dockerImageReq == nil {
		t.Fatal("expected importer to receive docker image request")
	}
	if got, want := importer.dockerImageReq.ImageRef, "registry.push.example/fugue-apps/template-runtime@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"; got != want {
		t.Fatalf("expected controller-reachable image ref %q, got %q", want, got)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil || deployOp.DesiredSource == nil {
		t.Fatalf("expected deploy operation with desired spec/source, got %+v", deployOp)
	}
	if got := deployOp.DesiredSpec.Image; got != expectedRuntimeImageRef {
		t.Fatalf("expected runtime image rewrite, got %q", got)
	}
	if got := deployOp.DesiredSource.ImageRef; got != originalRuntimeImageRef {
		t.Fatalf("expected source image ref to preserve requested pull ref, got %q", got)
	}
	if got := deployOp.DesiredSource.ResolvedImageRef; got != "registry.push.example/fugue-apps/demo:image-abc123" {
		t.Fatalf("expected resolved image ref to be persisted, got %q", got)
	}
}

func TestExecuteManagedImportOperationRefreshesExistingRestartToken(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:        "registry.pull.example/fugue-apps/demo:git-abc123",
		Replicas:     1,
		RuntimeID:    "runtime_managed_shared",
		RestartToken: "restart_old",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyDockerfile,
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{}
	svc := &Service{
		Store:               stateStore,
		Logger:              log.New(io.Discard, "", 0),
		importer:            importer,
		registryPushBase:    "registry.push.example",
		registryPullBase:    "registry.pull.example",
		inspectManagedImage: inspectManagedImageAlwaysExists,
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil {
		t.Fatalf("expected deploy operation with desired spec, got %+v", deployOp)
	}
	if deployOp.DesiredSpec.RestartToken == "restart_old" {
		t.Fatalf("expected rebuild to refresh restart token, got %q", deployOp.DesiredSpec.RestartToken)
	}
	if !strings.HasPrefix(deployOp.DesiredSpec.RestartToken, "restart_") {
		t.Fatalf("expected restart token prefix, got %q", deployOp.DesiredSpec.RestartToken)
	}
}

func TestExecuteManagedImportOperationRecoversUploadManagedImageRefFromImportResult(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	upload, err := stateStore.CreateSourceUpload(tenant.ID, "demo.tgz", "application/gzip", []byte("archive-bytes"))
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}
	expectedManagedImageRef := "registry.push.example/fugue-apps/demo:upload-" + upload.SHA256[:12]
	expectedRuntimeImageRef := mustRewriteImportedDigestRef(
		t,
		expectedManagedImageRef,
		"registry.push.example",
		"registry.pull.example",
		"sha256:"+upload.SHA256,
	)

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         upload.ID,
		UploadFilename:   upload.Filename,
		ArchiveSHA256:    upload.SHA256,
		ArchiveSizeBytes: upload.SizeBytes,
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DockerfilePath:   "Dockerfile",
		BuildContextDir:  ".",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{
		uploadOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				BuildStrategy:        model.AppBuildStrategyDockerfile,
				ImageRef:             expectedManagedImageRef,
				BuildJobName:         "fugue-build-demo-upload",
				DetectedPort:         8080,
				ExposesPublicService: true,
			},
			Source: model.AppSource{
				Type:            model.AppSourceTypeUpload,
				BuildStrategy:   model.AppBuildStrategyDockerfile,
				DockerfilePath:  "Dockerfile",
				BuildContextDir: ".",
			},
		},
	}
	svc := &Service{
		Store:            stateStore,
		Config:           config.ControllerConfig{SourceUploadBaseURL: "http://source.example"},
		Logger:           log.New(io.Discard, "", 0),
		importer:         importer,
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.pull.example",
		resolveManagedImageDigestRef: resolveManagedImageDigestRefStub(map[string]string{
			expectedManagedImageRef: "sha256:" + upload.SHA256,
		}),
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			return imageRef == expectedManagedImageRef, nil, nil
		},
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.uploadReq == nil {
		t.Fatal("expected importer to receive upload request")
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil {
		t.Fatalf("expected deploy operation with desired spec, got %+v", deployOp)
	}
	if got := deployOp.DesiredSpec.Image; got != expectedRuntimeImageRef {
		t.Fatalf("expected recovered runtime image %q, got %q", expectedRuntimeImageRef, got)
	}
	if deployOp.DesiredSource == nil {
		t.Fatal("expected desired source on recovered deploy operation")
	}
	if got := deployOp.DesiredSource.ResolvedImageRef; got != expectedManagedImageRef {
		t.Fatalf("expected recovered managed image ref %q, got %q", expectedManagedImageRef, got)
	}
	if got := deployOp.DesiredSource.UploadID; got != upload.ID {
		t.Fatalf("expected upload metadata to be preserved, got upload_id %q", got)
	}
	if got := deployOp.DesiredSource.ArchiveSHA256; got != upload.SHA256 {
		t.Fatalf("expected upload sha %q, got %q", upload.SHA256, got)
	}
}

func TestExecuteManagedImportOperationPreservesGitHubSourceAfterUploadOverride(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	upload, err := stateStore.CreateSourceUpload(tenant.ID, "demo.tgz", "application/gzip", []byte("archive-bytes"))
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}
	expectedManagedImageRef := "registry.push.example/fugue-apps/demo:upload-" + upload.SHA256[:12]
	expectedRuntimeImageRef := mustRewriteImportedDigestRef(
		t,
		expectedManagedImageRef,
		"registry.push.example",
		"registry.pull.example",
		"sha256:"+upload.SHA256,
	)

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:        "registry.pull.example/fugue-apps/demo:git-current",
		Replicas:     1,
		RuntimeID:    "runtime_managed_shared",
		RestartToken: "restart_old",
	}, model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		RepoBranch:       "main",
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		CommitSHA:        "git-current",
		DockerfilePath:   "Dockerfile",
		BuildContextDir:  ".",
		ImageNameSuffix:  "gateway",
		ComposeService:   "gateway",
		ComposeDependsOn: []string{"runtime"},
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	overrideSource := model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         upload.ID,
		UploadFilename:   upload.Filename,
		ArchiveSHA256:    upload.SHA256,
		ArchiveSizeBytes: upload.SizeBytes,
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DockerfilePath:   "Dockerfile",
		BuildContextDir:  ".",
		ImageNameSuffix:  "gateway",
		ComposeService:   "gateway",
		ComposeDependsOn: []string{"runtime"},
	}
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:            tenant.ID,
		Type:                model.OperationTypeImport,
		RequestedByType:     model.ActorTypeAPIKey,
		RequestedByID:       "test-key",
		AppID:               app.ID,
		DesiredSpec:         &specCopy,
		DesiredSource:       &overrideSource,
		DesiredOriginSource: model.AppOriginSource(app),
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{
		uploadOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				BuildStrategy:        model.AppBuildStrategyDockerfile,
				ImageRef:             expectedManagedImageRef,
				BuildJobName:         "fugue-build-demo-upload",
				DetectedPort:         8080,
				ExposesPublicService: true,
				DetectedProvider:     "dockerfile",
			},
			Source: model.AppSource{
				Type:             model.AppSourceTypeUpload,
				BuildStrategy:    model.AppBuildStrategyDockerfile,
				DockerfilePath:   "Dockerfile",
				BuildContextDir:  ".",
				ImageNameSuffix:  "gateway",
				ComposeService:   "gateway",
				DetectedProvider: "dockerfile",
			},
		},
	}
	svc := &Service{
		Store:            stateStore,
		Config:           config.ControllerConfig{SourceUploadBaseURL: "http://source.example"},
		Logger:           log.New(io.Discard, "", 0),
		importer:         importer,
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.pull.example",
		resolveManagedImageDigestRef: resolveManagedImageDigestRefStub(map[string]string{
			expectedManagedImageRef: "sha256:" + upload.SHA256,
		}),
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			return imageRef == expectedManagedImageRef, nil, nil
		},
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSource == nil {
		t.Fatalf("expected deploy operation with desired source, got %+v", deployOp)
	}
	if got := deployOp.DesiredSpec.Image; got != expectedRuntimeImageRef {
		t.Fatalf("expected runtime image %q, got %q", expectedRuntimeImageRef, got)
	}
	if got := deployOp.DesiredSource.Type; got != model.AppSourceTypeUpload {
		t.Fatalf("expected deploy build source type %q, got %q", model.AppSourceTypeUpload, got)
	}
	if got := deployOp.DesiredSource.UploadID; got != upload.ID {
		t.Fatalf("expected upload provenance to be preserved, got upload_id %q", got)
	}
	if got := deployOp.DesiredSource.ComposeService; got != "gateway" {
		t.Fatalf("expected compose service to be preserved, got %q", got)
	}
	if !reflect.DeepEqual(deployOp.DesiredSource.ComposeDependsOn, []string{"runtime"}) {
		t.Fatalf("expected compose dependencies to be preserved, got %v", deployOp.DesiredSource.ComposeDependsOn)
	}
	if got := deployOp.DesiredSource.ResolvedImageRef; got != expectedManagedImageRef {
		t.Fatalf("expected build managed image ref %q, got %q", expectedManagedImageRef, got)
	}
	if deployOp.DesiredOriginSource == nil {
		t.Fatal("expected deploy operation to preserve origin source ownership")
	}
	if got := deployOp.DesiredOriginSource.Type; got != model.AppSourceTypeGitHubPublic {
		t.Fatalf("expected deploy origin source type %q, got %q", model.AppSourceTypeGitHubPublic, got)
	}
	if got := deployOp.DesiredOriginSource.RepoURL; got != "https://github.com/example/demo" {
		t.Fatalf("expected deploy origin repo url to be preserved, got %q", got)
	}
	if got := deployOp.DesiredOriginSource.CommitSHA; got != "git-current" {
		t.Fatalf("expected deploy origin commit sha to be preserved, got %q", got)
	}
	if got := deployOp.DesiredOriginSource.ComposeService; got != "gateway" {
		t.Fatalf("expected deploy origin compose service to be preserved, got %q", got)
	}

	completedImport, err := stateStore.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("get completed import operation: %v", err)
	}
	if completedImport.DesiredSource == nil || completedImport.DesiredSource.Type != model.AppSourceTypeUpload {
		t.Fatalf("expected import operation to retain upload source history, got %+v", completedImport.DesiredSource)
	}
	if completedImport.DesiredOriginSource == nil || completedImport.DesiredOriginSource.Type != model.AppSourceTypeGitHubPublic {
		t.Fatalf("expected import operation to retain github ownership metadata, got %+v", completedImport.DesiredOriginSource)
	}

	if _, err := stateStore.CompleteManagedOperation(deployOp.ID, "/tmp/demo.yaml", "deployed"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}
	persistedApp, err := stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after deploy: %v", err)
	}
	if persistedApp.Source == nil {
		t.Fatal("expected app source after deploy")
	}
	if got := persistedApp.Source.Type; got != model.AppSourceTypeUpload {
		t.Fatalf("expected app build source type %q, got %q", model.AppSourceTypeUpload, got)
	}
	if got := persistedApp.Source.UploadID; got != upload.ID {
		t.Fatalf("expected persisted upload provenance, got %q", got)
	}
	if got := persistedApp.Source.ResolvedImageRef; got != expectedManagedImageRef {
		t.Fatalf("expected persisted build managed image ref %q, got %q", expectedManagedImageRef, got)
	}
	if persistedApp.OriginSource == nil {
		t.Fatal("expected persisted origin source after deploy")
	}
	if got := persistedApp.OriginSource.Type; got != model.AppSourceTypeGitHubPublic {
		t.Fatalf("expected persisted origin source type %q, got %q", model.AppSourceTypeGitHubPublic, got)
	}
	if got := persistedApp.OriginSource.RepoURL; got != "https://github.com/example/demo" {
		t.Fatalf("expected persisted origin repo url, got %q", got)
	}
	if got := persistedApp.OriginSource.CommitSHA; got != "git-current" {
		t.Fatalf("expected persisted origin commit sha, got %q", got)
	}
	if persistedApp.BuildSource == nil || persistedApp.BuildSource.Type != model.AppSourceTypeUpload {
		t.Fatalf("expected persisted build source to track upload override, got %+v", persistedApp.BuildSource)
	}
}

func TestExecuteManagedImportOperationPropagatesUploadImporterErrors(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	upload, err := stateStore.CreateSourceUpload(tenant.ID, "demo.tgz", "application/gzip", []byte("archive-bytes"))
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         upload.ID,
		UploadFilename:   upload.Filename,
		ArchiveSHA256:    upload.SHA256,
		ArchiveSizeBytes: upload.SizeBytes,
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DockerfilePath:   "Dockerfile",
		BuildContextDir:  ".",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importerErr := errors.New("select builder placement: no eligible builder nodes for profile heavy")
	importer := &recordingImporter{uploadErr: importerErr}
	svc := &Service{
		Store:               stateStore,
		Config:              config.ControllerConfig{SourceUploadBaseURL: "http://source.example"},
		Logger:              log.New(io.Discard, "", 0),
		importer:            importer,
		registryPushBase:    "registry.push.example",
		registryPullBase:    "registry.pull.example",
		inspectManagedImage: inspectManagedImageAlwaysExists,
	}

	err = svc.executeManagedImportOperation(context.Background(), op, app)
	if !errors.Is(err, importerErr) {
		t.Fatalf("expected importer error %q, got %v", importerErr, err)
	}
	if strings.Contains(err.Error(), "did not report a managed image reference") {
		t.Fatalf("expected upload importer error to be preserved, got %v", err)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			t.Fatalf("did not expect deploy op when upload import fails, got %+v", candidate)
		}
	}
}

func TestExecuteManagedImportOperationFailsWhenUploadBuildLacksBuilderEvidence(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	upload, err := stateStore.CreateSourceUpload(tenant.ID, "demo.tgz", "application/gzip", []byte("archive-bytes"))
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}
	expectedManagedImageRef := "registry.push.example/fugue-apps/demo:upload-" + upload.SHA256[:12]

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         upload.ID,
		UploadFilename:   upload.Filename,
		ArchiveSHA256:    upload.SHA256,
		ArchiveSizeBytes: upload.SizeBytes,
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DockerfilePath:   "Dockerfile",
		BuildContextDir:  ".",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{
		uploadOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				BuildStrategy:        model.AppBuildStrategyDockerfile,
				ImageRef:             expectedManagedImageRef,
				DetectedPort:         8080,
				ExposesPublicService: true,
			},
			Source: model.AppSource{
				Type:            model.AppSourceTypeUpload,
				BuildStrategy:   model.AppBuildStrategyDockerfile,
				DockerfilePath:  "Dockerfile",
				BuildContextDir: ".",
			},
		},
	}
	svc := &Service{
		Store:            stateStore,
		Config:           config.ControllerConfig{SourceUploadBaseURL: "http://source.example"},
		Logger:           log.New(io.Discard, "", 0),
		importer:         importer,
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.pull.example",
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			t.Fatalf("unexpected registry inspection for image %q", imageRef)
			return false, nil, nil
		},
	}

	err = svc.executeManagedImportOperation(context.Background(), op, app)
	if err == nil {
		t.Fatal("expected import to fail when upload build omits builder evidence")
	}
	if !strings.Contains(err.Error(), "did not report builder job evidence") {
		t.Fatalf("expected missing builder evidence failure, got %v", err)
	}
}

func TestExecuteManagedImportOperationFailsWhenDirectManagedImageStaysMissing(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: "nginx:1.27",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	inspectCalls := 0
	svc := &Service{
		Store:                         stateStore,
		Logger:                        log.New(io.Discard, "", 0),
		importer:                      &recordingImporter{},
		registryPushBase:              "registry.push.example",
		registryPullBase:              "registry.pull.example",
		importImageInspectRetryDelay:  time.Millisecond,
		importImageInspectMaxAttempts: 3,
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			if imageRef != "registry.push.example/fugue-apps/demo:image-abc123" {
				t.Fatalf("unexpected direct image ref %q", imageRef)
			}
			inspectCalls++
			return false, nil, nil
		},
	}

	err = svc.executeManagedImportOperation(context.Background(), op, app)
	if err == nil {
		t.Fatal("expected import to fail when the managed image stays missing")
	}
	if inspectCalls != 3 {
		t.Fatalf("expected 3 registry inspect attempts before failing, got %d", inspectCalls)
	}
	if !strings.Contains(err.Error(), "were not confirmed in the registry") {
		t.Fatalf("expected registry confirmation failure, got %v", err)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			t.Fatalf("expected no deploy operation after missing direct image, got %+v", candidate)
		}
	}
}

func TestExecuteManagedImportOperationFailsWhenUploadImportOmitsManagedImageRef(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	upload, err := stateStore.CreateSourceUpload(tenant.ID, "demo.tgz", "application/gzip", []byte("archive-bytes"))
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         upload.ID,
		UploadFilename:   upload.Filename,
		ArchiveSHA256:    upload.SHA256,
		ArchiveSizeBytes: upload.SizeBytes,
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DockerfilePath:   "Dockerfile",
		BuildContextDir:  ".",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{
		uploadOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				BuildStrategy:        model.AppBuildStrategyDockerfile,
				DetectedPort:         8080,
				ExposesPublicService: true,
			},
			Source: model.AppSource{
				Type:            model.AppSourceTypeUpload,
				BuildStrategy:   model.AppBuildStrategyDockerfile,
				DockerfilePath:  "Dockerfile",
				BuildContextDir: ".",
			},
		},
	}
	svc := &Service{
		Store:                         stateStore,
		Config:                        config.ControllerConfig{SourceUploadBaseURL: "http://source.example"},
		Logger:                        log.New(io.Discard, "", 0),
		importer:                      importer,
		registryPushBase:              "registry.push.example",
		registryPullBase:              "registry.pull.example",
		importImageInspectRetryDelay:  time.Millisecond,
		importImageInspectMaxAttempts: 3,
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			t.Fatalf("unexpected registry inspection for image %q", imageRef)
			return false, nil, nil
		},
	}

	err = svc.executeManagedImportOperation(context.Background(), op, app)
	if err == nil {
		t.Fatal("expected import to fail when the importer omits the managed image reference")
	}
	if !strings.Contains(err.Error(), "did not report a managed image reference") {
		t.Fatalf("expected missing importer image failure, got %v", err)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			t.Fatalf("expected no deploy operation after inferred image confirmation failure, got %+v", candidate)
		}
	}
}

func TestExecuteManagedImportOperationStopsAfterForceDelete(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:       model.AppSourceTypeGitHubPublic,
		RepoURL:    "https://github.com/example/demo",
		RepoBranch: "main",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := newControlledImporter()
	svc := &Service{
		Store:               stateStore,
		Logger:              log.New(io.Discard, "", 0),
		importer:            importer,
		registryPushBase:    "registry.push.example",
		registryPullBase:    "registry.pull.example",
		inspectManagedImage: inspectManagedImageAlwaysExists,
	}

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- svc.executeManagedImportOperation(context.Background(), op, app)
	}()

	startedOpID := <-importer.started
	if startedOpID != op.ID {
		t.Fatalf("expected importer start for op %s, got %s", op.ID, startedOpID)
	}

	if _, err := stateStore.FailOperation(op.ID, "build canceled so the app can be force deleted"); err != nil {
		t.Fatalf("fail import operation: %v", err)
	}

	importer.release(op.ID)

	if err := <-resultCh; !errors.Is(err, errOperationNoLongerActive) {
		t.Fatalf("expected %v after force delete, got %v", errOperationNoLongerActive, err)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			t.Fatalf("expected no deploy operation after force delete, got %+v", candidate)
		}
	}
}

func TestExecuteManagedImportOperationSyncsBillingImageStorage(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: "nginx:1.27",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	svc := &Service{
		Store:                   stateStore,
		Logger:                  log.New(io.Discard, "", 0),
		importer:                &recordingImporter{},
		registryPushBase:        "registry.push.example",
		registryPullBase:        "registry.pull.example",
		syncBillingImageStorage: true,
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			if imageRef != "registry.push.example/fugue-apps/demo:image-abc123" {
				return false, nil, nil
			}
			return true, map[string]int64{
				"sha256:manifest": 32,
				"sha256:config":   64,
			}, nil
		},
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		summary, err := stateStore.GetTenantBillingSummary(tenant.ID)
		if err != nil {
			t.Fatalf("get billing summary: %v", err)
		}
		if got := summary.ManagedCommitted.StorageGibibytes; got == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected billing summary to include 1 GiB synced image storage, got %d", summary.ManagedCommitted.StorageGibibytes)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestExecuteManagedImportOperationDoesNotConstrainBuildPlacementByRuntimeLocation(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := stateStore.CreateRuntime("", "internal-cluster-tokyo", model.RuntimeTypeManagedShared, "in-cluster", map[string]string{
		"region":       "ap-northeast-1",
		"country_code": "JP",
	})
	if err != nil {
		t.Fatalf("create shared runtime: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyDockerfile,
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{}
	svc := &Service{
		Store:               stateStore,
		Logger:              log.New(io.Discard, "", 0),
		importer:            importer,
		registryPushBase:    "registry.push.example",
		inspectManagedImage: inspectManagedImageAlwaysExists,
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.githubReq == nil {
		t.Fatal("expected importer to receive github request")
	}

	selector := importer.githubReq.PlacementNodeSelector
	if len(selector) != 0 {
		t.Fatalf("expected source build to use the builder pool without app runtime placement selector, got %v", selector)
	}
}

func TestImportBuilderMemoryCeilingUsesTenantManagedHeadroom(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Builder Budget Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := stateStore.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    4_000,
		MemoryMebibytes:  4 * 1024,
		StorageGibibytes: 20,
	}); err != nil {
		t.Fatalf("update tenant billing: %v", err)
	}

	svc := &Service{
		Store:  stateStore,
		Logger: log.New(io.Discard, "", 0),
	}
	want := sourceimport.DefaultBuilderHeavyMemoryLimitBytes() + 4*1024*1024*1024
	if got := svc.importBuilderMemoryCeilingBytes(tenant.ID); got != want {
		t.Fatalf("expected builder ceiling %d bytes, got %d", want, got)
	}
}

func TestExecuteManagedImportOperationRefreshesComposeEnvWithoutOverwritingCustomValues(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := stateStore.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    1000,
		MemoryMebibytes:  2048,
		StorageGibibytes: 30,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}

	primaryApp, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-api", "", model.AppSpec{
		Env: map[string]string{
			"KEEP": "custom-value",
		},
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			ServiceName: "demo-api-postgres",
			Database:    "demo",
			User:        "demo",
			Password:    "secret",
		},
	}, model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		RepoBranch:       "main",
		BuildStrategy:    model.AppBuildStrategyBuildpacks,
		ImageNameSuffix:  "api",
		ComposeService:   "api",
		ComposeDependsOn: []string{"web"},
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("create primary app: %v", err)
	}

	if _, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-web", "", model.AppSpec{
		Ports:     []int{3000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:           model.AppSourceTypeGitHubPublic,
		RepoURL:        "https://github.com/example/demo",
		RepoBranch:     "main",
		BuildStrategy:  model.AppBuildStrategyBuildpacks,
		ComposeService: "web",
	}, model.AppRoute{}); err != nil {
		t.Fatalf("create sibling app: %v", err)
	}
	specCopy := primaryApp.Spec
	sourceCopy := *primaryApp.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           primaryApp.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{
		githubComposeEnv: map[string]string{
			"KEEP":    "default-value",
			"NEW_KEY": "from-compose",
			"PORT":    "8080",
		},
	}
	svc := &Service{
		Store:               stateStore,
		Logger:              log.New(io.Discard, "", 0),
		importer:            importer,
		registryPushBase:    "registry.push.example",
		inspectManagedImage: inspectManagedImageAlwaysExists,
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, primaryApp); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.githubComposeEnvReq == nil {
		t.Fatal("expected compose env refresh request")
	}

	wantHosts := map[string]string{
		"api": runtime.ComposeServiceAliasName(project.ID, "api"),
		"web": runtime.ComposeServiceAliasName(project.ID, "web"),
	}
	if !reflect.DeepEqual(importer.githubComposeEnvReq.AppHosts, wantHosts) {
		t.Fatalf("expected compose app hosts %v, got %v", wantHosts, importer.githubComposeEnvReq.AppHosts)
	}
	if spec, ok := importer.githubComposeEnvReq.ManagedPostgresByOwner["api"]; !ok || spec.ServiceName != "demo-api-postgres" {
		t.Fatalf("expected api postgres spec to be forwarded, got %v", importer.githubComposeEnvReq.ManagedPostgresByOwner)
	}

	ops, err := stateStore.ListOperations(tenant.ID, false)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil {
		t.Fatalf("expected deploy operation with desired spec, got %+v", deployOp)
	}
	if deployOp.DesiredSource == nil {
		t.Fatalf("expected deploy operation to keep desired source metadata, got %+v", deployOp)
	}
	if deployOp.DesiredSource.ImageNameSuffix != "api" {
		t.Fatalf("expected deploy image suffix api, got %q", deployOp.DesiredSource.ImageNameSuffix)
	}
	if deployOp.DesiredSource.ComposeService != "api" {
		t.Fatalf("expected deploy compose service api, got %q", deployOp.DesiredSource.ComposeService)
	}
	if !reflect.DeepEqual(deployOp.DesiredSource.ComposeDependsOn, []string{"web"}) {
		t.Fatalf("expected deploy compose dependencies [web], got %v", deployOp.DesiredSource.ComposeDependsOn)
	}
	if got := deployOp.DesiredSpec.Env["KEEP"]; got != "custom-value" {
		t.Fatalf("expected custom KEEP to be preserved, got %q", got)
	}
	if got := deployOp.DesiredSpec.Env["NEW_KEY"]; got != "from-compose" {
		t.Fatalf("expected NEW_KEY to be added from compose, got %q", got)
	}
	if got := deployOp.DesiredSpec.Env["PORT"]; got != "8080" {
		t.Fatalf("expected compose PORT to override build suggestion, got %q", got)
	}
}

func TestExecuteManagedImportOperationAppliesSuggestedStartupCommandWhenMissing(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyBuildpacks,
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{
		githubOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				BuildStrategy:           model.AppBuildStrategyBuildpacks,
				ImageRef:                "registry.push.example/fugue-apps/demo:git-abc123",
				DetectedPort:            5000,
				ExposesPublicService:    true,
				SuggestedStartupCommand: "python app.py",
				SuggestedEnv:            map[string]string{"PORT": "5000"},
			},
			Source: model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				RepoURL:          "https://github.com/example/demo",
				RepoBranch:       "main",
				BuildStrategy:    model.AppBuildStrategyBuildpacks,
				ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-abc123",
				DetectedProvider: "python",
			},
		},
	}
	svc := &Service{
		Store:               stateStore,
		Logger:              log.New(io.Discard, "", 0),
		importer:            importer,
		registryPushBase:    "registry.push.example",
		registryPullBase:    "registry.pull.example",
		inspectManagedImage: inspectManagedImageAlwaysExists,
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil {
		t.Fatalf("expected deploy operation with desired spec, got %+v", deployOp)
	}
	if !reflect.DeepEqual(deployOp.DesiredSpec.Command, []string{"sh", "-lc", "python app.py"}) {
		t.Fatalf("expected suggested startup command to be applied, got %#v", deployOp.DesiredSpec.Command)
	}
	if got := deployOp.DesiredSpec.Env["PORT"]; got != "5000" {
		t.Fatalf("expected suggested PORT env to be preserved, got %q", got)
	}

	completedImport, err := stateStore.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("get completed import operation: %v", err)
	}
	if completedImport.DesiredSpec == nil {
		t.Fatal("expected completed import operation to persist desired spec")
	}
	if !reflect.DeepEqual(completedImport.DesiredSpec.Command, []string{"sh", "-lc", "python app.py"}) {
		t.Fatalf("expected completed import operation to persist inferred command, got %#v", completedImport.DesiredSpec.Command)
	}
}

func TestExecuteManagedImportOperationDoesNotOverrideExplicitStartupCommand(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Command:   []string{"sh", "-lc", "python -m custom"},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyBuildpacks,
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{
		githubOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				BuildStrategy:           model.AppBuildStrategyBuildpacks,
				ImageRef:                "registry.push.example/fugue-apps/demo:git-abc123",
				DetectedPort:            5000,
				ExposesPublicService:    true,
				SuggestedStartupCommand: "python app.py",
			},
			Source: model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				RepoURL:          "https://github.com/example/demo",
				RepoBranch:       "main",
				BuildStrategy:    model.AppBuildStrategyBuildpacks,
				ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-abc123",
				DetectedProvider: "python",
			},
		},
	}
	svc := &Service{
		Store:               stateStore,
		Logger:              log.New(io.Discard, "", 0),
		importer:            importer,
		registryPushBase:    "registry.push.example",
		registryPullBase:    "registry.pull.example",
		inspectManagedImage: inspectManagedImageAlwaysExists,
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil {
		t.Fatalf("expected deploy operation with desired spec, got %+v", deployOp)
	}
	if !reflect.DeepEqual(deployOp.DesiredSpec.Command, []string{"sh", "-lc", "python -m custom"}) {
		t.Fatalf("expected explicit startup command to win, got %#v", deployOp.DesiredSpec.Command)
	}
}

func TestExecuteManagedImportOperationKeepsBackgroundAppsPortless(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "workers", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "worker", "", model.AppSpec{
		NetworkMode: model.AppNetworkModeBackground,
		Replicas:    1,
		RuntimeID:   "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeDockerImage,
		ImageRef:      "ghcr.io/example/worker:latest",
		BuildStrategy: model.AppBuildStrategyDockerfile,
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{}
	svc := &Service{
		Store:               stateStore,
		Logger:              log.New(io.Discard, "", 0),
		importer:            importer,
		registryPushBase:    "registry.push.example",
		registryPullBase:    "registry.pull.example",
		inspectManagedImage: inspectManagedImageAlwaysExists,
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil {
		t.Fatalf("expected deploy operation with desired spec, got %+v", deployOp)
	}
	if deployOp.DesiredSpec.NetworkMode != model.AppNetworkModeBackground {
		t.Fatalf("expected deploy spec network mode %q, got %q", model.AppNetworkModeBackground, deployOp.DesiredSpec.NetworkMode)
	}
	if len(deployOp.DesiredSpec.Ports) != 0 {
		t.Fatalf("expected background deploy spec to stay portless, got %v", deployOp.DesiredSpec.Ports)
	}
}

func TestExecuteManagedImportOperationAutoBackgroundsDockerImageWithoutPublicServiceSignal(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "workers", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "worker", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: "ghcr.io/example/worker:latest",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{
		dockerImageOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				DetectedProvider:     model.AppSourceTypeDockerImage,
				ImageRef:             "registry.push.example/fugue-apps/worker:image-abc123",
				DetectedPort:         8000,
				ExposesPublicService: false,
				DetectedStack:        "python",
			},
			Source: model.AppSource{
				Type:             model.AppSourceTypeDockerImage,
				ImageRef:         "ghcr.io/example/worker:latest",
				ResolvedImageRef: "registry.push.example/fugue-apps/worker:image-abc123",
				DetectedProvider: model.AppSourceTypeDockerImage,
				DetectedStack:    "python",
			},
		},
	}
	svc := &Service{
		Store:               stateStore,
		Logger:              log.New(io.Discard, "", 0),
		importer:            importer,
		registryPushBase:    "registry.push.example",
		registryPullBase:    "registry.pull.example",
		inspectManagedImage: inspectManagedImageAlwaysExists,
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil {
		t.Fatalf("expected deploy operation with desired spec, got %+v", deployOp)
	}
	if deployOp.DesiredSpec.NetworkMode != model.AppNetworkModeBackground {
		t.Fatalf("expected deploy spec network mode %q, got %q", model.AppNetworkModeBackground, deployOp.DesiredSpec.NetworkMode)
	}
	if len(deployOp.DesiredSpec.Ports) != 0 {
		t.Fatalf("expected auto-background deploy spec to stay portless, got %v", deployOp.DesiredSpec.Ports)
	}
}

func TestExecuteManagedImportOperationAutoBackgroundsSingleAppWithoutPublicServiceSignal(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "workers", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "worker", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/worker",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyBuildpacks,
	}, model.AppRoute{
		Hostname:   "worker.example.com",
		BaseDomain: "example.com",
		PublicURL:  "https://worker.example.com",
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{
		githubOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				BuildStrategy:        model.AppBuildStrategyBuildpacks,
				ImageRef:             "registry.push.example/fugue-apps/worker:git-abc123",
				DetectedPort:         8000,
				ExposesPublicService: false,
			},
			Source: model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				RepoURL:          "https://github.com/example/worker",
				RepoBranch:       "main",
				BuildStrategy:    model.AppBuildStrategyBuildpacks,
				ResolvedImageRef: "registry.push.example/fugue-apps/worker:git-abc123",
				DetectedProvider: "python",
			},
		},
	}
	svc := &Service{
		Store:               stateStore,
		Logger:              log.New(io.Discard, "", 0),
		importer:            importer,
		registryPushBase:    "registry.push.example",
		registryPullBase:    "registry.pull.example",
		inspectManagedImage: inspectManagedImageAlwaysExists,
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil {
		t.Fatalf("expected deploy operation with desired spec, got %+v", deployOp)
	}
	if deployOp.DesiredSpec.NetworkMode != model.AppNetworkModeBackground {
		t.Fatalf("expected deploy spec network mode %q, got %q", model.AppNetworkModeBackground, deployOp.DesiredSpec.NetworkMode)
	}
	if len(deployOp.DesiredSpec.Ports) != 0 {
		t.Fatalf("expected auto-background deploy spec to stay portless, got %v", deployOp.DesiredSpec.Ports)
	}
}

func TestExecuteManagedImportOperationKeepsTopologyServicesPublicWithoutPublicServiceSignal(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "stack", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-web", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:           model.AppSourceTypeGitHubPublic,
		RepoURL:        "https://github.com/example/stack",
		RepoBranch:     "main",
		BuildStrategy:  model.AppBuildStrategyBuildpacks,
		ComposeService: "web",
	}, model.AppRoute{
		Hostname:   "demo-web.example.com",
		BaseDomain: "example.com",
		PublicURL:  "https://demo-web.example.com",
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{
		githubOutput: &sourceimport.GitHubSourceImportOutput{
			ImportResult: sourceimport.GitHubImportResult{
				BuildStrategy:        model.AppBuildStrategyBuildpacks,
				ImageRef:             "registry.push.example/fugue-apps/demo-web:git-abc123",
				DetectedPort:         3000,
				ExposesPublicService: false,
			},
			Source: model.AppSource{
				Type:             model.AppSourceTypeGitHubPublic,
				RepoURL:          "https://github.com/example/stack",
				RepoBranch:       "main",
				BuildStrategy:    model.AppBuildStrategyBuildpacks,
				ComposeService:   "web",
				ResolvedImageRef: "registry.push.example/fugue-apps/demo-web:git-abc123",
				DetectedProvider: "nodejs",
			},
		},
	}
	svc := &Service{
		Store:               stateStore,
		Logger:              log.New(io.Discard, "", 0),
		importer:            importer,
		registryPushBase:    "registry.push.example",
		registryPullBase:    "registry.pull.example",
		inspectManagedImage: inspectManagedImageAlwaysExists,
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}

	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil {
		t.Fatalf("expected deploy operation with desired spec, got %+v", deployOp)
	}
	if deployOp.DesiredSpec.NetworkMode == model.AppNetworkModeBackground {
		t.Fatalf("expected topology deploy spec to remain routable, got %q", deployOp.DesiredSpec.NetworkMode)
	}
	if !reflect.DeepEqual(deployOp.DesiredSpec.Ports, []int{3000}) {
		t.Fatalf("expected topology deploy spec to keep detected port 3000, got %v", deployOp.DesiredSpec.Ports)
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func clonePostgresSpecMap(values map[string]model.AppPostgresSpec) map[string]model.AppPostgresSpec {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]model.AppPostgresSpec, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
