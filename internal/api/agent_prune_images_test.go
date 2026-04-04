package api

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestAgentCompleteDeployOperationPrunesExcessAppImages(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Agent Prune Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	token, secret, err := stateStore.CreateEnrollmentToken(tenant.ID, "default", time.Hour)
	if err != nil {
		t.Fatalf("create enrollment token: %v", err)
	}
	if token.ID == "" || secret == "" {
		t.Fatal("expected enrollment token secret")
	}
	externalRuntime, runtimeKey, err := stateStore.ConsumeEnrollmentToken(secret, "worker-1", "https://worker-1.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("consume enrollment token: %v", err)
	}

	const (
		pushBase = "registry.push.example"
		pullBase = "registry.pull.example"
	)
	oldImageRef := pushBase + "/fugue-apps/example-demo:git-old"
	oldRuntimeImageRef := pullBase + "/fugue-apps/example-demo:git-old"
	currentImageRef := pushBase + "/fugue-apps/example-demo:git-current"
	currentRuntimeImageRef := pullBase + "/fugue-apps/example-demo:git-current"

	oldSpec := model.AppSpec{
		Image:            oldRuntimeImageRef,
		ImageMirrorLimit: 1,
		Ports:            []int{80},
		Replicas:         1,
		RuntimeID:        externalRuntime.ID,
	}
	oldSource := model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		ResolvedImageRef: oldImageRef,
	}
	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", oldSpec, oldSource, model.AppRoute{
		Hostname:    "demo.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo.apps.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

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
	claimedOld, found, err := stateStore.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim old deploy operation: %v", err)
	}
	if !found {
		t.Fatal("expected old deploy operation to be claimable")
	}
	if claimedOld.ID != oldDeployOp.ID {
		t.Fatalf("expected claimed old deploy operation %s, got %s", oldDeployOp.ID, claimedOld.ID)
	}
	if _, err := stateStore.CompleteAgentOperation(oldDeployOp.ID, externalRuntime.ID, "/tmp/old.yaml", "old deployed"); err != nil {
		t.Fatalf("complete old deploy operation: %v", err)
	}

	currentSpec := oldSpec
	currentSpec.Image = currentRuntimeImageRef
	currentSource := oldSource
	currentSource.ResolvedImageRef = currentImageRef
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
	claimedCurrent, found, err := stateStore.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim current deploy operation: %v", err)
	}
	if !found {
		t.Fatal("expected current deploy operation to be claimable")
	}
	if claimedCurrent.ID != currentDeployOp.ID {
		t.Fatalf("expected claimed current deploy operation %s, got %s", currentDeployOp.ID, claimedCurrent.ID)
	}
	if claimedCurrent.Status != model.OperationStatusWaitingAgent || claimedCurrent.AssignedRuntimeID != externalRuntime.ID {
		t.Fatalf("expected waiting agent deploy operation assigned to %s, got %+v", externalRuntime.ID, claimedCurrent)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{
		ControlPlaneNamespace:       "fugue-system",
		ControlPlaneReleaseInstance: "fugue",
		RegistryPushBase:            pushBase,
		RegistryPullBase:            pullBase,
	})
	fakeRegistry := &fakeAppImageRegistry{
		images: map[string]appImageRegistryInspectResult{
			oldImageRef: {
				ImageRef: oldImageRef,
				Digest:   "sha256:manifest-old",
				Exists:   true,
				BlobSizes: map[string]int64{
					"sha256:manifest-old": 10,
					"sha256:config-old":   20,
				},
			},
			currentImageRef: {
				ImageRef: currentImageRef,
				Digest:   "sha256:manifest-current",
				Exists:   true,
				BlobSizes: map[string]int64{
					"sha256:manifest-current": 10,
					"sha256:config-current":   20,
				},
			},
		},
	}
	server.appImageRegistry = fakeRegistry

	pod := kubePodInfo{}
	pod.Metadata.Name = "fugue-fugue-registry-abc123"
	pod.Metadata.CreationTimestamp = time.Date(2026, 4, 4, 9, 0, 0, 0, time.UTC)
	pod.Status.Phase = "Running"
	server.newFilesystemPodLister = func(namespace string) (filesystemPodLister, error) {
		if namespace != "fugue-system" {
			t.Fatalf("expected control-plane namespace fugue-system, got %q", namespace)
		}
		return fakeFilesystemPodLister{pods: []kubePodInfo{pod}}, nil
	}
	runner := &fakeFilesystemExecRunner{
		outputs: [][]byte{{}},
	}
	server.filesystemExecRunner = runner

	req := httptest.NewRequest(http.MethodPost, "/v1/agent/operations/"+currentDeployOp.ID+"/complete", strings.NewReader(`{"manifest_path":"/tmp/demo.yaml","message":"deployed"}`))
	req.Header.Set("Authorization", "Bearer "+runtimeKey)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	if len(fakeRegistry.deleted) != 1 || fakeRegistry.deleted[0] != oldImageRef {
		t.Fatalf("expected deleted image ref %q, got %#v", oldImageRef, fakeRegistry.deleted)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("expected registry GC exec call, got %d", len(runner.calls))
	}
}
