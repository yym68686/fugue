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

func TestAgentCompleteDeleteOperationDeletesAppImages(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Agent Delete Tenant")
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
	imageRef := pushBase + "/fugue-apps/example-demo:git-current"
	runtimeImageRef := pullBase + "/fugue-apps/example-demo:git-current"

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     runtimeImageRef,
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: externalRuntime.ID,
	}, model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		ResolvedImageRef: imageRef,
	}, model.AppRoute{
		Hostname:    "demo.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo.apps.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	deleteOp, err := stateStore.CreateOperation(model.Operation{
		TenantID: tenant.ID,
		Type:     model.OperationTypeDelete,
		AppID:    app.ID,
	})
	if err != nil {
		t.Fatalf("create delete operation: %v", err)
	}
	claimed, found, err := stateStore.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim delete operation: %v", err)
	}
	if !found {
		t.Fatal("expected delete operation to be claimable")
	}
	if claimed.ID != deleteOp.ID {
		t.Fatalf("expected claimed delete operation %s, got %s", deleteOp.ID, claimed.ID)
	}
	if claimed.Status != model.OperationStatusWaitingAgent || claimed.AssignedRuntimeID != externalRuntime.ID {
		t.Fatalf("expected waiting agent delete operation assigned to %s, got %+v", externalRuntime.ID, claimed)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{
		ControlPlaneNamespace:       "fugue-system",
		ControlPlaneReleaseInstance: "fugue",
		RegistryPushBase:            pushBase,
		RegistryPullBase:            pullBase,
	})
	fakeRegistry := &fakeAppImageRegistry{
		images: map[string]appImageRegistryInspectResult{
			imageRef: {
				ImageRef: imageRef,
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

	req := httptest.NewRequest(http.MethodPost, "/v1/agent/operations/"+deleteOp.ID+"/complete", strings.NewReader(`{"manifest_path":"/tmp/demo.yaml","message":"deleted"}`))
	req.Header.Set("Authorization", "Bearer "+runtimeKey)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	if len(fakeRegistry.deleted) != 1 || fakeRegistry.deleted[0] != imageRef {
		t.Fatalf("expected deleted image ref %q, got %#v", imageRef, fakeRegistry.deleted)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("expected registry GC exec call, got %d", len(runner.calls))
	}
}
