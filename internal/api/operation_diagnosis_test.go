package api

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

func TestGetOperationDiagnosisExplainsMissingManagedImage(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, _, _, app, fakeRegistry, _, newImageRef, _ := setupAppImagesTestServer(t)
	missing := fakeRegistry.images[newImageRef]
	missing.Exists = false
	missing.SizeBytes = 0
	missing.BlobSizes = nil
	fakeRegistry.images[newImageRef] = missing

	app, err := stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload app: %v", err)
	}

	spec := app.Spec
	source := app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "tester",
		AppID:           app.ID,
		DesiredSpec:     &spec,
		DesiredSource:   source,
	})
	if err != nil {
		t.Fatalf("create pending deploy operation: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Diagnosis model.OperationDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Diagnosis.Category != "deploy-image-missing" {
		t.Fatalf("expected deploy-image-missing diagnosis, got %+v", response.Diagnosis)
	}
	for _, want := range []string{
		newImageRef,
		app.Spec.Image,
		"fugue app logs build demo",
		"fugue app overview demo",
	} {
		if !strings.Contains(response.Diagnosis.Summary+"\n"+response.Diagnosis.Hint, want) {
			t.Fatalf("expected diagnosis to contain %q, got %+v", want, response.Diagnosis)
		}
	}
}

func TestDiagnoseFailedOperationExplainsMissingManifest(t *testing.T) {
	t.Parallel()

	server := &Server{
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.fugue.internal:5000",
	}
	app := model.App{
		ID:   "app-demo",
		Name: "demo",
		Spec: model.AppSpec{
			Image: "10.128.0.2:30500/fugue-apps/example-demo@sha256:abc123",
		},
	}
	spec := app.Spec
	source := model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ResolvedImageRef: "registry.push.example/fugue-apps/example-demo@sha256:abc123",
	}
	op := model.Operation{
		Type:          model.OperationTypeDeploy,
		Status:        model.OperationStatusFailed,
		AppID:         app.ID,
		DesiredSpec:   &spec,
		DesiredSource: &source,
		ErrorMessage:  "resolve image digest: MANIFEST_UNKNOWN: manifest unknown",
	}

	diagnosis, err := server.diagnoseFailedOperation(context.Background(), op, app, true)
	if err != nil {
		t.Fatalf("diagnose failed operation: %v", err)
	}
	if diagnosis.Category != "image-manifest-missing" {
		t.Fatalf("expected image-manifest-missing, got %+v", diagnosis)
	}
	joinedEvidence := strings.Join(diagnosis.Evidence, "\n")
	for _, want := range []string{
		"10.128.0.2:30500/fugue-apps/example-demo@sha256:abc123",
		"registry.push.example/fugue-apps/example-demo@sha256:abc123",
	} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence to contain %q, got %+v", want, diagnosis.Evidence)
		}
	}
}

func TestOperationDiagnosisIncludesControllerLaneOccupants(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Lane Diagnosis Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)
	project, err := stateStore.CreateProject(tenant.ID, "ops", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "reader", []string{"app.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	appA, err := stateStore.CreateApp(tenant.ID, project.ID, "active", "", model.AppSpec{Image: "ghcr.io/example/active", Replicas: 1, RuntimeID: "runtime_managed_shared"})
	if err != nil {
		t.Fatalf("create active app: %v", err)
	}
	appB, err := stateStore.CreateApp(tenant.ID, project.ID, "pending", "", model.AppSpec{Image: "ghcr.io/example/pending", Replicas: 1, RuntimeID: "runtime_managed_shared"})
	if err != nil {
		t.Fatalf("create pending app: %v", err)
	}
	activeSpec := appA.Spec
	activeOp, err := stateStore.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDeploy, AppID: appA.ID, DesiredSpec: &activeSpec})
	if err != nil {
		t.Fatalf("create active op: %v", err)
	}
	claimed, found, err := stateStore.TryClaimPendingOperation(activeOp.ID)
	if err != nil || !found {
		t.Fatalf("claim active op found=%v err=%v", found, err)
	}
	pendingSpec := appB.Spec
	pendingOp, err := stateStore.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDeploy, AppID: appB.ID, DesiredSpec: &pendingSpec})
	if err != nil {
		t.Fatalf("create pending op: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+pendingOp.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Diagnosis model.OperationDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Diagnosis.ControllerLane == nil {
		t.Fatalf("expected controller lane diagnosis, got %+v", response.Diagnosis)
	}
	if response.Diagnosis.ControllerLane.Lane != model.OperationControllerLaneForegroundActivate {
		t.Fatalf("unexpected lane: %+v", response.Diagnosis.ControllerLane)
	}
	if len(response.Diagnosis.ControllerLane.Active) != 1 || response.Diagnosis.ControllerLane.Active[0].OperationID != claimed.ID {
		t.Fatalf("expected active lane occupant %s, got %+v", claimed.ID, response.Diagnosis.ControllerLane.Active)
	}
}

func TestGetOperationDiagnosisExplainsBuilderPlacementFailure(t *testing.T) {
	stateStore, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	server.inspectBuilderPlacement = func(ctx context.Context, namespace string, policy sourceimport.BuilderPodPolicy, profile, buildStrategy string, stateful bool, requiredNodeLabels map[string]string) (model.BuilderPlacementInspection, error) {
		return model.BuilderPlacementInspection{
			Profile:       profile,
			BuildStrategy: buildStrategy,
			Demand: model.BuilderResourceSnapshot{
				CPUMilli:       750,
				MemoryBytes:    1 << 30,
				EphemeralBytes: 3 << 30,
			},
			Reservations: []model.BuilderPlacementReservationInspection{
				{Name: "reservation-a", NodeName: "gcp1"},
			},
			Locks: []model.BuilderPlacementLockInspection{
				{Name: "lock-gcp1", NodeName: "gcp1", HolderIdentity: "build-demo"},
			},
			Nodes: []model.BuilderPlacementNodeInspection{
				{NodeName: "gcp1", Eligible: false, Ready: true, Reasons: []string{"DiskPressure=True"}},
				{NodeName: "gcp2", Eligible: true, Ready: true, Rank: 1},
			},
		}, nil
	}

	source := model.AppSource{
		Type:          model.AppSourceTypeUpload,
		BuildStrategy: model.AppBuildStrategyDockerfile,
	}
	spec := app.Spec
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "tester",
		AppID:           app.ID,
		DesiredSpec:     &spec,
		DesiredSource:   &source,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}
	op, err = stateStore.FailOperation(op.ID, "select builder placement: no eligible builder nodes for profile heavy")
	if err != nil {
		t.Fatalf("fail import operation: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Diagnosis model.OperationDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Diagnosis.Category != "builder-no-eligible-nodes" {
		t.Fatalf("expected builder-no-eligible-nodes diagnosis, got %+v", response.Diagnosis)
	}
	if response.Diagnosis.BuilderPlacement == nil {
		t.Fatalf("expected builder placement inspection, got %+v", response.Diagnosis)
	}
	if response.Diagnosis.BuilderPlacement.Profile != "heavy" {
		t.Fatalf("expected heavy builder profile, got %+v", response.Diagnosis.BuilderPlacement)
	}
	joinedEvidence := strings.Join(response.Diagnosis.Evidence, "\n")
	for _, want := range []string{
		"active builder reservations: reservation-a@gcp1",
		"active builder locks: gcp1 held by build-demo",
		"excluded nodes: gcp1: DiskPressure=True",
	} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence to contain %q, got %+v", want, response.Diagnosis.Evidence)
		}
	}
}
