package api

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
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

func TestDiagnosePendingImageReplicationDoesNotReportQueueETA(t *testing.T) {
	t.Parallel()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Replication Diagnosis Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "replication-diagnosis", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "replication-app", "", model.AppSpec{Image: "nginx:latest", Replicas: 1})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	spec := app.Spec
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &spec,
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	if _, claimed, err := stateStore.TryClaimPendingOperation(op.ID); err != nil || !claimed {
		t.Fatalf("claim operation: claimed=%v err=%v", claimed, err)
	}
	if _, err := stateStore.RequeueManagedOperation(op.ID, model.OperationResultDeployImageReplicationPending+": image is being replicated"); err != nil {
		t.Fatalf("requeue operation: %v", err)
	}
	op, err = stateStore.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("reload operation: %v", err)
	}
	server := &Server{store: stateStore}
	diagnosis, err := server.diagnosePendingOperation(op, model.App{}, false)
	if err != nil {
		t.Fatalf("diagnose operation: %v", err)
	}
	if diagnosis.Category != "deploy-image-replication-pending" {
		t.Fatalf("category = %q, want deploy-image-replication-pending", diagnosis.Category)
	}
	if diagnosis.ControllerLane != nil {
		t.Fatalf("expected no queue ETA for replication wait, got %+v", diagnosis.ControllerLane)
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
	observedAt := time.Now().UTC().Add(-time.Hour)
	inspection := model.BuilderPlacementInspection{
		ObservedAt:    &observedAt,
		Profile:       "heavy",
		BuildStrategy: "dockerfile",
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
	_, err = stateStore.RecordOperationEvidence(model.OperationEvidence{
		TenantID: app.TenantID, AppID: app.ID, OperationID: op.ID,
		Type: model.OperationEvidenceTypeBuildAttempt, Source: "import_controller", Severity: "error", Confidence: "confirmed",
		ObservedAt: observedAt, Summary: "Build attempt rejected", PayloadVersion: 1,
		Payload: map[string]any{"build_attempt": model.BuilderAttemptEvidence{Attempt: 3, Outcome: "rejected"},
			"builder_diagnostics": map[string]any{"builder_placement": inspection}},
	})
	if err != nil {
		t.Fatal(err)
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
	if !response.Diagnosis.BuilderPlacement.ObservedAt.Equal(observedAt) {
		t.Fatal("diagnosis replaced the historical observation with current state")
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
	if category := diagnosisBuilderPlacementCategory("select builder placement: no eligible builder nodes: insufficient resources for profile heavy (minimum memory=536870912 bytes)"); category != "builder-insufficient-resources" {
		t.Fatalf("capacity failure mislabeled as node eligibility: %s", category)
	}
	if summary := diagnosisBuilderPlacementSummary("builder-insufficient-resources", "heavy", "dockerfile", ""); !strings.Contains(summary, "unreserved") || strings.Contains(summary, "readiness") {
		t.Fatalf("capacity failure got misleading diagnosis: %s", summary)
	}
	// A later rejection without a snapshot must never inherit an earlier retry's
	// profile, OOM floor, storage tier or node observations.
	_, err = stateStore.RecordOperationEvidence(model.OperationEvidence{
		TenantID: app.TenantID, AppID: app.ID, OperationID: op.ID,
		Type: model.OperationEvidenceTypeBuildAttempt, Source: "import_controller", Severity: "error", Confidence: "confirmed",
		Summary: "Build attempt 4 rejected", PayloadVersion: 1,
		Payload: map[string]any{"build_attempt": model.BuilderAttemptEvidence{Attempt: 4, Outcome: "rejected"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/diagnosis", apiKey, nil)
	response.Diagnosis = model.OperationDiagnosis{}
	mustDecodeJSON(t, recorder, &response)
	if response.Diagnosis.BuilderPlacement != nil || response.Diagnosis.Confidence != model.OperationEvidenceConfidenceInsufficientEvidence || len(response.Diagnosis.MissingEvidence) == 0 {
		t.Fatalf("later attempt inherited stale admission inputs: %+v", response.Diagnosis)
	}
}

func TestLegacyBuilderAdmissionDiagnosisDoesNotGuessAutoProfile(t *testing.T) {
	state, server, key, app, op := setupOperationEvidenceAPITest(t)
	_, err := state.FailOperation(op.ID, "select builder placement: builder_memory_not_increased: no eligible memory allocation")
	if err != nil {
		t.Fatal(err)
	}
	response := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/diagnosis", key, nil)
	var payload struct {
		Diagnosis model.OperationDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, response, &payload)
	if response.Code != http.StatusOK || payload.Diagnosis.BuilderPlacement != nil || payload.Diagnosis.Confidence != model.OperationEvidenceConfidenceInsufficientEvidence || len(payload.Diagnosis.MissingEvidence) == 0 {
		t.Fatalf("legacy failure was reconstructed from guessed config: %+v app=%s", payload, app.ID)
	}
}

func TestOperationDiagnosisIncludesFailedBackupHint(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Backup Diagnosis Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "ops", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: "runtime_managed_shared",
		Replicas:  1,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "reader", []string{"app.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backup backend: %v", err)
	}
	target := model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: tenant.ID, ProjectID: project.ID, AppID: app.ID, Database: "appdb"}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		TenantID:  tenant.ID,
		ProjectID: project.ID,
		AppID:     app.ID,
		Name:      "app-db",
		Target:    target,
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
	})
	if err != nil {
		t.Fatalf("create backup policy: %v", err)
	}
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		PolicyID:     policy.ID,
		TenantID:     tenant.ID,
		ProjectID:    project.ID,
		AppID:        app.ID,
		Target:       target,
		BackendID:    backend.ID,
		Trigger:      model.BackupRunTriggerManual,
		Status:       model.BackupRunStatusFailed,
		ErrorCode:    "pg_dump_failed",
		ErrorMessage: "pg_dump exited non-zero",
	})
	if err != nil {
		t.Fatalf("create failed backup run: %v", err)
	}
	runs, err := stateStore.ListBackupRuns(store.BackupRunFilter{AppID: app.ID, PlatformAdmin: true, Limit: 20})
	if err != nil {
		t.Fatalf("list backup runs: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != run.ID {
		t.Fatalf("expected failed backup run to be app-visible, got %+v", runs)
	}
	spec := app.Spec
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &spec,
	})
	if err != nil {
		t.Fatalf("create op: %v", err)
	}
	op, err = stateStore.FailOperation(op.ID, "deploy failed")
	if err != nil {
		t.Fatalf("fail op: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Diagnosis model.OperationDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &response)
	joinedEvidence := strings.Join(response.Diagnosis.Evidence, "\n")
	for _, want := range []string{"backup failed", run.ID, "pg_dump_failed"} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected backup evidence %q, got %+v", want, response.Diagnosis)
		}
	}
	if !strings.Contains(response.Diagnosis.Hint, "fugue app backup status/show") {
		t.Fatalf("expected backup hint, got %+v", response.Diagnosis)
	}
}
