package api

import (
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func setupOperationEvidenceAPITest(t *testing.T) (*store.Store, *Server, string, model.App, model.Operation) {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Evidence API Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)
	if _, err := stateStore.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    4000,
		MemoryMebibytes:  8192,
		StorageGibibytes: 80,
	}); err != nil {
		t.Fatalf("raise evidence test billing cap: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "ops", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "reader", []string{"app.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "api", "", model.AppSpec{
		Image:     "ghcr.io/example/api",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Env: map[string]string{
			"API_TOKEN": "debug-bundle-env-secret",
			"LOG_LEVEL": "info",
		},
		Files: []model.AppFile{{
			Path:    "/run/secret.txt",
			Content: "debug-bundle-file-secret",
			Secret:  true,
		}},
		PersistentStorage: &model.AppPersistentStorageSpec{
			Mounts: []model.AppPersistentStorageMount{{
				Path:        "/data/seed.txt",
				SeedContent: "debug-bundle-seed-secret",
				Secret:      true,
			}},
		},
		Postgres: &model.AppPostgresSpec{
			Database:    "api",
			User:        "api",
			Password:    "debug-bundle-app-postgres-secret",
			ServiceName: "api-db",
			RuntimeID:   "runtime_managed_shared",
		},
		RestartToken: "debug-bundle-restart-secret",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	service, err := stateStore.CreateBackingService(tenant.ID, project.ID, "shared-db", "", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{
			Database:    "shared",
			User:        "shared",
			Password:    "debug-bundle-service-postgres-secret",
			ServiceName: "shared-db",
			RuntimeID:   "runtime_managed_shared",
		},
	})
	if err != nil {
		t.Fatalf("create backing service: %v", err)
	}
	if _, err := stateStore.BindBackingService(tenant.ID, app.ID, service.ID, "shared", map[string]string{
		"DATABASE_URL": "postgres://debug-bundle-binding-secret",
	}); err != nil {
		t.Fatalf("bind backing service: %v", err)
	}
	spec := app.Spec
	spec.Env["OP_TOKEN"] = "debug-bundle-operation-env-secret"
	spec.RestartToken = "debug-bundle-operation-restart-secret"
	op, err := stateStore.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDeploy, AppID: app.ID, DesiredSpec: &spec})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	return stateStore, server, apiKey, app, op
}

func TestOperationEvidenceAPIListsTimelineBundleAndEnforcesTenant(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, op := setupOperationEvidenceAPITest(t)
	const evidencePayloadSecret = "operation-evidence-payload-secret-sentinel"
	recorded, err := stateStore.RecordOperationEvidence(model.OperationEvidence{
		TenantID:        app.TenantID,
		ProjectID:       app.ProjectID,
		AppID:           app.ID,
		OperationID:     op.ID,
		Type:            model.OperationEvidenceTypeRolloutPreviousLogs,
		Source:          model.OperationEvidenceSourceAppLogs,
		Severity:        model.OperationEvidenceSeverityError,
		Confidence:      model.OperationEvidenceConfidenceConfirmed,
		Summary:         "captured previous logs",
		Message:         "startup failed; DATABASE_URL=postgres://api:" + evidencePayloadSecret + "@database:5432/app",
		RedactionStatus: model.OperationEvidenceRedactionRedacted,
		Payload: map[string]any{
			"log_tail": "startup failed: apply schema",
			"snapshot": map[string]any{
				"spec": map[string]any{
					"env":      map[string]any{"API_TOKEN": evidencePayloadSecret, "LOG_LEVEL": "debug"},
					"postgres": map[string]any{"password": evidencePayloadSecret},
					"files": []any{map[string]any{
						"path":    "/run/secret",
						"content": evidencePayloadSecret,
						"secret":  true,
					}},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("record evidence: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/evidence", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected evidence status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var evidenceResponse struct {
		Evidence []model.OperationEvidence `json:"evidence"`
	}
	mustDecodeJSON(t, recorder, &evidenceResponse)
	if len(evidenceResponse.Evidence) != 1 || evidenceResponse.Evidence[0].ID != recorded.ID {
		t.Fatalf("expected recorded evidence, got %+v", evidenceResponse.Evidence)
	}
	if len(evidenceResponse.Evidence[0].Payload) != 0 {
		t.Fatalf("expected payload omitted by default, got %+v", evidenceResponse.Evidence[0].Payload)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/evidence?include_payload=true", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected evidence payload status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &evidenceResponse)
	if evidenceResponse.Evidence[0].Payload["log_tail"] != "startup failed: apply schema" {
		t.Fatalf("expected payload when requested, got %+v", evidenceResponse.Evidence[0].Payload)
	}
	if strings.Contains(recorder.Body.String(), evidencePayloadSecret) || !strings.Contains(recorder.Body.String(), apiRedactedSecretValue) {
		t.Fatalf("operation evidence payload was not deeply redacted: %s", recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/timeline", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected timeline status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), model.OperationEvidenceTypeOperationCreated) || !strings.Contains(recorder.Body.String(), recorded.ID) {
		t.Fatalf("expected timeline to include operation and evidence, got %s", recorder.Body.String())
	}
	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/timeline?include_payload=true", apiKey, nil)
	if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), evidencePayloadSecret) || !strings.Contains(recorder.Body.String(), apiRedactedSecretValue) {
		t.Fatalf("operation timeline payload was not deeply redacted: status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/debug-bundle", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected debug bundle status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "operation_debug_bundle") || !strings.Contains(recorder.Body.String(), recorded.ID) {
		t.Fatalf("expected debug bundle metadata/evidence, got %s", recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), evidencePayloadSecret) {
		t.Fatalf("operation debug bundle leaked evidence payload secret: %s", recorder.Body.String())
	}
	assertDebugBundleSecretsRedacted(t, recorder.Body.String())

	attempt, err := stateStore.CreateReleaseAttempt(model.ReleaseAttempt{
		TenantID:          app.TenantID,
		ProjectID:         app.ProjectID,
		AppID:             app.ID,
		TriggerType:       model.ReleaseAttemptTriggerManualDeploy,
		TriggerActorType:  model.ReleaseAttemptActorUser,
		SourceOperationID: op.ID,
		RootOperationID:   op.ID,
		Status:            model.ReleaseAttemptStatusCompleted,
		Confidence:        model.OperationEvidenceConfidenceConfirmed,
		Summary:           "completed",
		DesiredSource:     map[string]any{"values": "redacted"},
	})
	if err != nil {
		t.Fatalf("create release attempt: %v", err)
	}
	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/release-attempts/"+attempt.ID+"/debug-bundle", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected release debug bundle status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "release_debug_bundle") || !strings.Contains(recorder.Body.String(), attempt.ID) {
		t.Fatalf("expected release debug bundle metadata/attempt, got %s", recorder.Body.String())
	}
	assertDebugBundleSecretsRedacted(t, recorder.Body.String())

	otherTenant, err := stateStore.CreateTenant("Other Tenant")
	if err != nil {
		t.Fatalf("create other tenant: %v", err)
	}
	_, otherKey, err := stateStore.CreateAPIKey(otherTenant.ID, "reader", []string{"app.read"})
	if err != nil {
		t.Fatalf("create other api key: %v", err)
	}
	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/evidence", otherKey, nil)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected tenant isolation status 403, got %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func assertDebugBundleSecretsRedacted(t *testing.T, body string) {
	t.Helper()

	for _, leaked := range []string{
		"debug-bundle-env-secret",
		"debug-bundle-file-secret",
		"debug-bundle-seed-secret",
		"debug-bundle-app-postgres-secret",
		"debug-bundle-restart-secret",
		"debug-bundle-service-postgres-secret",
		"debug-bundle-binding-secret",
		"debug-bundle-operation-env-secret",
		"debug-bundle-operation-restart-secret",
	} {
		if strings.Contains(body, leaked) {
			t.Fatalf("debug bundle leaked secret sentinel %q in body=%s", leaked, body)
		}
	}
	if !strings.Contains(body, apiRedactedSecretValue) {
		t.Fatalf("expected debug bundle to contain redacted markers, got %s", body)
	}
}

func TestOperationDiagnosisConfidenceUsesEvidenceAndMissingEvidence(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, op := setupOperationEvidenceAPITest(t)
	failed, err := stateStore.FailOperation(op.ID, "managed app rollout failed")
	if err != nil {
		t.Fatalf("fail operation: %v", err)
	}
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+failed.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected diagnosis status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var diagnosisResponse struct {
		Diagnosis model.OperationDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &diagnosisResponse)
	if diagnosisResponse.Diagnosis.Confidence != model.OperationEvidenceConfidenceInsufficientEvidence {
		t.Fatalf("expected insufficient evidence diagnosis, got %+v", diagnosisResponse.Diagnosis)
	}
	if !containsString(diagnosisResponse.Diagnosis.MissingEvidence, "previous_container_logs") {
		t.Fatalf("expected previous logs missing evidence, got %+v", diagnosisResponse.Diagnosis.MissingEvidence)
	}

	secondSpec := app.Spec
	secondOp, err := stateStore.CreateOperation(model.Operation{TenantID: app.TenantID, Type: model.OperationTypeDeploy, AppID: app.ID, DesiredSpec: &secondSpec})
	if err != nil {
		t.Fatalf("create second operation: %v", err)
	}
	failed, err = stateStore.FailOperation(secondOp.ID, "managed app rollout failed")
	if err != nil {
		t.Fatalf("fail second operation: %v", err)
	}
	evidence, err := stateStore.RecordOperationEvidence(model.OperationEvidence{
		TenantID:        app.TenantID,
		ProjectID:       app.ProjectID,
		AppID:           app.ID,
		OperationID:     failed.ID,
		Type:            model.OperationEvidenceTypeRolloutPreviousLogs,
		Source:          model.OperationEvidenceSourceAppLogs,
		Severity:        model.OperationEvidenceSeverityError,
		Confidence:      model.OperationEvidenceConfidenceConfirmed,
		Summary:         "captured previous logs",
		Message:         "startup failed",
		RedactionStatus: model.OperationEvidenceRedactionRedacted,
		Payload:         map[string]any{"log_tail": "startup failed: apply schema: ERROR: deadlock detected"},
	})
	if err != nil {
		t.Fatalf("record previous log evidence: %v", err)
	}
	diagnosisResponse = struct {
		Diagnosis model.OperationDiagnosis `json:"diagnosis"`
	}{}
	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+failed.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected confirmed diagnosis status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &diagnosisResponse)
	if diagnosisResponse.Diagnosis.Confidence != model.OperationEvidenceConfidenceConfirmed || diagnosisResponse.Diagnosis.PrimaryEvidenceID != evidence.ID {
		t.Fatalf("expected confirmed diagnosis with primary evidence, got %+v", diagnosisResponse.Diagnosis)
	}
	if diagnosisResponse.Diagnosis.ConfirmedCause == nil || diagnosisResponse.Diagnosis.ConfirmedCause.Category != "application_startup_failure" {
		t.Fatalf("expected confirmed application startup cause, got %+v", diagnosisResponse.Diagnosis.ConfirmedCause)
	}

	thirdSpec := app.Spec
	thirdOp, err := stateStore.CreateOperation(model.Operation{TenantID: app.TenantID, Type: model.OperationTypeDeploy, AppID: app.ID, DesiredSpec: &thirdSpec})
	if err != nil {
		t.Fatalf("create third operation: %v", err)
	}
	failed, err = stateStore.FailOperation(thirdOp.ID, "managed app rollout failed")
	if err != nil {
		t.Fatalf("fail third operation: %v", err)
	}
	_, err = stateStore.RecordOperationEvidence(model.OperationEvidence{
		TenantID:        app.TenantID,
		ProjectID:       app.ProjectID,
		AppID:           app.ID,
		OperationID:     failed.ID,
		Type:            model.OperationEvidenceTypeRolloutContainerTerminated,
		Source:          model.OperationEvidenceSourceRolloutObserver,
		Severity:        model.OperationEvidenceSeverityError,
		Confidence:      model.OperationEvidenceConfidenceConfirmed,
		Summary:         "container exited",
		Reason:          "Error",
		RedactionStatus: model.OperationEvidenceRedactionNone,
	})
	if err != nil {
		t.Fatalf("record container evidence: %v", err)
	}
	currentLogEvidence, err := stateStore.RecordOperationEvidence(model.OperationEvidence{
		TenantID:        app.TenantID,
		ProjectID:       app.ProjectID,
		AppID:           app.ID,
		OperationID:     failed.ID,
		Type:            model.OperationEvidenceTypeRolloutCurrentLogs,
		Source:          model.OperationEvidenceSourceAppLogs,
		Severity:        model.OperationEvidenceSeverityError,
		Confidence:      model.OperationEvidenceConfidenceConfirmed,
		Summary:         "captured current logs",
		Message:         "startup failed",
		RedactionStatus: model.OperationEvidenceRedactionRedacted,
		Payload:         map[string]any{"log_tail": "startup failed: apply schema: ERROR: deadlock detected"},
	})
	if err != nil {
		t.Fatalf("record current log evidence: %v", err)
	}
	diagnosisResponse = struct {
		Diagnosis model.OperationDiagnosis `json:"diagnosis"`
	}{}
	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+failed.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected current log diagnosis status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &diagnosisResponse)
	if diagnosisResponse.Diagnosis.PrimaryEvidenceID != currentLogEvidence.ID {
		t.Fatalf("expected current log evidence to beat container termination, got %+v", diagnosisResponse.Diagnosis)
	}
	if diagnosisResponse.Diagnosis.ConfirmedCause == nil || diagnosisResponse.Diagnosis.ConfirmedCause.Source != "current_container_logs" {
		t.Fatalf("expected current container log cause, got %+v", diagnosisResponse.Diagnosis.ConfirmedCause)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func TestOperationDiagnosisConfidenceUsesKubernetesEventAndProbableEvidence(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, op := setupOperationEvidenceAPITest(t)
	failed, err := stateStore.FailOperation(op.ID, "managed app rollout failed")
	if err != nil {
		t.Fatalf("fail operation: %v", err)
	}
	evidence, err := stateStore.RecordOperationEvidence(model.OperationEvidence{
		TenantID:        app.TenantID,
		ProjectID:       app.ProjectID,
		AppID:           app.ID,
		OperationID:     failed.ID,
		Type:            model.OperationEvidenceTypeImagePullFailure,
		Source:          model.OperationEvidenceSourceKubernetesAPI,
		Severity:        model.OperationEvidenceSeverityError,
		Confidence:      model.OperationEvidenceConfidenceConfirmed,
		Summary:         "ErrImagePull",
		Message:         "pull access denied",
		Reason:          "ErrImagePull",
		RedactionStatus: model.OperationEvidenceRedactionNone,
	})
	if err != nil {
		t.Fatalf("record image pull evidence: %v", err)
	}
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+failed.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected diagnosis status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var diagnosisResponse struct {
		Diagnosis model.OperationDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &diagnosisResponse)
	if diagnosisResponse.Diagnosis.Confidence != model.OperationEvidenceConfidenceConfirmed || diagnosisResponse.Diagnosis.PrimaryEvidenceID != evidence.ID {
		t.Fatalf("expected confirmed event diagnosis, got %+v", diagnosisResponse.Diagnosis)
	}
	if diagnosisResponse.Diagnosis.ConfirmedCause == nil || diagnosisResponse.Diagnosis.ConfirmedCause.Category != "image_pull_failure" {
		t.Fatalf("expected image pull confirmed cause, got %+v", diagnosisResponse.Diagnosis.ConfirmedCause)
	}

	spec := app.Spec
	secondOp, err := stateStore.CreateOperation(model.Operation{TenantID: app.TenantID, Type: model.OperationTypeDeploy, AppID: app.ID, DesiredSpec: &spec})
	if err != nil {
		t.Fatalf("create second operation: %v", err)
	}
	failed, err = stateStore.FailOperation(secondOp.ID, "managed app rollout failed")
	if err != nil {
		t.Fatalf("fail second operation: %v", err)
	}
	evidence, err = stateStore.RecordOperationEvidence(model.OperationEvidence{
		TenantID:        app.TenantID,
		ProjectID:       app.ProjectID,
		AppID:           app.ID,
		OperationID:     failed.ID,
		Type:            model.OperationEvidenceTypeRolloutPodFailure,
		Source:          model.OperationEvidenceSourceRolloutObserver,
		Severity:        model.OperationEvidenceSeverityError,
		Confidence:      model.OperationEvidenceConfidenceEvidenceBacked,
		Summary:         "pod failed without previous logs",
		RedactionStatus: model.OperationEvidenceRedactionNone,
	})
	if err != nil {
		t.Fatalf("record probable evidence: %v", err)
	}
	diagnosisResponse = struct {
		Diagnosis model.OperationDiagnosis `json:"diagnosis"`
	}{}
	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+failed.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected probable diagnosis status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &diagnosisResponse)
	if diagnosisResponse.Diagnosis.Confidence != model.OperationEvidenceConfidenceEvidenceBacked || diagnosisResponse.Diagnosis.PrimaryEvidenceID != evidence.ID {
		t.Fatalf("expected evidence-backed diagnosis, got %+v", diagnosisResponse.Diagnosis)
	}
	if diagnosisResponse.Diagnosis.ProbableCause == nil || diagnosisResponse.Diagnosis.ConfirmedCause != nil {
		t.Fatalf("expected probable cause only, got confirmed=%+v probable=%+v", diagnosisResponse.Diagnosis.ConfirmedCause, diagnosisResponse.Diagnosis.ProbableCause)
	}
}
