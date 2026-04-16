package api

import (
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
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
