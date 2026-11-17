package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestIncidentRecoveryAuthorizationAndPrivateProbeInput(t *testing.T) {
	st, server, key, app := setupAppConfigTestServer(t, model.AppSpec{Image: "registry.example/service:v1", RuntimeID: "runtime_managed_shared", Replicas: 1, Ports: []int{8080}})
	zero := int64(0)
	req := model.AppRecoveryStartRequest{AuthorizeOverlap: true, StorageStrategy: "fresh", RetentionSeconds: &zero, Probes: []model.AppReleaseProbe{{Path: "/business", Body: "private-business-payload", Headers: map[string]string{"Authorization": "Bearer private-probe-credential"}, ExpectedBodyContains: "accepted"}}}
	endpoint := "/v1/apps/" + app.ID + "/recoveries"
	_, readKey, err := st.CreateAPIKey(app.TenantID, "observer", []string{"app.read"})
	if err != nil {
		t.Fatal(err)
	}
	denied := performJSONRequest(t, server, http.MethodPost, endpoint, readKey, req)
	if denied.Code != http.StatusForbidden {
		t.Fatal("observer started recovery", denied.Code)
	}
	created := performJSONRequest(t, server, http.MethodPost, endpoint, key, req)
	if created.Code != http.StatusAccepted {
		t.Fatal(created.Code, created.Body.String())
	}
	var record model.AppRecovery
	if err := json.Unmarshal(created.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	for _, response := range []string{created.Body.String(), performJSONRequest(t, server, http.MethodGet, endpoint, key, nil).Body.String(), performJSONRequest(t, server, http.MethodGet, endpoint+"/"+record.ID, key, nil).Body.String()} {
		for _, secret := range []string{"private-probe-credential", "private-business-payload", "source_spec", "replacement_spec"} {
			if strings.Contains(response, secret) {
				t.Fatalf("private input leaked: %s", secret)
			}
		}
	}
	records, err := st.ListAppRecoveries(app.ID)
	if err != nil || len(records) != 1 || records[0].Request.Probes[0].Headers["Authorization"] != "Bearer private-probe-credential" {
		t.Fatal("execution input was not durably retained", err)
	}
	conflict := performJSONRequest(t, server, http.MethodPost, endpoint, key, req)
	if conflict.Code != http.StatusConflict {
		t.Fatal("duplicate recovery accepted", conflict.Code)
	}
	released := performJSONRequest(t, server, http.MethodPost, endpoint+"/"+record.ID+"/release", key, nil)
	if released.Code != http.StatusAccepted {
		t.Fatal(released.Code, released.Body.String())
	}
}
