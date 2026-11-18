package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestAppStorageUnmountPlanAndAtomicExecution(t *testing.T) {
	spec := appObservabilityTestSpec()
	spec.RuntimeID = "runtime_managed_shared"
	spec.Replicas = 1
	spec.Ports = []int{8080}
	spec.Env = map[string]string{"PROBE_TOKEN": "not-for-api-output"}
	spec.PersistentStorage = &model.AppPersistentStorageSpec{
		Mode: model.AppPersistentStorageModeMovableRWO, StorageClassName: model.AppStorageClassFugueLocalRWO, StorageSize: "1Gi",
		Mounts: []model.AppPersistentStorageMount{
			{Kind: "file", Path: "/etc/app.conf", Secret: true, Mode: 0o400, SeedContent: "do-not-expose"},
			{Kind: "directory", Path: "/data", Mode: 0o755},
		},
	}
	st, server, key, app := setupAppConfigTestServer(t, spec)
	path := "/v1/apps/" + app.ID + "/storage/unmount"
	rep := performJSONRequest(t, server, http.MethodGet, path+"?path="+url.QueryEscape("/etc/app.conf"), key, nil)
	if rep.Code != http.StatusOK {
		t.Fatalf("preview HTTP %d: %s", rep.Code, rep.Body)
	}
	var plan model.AppStorageUnmountPlan
	if err := json.Unmarshal(rep.Body.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	if !plan.Allowed || !plan.ReusePVC || plan.ClaimName == "" || plan.Path != "/etc/app.conf" ||
		!reflect.DeepEqual(plan.RetainedMounts, []string{"/data"}) || strings.Contains(rep.Body.String(), "do-not-expose") {
		t.Fatalf("unexpected plan: %s", rep.Body)
	}
	tag := rep.Header().Get("ETag")
	if tag != `"`+plan.SpecSHA256+`"` {
		t.Fatalf("ETag is not bound to the read-only spec plan: %q", tag)
	}
	if ops, err := st.ListOperationsByApp(app.TenantID, false, app.ID); err != nil || len(ops) != 0 {
		t.Fatalf("preview changed operation state: %v (%d)", err, len(ops))
	}
	body := `{"path":"/etc/app.conf","authorize_overlap":true,"probe":{"path":"/business","bearer_env":"PROBE_TOKEN","expected_body_contains":"opaque-probe-value"}}`
	post := func(match, token, raw string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(raw))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("If-Match", match)
		r.Header.Set("Content-Type", "application/json")
		out := httptest.NewRecorder()
		server.Handler().ServeHTTP(out, r)
		return out
	}
	readOnly, readToken, err := st.CreateAPIKey(app.TenantID, "read-only", []string{"app.read"})
	if err != nil || readOnly.ID == "" {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		tag, key, body string
		status         int
	}{
		{"", key, body, http.StatusBadRequest},
		{`"` + strings.Repeat("0", 64) + `"`, key, body, http.StatusPreconditionFailed},
		{tag, readToken, body, http.StatusForbidden},
		{tag, key, strings.Replace(body, "true", "false", 1), http.StatusBadRequest},
	} {
		if out := post(tt.tag, tt.key, tt.body); out.Code != tt.status {
			t.Errorf("expected HTTP %d, got %d: %s", tt.status, out.Code, out.Body)
		}
	}
	first := post(tag, key, body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("execute HTTP %d: %s", first.Code, first.Body)
	}
	if strings.Contains(first.Body.String(), "opaque-probe-value") || strings.Contains(first.Body.String(), "not-for-api-output") || strings.Contains(first.Body.String(), "do-not-expose") {
		t.Fatalf("execution returned private probe or app data")
	}
	if second := post(tag, key, body); second.Code != http.StatusPreconditionFailed {
		t.Fatalf("duplicate operation was not fenced: %d %s", second.Code, second.Body)
	}
	queued, err := st.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil || len(queued) != 1 {
		t.Fatalf("expected one operation, got %d: %v", len(queued), err)
	}
	op := queued[0]
	expected := app.Spec.DeepCopy()
	expected.PersistentStorage.Mounts = expected.PersistentStorage.Mounts[1:]
	if op.Type != model.OperationTypeAppStorageUnmount || op.ExecutionMode != model.ExecutionModeManaged || op.StorageUnmount == nil ||
		model.AppSpecSHA256(*op.DesiredSpec) != model.AppSpecSHA256(expected) {
		t.Fatalf("unmount did not queue the exact managed storage mutation")
	}
	current, err := st.GetApp(app.ID)
	if err != nil || model.AppSpecSHA256(current.Spec) != plan.SpecSHA256 || current.Status.Phase != app.Status.Phase {
		t.Fatalf("queuing unmount changed committed app spec before verification: %v", err)
	}
}
