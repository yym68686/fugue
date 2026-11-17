package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestRecoveryCLIInspectAndReleaseUseCapturedIdentity(t *testing.T) {
	post := 0
	app := model.App{ID: "app_fixture", Name: "gateway", Spec: model.AppSpec{Replicas: 1}}
	record := model.AppRecovery{ID: "recovery_fixture", AppID: app.ID, Phase: "retained", Namespace: "tenant-fixture", EvidenceStatus: "intact", SourcePods: []model.AppRecoveryPod{{Name: "original-pod", UID: "original-uid", Node: "worker", Containers: []model.AppRecoveryContainer{{Name: "business-worker", ID: "containerd://original", RestartCount: 0}}}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("missing API authorization")
		}
		switch r.URL.Path {
		case "/v1/apps":
			json.NewEncoder(w).Encode(map[string]any{"apps": []model.App{app}})
		case "/v1/apps/app_fixture/recoveries/recovery_fixture":
			if r.Method != http.MethodGet {
				t.Error("inspect mutated resources")
			}
			json.NewEncoder(w).Encode(record)
		case "/v1/apps/app_fixture/recoveries/recovery_fixture/release":
			if r.Method != http.MethodPost {
				t.Error("release used wrong method")
			}
			post++
			json.NewEncoder(w).Encode(record)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer server.Close()
	for _, action := range []string{"inspect", "release"} {
		var out, stderr bytes.Buffer
		err := runWithStreams([]string{"--base-url", server.URL, "--token", "fixture-token", "app", "recovery", action, app.ID, record.ID}, &out, &stderr)
		if err != nil {
			t.Fatal(err, stderr.String())
		}
		if action == "inspect" && (!strings.Contains(out.String(), "UID=original-uid") || !strings.Contains(out.String(), "--container business-worker")) {
			t.Fatal("investigation command lost container identity", out.String())
		}
	}
	if post != 1 {
		t.Fatal("release intent not submitted exactly once", post)
	}
}
