package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/model"
	"fugue/internal/pgretire"
)

func TestSourceRetirementCLIRequiresExactConfirmation(t *testing.T) {
	for _, flags := range [][]string{{"--apply"}, {"--apply", "--expected-source-uid", "source"}, {"--apply", "--expected-plan-hash", "hash"}} {
		var out, stderr bytes.Buffer
		args := append([]string{"--base-url", "http://127.0.0.1:1", "--token", "test", "service", "postgres", "retire-source", "main-db"}, flags...)
		if runWithStreams(args, &out, &stderr) == nil {
			t.Fatal("unsafe confirmation accepted")
		}
	}
	for _, apply := range []bool{false, true} {
		p := model.PostgresSourceRetirementPlan{ServiceID: "svc_pg", AppID: "client", SourceUID: "source-uid", TargetUID: "target-uid", PreserveStorage: true}
		p.ProtectedVolumes = []model.PostgresSourceRetirementProtectedVolume{{PostgresSourceRetirementVolume: model.PostgresSourceRetirementVolume{PVC: "unowned-archive", PVCUID: "protected-claim", PV: "protected-pv", PVUID: "protected-volume"}, ConfigurationHash: "protected-configuration"}}
		p.PlanHash = pgretire.PlanHash(p)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/backing-services" {
				w.Write([]byte(resizeServiceListJSON))
				return
			}
			if r.URL.Path != "/v1/backing-services/svc_pg/retire-source" {
				t.Error("unexpected endpoint", r.URL.Path)
				w.WriteHeader(400)
				return
			}
			var request map[string]any
			json.NewDecoder(r.Body).Decode(&request)
			if request["apply"] != apply || (apply && (request["expected_source_uid"] != p.SourceUID || request["expected_plan_hash"] != p.PlanHash)) {
				t.Error("CLI confirmation changed", request)
			}
			response := map[string]any{"plan": p, "dry_run": !apply}
			if apply {
				response["operation"] = model.Operation{ID: "op", Type: pgretire.OperationType, ServiceID: p.ServiceID, Status: model.OperationStatusPending}
			}
			json.NewEncoder(w).Encode(response)
		}))
		args := []string{"--base-url", server.URL, "--token", "test", "--json", "service", "postgres", "retire-source", "main-db"}
		if apply {
			args = append(args, "--apply", "--wait=false", "--expected-source-uid", p.SourceUID, "--expected-plan-hash", p.PlanHash)
		}
		var out, stderr bytes.Buffer
		err := runWithStreams(args, &out, &stderr)
		server.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}
