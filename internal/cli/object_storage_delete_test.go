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

func TestS3DeletePreviewAndConfirmedResumableBatches(t *testing.T) {
	for _, scenario := range []string{"preview", "wait", "one-batch", "wrong-bucket", "ambiguous", "provider-failure"} {
		t.Run(scenario, func(t *testing.T) {
			v := model.ObjectStore{ID: "object_store_sample", Name: "events", Bucket: "fugue-objects-object-store-sample"}
			plans, deletes := 0, 0
			fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == "GET" && r.URL.Path == "/v1/object-stores":
					items := []model.ObjectStore{v}
					if scenario == "ambiguous" {
						other := v
						other.ID = "object_store_other"
						items = append(items, other)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"stores": items})
				case r.Method == "POST" && r.URL.Path == "/v1/object-stores/"+v.ID+"/deletion-plan":
					plans++
					_ = json.NewEncoder(w).Encode(model.ObjectStoreDeleteResponse{Store: v, Inventory: model.ObjectStoreDeleteInventory{Source: "cached", PlanSHA256: strings.Repeat("a", 64)}})
				case r.Method == "DELETE" && r.URL.Path == "/v1/object-stores/"+v.ID:
					deletes++
					var req map[string]any
					_ = json.NewDecoder(r.Body).Decode(&req)
					if req["confirm_bucket"] != v.Bucket || req["plan_sha256"] != strings.Repeat("a", 64) || req["ack_no_external_writers"] != true {
						t.Error("missing destructive confirmation")
					}
					if scenario == "provider-failure" {
						w.WriteHeader(502)
						w.Write([]byte(`{"error":"provider failed"}`))
						return
					}
					if deletes == 1 {
						w.WriteHeader(202)
					}
					_ = json.NewEncoder(w).Encode(model.ObjectStoreDeleteResponse{Store: v, Deleted: deletes > 1})
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL)
					w.WriteHeader(400)
				}
			}))
			defer fake.Close()
			args := []string{"--base-url", fake.URL, "--token", "synthetic", "--tenant-id", "tenant_sample", "--project-id", "project_sample", "--json", "s3", "delete", "events"}
			if scenario != "preview" && scenario != "ambiguous" {
				args = append(args, "--execute", "--confirm-bucket", v.Bucket, "--plan-sha256", strings.Repeat("a", 64), "--ack-no-external-writers")
			}
			if scenario == "wrong-bucket" {
				args = append(args, "--confirm-bucket", "wrong")
			}
			if scenario == "one-batch" {
				args = append(args, "--wait=false")
			}
			var stdout, stderr bytes.Buffer
			err := runWithStreams(args, &stdout, &stderr)
			wantError := scenario == "wrong-bucket" || scenario == "ambiguous" || scenario == "provider-failure"
			if (err != nil) != wantError {
				t.Fatalf("err=%v output=%s stderr=%s", err, stdout.String(), stderr.String())
			}
			wantDeletes := map[string]int{"wait": 2, "one-batch": 1, "provider-failure": 1}[scenario]
			if deletes != wantDeletes || (scenario == "preview" && plans != 1) || (scenario != "preview" && plans != 0) {
				t.Fatalf("plans=%d deletes=%d", plans, deletes)
			}
			if !wantError {
				var out model.ObjectStoreDeleteResponse
				if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
					t.Fatal(err)
				}
				if out.Deleted != (scenario == "wait") {
					t.Fatal("incorrect completion")
				}
			}
		})
	}
}

func TestS3DeleteRejectsIncompleteAcknowledgementsWithoutRequests(t *testing.T) {
	for _, flags := range [][]string{{"--execute"}, {"--execute", "--confirm-bucket", "target", "--plan-sha256", strings.Repeat("a", 64)}, {"--confirm-bucket", "target"}, {"--execute", "--confirm-bucket", "target", "--plan-sha256", strings.Repeat("z", 64), "--ack-no-external-writers"}, {"--max-batches", "0"}} {
		var stdout, stderr bytes.Buffer
		args := append([]string{"--base-url", "http://127.0.0.1:1", "--token", "synthetic", "s3", "delete", "events"}, flags...)
		err := runWithStreams(args, &stdout, &stderr)
		if err == nil || strings.Contains(err.Error(), "connect") {
			t.Fatalf("bad flags contacted API: %v", err)
		}
	}
}
