package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminStorageCommandsAreReadOnlyAndKeepUnknownSources(t *testing.T) {
	for _, command := range []string{"overview", "ls", "reconcile"} {
		t.Run(command, func(t *testing.T) {
			requests := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != http.MethodGet || r.URL.Path != "/v1/admin/storage" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"buckets_complete":false,"metrics_status":"unavailable","billing_status":"unavailable","buckets":[],"namespaces":[],"longhorn":[],"warnings":["partial inventory"]}`))
			}))
			defer srv.Close()
			var stdout, stderr bytes.Buffer
			if err := runWithStreams([]string{"--base-url", srv.URL, "--token", "test-token", "admin", "storage", command, "--json"}, &stdout, &stderr); err != nil {
				t.Fatal(err)
			}
			if requests != 1 || !strings.Contains(stdout.String(), `"billing_status": "unavailable"`) || !strings.Contains(stdout.String(), `"buckets_complete": false`) {
				t.Fatalf("unexpected output requests=%d %s", requests, stdout.String())
			}
		})
	}
}

func TestAdminOrphanManifestAdoptionUsesBothPreviewVersions(t *testing.T) {
	for _, mode := range []string{"preview", "confirm", "blocked", "missing-version"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != http.MethodPost || r.URL.Path != "/v1/admin/storage/orphan-manifests/adopt" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				execute, _ := body["execute"].(bool)
				if calls == 1 && execute {
					t.Error("first request must preview")
				}
				if calls == 2 && (!execute || body["expected_etag"] != "manifest-version" || body["expected_config_etag"] != "config-version") {
					t.Error("execution omitted preview identity")
				}
				version := "config-version"
				if mode == "missing-version" {
					version = ""
				}
				json.NewEncoder(w).Encode(map[string]any{"adoption": map[string]any{"can_adopt": mode != "blocked", "etag": "manifest-version", "config_etag": version, "executed": execute, "protected": true, "restore_verification": "not_run", "blockers": []string{}}})
			}))
			defer srv.Close()
			args := []string{"--base-url", srv.URL, "--token", "test", "admin", "storage", "orphan-manifest", "adopt", "backup_run_fixture", "--object-key", "backup-root/control-plane/backup_run_fixture/manifest.json", "--json"}
			if mode != "preview" {
				args = append(args, "--confirm")
			}
			var stdout, stderr bytes.Buffer
			err := runWithStreams(args, &stdout, &stderr)
			if (err != nil) != (mode == "blocked" || mode == "missing-version") {
				t.Fatalf("mode=%s error=%v", mode, err)
			}
			wantCalls := 1
			if mode == "confirm" {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("calls=%d want=%d", calls, wantCalls)
			}
		})
	}
}

func TestAdminOrphanManifestCleanupPreviewsAndRechecksBeforeDelete(t *testing.T) {
	for _, test := range []struct {
		name             string
		confirm          bool
		previewCanDelete bool
		wantRequests     int
		wantExecuted     bool
		wantError        bool
	}{
		{name: "preview only", previewCanDelete: true, wantRequests: 1},
		{name: "confirm rejected by reconciliation", confirm: true, previewCanDelete: false, wantRequests: 1, wantError: true},
		{name: "confirm rechecks and executes", confirm: true, previewCanDelete: true, wantRequests: 2, wantExecuted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != http.MethodPost || r.URL.Path != "/v1/admin/storage/orphan-manifests/cleanup" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
				}
				var req struct {
					RunID        string `json:"run_id"`
					ObjectKey    string `json:"object_key"`
					Execute      bool   `json:"execute"`
					ExpectedETag string `json:"expected_etag"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("decode request: %v", err)
				}
				if req.RunID != "backup_run_1" || req.ObjectKey != "backup-root/control-plane/backup_run_1/manifest.json" {
					t.Errorf("unexpected cleanup request: %+v", req)
				}
				if requests == 1 && req.Execute {
					t.Error("first call must be a preview")
				}
				if requests == 2 && !req.Execute {
					t.Error("second call must request server-side execution")
				}
				if requests == 2 && req.ExpectedETag != "manifest-etag" {
					t.Error("execution must be bound to the reviewed ETag")
				}
				w.Header().Set("Content-Type", "application/json")
				if req.Execute {
					_, _ = w.Write([]byte(`{"cleanup":{"run_id":"backup_run_1","object_key":"backup-root/control-plane/backup_run_1/manifest.json","run_status":"failed","can_delete":true,"executed":true,"blockers":[]}}`))
					return
				}
				canDelete := test.previewCanDelete
				blockers := `[]`
				if !canDelete {
					blockers = `["Longhorn remote backup still exists"]`
				}
				_, _ = fmt.Fprintf(w, `{"cleanup":{"run_id":"backup_run_1","object_key":"backup-root/control-plane/backup_run_1/manifest.json","etag":"manifest-etag","run_status":"failed","can_delete":%t,"executed":false,"blockers":%s}}`, canDelete, blockers)
			}))
			defer srv.Close()
			args := []string{"--base-url", srv.URL, "--token", "test-token", "admin", "storage", "orphan-manifest", "cleanup", "backup_run_1", "--object-key", "backup-root/control-plane/backup_run_1/manifest.json", "--json"}
			if test.confirm {
				args = append(args, "--confirm")
			}
			var stdout, stderr bytes.Buffer
			err := runWithStreams(args, &stdout, &stderr)
			if test.wantError && err == nil {
				t.Fatal("cleanup command unexpectedly succeeded")
			}
			if !test.wantError && err != nil {
				t.Fatalf("cleanup command failed: %v; stderr=%s", err, stderr.String())
			}
			if requests != test.wantRequests {
				t.Fatalf("requests=%d, want %d; stdout=%s", requests, test.wantRequests, stdout.String())
			}
			want := `"executed": ` + fmt.Sprint(test.wantExecuted)
			if !strings.Contains(stdout.String(), want) {
				t.Fatalf("output missing %s: %s", want, stdout.String())
			}
		})
	}
}
