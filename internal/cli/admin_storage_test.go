package cli

import (
	"bytes"
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
