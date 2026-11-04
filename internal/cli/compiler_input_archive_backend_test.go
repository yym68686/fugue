package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPlatformArchiveCLIRequiresExplicitExecution(t *testing.T) {
	for _, action := range []string{"status", "enable", "execute"} {
		t.Run(action, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/admin/storage/compiler-input-archive" {
					t.Error("wrong platform endpoint")
				}
				if action == "status" {
					if r.Method != "GET" {
						t.Error("status mutated")
					}
				} else {
					var body struct {
						Execute bool `json:"execute"`
					}
					_ = json.NewDecoder(r.Body).Decode(&body)
					if r.Method != "POST" || body.Execute != (action == "execute") {
						t.Error("execution intent changed")
					}
				}
				_, _ = w.Write([]byte(`{"configured":false,"executed":false,"state":"unconfigured"}`))
			}))
			defer srv.Close()
			command := action
			if action == "execute" {
				command = "enable"
			}
			args := []string{"--base-url", srv.URL, "--token", "fixture", "admin", "storage", "archive", command, "--json"}
			if action == "execute" {
				args = append(args, "--execute")
			}
			var stdout, stderr bytes.Buffer
			if err := runWithStreams(args, &stdout, &stderr); err != nil {
				t.Fatal(err)
			}
		})
	}
}
