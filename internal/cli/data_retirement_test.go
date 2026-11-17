package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWorkspaceRetireCLIPreviewsBeforeConfirmation(t *testing.T) {
	for _, mode := range []string{"preview", "confirm", "blocked"} {
		t.Run(mode, func(t *testing.T) {
			calls, executed := 0, false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == "GET" {
					w.Write([]byte(`{"workspace":{"id":"ws-fixture","name":"dataset"}}`))
					return
				}
				calls++
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				execute, _ := body["execute"].(bool)
				if execute {
					executed = true
					if calls != 2 {
						t.Error("did not preview first")
					}
					if body["cancel_planned_transfers"] != true {
						t.Error("explicit option lost")
					}
					w.WriteHeader(202)
					w.Write([]byte(`{"id":"job-fixture","status":"queued","workspace_retirement":{"phase":"evicting"}}`))
					return
				}
				if mode == "blocked" {
					w.Write([]byte(`{"allowed":false,"blockers":[{"kind":"app"}]}`))
				} else {
					w.Write([]byte(`{"allowed":true,"estimated_unique_bytes":12}`))
				}
			}))
			defer srv.Close()
			args := []string{"--base-url", srv.URL, "--token", "fixture-token", "data", "workspace", "retire", "dataset", "--json", "--cancel-planned-transfers"}
			if mode != "preview" {
				args = append(args, "--confirm")
			}
			var out, stderr bytes.Buffer
			err := runWithStreams(args, &out, &stderr)
			if mode == "blocked" {
				if err == nil || executed {
					t.Fatal("blockers bypassed", err)
				}
			} else if err != nil {
				t.Fatal(err, stderr.String())
			}
			if executed != (mode == "confirm") {
				t.Fatal("unexpected execution", executed)
			}
			if mode == "preview" && !strings.Contains(out.String(), "estimated_unique_bytes") {
				t.Fatal(out.String())
			}
		})
	}
}
