package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStorageMaintenanceCommandPreviewAndTerminalFailure(t *testing.T) {
	for _, confirm := range []bool{false, true} {
		t.Run(map[bool]string{false: "preview", true: "execute"}[confirm], func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method == "POST" {
					var body struct {
						Execute bool   `json:"execute"`
						Kind    string `json:"kind"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if body.Execute != confirm || body.Kind != "retention" {
						t.Errorf("unsafe request %+v", body)
					}
					w.Write([]byte(`{"id":"job","status":"queued"}`))
				} else {
					w.Write([]byte(`{"id":"job","status":"failed","error":"safety interlock"}`))
				}
			}))
			defer srv.Close()
			args := []string{"--base-url", srv.URL, "--token", "test", "admin", "storage", "jobs", "run", "retention", "--json"}
			if confirm {
				args = append(args, "--confirm", "--wait")
			}
			var out, stderr bytes.Buffer
			err := runWithStreams(args, &out, &stderr)
			if (err != nil) != confirm || calls != map[bool]int{false: 1, true: 2}[confirm] {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}
