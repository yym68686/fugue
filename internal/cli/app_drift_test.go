package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDriftCheckDoesNotTreatUnknownAsSuccess(t *testing.T) {
	for _, tc := range []struct {
		state string
		code  int
	}{{"in_sync", 0}, {"drifted", 5}, {"inconclusive", 6}} {
		t.Run(tc.state, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/apps" {
					fmt.Fprint(w, `{"apps":[{"id":"app_test","name":"demo"}]}`)
				} else {
					json.NewEncoder(w).Encode(model.AppRuntimeState{SchemaVersion: 1, AppID: "app_test", DesiredSpecHash: strings.Repeat("a", 64), State: tc.state})
				}
			}))
			defer srv.Close()
			var out, stderr bytes.Buffer
			err := runWithStreams([]string{"--base-url", srv.URL, "--token", "test", "--json", "app", "drift", "check", "demo"}, &out, &stderr)
			if ExitCodeForError(err) != tc.code {
				t.Fatalf("%v output=%s", err, out.String())
			}
		})
	}
}
func TestReconcilePlanNeverWritesAndApplySendsPrecondition(t *testing.T) {
	for _, apply := range []bool{false, true} {
		t.Run(fmt.Sprint(apply), func(t *testing.T) {
			writes := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == "POST":
					writes++
					if r.Header.Get("If-Match") != `"`+strings.Repeat("a", 64)+`"` {
						t.Error("missing CAS precondition")
					}
					w.WriteHeader(412)
					fmt.Fprint(w, `{"error":"intent changed"}`)
				case r.URL.Path == "/v1/apps":
					fmt.Fprint(w, `{"apps":[{"id":"app_test","name":"demo"}]}`)
				default:
					json.NewEncoder(w).Encode(model.AppRuntimeState{SchemaVersion: 1, AppID: "app_test", DesiredSpecHash: strings.Repeat("a", 64), State: "drifted"})
				}
			}))
			defer srv.Close()
			args := []string{"--base-url", srv.URL, "--token", "test", "--json", "app", "reconcile", "demo"}
			if apply {
				args = append(args, "--apply")
			}
			var out, stderr bytes.Buffer
			err := runWithStreams(args, &out, &stderr)
			if apply {
				if writes != 1 || ExitCodeForError(err) != 2 {
					t.Fatalf("writes=%d err=%v", writes, err)
				}
			} else if writes != 0 || err != nil {
				t.Fatalf("plan mutated state: %d %v", writes, err)
			}
		})
	}
}
