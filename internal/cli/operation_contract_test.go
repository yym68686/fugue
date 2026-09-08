package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOperationWaitUsesTerminalStateInTextAndJSON(t *testing.T) {
	for _, jsonMode := range []bool{false, true} {
		for _, status := range []string{"completed", "failed"} {
			t.Run(fmt.Sprintf("%t/%s", jsonMode, status), func(t *testing.T) {
				var calls atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					s := "pending"
					if calls.Add(1) > 1 {
						s = status
					}
					fmt.Fprintf(w, `{"operation":{"id":"op_test","status":%q}}`, s)
				}))
				defer srv.Close()
				args := []string{"--base-url", srv.URL, "--token", "test", "operation", "wait", "op_test", "--interval", "1ms", "--timeout", "1s"}
				if jsonMode {
					args = append(args, "--json")
				}
				var out, stderr bytes.Buffer
				err := runWithStreams(args, &out, &stderr)
				if calls.Load() != 2 {
					t.Fatalf("got %d requests", calls.Load())
				}
				want := 0
				if status == "failed" {
					want = ExitCodeSystemFault
				}
				if ExitCodeForError(err) != want {
					t.Fatalf("exit=%d err=%v", ExitCodeForError(err), err)
				}
				if jsonMode {
					var payload map[string]any
					if err := json.Unmarshal(out.Bytes(), &payload); err != nil {
						t.Fatal(err)
					}
					if payload["operation"].(map[string]any)["status"] != status {
						t.Fatal(payload)
					}
				}
			})
		}
	}
}
func TestOperationWaitCancelsInflightReadAtDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer srv.Close()
	client, _ := newClientWithOptions(srv.URL, "test", clientOptions{RequireToken: true})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	last, err := waitOperation(ctx, client, "op_test", time.Millisecond)
	if time.Since(start) > time.Second || last.ID != "op_test" || ExitCodeForError(err) != 6 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("last=%+v err=%v", last, err)
	}
}
func TestHTTPErrorClassificationIgnoresMessage(t *testing.T) {
	for status, want := range map[int]int{400: 2, 401: 3, 403: 3, 404: 4, 409: 2, 412: 2, 422: 2, 429: 5, 500: 5, 502: 5, 503: 5, 504: 5} {
		err := &apiServerError{StatusCode: status, Response: apiError{Error: "opaque failure"}}
		if got := ExitCodeForError(err); got != want {
			t.Errorf("%d: got %d want %d", status, got, want)
		}
	}
}
func TestUnknownFlagEmitsSingleJSONFailure(t *testing.T) {
	var out, stderr bytes.Buffer
	err := runWithStreams([]string{"operation", "show", "op_test", "--typo", "--json"}, &out, &stderr)
	if ExitCodeForError(err) != 2 {
		t.Fatalf("%v", err)
	}
	var payload map[string]any
	if e := json.Unmarshal(out.Bytes(), &payload); e != nil {
		t.Fatalf("%v %s", e, out.String())
	}
}
func TestRemovedLeafDoesNotMakeBusinessRequests(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("removed command made an HTTP request") }))
	defer srv.Close()
	var out, stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", srv.URL, "--token", "synthetic-private-token", "env", "ls", "demo", "--json"}, &out, &stderr)
	if ExitCodeForError(err) != ExitCodeUserInput {
		t.Fatal(err)
	}
	var payload map[string]any
	if json.Unmarshal(out.Bytes(), &payload) != nil {
		t.Fatal(out.String())
	}
	if !strings.Contains(out.String(), "fugue app env ls") || strings.Contains(out.String(), "synthetic-private-token") {
		t.Fatal(out.String())
	}
}

func TestOverviewRecordsMissingEvidenceAndPolicyRedactsSpec(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		app := `{"id":"app_test","name":"demo","spec":{"env":{"TOKEN":"synthetic-secret"}}}`
		switch r.URL.Path {
		case "/v1/apps":
			fmt.Fprintf(w, `{"apps":[%s]}`, app)
		case "/v1/apps/app_test":
			fmt.Fprintf(w, `{"app":%s}`, app)
		default:
			w.WriteHeader(403)
			fmt.Fprint(w, `{"error":"opaque"}`)
		}
	}))
	defer srv.Close()
	for _, args := range [][]string{{"app", "overview", "demo", "--require-complete"}, {"app", "image", "retention", "show", "demo"}} {
		var out, stderr bytes.Buffer
		command := append([]string{"--base-url", srv.URL, "--token", "test", "--json"}, args...)
		err := runWithStreams(command, &out, &stderr)
		if strings.Contains(out.String(), "synthetic-secret") {
			t.Fatal("unredacted embedded app")
		}
		var payload map[string]any
		if decodeErr := json.Unmarshal(out.Bytes(), &payload); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if args[1] == "overview" {
			if ExitCodeForError(err) != 6 || payload["completeness"] != "partial" || len(payload["missing_evidence"].([]any)) == 0 {
				t.Fatalf("%v %s", err, out.String())
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
}
