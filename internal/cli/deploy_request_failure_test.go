package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestDeployCommandWritesUploadRejectionResultWithoutOperationLookup(t *testing.T) {
	t.Setenv("FUGUE_SKIP_UPDATE_CHECK", "1")
	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "Dockerfile"), "FROM scratch\n")
	posts := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/apps":
			_, _ = fmt.Fprint(w, `{"apps":[{"id":"app_example","name":"example","tenant_id":"tenant_example","project_id":"project_example"}]}`)
		case "/v1/apps/app_example":
			_, _ = fmt.Fprint(w, `{"app":{"id":"app_example","name":"example","tenant_id":"tenant_example","project_id":"project_example"}}`)
		case "/v1/apps/import-upload":
			posts++
			w.Header().Set("X-Fugue-Edge-Request-Id", "edge_abcd_1234")
			w.WriteHeader(http.StatusRequestTimeout)
			_, _ = fmt.Fprint(w, `{"error":"source upload timed out","code":"request_timeout"}`)
		default:
			t.Errorf("unexpected lookup or retry: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	output := filepath.Join(t.TempDir(), "result.json")
	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", backend.URL, "--token", "test-only-token", "--json", "--output-file", output, "deploy", dir, "--app", "app_example"}, &stdout, &stderr)
	if ExitCodeForError(err) != ExitCodeSystemFault || posts != 1 {
		t.Fatalf("exit=%v submissions=%d output=%s stderr=%s", err, posts, stdout.String(), stderr.String())
	}
	var payload struct {
		Result deploymentResult `json:"result"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Result.Outcome != "failed" || payload.Result.FailedStage != "upload" || payload.Result.Request.RequestID != "edge_abcd_1234" || len(payload.Result.Operations) != 0 {
		t.Fatalf("unexpected result: %+v", payload.Result)
	}
	mirrored, err := os.ReadFile(output)
	if err != nil || string(mirrored) != stdout.String() {
		t.Fatal("failure result was not mirrored exactly")
	}
}

func TestUploadHTTPFailurePreservesSafeEvidenceWithoutRetry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body, outcome, cause string
		status                     int
	}{
		{"timeout", `{"error":"private database password=hidden","code":"request_timeout"}`, "failed", "request_timeout", 408},
		{"size", `{"error":"private storage address"}`, "failed", "request_too_large", 413},
		{"admission", `{"error":"private concurrency details"}`, "failed", "request_rate_limited", 429},
		{"server", `{"error":"private transaction details"}`, "unknown", "http_error_response", 503},
		{"proxy", `private proxy address`, "unknown", "http_error_response", 408},
		{"invalid-acceptance", `{private`, "unknown", "response_invalid", 202},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != http.MethodPost || r.URL.Path != "/v1/apps/import-upload" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				w.Header().Set("X-Fugue-Edge-Request-Id", "edge_abcdef_123456")
				w.Header().Set("X-Fugue-Trace-Id", strings.Repeat("a", 32))
				w.Header().Set("Set-Cookie", "secret-session")
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer backend.Close()
			client, err := newClientWithOptions(backend.URL, "test-token", clientOptions{RequireToken: true})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.ImportUpload(importUploadRequest{AppID: "app_example"}, "source.tgz", []byte("synthetic source"))
			if err == nil {
				t.Fatal("expected failure")
			}
			for _, jsonMode := range []bool{false, true} {
				var out bytes.Buffer
				c := newCLI(&out, &bytes.Buffer{})
				c.root.JSONOutput = jsonMode
				c.deployment = &deploymentCommandState{requestStarted: true, submissionStage: "upload"}
				renderedErr := c.renderDeploymentError(err)
				var resultErr *deploymentResultError
				if !errors.As(renderedErr, &resultErr) {
					t.Fatal(renderedErr)
				}
				result := resultErr.Result
				if result.Outcome != tc.outcome || result.Request.HTTPStatus != tc.status || result.Request.Stage != "upload" || result.Causes[0].Code != tc.cause {
					t.Fatalf("unexpected result: %+v", result)
				}
				if result.Request.RequestID != "edge_abcdef_123456" || result.Request.TraceID != strings.Repeat("a", 32) {
					t.Fatalf("lost correlation: %+v", result.Request)
				}
				if strings.Contains(out.String(), "private") || strings.Contains(out.String(), "secret-session") || strings.Contains(out.String(), "fugue operation result") || strings.Contains(out.String(), "final_operation_state") {
					t.Fatalf("unsafe or unusable result: %s", out.String())
				}
				wantCode := ExitCodeIndeterminate
				if tc.outcome == "failed" {
					wantCode = ExitCodeSystemFault
				}
				if ExitCodeForError(renderedErr) != wantCode {
					t.Fatalf("wrong exit code: %v", renderedErr)
				}
			}
			if requests != 1 {
				t.Fatalf("deployment was retried %d times", requests)
			}
		})
	}
}

func TestDeploymentTransportFailureAndObservationAreNotRejections(t *testing.T) {
	t.Parallel()
	for _, observing := range []bool{false, true} {
		c := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
		c.deployment = &deploymentCommandState{requestStarted: true, submissionStage: "upload"}
		if observing {
			c.deployment.operations = []model.Operation{{ID: "op_running", Status: "running"}}
		}
		for _, err := range []error{&url.Error{Op: "Post", URL: "https://private.test?token=secret", Err: context.DeadlineExceeded}, context.Canceled, &apiServerError{StatusCode: 408, Response: apiError{Error: "private"}}} {
			if !observing {
				var apiErr *apiServerError
				if errors.As(err, &apiErr) {
					continue
				}
			}
			result, code := c.deploymentRequestFailureResult(err)
			if result.Outcome != "unknown" || code != ExitCodeIndeterminate {
				t.Fatalf("claimed terminal state: %+v", result)
			}
			if observing && result.Request.Stage != "observation" {
				t.Fatalf("wrong stage: %+v", result.Request)
			}
			if strings.Contains(fmt.Sprint(result), "private.test") {
				t.Fatal("leaked transport URL")
			}
		}
	}
}

func TestUploadTruncatedResponseRetainsStatusAndCorrelation(t *testing.T) {
	t.Parallel()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.Header().Set("X-Fugue-Edge-Request-Id", "edge_abcdef_123456")
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprint(w, "{")
	}))
	defer backend.Close()
	client, _ := newClientWithOptions(backend.URL, "test-token", clientOptions{RequireToken: true})
	_, err := client.ImportUpload(importUploadRequest{}, "source.tgz", []byte("source"))
	c := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	c.deployment = &deploymentCommandState{requestStarted: true, submissionStage: "upload"}
	result, _ := c.deploymentRequestFailureResult(err)
	if result.Outcome != "unknown" || result.Request.HTTPStatus != 202 || result.Request.RequestID != "edge_abcdef_123456" || result.Causes[0].Code != "response_incomplete" {
		t.Fatalf("lost partial response evidence: %+v", result)
	}
	result, _ = c.deploymentRequestFailureResult(&apiServerError{StatusCode: 408, Headers: http.Header{"X-Fugue-Edge-Request-Id": {"edge_secret?token=private"}, "X-Fugue-Trace-Id": {"password=private"}}})
	if result.Request.RequestID != "" || result.Request.TraceID != "" {
		t.Fatal("unsafe correlation header escaped the allowlist")
	}
}
