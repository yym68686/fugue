package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDeployFailureReturnsSafeJSONAndOutputFile(t *testing.T) {
	for _, jsonMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "text", true: "json"}[jsonMode], func(t *testing.T) {
			mutations := 0
			sensitive := "node-secret-77 10.42.99.8 registry.private.svc.cluster.local password=hunter-secret"
			op := model.Operation{ID: "op_failed", AppID: "app_demo", Type: "import", Status: "failed", ErrorMessage: "kaniko job job-secret on node " + sensitive + ": context deadline exceeded"}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/apps":
					_ = json.NewEncoder(w).Encode(map[string]any{"apps": []model.App{{ID: "app_demo", Name: "demo", Source: &model.AppSource{Type: "docker-image"}}}})
				case "/v1/apps/app_demo":
					_ = json.NewEncoder(w).Encode(map[string]any{"app": model.App{ID: "app_demo", Name: "demo", Source: &model.AppSource{Type: "docker-image"}}})
				case "/v1/apps/app_demo/rebuild":
					mutations++
					_ = json.NewEncoder(w).Encode(map[string]any{"operation": model.Operation{ID: op.ID, AppID: op.AppID, Type: op.Type, Status: "pending"}})
				case "/v1/operations/op_failed":
					_ = json.NewEncoder(w).Encode(map[string]any{"operation": op})
				case "/v1/operations/op_failed/evidence":
					if r.Method != "GET" {
						t.Error("evidence read mutated state")
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"evidence": []model.OperationEvidence{
						{ID: "evid_second", OperationID: op.ID, AppID: op.AppID, Type: "build_attempt", PayloadVersion: 1, Message: sensitive, NodeName: "node-secret-77", Payload: map[string]any{"build_attempt": map[string]any{"attempt": 2, "outcome": "failed", "started_at": "2026-01-01T00:01:00Z", "finished_at": "2026-01-01T00:02:00Z", "causes": []string{"builder_memory_unavailable", sensitive}, "missing_evidence": []string{sensitive}}, "raw": sensitive}},
						{ID: "evid_first", OperationID: op.ID, AppID: op.AppID, Type: "build_attempt", PayloadVersion: 1, Payload: map[string]any{"build_attempt": map[string]any{"attempt": 1, "outcome": "failed", "started_at": "2026-01-01T00:00:00Z", "finished_at": "2026-01-01T00:01:00Z", "causes": []string{"ephemeral_storage_limit_exceeded"}, "ephemeral_limit_bytes": 8589934592}}},
						{ID: "evid_unrelated", OperationID: "op_other", Type: "build_attempt", PayloadVersion: 1, Message: sensitive},
					}})
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			output := filepath.Join(t.TempDir(), "result.json")
			args := []string{"--base-url", server.URL, "--token", "tenant-secret", "--output-file", output, "deploy", "image", "example/demo:v2", "--app", "demo"}
			if jsonMode {
				args = append(args, "--json", "--redact=false", "--confirm-raw-output")
			}
			var stdout, stderr bytes.Buffer
			err := runWithStreams(args, &stdout, &stderr)
			if err == nil || ExitCodeForError(err) == 0 {
				t.Fatal("failed deployment must exit nonzero")
			}
			if mutations != 1 {
				t.Fatalf("diagnostic retried deployment: %d mutations", mutations)
			}
			all := stdout.String() + stderr.String() + err.Error()
			for _, secret := range []string{"node-secret-77", "10.42.99.8", "registry.private", "hunter-secret", "job-secret", "tenant-secret"} {
				if strings.Contains(all, secret) {
					t.Fatalf("sensitive output %q", secret)
				}
			}
			mirrored, readErr := os.ReadFile(output)
			if readErr != nil || string(mirrored) != stdout.String() {
				t.Fatal("output file must exactly match stdout")
			}
			if jsonMode {
				var payload struct {
					Result deploymentResult `json:"result"`
				}
				if json.Unmarshal(stdout.Bytes(), &payload) != nil {
					t.Fatalf("invalid final JSON: %s", stdout.String())
				}
				if payload.Result.Outcome != "failed" || payload.Result.FailedStage != "build" || len(payload.Result.Attempts) != 2 || payload.Result.Attempts[0].Attempt != 1 {
					t.Fatalf("bad result %+v", payload.Result)
				}
				if payload.Result.Attempts[0].EphemeralLimitBytes != 8589934592 {
					t.Fatal("resource evidence lost")
				}
				if len(payload.Result.Attempts[1].Causes) != 1 {
					t.Fatal("unknown server strings must not pass through")
				}
			} else {
				for _, text := range []string{"outcome=failed", "attempt=1", "attempt=2", "temporary storage limit", "memory request"} {
					if !strings.Contains(stdout.String(), text) {
						t.Errorf("missing %q", text)
					}
				}
			}
		})
	}
}
func TestDeployUnknownDoesNotAssertRemoteFailure(t *testing.T) {
	var stdout, stderr bytes.Buffer
	c := newCLI(&stdout, &stderr)
	c.root.JSONOutput = true
	c.deployment = &deploymentCommandState{requestStarted: true, operations: []model.Operation{{ID: "op_running", Status: "running"}}}
	err := c.renderDeploymentError(errors.New("dial tcp 10.42.2.3:443 secret upstream"))
	if ExitCodeForError(err) != ExitCodeIndeterminate || !strings.Contains(stdout.String(), `"outcome": "unknown"`) || strings.Contains(stdout.String(), "10.42.") {
		t.Fatalf("wrong unknown result %s %v", stdout.String(), err)
	}
}
func TestDeployEvidenceUnavailableRetainsReportedFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "cluster private key", http.StatusForbidden)
	}))
	defer server.Close()
	client, _ := newClientWithOptions(server.URL, "token", clientOptions{ReadRetryCount: -1})
	c := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	result := c.deploymentFailureResult(client, model.Operation{ID: "op_failed", AppID: "app_demo", Type: "import", Status: "failed", ErrorMessage: "context deadline exceeded"})
	if result.Outcome != "failed" || len(result.MissingEvidence) == 0 || len(result.Causes) != 1 || result.Causes[0].Confidence != "reported" {
		t.Fatalf("wrong result %+v", result)
	}
}
func TestLinkedDeploymentRequiresExplicitSameAppLink(t *testing.T) {
	build := model.Operation{ID: "op_build", AppID: "app_demo", Type: "import", Status: "failed", CreatedAt: time.Now()}
	old := model.Operation{ID: "op_old", AppID: "app_demo", Type: "deploy", Status: "completed", CreatedAt: build.CreatedAt.Add(-time.Hour)}
	newer := model.Operation{ID: "op_new", AppID: "app_demo", Type: "deploy", Status: "completed", CreatedAt: build.CreatedAt.Add(time.Hour)}
	if linkedDeployOperation(build, []model.Operation{old, newer}) != nil {
		t.Fatal("failed import acquired unrelated deployment")
	}
	report := &appBuildArtifactReport{}
	enrichBuildArtifactReport(report, model.App{}, &build, []model.Operation{old, newer}, nil, nil)
	if report.LinkedDeployOperationID != "" || report.ManagedImageRef != "" {
		t.Fatalf("unrelated artifact linked: %+v", report)
	}
	build.Status = "completed"
	build.ResultMessage = "queued deploy operation op_new"
	if linkedDeployOperation(build, []model.Operation{newer}) == nil {
		t.Fatal("explicit link lost")
	}
	newer.AppID = "app_other"
	if linkedDeployOperation(build, []model.Operation{newer}) != nil {
		t.Fatal("cross app link accepted")
	}
}
func TestDeployWaitFollowsNewlyQueuedDeploy(t *testing.T) {
	oldPoll := deployWaitPollInterval
	deployWaitPollInterval = time.Millisecond
	defer func() { deployWaitPollInterval = oldPoll }()
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/operations/op_build":
			_ = json.NewEncoder(w).Encode(map[string]any{"operation": model.Operation{ID: "op_build", AppID: "app_demo", Type: "import", Status: "completed", ResultMessage: "queued deploy operation op_deploy"}})
		case "/v1/operations/op_deploy":
			called = true
			_ = json.NewEncoder(w).Encode(map[string]any{"operation": model.Operation{ID: "op_deploy", AppID: "app_demo", Type: "deploy", Status: "failed", ErrorMessage: "sensitive-cluster"}})
		case "/v1/operations/op_deploy/evidence":
			_ = json.NewEncoder(w).Encode(map[string]any{"evidence": []any{}})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{"app": model.App{ID: "app_demo"}})
		}
	}))
	defer server.Close()
	client, _ := newClientWithOptions(server.URL, "token", clientOptions{ReadRetryCount: -1})
	c := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	c.root.JSONOutput = true
	c.deployment = &deploymentCommandState{}
	_, _, err := c.waitForImportBundle(client, importBundle{PrimaryApp: model.App{ID: "app_demo"}, Apps: []model.App{{ID: "app_demo"}}, Operations: []model.Operation{{ID: "op_build", AppID: "app_demo", Type: "import"}}})
	if err == nil || !called {
		t.Fatal("must not succeed while linked deployment has not been checked")
	}
}

func TestUserDeployProgressNeverRendersClusterStatus(t *testing.T) {
	var stdout, stderr bytes.Buffer
	c := newCLI(&stdout, &stderr)
	c.deployment = &deploymentCommandState{}
	var hash [32]byte
	have := false
	// A nil client is intentional: user progress must not query admin inventory.
	err := c.renderDeployProgressSnapshot(nil, nil, []model.Operation{{ID: "op_demo", ErrorMessage: "private-cluster"}}, projectStatusFilters{}, &hash, &have)
	if err != nil || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatal("user deploy rendered cluster snapshot")
	}
	c.progressf("warning=preflight unavailable: %v", errors.New("10.42.1.1 password=private"))
	if strings.Contains(stderr.String(), "10.42.") || strings.Contains(stderr.String(), "private") {
		t.Fatal("warning leaked backend error")
	}
}
func TestServingVersionCannotTurnFailedBuildIntoSuccess(t *testing.T) {
	ready := 1
	now := time.Now().UTC()
	app := model.App{ID: "app_demo", ObservedStatus: &model.AppObservedStatus{Phase: "deployed", ReadyReplicas: &ready, Fresh: true, ObservedAt: now}}
	state := publicServingState(app)
	if state.Status != "ready" {
		t.Fatal("fresh serving observation lost")
	}
	app.ObservedStatus.ObservedAt = now.Add(-time.Hour)
	if publicServingState(app).Status != "unknown" {
		t.Fatal("stale observation called healthy")
	}
}
func TestExplicitDeploymentFieldOverridesLegacyMessage(t *testing.T) {
	op := model.Operation{QueuedDeployOperationID: "op_explicit", ResultMessage: "queued deploy operation op_legacy"}
	if explicitQueuedDeployOperationID(op) != "op_explicit" {
		t.Fatal("new contract not preferred")
	}
}

func TestCanceledOperationHasTerminalSafeOutcome(t *testing.T) {
	c := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	result := c.deploymentFailureResult(nil, model.Operation{ID: "op_cancel", Type: "deploy", Status: model.OperationStatusCanceled, ErrorMessage: "internal reason secret"})
	if result.Outcome != "failed" || len(result.Causes) != 1 || result.Causes[0].Code != "operation_cancelled" {
		t.Fatalf("bad canceled result %+v", result)
	}
}
