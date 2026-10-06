package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestNonDeploymentOperationFailurePreservesTerminalEvidence(t *testing.T) {
	for _, status := range []string{"failed", "canceled", "cancelled", "superseded"} {
		t.Run(status, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/operations/op_recovery" {
					t.Errorf("unexpected diagnostic request: %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				json.NewEncoder(w).Encode(map[string]any{"operation": model.Operation{ID: "op_recovery", Type: "database-recover", Status: status, ErrorMessage: "host task nodeupdate_demo failed: insufficient host filesystem headroom: available_bytes=100 growth_bytes=90 reserve_bytes=20 password=private-value"}})
			}))
			defer server.Close()
			client, err := newClientWithOptions(server.URL, "token", clientOptions{RequireToken: true})
			if err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			cli := newCLI(&stdout, &stderr)
			_, err = cli.waitForOperations(client, []model.Operation{{ID: "op_recovery", Status: "running"}})
			var failure *operationCommandFailure
			if !errors.As(err, &failure) {
				t.Fatalf("lost operation failure: %T %v", err, err)
			}
			cli.renderCommandError(fmt.Errorf("move project: %w", err))
			var payload struct {
				Outcome     string                 `json:"outcome"`
				Operation   failedCommandOperation `json:"operation"`
				NextActions []string               `json:"next_actions"`
			}
			if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Outcome != "failed" || payload.Operation.ID != "op_recovery" || payload.Operation.Status != status || payload.Operation.Type != "database-recover" {
				t.Fatalf("incorrect terminal result: %+v", payload)
			}
			if !strings.Contains(payload.Operation.ErrorMessage, "available_bytes=100") || !strings.Contains(payload.Operation.ErrorMessage, "nodeupdate_demo") || len(payload.NextActions) != 2 {
				t.Fatalf("lost evidence or next action: %+v", payload)
			}
			if strings.Contains(stdout.String(), "private-value") || strings.Contains(err.Error(), "private-value") || strings.Contains(stdout.String(), "Deployment failed") {
				t.Fatalf("secret or generic deployment message leaked: %s", stdout.String())
			}
			if ExitCodeForError(err) != ExitCodeSystemFault {
				t.Fatalf("wrong exit code: %v", err)
			}
		})
	}
}

func TestDeploymentFailureKeepsStructuredDeploymentResult(t *testing.T) {
	cli := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	cli.deployment = &deploymentCommandState{}
	err := cli.operationFailure(nil, model.Operation{ID: "op_deploy", Type: model.OperationTypeDeploy, Status: "failed"})
	var deployment *deploymentResultError
	if !errors.As(err, &deployment) {
		t.Fatalf("deployment result changed: %T", err)
	}
}

func TestOperationFailureDoesNotInventMissingEvidence(t *testing.T) {
	err := newOperationCommandFailure(model.Operation{ID: "op_unknown", Type: "database-recover", Status: "failed"})
	if err.Operation.ErrorMessage != "" || !strings.Contains(err.Error(), "no failure reason was returned") {
		t.Fatal(err)
	}
}
