package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/routeprobe"
)

func TestAgentResponseObservationsDistinguishCaptureFromSigningWithoutLeaking(t *testing.T) {
	for _, scenario := range []string{"success", "capture", "keyring"} {
		t.Run(scenario, func(t *testing.T) {
			f := setupAgentAuthorityFixture(t)
			want, code := "agent-response-ok", 200
			switch scenario {
			case "capture":
				want, code = "agent-response-capture-failed", 503
				f.s.agentEdgeProbe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
					return routeprobe.Proof{}, errors.New("private diagnostic credential must not escape")
				}
			case "keyring":
				want, code = "agent-response-keyring-unavailable", 503
				f.s.agentEdgeSigningKeyFile = t.TempDir() + "/missing-private-keyring"
			}
			response := performJSONRequest(t, f.s, http.MethodGet, "/v1/agent/edge-candidates", f.runtimeKey, nil)
			if response.Code != code {
				t.Fatal(response.Code, response.Body.String())
			}
			if code == 503 && (!strings.Contains(response.Body.String(), errAgentEdgeUnavailable.Error()) || strings.Contains(response.Body.String(), "private")) {
				t.Fatal("public error contract changed or private diagnostic leaked", response.Body.String())
			}
			value, err := f.s.OperationSnapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(value)
			var snapshot struct {
				Stages []struct {
					Stage string `json:"stage"`
					Count uint64 `json:"count"`
				} `json:"stages"`
			}
			if json.Unmarshal(raw, &snapshot) != nil {
				t.Fatal("invalid diagnostic snapshot")
			}
			count := uint64(0)
			for _, stage := range snapshot.Stages {
				if strings.HasPrefix(stage.Stage, "agent-response-") {
					if stage.Stage == want && stage.Count != 1 {
						t.Fatal(stage)
					}
					count += stage.Count
				}
			}
			if count != 1 {
				t.Fatal("response did not have one exact fixed outcome", count)
			}
		})
	}
}
