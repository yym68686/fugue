package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/agentedge"
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
			const callerID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			req := httptest.NewRequest(http.MethodGet, "/v1/agent/edge-candidates", nil)
			req.Header.Set("Authorization", "Bearer "+f.runtimeKey)
			req.Header.Set(agentedge.CaptureFailureIDHeader, callerID)
			response := httptest.NewRecorder()
			f.s.Handler().ServeHTTP(response, req)
			if response.Code != code {
				t.Fatal(response.Code, response.Body.String())
			}
			if code == 503 && (!strings.Contains(response.Body.String(), errAgentEdgeUnavailable.Error()) || strings.Contains(response.Body.String(), "private")) {
				t.Fatal("public error contract changed or private diagnostic leaked", response.Body.String())
			}
			ids := response.Header().Values(agentedge.CaptureFailureIDHeader)
			if scenario == "capture" {
				if len(ids) != 1 || len(ids[0]) != 32 || ids[0] == callerID ||
					strings.Trim(ids[0], "0123456789abcdef") != "" || strings.Contains(response.Body.String(), ids[0]) {
					t.Fatal("expected one opaque server-generated response ID")
				}
			} else if len(ids) != 0 {
				t.Fatal("non-capture result received a capture failure ID")
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
				Captures struct {
					AgentFailures struct {
						Receipts []captureReceipt `json:"receipts"`
					} `json:"agent_failures"`
				} `json:"captures"`
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
			receipts := snapshot.Captures.AgentFailures.Receipts
			if scenario == "capture" {
				if len(receipts) != 1 || receipts[0].AgentFailureID != ids[0] {
					t.Fatal("response did not match its private capture receipt")
				}
				next := performJSONRequest(t, f.s, http.MethodGet, "/v1/agent/edge-candidates", f.runtimeKey, nil)
				if id := next.Header().Get(agentedge.CaptureFailureIDHeader); id == "" || id == ids[0] {
					t.Fatal("two failed captures did not get independent IDs")
				}
				unauthorized := performJSONRequest(t, f.s, http.MethodGet, "/v1/agent/edge-candidates", f.admin, nil)
				if unauthorized.Code != http.StatusForbidden || unauthorized.Header().Get(agentedge.CaptureFailureIDHeader) != "" {
					t.Fatal("unauthorized request received capture evidence")
				}
			} else if len(receipts) != 0 {
				t.Fatal("non-capture result produced private failure evidence")
			}
		})
	}
}

func TestConcurrentAgentCaptureFailureIDsMatchDistinctReceipts(t *testing.T) {
	f := setupAgentAuthorityFixture(t)
	f.s.agentEdgeProbe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		return routeprobe.Proof{}, errors.New("synthetic route observation failure")
	}
	const count = 6
	type response struct {
		code int
		id   string
	}
	handler := f.s.Handler()
	ready, results := make(chan struct{}), make(chan response, count)
	for range count {
		go func() {
			req := httptest.NewRequest(http.MethodGet, "/v1/agent/edge-candidates", nil)
			req.Header.Set("Authorization", "Bearer "+f.runtimeKey)
			recorder := httptest.NewRecorder()
			<-ready
			handler.ServeHTTP(recorder, req)
			results <- response{recorder.Code, recorder.Header().Get(agentedge.CaptureFailureIDHeader)}
		}()
	}
	close(ready)
	ids := make(map[string]bool, count)
	for range count {
		result := <-results
		if result.code != http.StatusServiceUnavailable || result.id == "" || ids[result.id] {
			t.Errorf("request did not produce a distinct capture failure: status=%d", result.code)
		}
		ids[result.id] = true
	}
	ring := f.s.captureSnapshot().(map[string]any)["agent_failures"].(map[string]any)
	receipts := ring["receipts"].([]captureReceipt)
	if len(receipts) != count {
		t.Fatalf("retained %d of %d concurrent failure receipts", len(receipts), count)
	}
	for _, receipt := range receipts {
		if !ids[receipt.AgentFailureID] {
			t.Fatal("unmatched or duplicate private capture receipt")
		}
		delete(ids, receipt.AgentFailureID)
	}
	if len(ids) != 0 {
		t.Fatal("HTTP failure ID missing from private receipts")
	}
}
