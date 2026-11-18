package dnsserver

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
)

func TestReportDNSAssignmentFailureJoinsOneExactResponse(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef"
	for _, test := range []struct {
		name, endpoint, step, wantID string
		wantStatus                   int
		wantPosts                    int32
	}{
		{"assignment", "assignment", "assignment_recheck", id, 503, 0},
		{"heartbeat", "trusted-heartbeat", "heartbeat_post", id, 503, 1},
		{"old API", "trusted-heartbeat", "heartbeat_post", "", 503, 1},
		{"no heartbeat response", "trusted-heartbeat", "heartbeat_post", "", 0, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := model.PlatformConsumerAssignment{ScopeKey: "global", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle}
			var calls, posts atomic.Int32
			const prefix = "/v1/platform-state/consumers/"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls.Add(1)
				if req.Method == http.MethodPost {
					posts.Add(1)
				}
				if req.URL.Path == prefix+test.endpoint {
					if test.wantStatus == 0 {
						conn, _, err := http.NewResponseController(w).Hijack()
						if err != nil {
							t.Error("cannot simulate closed transport", err)
							return
						}
						_ = conn.Close()
						return
					}
					if test.wantID != "" {
						w.Header().Set(platformconsumer.FailureIDHeader, test.wantID)
					}
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte("private failure fixture"))
					return
				}
				if req.URL.Path != prefix+"assignment" {
					t.Error("unexpected API call", req.URL.Path)
				}
				_ = json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{a}})
			}))
			defer server.Close()
			var out bytes.Buffer
			logger := log.New(&out, "", 0)
			service := NewService(config.DNSConfig{CachePath: filepath.Join(t.TempDir(), "dns")}, logger)
			state := &dnsServingState{}
			service.platformServing.Store(state)
			ctx, _ := startDNSSyncTrace(t.Context(), logger, "serving")
			err := service.reportDNSAssignmentState(ctx, platformconsumer.Client{BaseURL: server.URL}, platformconsumer.Identity{Token: "private token fixture"}, a, state, platformcontrol.PlatformConsumerHeartbeatEnvelope{})
			if !platformconsumer.IsUnavailable(err) || posts.Load() != test.wantPosts || calls.Load() != test.wantPosts+1 || service.platformServing.Load() != state {
				t.Fatal("diagnostics changed the report path", err, calls.Load(), posts.Load())
			}
			var observation struct {
				Step    string                                      `json:"failure_step"`
				Request *platformconsumer.RequestFailureObservation `json:"request_failure"`
			}
			if json.Unmarshal(out.Bytes(), &observation) != nil || observation.Step != test.step || observation.Request == nil ||
				observation.Request.FailureID != test.wantID || observation.Request.HTTPStatus != test.wantStatus ||
				observation.Request.FirstResponseByte != (test.wantStatus != 0) || bytes.Contains(out.Bytes(), []byte("private")) {
				t.Fatal("wrong request witness or private data retained", out.String())
			}
		})
	}
}
