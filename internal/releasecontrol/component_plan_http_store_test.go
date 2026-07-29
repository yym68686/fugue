package releasecontrol

import (
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

	"fugue/internal/componentmanifest"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func TestHTTPComponentPlanStoreReconcilesThroughVersionedAPI(t *testing.T) {
	t.Parallel()

	_, artifact := testValidatedComponentPlanArtifact(t)
	envelope, err := componentmanifest.DecodeShadowArtifactContent(artifact.Content)
	if err != nil {
		t.Fatalf("decode test envelope: %v", err)
	}
	principal := model.Principal{
		ActorType: model.ActorTypeAPIKey,
		ActorID:   "release-control-key",
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}
	release := model.PlatformArtifactRelease{
		ID:             "artifactrel_1_abcdef",
		ArtifactID:     artifact.ID,
		ArtifactKind:   artifact.ArtifactKind,
		Scope:          artifact.Scope,
		ScopeKey:       artifact.ScopeKey,
		Generation:     artifact.Generation,
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		Status:         model.PlatformArtifactReleaseStatusActive,
		LaneKey: platformsafety.ReleaseLaneKey(
			model.PlatformArtifactKindComponentReleasePlan,
			artifact.ScopeKey,
			model.PlatformArtifactReleaseChannelShadow,
		),
		FencingToken:        7,
		Version:             3,
		IdempotencyKey:      envelope.CoordinationPlan.IdempotencyKey,
		CandidateGeneration: artifact.Generation,
		Reason:              componentPlanReleaseReason,
		ReleasedByType:      principal.ActorType,
		ReleasedByID:        principal.ActorID,
	}
	message := model.PlatformReleaseMessage{
		ID:             "artifactmsg_1_abcdef",
		ReleaseID:      release.ID,
		ArtifactID:     artifact.ID,
		ArtifactKind:   artifact.ArtifactKind,
		Scope:          artifact.Scope,
		ScopeKey:       artifact.ScopeKey,
		Generation:     artifact.Generation,
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		MessageType:    model.PlatformReleaseMessageTypeRelease,
	}

	var getCalls atomic.Int32
	var releaseCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer shadow-token" {
			t.Errorf("authorization header = %q", got)
		}
		if got := r.Header.Get("X-Fugue-Contract-Version"); got != ComponentPlanAPIContractV1 {
			t.Errorf("contract header = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("accept header = %q", got)
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/prefix/v1/auth/context":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"principal": map[string]any{
					"actor_type": principal.ActorType, "actor_id": principal.ActorID,
					"scopes": []string{"platform.admin"}, "platform_admin": true,
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/prefix/v1/admin/artifacts/"+artifact.ID:
			getCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"artifact":              artifact,
				"future_minor_addition": true,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/prefix/v1/admin/artifacts/"+artifact.ID+"/release":
			releaseCalls.Add(1)
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("content type = %q", got)
			}
			var request model.PlatformArtifactReleaseRequest
			decoder := json.NewDecoder(r.Body)
			decoder.DisallowUnknownFields()
			if decodeErr := decoder.Decode(&request); decodeErr != nil {
				t.Errorf("decode release request: %v", decodeErr)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if request.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow ||
				request.Reason != componentPlanReleaseReason ||
				request.IdempotencyKey != envelope.CoordinationPlan.IdempotencyKey ||
				request.SoftOverride || request.ForcePublish || request.KernelBreakGlass != nil {
				t.Errorf("unsafe release request: %+v", request)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(model.PlatformArtifactReleaseResponse{
				Artifact: artifact,
				Release:  release,
				Message:  message,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	apiStore, err := NewHTTPComponentPlanStore(HTTPComponentPlanStoreConfig{
		BaseURL:     server.URL + "/prefix",
		BearerToken: "shadow-token",
	})
	if err != nil {
		t.Fatalf("new HTTP store: %v", err)
	}
	resolved, err := apiStore.ResolvePrincipal(context.Background())
	if err != nil {
		t.Fatalf("resolve principal: %v", err)
	}
	status, err := ReconcileComponentPlan(context.Background(), apiStore, ComponentPlanSpec{
		ArtifactID: artifact.ID, ContentHash: artifact.ContentHash, Generation: artifact.Generation,
	}, resolved)
	if err != nil {
		t.Fatalf("reconcile through API: %v", err)
	}
	if status.ReleaseID != release.ID || status.FencingToken != release.FencingToken ||
		status.LaneVersion != release.Version || status.ProductionMutationAllowed || !status.ObservationOnly {
		t.Fatalf("unexpected reconciled status: %+v", status)
	}
	if getCalls.Load() != 1 || releaseCalls.Load() != 1 {
		t.Fatalf("API calls: get=%d release=%d", getCalls.Load(), releaseCalls.Load())
	}
}

func TestNewHTTPComponentPlanStoreRejectsUnsafeTransportConfiguration(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*HTTPComponentPlanStoreConfig){
		"missing base URL":   func(cfg *HTTPComponentPlanStoreConfig) { cfg.BaseURL = "" },
		"relative base URL":  func(cfg *HTTPComponentPlanStoreConfig) { cfg.BaseURL = "/api" },
		"credentials in URL": func(cfg *HTTPComponentPlanStoreConfig) { cfg.BaseURL = "https://user:secret@example.test" },
		"query in URL":       func(cfg *HTTPComponentPlanStoreConfig) { cfg.BaseURL = "https://example.test?token=secret" },
		"non HTTP scheme":    func(cfg *HTTPComponentPlanStoreConfig) { cfg.BaseURL = "file:///tmp/api" },
		"path traversal":     func(cfg *HTTPComponentPlanStoreConfig) { cfg.BaseURL = "https://example.test/a/../b" },
		"missing token":      func(cfg *HTTPComponentPlanStoreConfig) { cfg.BearerToken = "" },
		"header injection":   func(cfg *HTTPComponentPlanStoreConfig) { cfg.BearerToken = "token\r\nX-Leak: yes" },
		"unbounded timeout": func(cfg *HTTPComponentPlanStoreConfig) {
			cfg.RequestTimeout = maxComponentPlanAPIRequestTimeout + time.Nanosecond
		},
		"unbounded response": func(cfg *HTTPComponentPlanStoreConfig) { cfg.MaxResponseBytes = maxComponentPlanAPIResponseBytes + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := HTTPComponentPlanStoreConfig{BaseURL: "https://example.test", BearerToken: "token"}
			mutate(&cfg)
			if _, err := NewHTTPComponentPlanStore(cfg); !errors.Is(err, ErrComponentPlanAPI) {
				t.Fatalf("error = %v, want ErrComponentPlanAPI", err)
			}
		})
	}
}

func TestHTTPComponentPlanStoreRejectsProductionCapableRequestsBeforeNetwork(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()
	apiStore, err := NewHTTPComponentPlanStore(HTTPComponentPlanStoreConfig{BaseURL: server.URL, BearerToken: "token"})
	if err != nil {
		t.Fatalf("new HTTP store: %v", err)
	}
	principal := testReleaseControlPrincipal()
	valid := model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		Reason:         componentPlanReleaseReason,
		IdempotencyKey: "component-shadow/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	for name, mutate := range map[string]func(*model.PlatformArtifactReleaseRequest){
		"full": func(req *model.PlatformArtifactReleaseRequest) {
			req.ReleaseChannel = model.PlatformArtifactReleaseChannelFull
		},
		"gray": func(req *model.PlatformArtifactReleaseRequest) {
			req.ReleaseChannel = model.PlatformArtifactReleaseChannelGray
		},
		"canary":        func(req *model.PlatformArtifactReleaseRequest) { req.CanaryRuleRef = "unsafe" },
		"soft override": func(req *model.PlatformArtifactReleaseRequest) { req.SoftOverride = true },
		"force":         func(req *model.PlatformArtifactReleaseRequest) { req.ForcePublish = true },
		"break glass": func(req *model.PlatformArtifactReleaseRequest) {
			req.KernelBreakGlass = &model.PlatformKernelBreakGlassRequest{}
		},
		"reason drift":      func(req *model.PlatformArtifactReleaseRequest) { req.Reason = "different" },
		"idempotency drift": func(req *model.PlatformArtifactReleaseRequest) { req.IdempotencyKey = "different" },
	} {
		t.Run(name, func(t *testing.T) {
			request := valid
			mutate(&request)
			_, _, _, _, err := apiStore.ReleasePlatformArtifact(context.Background(), "artifact_1_abcdef", request, principal)
			if !errors.Is(err, ErrComponentPlanAPI) {
				t.Fatalf("error = %v, want ErrComponentPlanAPI", err)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("unsafe requests reached network %d times", calls.Load())
	}
}

func TestHTTPComponentPlanStoreBoundsResponsesAndRejectsRedirects(t *testing.T) {
	t.Parallel()

	t.Run("response limit", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"artifact":"`+strings.Repeat("x", 128)+`"}`)
		}))
		defer server.Close()
		apiStore, err := NewHTTPComponentPlanStore(HTTPComponentPlanStoreConfig{
			BaseURL: server.URL, BearerToken: "token", MaxResponseBytes: 64,
		})
		if err != nil {
			t.Fatalf("new HTTP store: %v", err)
		}
		if _, err := apiStore.GetPlatformArtifact(context.Background(), "artifact_1_abcdef"); !errors.Is(err, ErrComponentPlanAPI) || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("bounded response error = %v", err)
		}
	})

	t.Run("redirect", func(t *testing.T) {
		var sinkCalls atomic.Int32
		sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			sinkCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{}`)
		}))
		defer sink.Close()
		redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, sink.URL, http.StatusTemporaryRedirect)
		}))
		defer redirect.Close()
		apiStore, err := NewHTTPComponentPlanStore(HTTPComponentPlanStoreConfig{BaseURL: redirect.URL, BearerToken: "secret-token"})
		if err != nil {
			t.Fatalf("new HTTP store: %v", err)
		}
		if _, err := apiStore.GetPlatformArtifact(context.Background(), "artifact_1_abcdef"); !errors.Is(err, ErrComponentPlanAPI) {
			t.Fatalf("redirect error = %v", err)
		}
		if sinkCalls.Load() != 0 {
			t.Fatalf("redirect leaked request to sink %d times", sinkCalls.Load())
		}
	})
}

func TestHTTPComponentPlanStoreHonorsCancellationAndRedactsCredential(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	apiStore, err := NewHTTPComponentPlanStore(HTTPComponentPlanStoreConfig{
		BaseURL: server.URL, BearerToken: "never-print-this-token", RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new HTTP store: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = apiStore.GetPlatformArtifact(ctx, "artifact_1_abcdef")
	if !errors.Is(err, ErrComponentPlanAPI) || !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "never-print-this-token") {
		t.Fatalf("cancellation error = %v", err)
	}
}

func TestHTTPComponentPlanStoreClassifiesAndRedactsRemoteErrors(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		status    int
		retryable bool
	}{
		"conflict":     {status: http.StatusConflict, retryable: false},
		"rate limited": {status: http.StatusTooManyRequests, retryable: true},
		"unavailable":  {status: http.StatusServiceUnavailable, retryable: true},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.status)
				_, _ = fmt.Fprint(w, `{"error":"remote echoed secret-token","code":"secret-token-code"}`)
			}))
			defer server.Close()
			apiStore, err := NewHTTPComponentPlanStore(HTTPComponentPlanStoreConfig{BaseURL: server.URL, BearerToken: "secret-token"})
			if err != nil {
				t.Fatalf("new HTTP store: %v", err)
			}
			_, err = apiStore.GetPlatformArtifact(context.Background(), "artifact_1_abcdef")
			var statusErr *ComponentPlanAPIStatusError
			if !errors.Is(err, ErrComponentPlanAPI) || !errors.As(err, &statusErr) {
				t.Fatalf("status error = %v", err)
			}
			if statusErr.StatusCode != test.status || statusErr.Retryable() != test.retryable {
				t.Fatalf("status classification = %+v retryable=%v", statusErr, statusErr.Retryable())
			}
			if strings.Contains(err.Error(), "secret-token") {
				t.Fatalf("status error leaked credential: %v", err)
			}
		})
	}
}

func TestHTTPComponentPlanStoreRedactsTransportError(t *testing.T) {
	t.Parallel()

	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("upstream reflected %s", request.Header.Get("Authorization"))
	})}
	apiStore, err := NewHTTPComponentPlanStore(HTTPComponentPlanStoreConfig{
		BaseURL: "https://example.test", BearerToken: "transport-secret", Client: client,
	})
	if err != nil {
		t.Fatalf("new HTTP store: %v", err)
	}
	_, err = apiStore.GetPlatformArtifact(context.Background(), "artifact_1_abcdef")
	if !errors.Is(err, ErrComponentPlanAPI) || strings.Contains(err.Error(), "transport-secret") {
		t.Fatalf("transport error = %v", err)
	}
}

func TestReconcileComponentPlanPreservesAPIRetryClassification(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprint(w, `{"error":"temporarily unavailable","code":"temporarily_unavailable"}`)
	}))
	defer server.Close()
	apiStore, err := NewHTTPComponentPlanStore(HTTPComponentPlanStoreConfig{BaseURL: server.URL, BearerToken: "token"})
	if err != nil {
		t.Fatalf("new HTTP store: %v", err)
	}
	_, err = ReconcileComponentPlan(context.Background(), apiStore, ComponentPlanSpec{
		ArtifactID:  "artifact_1_abcdef",
		ContentHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Generation:  "git-1111111111111111111111111111111111111111",
	}, testReleaseControlPrincipal())
	var statusErr *ComponentPlanAPIStatusError
	if !errors.Is(err, ErrComponentPlanReconcile) || !errors.Is(err, ErrComponentPlanAPI) ||
		!errors.As(err, &statusErr) || !statusErr.Retryable() {
		t.Fatalf("reconcile error lost API classification: %v", err)
	}
}

func TestHTTPComponentPlanStoreRejectsMismatchedAuthContext(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"principal": map[string]any{
				"actor_type": "api-key", "actor_id": "key-1",
				"scopes": []string{"platform.admin"}, "platform_admin": false,
			},
		})
	}))
	defer server.Close()
	apiStore, err := NewHTTPComponentPlanStore(HTTPComponentPlanStoreConfig{BaseURL: server.URL, BearerToken: "token"})
	if err != nil {
		t.Fatalf("new HTTP store: %v", err)
	}
	if _, err := apiStore.ResolvePrincipal(context.Background()); !errors.Is(err, ErrComponentPlanAPI) {
		t.Fatalf("auth context error = %v", err)
	}
}

var _ ComponentPlanStore = (*HTTPComponentPlanStore)(nil)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}
