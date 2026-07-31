package releasecontrol

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestComponentPlanServiceDisabledDoesNoIO(t *testing.T) {
	t.Parallel()

	service, err := NewComponentPlanService(ComponentPlanServiceConfig{
		Enabled:    false,
		SpecPath:   "relative-does-not-exist",
		TokenPath:  "relative-does-not-exist",
		APIBaseURL: "not-a-url",
	}, nil)
	if err != nil {
		t.Fatalf("new disabled service: %v", err)
	}
	var factoryCalls atomic.Int64
	service.newStore = func(HTTPComponentPlanStoreConfig) (componentPlanPrincipalStore, error) {
		factoryCalls.Add(1)
		return nil, errors.New("must not run")
	}
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("disabled reconcile: %v", err)
	}
	snapshot := service.Snapshot()
	if snapshot.Ready || snapshot.Mode != ComponentPlanServiceModeOff || snapshot.AttemptCount != 0 ||
		!snapshot.ObservationOnly || snapshot.ProductionMutationAllowed || factoryCalls.Load() != 0 {
		t.Fatalf("unsafe disabled snapshot: %+v factory_calls=%d", snapshot, factoryCalls.Load())
	}

	for path, expectedStatus := range map[string]int{
		"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable, "/v1/status": http.StatusOK,
	} {
		recorder := httptest.NewRecorder()
		service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != expectedStatus || strings.Contains(recorder.Body.String(), "relative-does-not-exist") {
			t.Fatalf("disabled endpoint %s: status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
}

func TestComponentPlanServiceReconcilesRecoversAndRetainsLastKnownGood(t *testing.T) {
	t.Parallel()

	stateStore, artifact := testValidatedComponentPlanArtifact(t)
	spec := testComponentPlanSpec(artifact)
	tempDir := t.TempDir()
	specPath := filepath.Join(tempDir, "spec.json")
	tokenPath := filepath.Join(tempDir, "token")
	writeComponentPlanTestJSON(t, specPath, spec)
	if err := os.WriteFile(tokenPath, []byte("first-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	service, err := NewComponentPlanService(ComponentPlanServiceConfig{
		Enabled:        true,
		SpecPath:       specPath,
		TokenPath:      tokenPath,
		APIBaseURL:     "https://api.example.test/internal",
		Interval:       time.Second,
		AttemptTimeout: 5 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	var factoryCalls atomic.Int64
	var token atomic.Value
	service.newStore = func(cfg HTTPComponentPlanStoreConfig) (componentPlanPrincipalStore, error) {
		factoryCalls.Add(1)
		token.Store(cfg.BearerToken)
		return serviceTestClient{
			ComponentPlanStore: testComponentPlanStore{stateStore},
			principal:          testReleaseControlPrincipal(),
		}, nil
	}
	service.now = func() time.Time { return time.Date(2026, 7, 29, 18, 0, 0, 0, time.UTC) }

	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	first := service.Snapshot()
	if !first.Ready || first.Mode != ComponentPlanServiceModeShadow || first.AttemptCount != 1 ||
		first.CurrentStatus == nil || first.LastKnownGood == nil || first.CurrentStatus.Digest != first.LastKnownGood.Digest ||
		first.LastSuccessAt == nil || token.Load() != "first-token" {
		t.Fatalf("unexpected successful snapshot: %+v token=%v", first, token.Load())
	}
	firstDigest := first.LastKnownGood.Digest

	if err := os.WriteFile(specPath, []byte(`{"apiVersion":"release-control.fugue.dev/v2"}`), 0o600); err != nil {
		t.Fatalf("write malformed spec: %v", err)
	}
	if err := service.ReconcileOnce(context.Background()); !errors.Is(err, ErrComponentPlanSpec) {
		t.Fatalf("malformed spec error = %v", err)
	}
	failed := service.Snapshot()
	if failed.Ready || failed.Reconciling || failed.FailureCode != "spec_invalid" || failed.ConsecutiveFailures != 1 || failed.CurrentStatus != nil ||
		failed.LastKnownGood == nil || failed.LastKnownGood.Digest != firstDigest || factoryCalls.Load() != 1 {
		t.Fatalf("failed lane lost isolation or LKG: %+v factory_calls=%d", failed, factoryCalls.Load())
	}

	writeComponentPlanTestJSON(t, specPath, spec)
	if err := os.WriteFile(tokenPath, []byte("rotated-token\n"), 0o600); err != nil {
		t.Fatalf("rotate token: %v", err)
	}
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("recovery reconcile: %v", err)
	}
	recovered := service.Snapshot()
	if !recovered.Ready || recovered.FailureCode != "" || recovered.ConsecutiveFailures != 0 ||
		recovered.LastKnownGood == nil || recovered.LastKnownGood.Digest != firstDigest ||
		factoryCalls.Load() != 2 || token.Load() != "rotated-token" {
		t.Fatalf("lane-local recovery failed: %+v factory_calls=%d token=%v", recovered, factoryCalls.Load(), token.Load())
	}
}

func TestComponentPlanServiceUsesOnlyTheVersionedHTTPBoundary(t *testing.T) {
	t.Parallel()

	stateStore, artifact := testValidatedComponentPlanArtifact(t)
	principal := testReleaseControlPrincipal()
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer service-token" ||
			r.Header.Get("X-Fugue-Contract-Version") != ComponentPlanAPIContractV1 {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
			return
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/auth/context":
			_ = json.NewEncoder(w).Encode(map[string]any{"principal": map[string]any{
				"actor_type": principal.ActorType,
				"actor_id":   principal.ActorID,
				"scopes": []string{
					"artifact.read", "artifact.release_shadow", model.PlatformComponentPlanObserveScope,
				},
				"platform_admin": false,
			}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/admin/artifacts/"+artifact.ID:
			_ = json.NewEncoder(w).Encode(model.PlatformArtifactResponse{Artifact: artifact})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/admin/artifacts/"+artifact.ID+"/release":
			var request model.PlatformArtifactReleaseRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			returnedArtifact, release, message, lkg, err := stateStore.ReleasePlatformArtifact(artifact.ID, request, principal)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(model.PlatformArtifactReleaseResponse{
				Artifact: returnedArtifact, Release: release, Message: message, LKG: lkg,
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	tempDir := t.TempDir()
	specPath := filepath.Join(tempDir, "spec.json")
	tokenPath := filepath.Join(tempDir, "token")
	writeComponentPlanTestJSON(t, specPath, testComponentPlanSpec(artifact))
	if err := os.WriteFile(tokenPath, []byte("service-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	service, err := NewComponentPlanService(ComponentPlanServiceConfig{
		Enabled: true, SpecPath: specPath, TokenPath: tokenPath, APIBaseURL: server.URL,
		Interval: time.Second, AttemptTimeout: 5 * time.Second, RequestTimeout: time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcile through HTTP: %v", err)
	}
	snapshot := service.Snapshot()
	if calls.Load() != 3 || !snapshot.Ready || snapshot.LastKnownGood == nil ||
		snapshot.LastKnownGood.ArtifactID != artifact.ID || snapshot.ProductionMutationAllowed {
		t.Fatalf("HTTP boundary drifted: calls=%d snapshot=%+v", calls.Load(), snapshot)
	}
}

func TestComponentPlanServiceReadinessFailsClosedWithoutLeakingErrors(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	specPath := filepath.Join(tempDir, "spec.json")
	tokenPath := filepath.Join(tempDir, "token")
	if err := os.WriteFile(specPath, []byte(`{"unexpected":"secret-value"}`), 0o600); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	if err := os.WriteFile(tokenPath, []byte("never-expose-this-token"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	service, err := NewComponentPlanService(ComponentPlanServiceConfig{
		Enabled: true, SpecPath: specPath, TokenPath: tokenPath,
		APIBaseURL: "https://api.example.test", Interval: time.Second, AttemptTimeout: time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	if err := service.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("invalid spec unexpectedly reconciled")
	}
	recorder := httptest.NewRecorder()
	service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"failureCode":"spec_invalid"`) {
		t.Fatalf("readiness did not fail closed: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	for _, secret := range []string{"secret-value", "never-expose-this-token", specPath, tokenPath} {
		if strings.Contains(recorder.Body.String(), secret) {
			t.Fatalf("readiness leaked %q: %s", secret, recorder.Body.String())
		}
	}
}

func TestComponentPlanServiceConfigurationIsFailClosed(t *testing.T) {
	t.Parallel()

	base := ComponentPlanServiceConfig{
		Enabled: true, APIBaseURL: "https://api.example.test",
		SpecPath: "/run/fugue/spec.json", TokenPath: "/run/secrets/fugue/token",
	}
	for name, mutate := range map[string]func(*ComponentPlanServiceConfig){
		"missing API":       func(cfg *ComponentPlanServiceConfig) { cfg.APIBaseURL = "" },
		"relative spec":     func(cfg *ComponentPlanServiceConfig) { cfg.SpecPath = "spec.json" },
		"relative token":    func(cfg *ComponentPlanServiceConfig) { cfg.TokenPath = "token" },
		"same files":        func(cfg *ComponentPlanServiceConfig) { cfg.TokenPath = cfg.SpecPath },
		"negative interval": func(cfg *ComponentPlanServiceConfig) { cfg.Interval = -time.Second },
		"short interval":    func(cfg *ComponentPlanServiceConfig) { cfg.Interval = 500 * time.Millisecond },
		"excess interval":   func(cfg *ComponentPlanServiceConfig) { cfg.Interval = maxComponentPlanInterval + time.Second },
		"negative attempt":  func(cfg *ComponentPlanServiceConfig) { cfg.AttemptTimeout = -time.Second },
		"short attempt":     func(cfg *ComponentPlanServiceConfig) { cfg.AttemptTimeout = 500 * time.Millisecond },
		"excess attempt":    func(cfg *ComponentPlanServiceConfig) { cfg.AttemptTimeout = maxComponentPlanAttemptTTL + time.Second },
		"invalid API":       func(cfg *ComponentPlanServiceConfig) { cfg.APIBaseURL = "file:///tmp/api" },
		"negative request":  func(cfg *ComponentPlanServiceConfig) { cfg.RequestTimeout = -time.Second },
		"excess request": func(cfg *ComponentPlanServiceConfig) {
			cfg.RequestTimeout = maxComponentPlanAPIRequestTimeout + time.Second
		},
		"negative response": func(cfg *ComponentPlanServiceConfig) { cfg.MaxResponseBytes = -1 },
		"excess response":   func(cfg *ComponentPlanServiceConfig) { cfg.MaxResponseBytes = maxComponentPlanAPIResponseBytes + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			mutate(&cfg)
			if _, err := NewComponentPlanService(cfg, nil); !errors.Is(err, ErrComponentPlanServiceConfig) {
				t.Fatalf("configuration error = %v", err)
			}
		})
	}
}

func TestReadComponentPlanSpecRequiresOneStrictVersionedObject(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "spec.json")
	valid := ComponentPlanSpec{
		APIVersion: ComponentPlanSpecAPIVersion, Kind: ComponentPlanSpecKind,
		ArtifactID: "artifact_1_abcdef", ContentHash: "sha256:" + strings.Repeat("a", 64),
		Generation: "git-" + strings.Repeat("1", 40),
	}
	writeComponentPlanTestJSON(t, path, valid)
	if got, err := readComponentPlanSpec(path); err != nil || got != valid {
		t.Fatalf("read valid spec: got=%+v err=%v", got, err)
	}
	for name, content := range map[string]string{
		"unknown field": `{"apiVersion":"release-control.fugue.dev/v1","kind":"ComponentPlanSpec","artifactId":"artifact_1","contentHash":"sha256:` + strings.Repeat("a", 64) + `","generation":"git-` + strings.Repeat("1", 40) + `","extra":true}`,
		"trailing JSON": `{"apiVersion":"release-control.fugue.dev/v1"} {}`,
		"wrong version": `{"apiVersion":"release-control.fugue.dev/v2","kind":"ComponentPlanSpec","artifactId":"artifact_1","contentHash":"sha256:` + strings.Repeat("a", 64) + `","generation":"git-` + strings.Repeat("1", 40) + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatalf("write invalid spec: %v", err)
			}
			if _, err := readComponentPlanSpec(path); !errors.Is(err, ErrComponentPlanSpec) {
				t.Fatalf("invalid spec error = %v", err)
			}
		})
	}
}

func TestValidateComponentPlanSpecRejectsIdentityDrift(t *testing.T) {
	t.Parallel()

	valid := ComponentPlanSpec{
		APIVersion: ComponentPlanSpecAPIVersion, Kind: ComponentPlanSpecKind,
		ArtifactID: "artifact_1_abcdef", ContentHash: "sha256:" + strings.Repeat("a", 64),
		Generation: "git-" + strings.Repeat("1", 40),
	}
	for name, mutate := range map[string]func(*ComponentPlanSpec){
		"missing version":  func(spec *ComponentPlanSpec) { spec.APIVersion = "" },
		"artifact spaces":  func(spec *ComponentPlanSpec) { spec.ArtifactID = " artifact_1_abcdef" },
		"uppercase digest": func(spec *ComponentPlanSpec) { spec.ContentHash = "sha256:" + strings.Repeat("A", 64) },
		"mutable ref":      func(spec *ComponentPlanSpec) { spec.Generation = "main" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if err := ValidateComponentPlanSpec(candidate); !errors.Is(err, ErrComponentPlanSpec) {
				t.Fatalf("invalid spec error = %v", err)
			}
		})
	}
}

func TestReadComponentPlanTokenIsBoundedAndSingleLine(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("valid-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	if token, err := readComponentPlanToken(path); err != nil || token != "valid-token" {
		t.Fatalf("read token: token=%q err=%v", token, err)
	}
	for name, content := range map[string][]byte{
		"embedded newline": []byte("first\nsecond"),
		"empty":            []byte(" \n"),
		"oversized":        []byte(strings.Repeat("x", maxComponentPlanTokenBytes+1)),
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, content, 0o600); err != nil {
				t.Fatalf("write invalid token: %v", err)
			}
			if _, err := readComponentPlanToken(path); !errors.Is(err, ErrComponentPlanToken) {
				t.Fatalf("invalid token error = %v", err)
			}
		})
	}
}

func TestComponentPlanFailureCodePreservesAttemptDeadline(t *testing.T) {
	t.Parallel()

	wrapped := errors.Join(ErrComponentPlanAPI, context.DeadlineExceeded)
	if got := componentPlanFailureCode(wrapped); got != "attempt_timeout" {
		t.Fatalf("deadline classification = %q", got)
	}
	if got := componentPlanFailureCode(&ComponentPlanAPIStatusError{StatusCode: http.StatusForbidden}); got != "authorization_rejected" {
		t.Fatalf("authorization classification = %q", got)
	}
}

type serviceTestClient struct {
	ComponentPlanStore
	principal model.Principal
	err       error
}

func (client serviceTestClient) ResolvePrincipal(context.Context) (model.Principal, error) {
	return client.principal, client.err
}

func writeComponentPlanTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode JSON: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write JSON: %v", err)
	}
}

var _ componentPlanPrincipalStore = serviceTestClient{}
