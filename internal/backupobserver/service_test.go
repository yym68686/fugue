package backupobserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
)

const (
	observerBackendGeneration = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	observerContentDigest     = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	observerManifestDigest    = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func TestDisabledServicePerformsNoInputOrRemoteIO(t *testing.T) {
	service, err := NewService(ServiceConfig{
		SpecPath:   "relative-does-not-exist",
		TokenPath:  "also-missing",
		APIBaseURL: "not-a-url",
	}, nil)
	if err != nil {
		t.Fatalf("new disabled service: %v", err)
	}
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("disabled reconcile: %v", err)
	}
	snapshot := service.Snapshot()
	if snapshot.Mode != ServiceModeDisabled || snapshot.Ready || snapshot.AttemptCount != 0 ||
		!snapshot.ObservationOnly || snapshot.ProductionMutationAllowed {
		t.Fatalf("disabled boundary drifted: %+v", snapshot)
	}
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()
	service.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("disabled readiness = %d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
}

func TestServiceReconcilesExactHTTPStatusAndRetainsLKGOnOutage(t *testing.T) {
	now := time.Date(2026, 7, 30, 3, 0, 0, 0, time.UTC)
	spec := observerSpec(t, "run-1", "request-1", "app/app-1/database")
	status := observerStatus(t, spec, now, 2*time.Minute)
	var fail atomic.Bool
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.Method != http.MethodGet || request.URL.Path != "/v1/backup-control/runs/run-1/observation" ||
			request.URL.Query().Get("spec_digest") != spec.Digest || request.Header.Get("Authorization") != "Bearer observer-token" ||
			request.Header.Get("Accept") != "application/json" {
			t.Errorf("unexpected observation request: method=%s url=%s headers=%v", request.Method, request.URL.String(), request.Header)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"message":"must never enter observer state"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "private, no-store")
		_ = json.NewEncoder(w).Encode(status)
	}))
	defer server.Close()

	service := observerServiceFixture(t, spec, server.URL, now)
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	first := service.Snapshot()
	if !first.Ready || first.CurrentStatus == nil || first.LastKnownGood == nil ||
		first.CurrentStatus.Digest != status.Digest || first.LastKnownGood.Digest != status.Digest ||
		first.CellKey != spec.CellKey || first.AttemptCount != 1 || first.ConsecutiveFailures != 0 {
		t.Fatalf("successful snapshot drifted: %+v", first)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	for _, forbidden := range []string{"observer-token", "must never enter", "authorization"} {
		if bytes.Contains(bytes.ToLower(encoded), bytes.ToLower([]byte(forbidden))) {
			t.Fatalf("snapshot leaked %q: %s", forbidden, encoded)
		}
	}

	fail.Store(true)
	if err := service.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("remote outage unexpectedly reconciled")
	}
	failed := service.Snapshot()
	if failed.Ready || failed.CurrentStatus != nil || failed.LastKnownGood == nil ||
		failed.LastKnownGood.Digest != status.Digest || failed.FailureCode != "api_retryable" ||
		failed.ConsecutiveFailures != 1 || failed.AttemptCount != 2 || requests.Load() != 2 {
		t.Fatalf("outage did not remain lane-local or retain LKG: %+v requests=%d", failed, requests.Load())
	}
}

func TestServiceRejectsCellDriftBeforeCredentialOrNetwork(t *testing.T) {
	now := time.Date(2026, 7, 30, 3, 0, 0, 0, time.UTC)
	expected := observerSpec(t, "run-1", "request-1", "app/app-1/database")
	drifted := observerSpec(t, "run-2", "request-2", "app/app-2/database")
	directory := t.TempDir()
	specPath := filepath.Join(directory, "spec.json")
	writeJSONFile(t, specPath, drifted, 0o600)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	service, err := NewService(ServiceConfig{
		Enabled:                   true,
		ExpectedCellKey:           expected.CellKey,
		SpecPath:                  specPath,
		TokenPath:                 filepath.Join(directory, "missing-token"),
		APIBaseURL:                server.URL,
		Interval:                  time.Second,
		AttemptTimeout:            time.Second,
		AllowInsecureHTTPForTests: true,
	}, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.now = func() time.Time { return now }
	if err := service.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("cross-cell spec unexpectedly reconciled")
	}
	snapshot := service.Snapshot()
	if snapshot.FailureCode != "cell_mismatch" || snapshot.DesiredSpec != nil || requests.Load() != 0 {
		t.Fatalf("cell mismatch crossed its local boundary: %+v requests=%d", snapshot, requests.Load())
	}
}

func TestSnapshotReadinessExpiresWithoutDestroyingObservation(t *testing.T) {
	now := time.Date(2026, 7, 30, 3, 0, 0, 0, time.UTC)
	spec := observerSpec(t, "run-1", "request-1", "app/app-1/database")
	status := observerStatus(t, spec, now, time.Minute)
	server := statusServer(t, status)
	defer server.Close()
	service := observerServiceFixture(t, spec, server.URL, now)
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	service.now = func() time.Time { return now.Add(time.Minute + time.Second) }
	snapshot := service.Snapshot()
	if snapshot.Ready || snapshot.FailureCode != "observation_expired" || snapshot.CurrentStatus == nil || snapshot.LastKnownGood == nil {
		t.Fatalf("expired observation was erased or remained ready: %+v", snapshot)
	}
}

func TestServiceRejectsFutureDatedObservation(t *testing.T) {
	now := time.Date(2026, 7, 30, 3, 0, 0, 0, time.UTC)
	spec := observerSpec(t, "run-1", "request-1", "app/app-1/database")
	status := observerStatus(t, spec, now.Add(maxObservationFutureSkew+time.Second), time.Minute)
	server := statusServer(t, status)
	defer server.Close()
	service := observerServiceFixture(t, spec, server.URL, now)
	if err := service.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("future-dated observation unexpectedly reconciled")
	}
	snapshot := service.Snapshot()
	if snapshot.Ready || snapshot.CurrentStatus != nil || snapshot.LastKnownGood != nil || snapshot.FailureCode != "observation_stale" {
		t.Fatalf("future observation escaped freshness boundary: %+v", snapshot)
	}
}

func TestHTTPObservationSourceRejectsRedirectUnsafeMetadataContractAndSize(t *testing.T) {
	now := time.Date(2026, 7, 30, 3, 0, 0, 0, time.UTC)
	spec := observerSpec(t, "run-1", "request-1", "app/app-1/database")
	status := observerStatus(t, spec, now, time.Minute)
	validDocument, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	unknownDocument := append([]byte(nil), validDocument[:len(validDocument)-1]...)
	unknownDocument = append(unknownDocument, []byte(`,"credential":"forbidden"}`)...)

	var redirectHits atomic.Int64
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectHits.Add(1)
	}))
	defer redirectTarget.Close()
	tests := []struct {
		name     string
		handler  http.Handler
		maxBytes int64
	}{
		{name: "redirect", handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Redirect(w, &http.Request{}, redirectTarget.URL, http.StatusFound)
		})},
		{name: "missing no-store", handler: responseHandler(validDocument, "application/json", "private")},
		{name: "wrong content type", handler: responseHandler(validDocument, "text/plain", "private, no-store")},
		{name: "unknown contract field", handler: responseHandler(unknownDocument, "application/json", "private, no-store")},
		{name: "oversized", handler: responseHandler(bytes.Repeat([]byte("x"), 257), "application/json", "private, no-store"), maxBytes: 256},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.handler)
			defer server.Close()
			source, err := NewHTTPObservationSource(HTTPObservationSourceConfig{
				BaseURL:                   server.URL,
				BearerToken:               "observer-token",
				RequestTimeout:            time.Second,
				MaxResponseBytes:          test.maxBytes,
				AllowInsecureHTTPForTests: true,
			})
			if err != nil {
				t.Fatalf("new source: %v", err)
			}
			if _, err := source.Observe(context.Background(), spec); err == nil {
				t.Fatal("unsafe observation response was accepted")
			}
		})
	}
	if redirectHits.Load() != 0 {
		t.Fatalf("bearer request followed redirect to %d target(s)", redirectHits.Load())
	}
}

func TestServiceConfigAndCredentialFilesFailClosed(t *testing.T) {
	spec := observerSpec(t, "run-1", "request-1", "app/app-1/database")
	base := ServiceConfig{
		Enabled:         true,
		ExpectedCellKey: spec.CellKey,
		SpecPath:        "/run/fugue/backup/spec.json",
		TokenPath:       "/run/secrets/backup/token",
		APIBaseURL:      "https://api.fugue.test",
		Interval:        time.Second,
		AttemptTimeout:  time.Second,
	}
	for name, mutate := range map[string]func(*ServiceConfig){
		"bad cell":         func(cfg *ServiceConfig) { cfg.ExpectedCellKey = "backup/all" },
		"relative spec":    func(cfg *ServiceConfig) { cfg.SpecPath = "spec.json" },
		"same paths":       func(cfg *ServiceConfig) { cfg.TokenPath = cfg.SpecPath },
		"plaintext API":    func(cfg *ServiceConfig) { cfg.APIBaseURL = "http://api.fugue.test" },
		"URL credential":   func(cfg *ServiceConfig) { cfg.APIBaseURL = "https://user:pass@api.fugue.test" },
		"noncanonical URL": func(cfg *ServiceConfig) { cfg.APIBaseURL = "https://api.fugue.test/a/../b" },
		"encoded URL path": func(cfg *ServiceConfig) { cfg.APIBaseURL = "https://api.fugue.test/%2e%2e/private" },
		"excess interval":  func(cfg *ServiceConfig) { cfg.Interval = 11 * time.Minute },
		"excess attempt":   func(cfg *ServiceConfig) { cfg.AttemptTimeout = 2 * time.Minute },
		"excess request":   func(cfg *ServiceConfig) { cfg.RequestTimeout = time.Minute },
		"excess response":  func(cfg *ServiceConfig) { cfg.MaxResponseBytes = 2 << 20 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			mutate(&cfg)
			if _, err := NewService(cfg, nil); err == nil {
				t.Fatal("unsafe service configuration was accepted")
			}
		})
	}
	if _, err := NewHTTPObservationSource(HTTPObservationSourceConfig{
		BaseURL:     "https://api.fugue.test",
		BearerToken: strings.Repeat("x", maxObservationTokenBytes+1),
	}); err == nil {
		t.Fatal("oversized bearer credential was accepted")
	}

	directory := t.TempDir()
	credential := filepath.Join(directory, "token")
	if err := os.WriteFile(credential, []byte("observer-token\n"), 0o644); err != nil {
		t.Fatalf("write broad credential: %v", err)
	}
	if _, err := readToken(credential); err == nil {
		t.Fatal("world-readable credential was accepted")
	}
	if err := os.Chmod(credential, 0o600); err != nil {
		t.Fatalf("chmod credential: %v", err)
	}
	symlink := filepath.Join(directory, "token-link")
	if err := os.Symlink(credential, symlink); err != nil {
		t.Fatalf("symlink credential: %v", err)
	}
	if _, err := readToken(symlink); err == nil {
		t.Fatal("symlinked credential was accepted")
	}
}

func TestBackupObserverProductionDependencyClosure(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list backupobserver dependencies: %v", err)
	}
	allowedLocal := map[string]bool{
		"fugue/internal/backupcontrol":  true,
		"fugue/internal/backupobserver": true,
	}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") && !allowedLocal[dependency] {
			t.Fatalf("backupobserver crossed component boundary through %q", dependency)
		}
		for _, forbidden := range []string{"database/", "os/exec", "k8s.io/", "github.com/aws/", "github.com/google/go-containerregistry", "github.com/jackc/"} {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden) {
				t.Fatalf("backupobserver imported mutation capability %q", dependency)
			}
		}
	}
	moduleCommand := exec.Command("go", "list", "-deps", "-f", "{{if .Module}}{{.Module.Path}} {{.ImportPath}}{{end}}", ".")
	moduleOutput, err := moduleCommand.Output()
	if err != nil {
		t.Fatalf("list backupobserver module dependencies: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(moduleOutput)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] != "fugue" {
			t.Fatalf("backupobserver imported external module %q through %q", fields[0], fields[1])
		}
	}
}

func observerServiceFixture(t *testing.T, spec backupcontrol.BackupRunSpec, baseURL string, now time.Time) *Service {
	t.Helper()
	directory := t.TempDir()
	specPath := filepath.Join(directory, "spec.json")
	tokenPath := filepath.Join(directory, "token")
	writeJSONFile(t, specPath, spec, 0o600)
	if err := os.WriteFile(tokenPath, []byte("observer-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	service, err := NewService(ServiceConfig{
		Enabled:                   true,
		ExpectedCellKey:           spec.CellKey,
		SpecPath:                  specPath,
		TokenPath:                 tokenPath,
		APIBaseURL:                baseURL,
		Interval:                  time.Second,
		AttemptTimeout:            2 * time.Second,
		RequestTimeout:            time.Second,
		AllowInsecureHTTPForTests: true,
	}, nil)
	if err != nil {
		t.Fatalf("new observer service: %v", err)
	}
	service.now = func() time.Time { return now }
	return service
}

func observerSpec(t *testing.T, runID, requestID, scope string) backupcontrol.BackupRunSpec {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		runID,
		requestID,
		backupcontrol.BackupTarget{Type: backupcontrol.TargetAppDatabase, ScopeKey: scope},
		"backend-1",
		observerBackendGeneration,
		3,
		120,
		900,
	)
	if err != nil {
		t.Fatalf("new observer spec: %v", err)
	}
	return spec
}

func observerStatus(t *testing.T, spec backupcontrol.BackupRunSpec, now time.Time, ttl time.Duration) backupcontrol.BackupRunStatus {
	t.Helper()
	status, err := backupcontrol.NewObservedBackupRunStatus(
		spec,
		backupcontrol.LegacyRunObservation{State: backupcontrol.ObservedStateSucceeded, Attempt: 1, Fence: 1, WorkerID: "worker-a"},
		&backupcontrol.BackupArtifactRef{
			ArtifactID:        "artifact-1",
			RunID:             spec.RunID,
			Kind:              spec.ArtifactKind,
			ContentDigest:     observerContentDigest,
			ManifestDigest:    observerManifestDigest,
			BackendGeneration: spec.BackendGeneration,
		},
		now,
		ttl,
	)
	if err != nil {
		t.Fatalf("new observer status: %v", err)
	}
	return status
}

func statusServer(t *testing.T, status backupcontrol.BackupRunStatus) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "private, no-store")
		if err := json.NewEncoder(w).Encode(status); err != nil {
			t.Errorf("encode status: %v", err)
		}
	}))
}

func responseHandler(body []byte, contentType, cacheControl string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		if cacheControl != "" {
			w.Header().Set("Cache-Control", cacheControl)
		}
		_, _ = w.Write(body)
	})
}

func writeJSONFile(t *testing.T, path string, value any, mode os.FileMode) {
	t.Helper()
	document, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %s: %v", path, err)
	}
	if err := os.WriteFile(path, document, mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestSnapshotReturnsDetachedNestedArtifacts(t *testing.T) {
	now := time.Date(2026, 7, 30, 3, 0, 0, 0, time.UTC)
	spec := observerSpec(t, "run-1", "request-1", "app/app-1/database")
	status := observerStatus(t, spec, now, time.Minute)
	server := statusServer(t, status)
	defer server.Close()
	service := observerServiceFixture(t, spec, server.URL, now)
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	first := service.Snapshot()
	first.DesiredSpec.RunID = "mutated"
	first.CurrentStatus.LastKnownGood.ArtifactID = "mutated"
	first.LastKnownGood.LastKnownGood.ArtifactID = "mutated-again"
	second := service.Snapshot()
	if second.DesiredSpec.RunID != spec.RunID || second.CurrentStatus.LastKnownGood.ArtifactID != "artifact-1" ||
		second.LastKnownGood.LastKnownGood.ArtifactID != "artifact-1" || reflect.DeepEqual(first, second) {
		t.Fatalf("snapshot mutation escaped copy boundary: first=%+v second=%+v", first, second)
	}
}

func TestObservationFailureCodePreservesCancellation(t *testing.T) {
	if got := observationFailureCode(context.DeadlineExceeded); got != "attempt_timeout" {
		t.Fatalf("deadline code = %q", got)
	}
	if got := observationFailureCode(context.Canceled); got != "canceled" {
		t.Fatalf("cancel code = %q", got)
	}
	if got := observationFailureCode(ErrObservationTransport); got != "api_unavailable" {
		t.Fatalf("transport code = %q", got)
	}
	if !errors.Is(&ObservationAPIStatusError{StatusCode: http.StatusServiceUnavailable}, ErrObservationAPI) {
		t.Fatal("status error lost API sentinel")
	}
}
