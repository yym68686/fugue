package backupobserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
)

func TestServicePersistsAndRestoresCellLocalLKGAcrossRestart(t *testing.T) {
	now := time.Date(2026, 7, 30, 5, 0, 0, 0, time.UTC)
	spec := observerSpec(t, "run-lkg-1", "request-lkg-1", "app/app-lkg/database")
	status := observerStatus(t, spec, now, 2*time.Minute)
	var unavailable atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if unavailable.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "private, no-store")
		_ = json.NewEncoder(w).Encode(status)
	}))
	defer server.Close()

	cfg, lkgPath := observerLKGServiceConfig(t, spec, server.URL)
	first, err := NewService(cfg, nil)
	if err != nil {
		t.Fatalf("new first service: %v", err)
	}
	first.now = func() time.Time { return now }
	if got := first.Snapshot().LKGState; got != LKGStateAbsent {
		t.Fatalf("initial LKG state=%q, want absent", got)
	}
	if err := first.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("persist first observation: %v", err)
	}
	firstSnapshot := first.Snapshot()
	if !firstSnapshot.Ready || firstSnapshot.LKGState != LKGStateCurrent ||
		firstSnapshot.LastKnownGood == nil || firstSnapshot.LastKnownGood.Digest != status.Digest {
		t.Fatalf("first service did not publish durable LKG: %+v", firstSnapshot)
	}
	document, err := os.ReadFile(lkgPath)
	if err != nil {
		t.Fatalf("read durable LKG: %v", err)
	}
	info, err := os.Lstat(lkgPath)
	if err != nil || info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("durable LKG mode=%v err=%v, want private regular 0600", info, err)
	}
	for _, forbidden := range []string{"observer-token", "authorization", "bucket", "objectKey", "secretAccessKey"} {
		if bytes.Contains(bytes.ToLower(document), bytes.ToLower([]byte(forbidden))) {
			t.Fatalf("durable LKG leaked %q: %s", forbidden, document)
		}
	}

	restarted, err := NewService(cfg, nil)
	if err != nil {
		t.Fatalf("new restarted service: %v", err)
	}
	restarted.now = func() time.Time { return now }
	restored := restarted.Snapshot()
	if restored.Ready || restored.CurrentStatus != nil || restored.LKGState != LKGStateCurrent ||
		restored.DesiredSpec == nil || restored.DesiredSpec.Digest != spec.Digest ||
		restored.LastKnownGood == nil || restored.LastKnownGood.Digest != status.Digest {
		t.Fatalf("restart did not restore exact unready LKG: %+v", restored)
	}
	unavailable.Store(true)
	if err := restarted.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("outage unexpectedly reconciled after restart")
	}
	failed := restarted.Snapshot()
	if failed.Ready || failed.CurrentStatus != nil || failed.LKGState != LKGStateCurrent ||
		failed.LastKnownGood == nil || failed.LastKnownGood.Digest != status.Digest || failed.FailureCode != "api_retryable" {
		t.Fatalf("restart outage did not retain cell-local LKG: %+v", failed)
	}
}

func TestBackupObserverLKGRecoversPreviousGenerationWhenCurrentIsCorrupt(t *testing.T) {
	now := time.Date(2026, 7, 30, 5, 0, 0, 0, time.UTC)
	spec := observerSpec(t, "run-lkg-2", "request-lkg-2", "app/app-lkg/database")
	first := observerStatus(t, spec, now, 2*time.Minute)
	second := observerStatus(t, spec, now.Add(time.Second), 2*time.Minute)
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatalf("mkdir state: %v", err)
	}
	path := filepath.Join(stateDir, "lkg.json")
	if err := persistBackupObserverLKG(path, spec.CellKey, spec, first, now); err != nil {
		t.Fatalf("persist first generation: %v", err)
	}
	if err := persistBackupObserverLKG(path, spec.CellKey, spec, second, now.Add(time.Second)); err != nil {
		t.Fatalf("persist second generation: %v", err)
	}
	previous, err := readPersistedLKG(previousLKGPath(path), spec.CellKey)
	if err != nil || previous.Status.Digest != first.Digest {
		t.Fatalf("previous generation=%+v err=%v, want first status", previous, err)
	}
	if err := os.WriteFile(path, []byte("{corrupt\n"), 0o600); err != nil {
		t.Fatalf("corrupt current generation: %v", err)
	}
	restored := restoreBackupObserverLKG(path, spec.CellKey)
	if restored.State != LKGStatePrevious || restored.Err == nil || restored.Spec.Digest != spec.Digest ||
		restored.Status.Digest != first.Digest {
		t.Fatalf("previous fallback drifted: %+v", restored)
	}
}

func TestBackupObserverLKGPersistenceUsesObservationClockSkewBoundary(t *testing.T) {
	now := time.Date(2026, 7, 30, 5, 0, 0, 0, time.UTC)
	spec := observerSpec(t, "run-lkg-skew", "request-lkg-skew", "app/app-lkg/database")
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatalf("mkdir state: %v", err)
	}
	path := filepath.Join(stateDir, "lkg.json")
	withinSkew := observerStatus(t, spec, now.Add(maxObservationFutureSkew), 2*time.Minute)
	if err := persistBackupObserverLKG(path, spec.CellKey, spec, withinSkew, now); err != nil {
		t.Fatalf("persist observation at allowed clock skew: %v", err)
	}
	beyondSkew := observerStatus(t, spec, now.Add(maxObservationFutureSkew+time.Second), 2*time.Minute)
	if err := persistBackupObserverLKG(path, spec.CellKey, spec, beyondSkew, now); err == nil {
		t.Fatal("persisted observation beyond the allowed clock skew")
	}
}

func TestBackupObserverLKGRejectsUnknownTamperedBroadAndSymlinkState(t *testing.T) {
	now := time.Date(2026, 7, 30, 5, 0, 0, 0, time.UTC)
	spec := observerSpec(t, "run-lkg-3", "request-lkg-3", "app/app-lkg/database")
	status := observerStatus(t, spec, now, 2*time.Minute)
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{name: "unknown field", mutate: func(t *testing.T, path string) {
			t.Helper()
			document, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read LKG: %v", err)
			}
			document = bytes.TrimSpace(document)
			document = append(document[:len(document)-1], []byte(`,"token":"forbidden"}`)...)
			if err := os.WriteFile(path, document, 0o600); err != nil {
				t.Fatalf("write unknown field: %v", err)
			}
		}},
		{name: "tampered status digest", mutate: func(t *testing.T, path string) {
			t.Helper()
			document, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read LKG: %v", err)
			}
			document = bytes.Replace(document, []byte(status.Digest), []byte("sha256:"+strings.Repeat("f", 64)), 1)
			if err := os.WriteFile(path, document, 0o600); err != nil {
				t.Fatalf("write tampered LKG: %v", err)
			}
		}},
		{name: "broad mode", mutate: func(t *testing.T, path string) {
			t.Helper()
			if err := os.Chmod(path, 0o640); err != nil {
				t.Fatalf("broaden LKG mode: %v", err)
			}
		}},
		{name: "world writable parent", mutate: func(t *testing.T, path string) {
			t.Helper()
			if err := os.Chmod(filepath.Dir(path), 0o777); err != nil {
				t.Fatalf("broaden LKG parent mode: %v", err)
			}
		}},
		{name: "symlink", mutate: func(t *testing.T, path string) {
			t.Helper()
			target := path + ".target"
			if err := os.Rename(path, target); err != nil {
				t.Fatalf("move LKG target: %v", err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatalf("link LKG: %v", err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			stateDir := filepath.Join(t.TempDir(), "state")
			if err := os.Mkdir(stateDir, 0o700); err != nil {
				t.Fatalf("mkdir state: %v", err)
			}
			path := filepath.Join(stateDir, "lkg.json")
			if err := persistBackupObserverLKG(path, spec.CellKey, spec, status, now); err != nil {
				t.Fatalf("persist LKG: %v", err)
			}
			test.mutate(t, path)
			restored := restoreBackupObserverLKG(path, spec.CellKey)
			if restored.State != LKGStateInvalid || restored.Status.Digest != "" {
				t.Fatalf("unsafe LKG was restored: %+v", restored)
			}
		})
	}
}

func TestServiceFailsClosedWhenDurableLKGCannotBePublished(t *testing.T) {
	now := time.Date(2026, 7, 30, 5, 0, 0, 0, time.UTC)
	spec := observerSpec(t, "run-lkg-4", "request-lkg-4", "app/app-lkg/database")
	status := observerStatus(t, spec, now, 2*time.Minute)
	server := statusServer(t, status)
	defer server.Close()
	cfg, path := observerLKGServiceConfig(t, spec, server.URL)
	service, err := NewService(cfg, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.now = func() time.Time { return now }
	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("persist initial LKG: %v", err)
	}
	initial, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read initial LKG: %v", err)
	}
	target := path + ".target"
	if err := os.Rename(path, target); err != nil {
		t.Fatalf("move initial LKG: %v", err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatalf("link unsafe LKG: %v", err)
	}
	if err := service.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("unpublishable LKG unexpectedly reconciled")
	}
	snapshot := service.Snapshot()
	if snapshot.Ready || snapshot.CurrentStatus != nil || snapshot.LastKnownGood == nil ||
		snapshot.LastKnownGood.Digest != status.Digest ||
		snapshot.LKGState != LKGStatePersistFailed || snapshot.FailureCode != "lkg_persist_failed" {
		t.Fatalf("LKG publication failure did not fail closed: %+v", snapshot)
	}
	document, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(document, initial) {
		t.Fatalf("unsafe symlink target changed: %q err=%v", document, err)
	}
}

func TestServiceConfigSeparatesLKGFromProjectedInputs(t *testing.T) {
	spec := observerSpec(t, "run-lkg-5", "request-lkg-5", "app/app-lkg/database")
	base := ServiceConfig{
		Enabled: true, ExpectedCellKey: spec.CellKey,
		SpecPath: "/run/fugue/backup-observer/spec/spec.json", TokenPath: "/run/fugue/backup-observer/token/token",
		APIBaseURL: "https://api.fugue.test", Interval: time.Second, AttemptTimeout: time.Second,
	}
	for name, path := range map[string]string{
		"relative":       "lkg.json",
		"spec directory": "/run/fugue/backup-observer/spec/lkg.json",
		"token ancestor": "/run/fugue/backup-observer/lkg.json",
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			cfg.LKGPath = path
			if _, err := NewService(cfg, nil); err == nil {
				t.Fatal("unsafe LKG path overlap was accepted")
			}
		})
	}
	base.LKGPath = "/var/lib/fugue-backup-observer/lkg.json"
	if _, err := NewService(base, nil); err != nil {
		t.Fatalf("separate LKG path rejected: %v", err)
	}
}

func observerLKGServiceConfig(t *testing.T, spec backupcontrol.BackupRunSpec, baseURL string) (ServiceConfig, string) {
	t.Helper()
	root := t.TempDir()
	inputDir := filepath.Join(root, "inputs")
	stateDir := filepath.Join(root, "state")
	if err := os.Mkdir(inputDir, 0o700); err != nil {
		t.Fatalf("mkdir inputs: %v", err)
	}
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatalf("mkdir state: %v", err)
	}
	specPath := filepath.Join(inputDir, "spec.json")
	tokenPath := filepath.Join(inputDir, "token")
	writeJSONFile(t, specPath, spec, 0o600)
	if err := os.WriteFile(tokenPath, []byte("observer-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	lkgPath := filepath.Join(stateDir, "lkg.json")
	return ServiceConfig{
		Enabled: true, ExpectedCellKey: spec.CellKey,
		SpecPath: specPath, TokenPath: tokenPath, LKGPath: lkgPath, APIBaseURL: baseURL,
		Interval: time.Second, AttemptTimeout: 2 * time.Second, RequestTimeout: time.Second,
		AllowInsecureHTTPForTests: true,
	}, lkgPath
}
