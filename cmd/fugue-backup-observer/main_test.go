package main

import (
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestObserverConfigDefaultsToSafeDisabledMode(t *testing.T) {
	cfg, err := observerConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	if cfg.BindAddr != defaultBindAddr || cfg.ShutdownTimeout != defaultShutdown || cfg.Service.Enabled ||
		cfg.Service.ExpectedCellKey != "" || cfg.Service.SpecPath != "" || cfg.Service.TokenPath != "" ||
		cfg.Service.APIBaseURL != "" || cfg.Service.Interval != defaultInterval ||
		cfg.Service.AttemptTimeout != defaultAttemptTimeout || cfg.Service.RequestTimeout != defaultRequestTimeout ||
		cfg.Service.MaxResponseBytes != defaultResponseBytes || cfg.Service.AllowInsecureHTTPForTests {
		t.Fatalf("default disabled boundary drifted: %+v", cfg)
	}
}

func TestObserverConfigEnablesOnlyExactBoundedHTTPSCell(t *testing.T) {
	values := map[string]string{
		"FUGUE_BACKUP_OBSERVER_ENABLED":            "true",
		"FUGUE_BACKUP_OBSERVER_BIND_ADDR":          "[::1]:18092",
		"FUGUE_BACKUP_OBSERVER_CELL_KEY":           "backup/app-database/0123456789abcdef",
		"FUGUE_BACKUP_OBSERVER_SPEC_FILE":          "/run/fugue/backup/spec.json",
		"FUGUE_BACKUP_OBSERVER_TOKEN_FILE":         "/run/secrets/backup-observer/token",
		"FUGUE_BACKUP_OBSERVER_LKG_FILE":           "/var/lib/fugue-backup-observer/lkg.json",
		"FUGUE_BACKUP_OBSERVER_API_BASE_URL":       "https://api.fugue.test/internal",
		"FUGUE_BACKUP_OBSERVER_RECONCILE_INTERVAL": "15s",
		"FUGUE_BACKUP_OBSERVER_ATTEMPT_TIMEOUT":    "18s",
		"FUGUE_BACKUP_OBSERVER_REQUEST_TIMEOUT":    "8s",
		"FUGUE_BACKUP_OBSERVER_SHUTDOWN_TIMEOUT":   "9s",
		"FUGUE_BACKUP_OBSERVER_MAX_RESPONSE_BYTES": "32768",
	}
	cfg, err := observerConfigFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("enabled config: %v", err)
	}
	if !cfg.Service.Enabled || cfg.BindAddr != values["FUGUE_BACKUP_OBSERVER_BIND_ADDR"] ||
		cfg.Service.ExpectedCellKey != values["FUGUE_BACKUP_OBSERVER_CELL_KEY"] ||
		cfg.Service.SpecPath != values["FUGUE_BACKUP_OBSERVER_SPEC_FILE"] ||
		cfg.Service.TokenPath != values["FUGUE_BACKUP_OBSERVER_TOKEN_FILE"] ||
		cfg.Service.LKGPath != values["FUGUE_BACKUP_OBSERVER_LKG_FILE"] ||
		cfg.Service.APIBaseURL != values["FUGUE_BACKUP_OBSERVER_API_BASE_URL"] ||
		cfg.Service.Interval != 15*time.Second || cfg.Service.AttemptTimeout != 18*time.Second ||
		cfg.Service.RequestTimeout != 8*time.Second || cfg.ShutdownTimeout != 9*time.Second ||
		cfg.Service.MaxResponseBytes != 32768 || cfg.Service.AllowInsecureHTTPForTests {
		t.Fatalf("enabled boundary drifted: %+v", cfg)
	}
}

func TestObserverConfigRejectsAmbiguousOrUnsafeValues(t *testing.T) {
	valid := map[string]string{
		"FUGUE_BACKUP_OBSERVER_ENABLED":      "true",
		"FUGUE_BACKUP_OBSERVER_CELL_KEY":     "backup/app-database/0123456789abcdef",
		"FUGUE_BACKUP_OBSERVER_SPEC_FILE":    "/run/fugue/backup/spec.json",
		"FUGUE_BACKUP_OBSERVER_TOKEN_FILE":   "/run/secrets/backup-observer/token",
		"FUGUE_BACKUP_OBSERVER_LKG_FILE":     "/var/lib/fugue-backup-observer/lkg.json",
		"FUGUE_BACKUP_OBSERVER_API_BASE_URL": "https://api.fugue.test",
	}
	for name, override := range map[string]map[string]string{
		"ambiguous bool":   {"FUGUE_BACKUP_OBSERVER_ENABLED": "1"},
		"public bind":      {"FUGUE_BACKUP_OBSERVER_BIND_ADDR": "0.0.0.0:8092"},
		"hostname bind":    {"FUGUE_BACKUP_OBSERVER_BIND_ADDR": "localhost:8092"},
		"missing cell":     {"FUGUE_BACKUP_OBSERVER_CELL_KEY": ""},
		"relative spec":    {"FUGUE_BACKUP_OBSERVER_SPEC_FILE": "spec.json"},
		"relative token":   {"FUGUE_BACKUP_OBSERVER_TOKEN_FILE": "token"},
		"relative LKG":     {"FUGUE_BACKUP_OBSERVER_LKG_FILE": "lkg.json"},
		"plaintext API":    {"FUGUE_BACKUP_OBSERVER_API_BASE_URL": "http://api.fugue.test"},
		"zero interval":    {"FUGUE_BACKUP_OBSERVER_RECONCILE_INTERVAL": "0s"},
		"excess interval":  {"FUGUE_BACKUP_OBSERVER_RECONCILE_INTERVAL": "11m"},
		"excess attempt":   {"FUGUE_BACKUP_OBSERVER_ATTEMPT_TIMEOUT": "61s"},
		"excess request":   {"FUGUE_BACKUP_OBSERVER_REQUEST_TIMEOUT": "31s"},
		"excess response":  {"FUGUE_BACKUP_OBSERVER_MAX_RESPONSE_BYTES": "1048577"},
		"invalid shutdown": {"FUGUE_BACKUP_OBSERVER_SHUTDOWN_TIMEOUT": "later"},
		"excess shutdown":  {"FUGUE_BACKUP_OBSERVER_SHUTDOWN_TIMEOUT": "61s"},
	} {
		t.Run(name, func(t *testing.T) {
			values := make(map[string]string, len(valid)+1)
			for key, value := range valid {
				values[key] = value
			}
			for key, value := range override {
				values[key] = value
			}
			if _, err := observerConfigFromEnv(func(key string) string { return values[key] }); err == nil {
				t.Fatal("unsafe configuration was accepted")
			}
		})
	}
}

func TestProbeIsLoopbackOnlyAndStatusExact(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})}
	defer server.Close()
	go func() { _ = server.Serve(listener) }()
	getenv := func(key string) string {
		if key == "FUGUE_BACKUP_OBSERVER_BIND_ADDR" {
			return listener.Addr().String()
		}
		return ""
	}
	if err := runProbe([]string{"probe", "health"}, getenv); err != nil {
		t.Fatalf("health probe: %v", err)
	}
	if err := runProbe([]string{"probe", "ready"}, getenv); err == nil {
		t.Fatal("unready endpoint passed the ready probe")
	}
	if err := runProbe([]string{"probe", "other"}, getenv); err == nil {
		t.Fatal("unknown probe target was accepted")
	}
	if err := runProbe([]string{"probe", "health"}, func(string) string { return "0.0.0.0:8092" }); err == nil {
		t.Fatal("probe accepted a non-loopback target")
	}
}

func TestObserverBinaryDependencyBoundary(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list observer binary dependencies: %v", err)
	}
	allowedLocal := map[string]bool{
		"fugue/cmd/fugue-backup-observer": true,
		"fugue/internal/backupcontrol":    true,
		"fugue/internal/backupobserver":   true,
	}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") && !allowedLocal[dependency] {
			t.Fatalf("observer binary crossed component boundary through %q", dependency)
		}
		for _, forbidden := range []string{"database/", "os/exec", "k8s.io/", "github.com/aws/", "github.com/google/go-containerregistry", "github.com/jackc/"} {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden) {
				t.Fatalf("observer binary imported mutation capability %q", dependency)
			}
		}
	}
}

func TestBackupObserverDockerfileHasExactSourceAndRuntimeBoundary(t *testing.T) {
	document, err := os.ReadFile("../../Dockerfile.backup-observer")
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	raw := string(document)
	var sourceCopies []string
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "COPY ") && !strings.HasPrefix(line, "COPY --from=") {
			sourceCopies = append(sourceCopies, line)
		}
	}
	wantCopies := []string{
		"COPY go.mod go.sum ./",
		"COPY cmd/fugue-backup-observer ./cmd/fugue-backup-observer",
		"COPY internal/backupcontrol ./internal/backupcontrol",
		"COPY internal/backupobserver ./internal/backupobserver",
	}
	if strings.Join(sourceCopies, "\n") != strings.Join(wantCopies, "\n") {
		t.Fatalf("Docker source closure drifted:\n%s", strings.Join(sourceCopies, "\n"))
	}
	for _, required := range []string{
		"golang:1.25-alpine@sha256:56961d79ea8129efddcc0b8643fd8a5416b4e6228cfd477e3fd61deb2672c587",
		"CGO_ENABLED=0",
		"FROM scratch",
		"USER 65532:65532",
		`ENTRYPOINT ["/usr/local/bin/fugue-backup-observer"]`,
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("Dockerfile is missing %q", required)
		}
	}
	for _, forbidden := range []string{"COPY internal/api", "COPY internal/model", "COPY internal/store", "COPY internal/controller", "EXPOSE ", "apk add curl", "apk add bash"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("Dockerfile widened runtime boundary through %q", forbidden)
		}
	}
}
