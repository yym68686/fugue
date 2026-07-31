package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	materializeragent "fugue/internal/backupmaterializer/agent"
	clientprojected "fugue/internal/backupmaterializer/client/projected"
	secretprojected "fugue/internal/backupmaterializer/secretreader/projected"
)

const (
	testCellKey = "backup/app-database/0123456789abcdef"
	testRunID   = "run-1"
	testJWT     = "header.materializer-payload.signature"
)

func TestConfigDefaultsToInertDisabledProcess(t *testing.T) {
	values := map[string]string{
		"FUGUE_BACKUP_MATERIALIZER_CELL_KEY":                   "private-invalid-cell",
		"FUGUE_BACKUP_MATERIALIZER_RUN_ID":                     "private invalid run",
		"FUGUE_BACKUP_MATERIALIZER_INPUT_API_BASE_URL":         "private invalid URL",
		"FUGUE_BACKUP_MATERIALIZER_INPUT_PROJECTION_ROOT":      "/private/missing/input",
		"FUGUE_BACKUP_MATERIALIZER_KUBERNETES_API_URL":         "private invalid Kubernetes URL",
		"FUGUE_BACKUP_MATERIALIZER_KUBERNETES_PROJECTION_ROOT": "/private/missing/kubernetes",
		"FUGUE_BACKUP_MATERIALIZER_RECONCILE_INTERVAL":         "invalid",
		"FUGUE_BACKUP_MATERIALIZER_ATTEMPT_TIMEOUT":            "invalid",
		"FUGUE_BACKUP_MATERIALIZER_INPUT_REQUEST_TIMEOUT":      "invalid",
		"FUGUE_BACKUP_MATERIALIZER_SECRET_REQUEST_TIMEOUT":     "invalid",
	}
	config, err := materializerConfigFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	if config.Enabled || config.BindAddr != defaultBindAddr || config.ShutdownTimeout != defaultShutdownTimeout ||
		config.CellKey != "" || config.RunID != "" || config.Interval != 0 || config.AttemptTimeout != 0 ||
		!reflect.DeepEqual(config.InputProjection, zeroInputProjection()) ||
		!reflect.DeepEqual(config.SecretProjection, zeroSecretProjection()) {
		t.Fatalf("disabled config retained a capability: %#v", config)
	}
	service, err := newMaterializerService(config, log.New(io.Discard, "private-prefix", 0))
	if err != nil || service.Enabled() || service.Snapshot().Mode != materializeragent.ModeDisabled {
		t.Fatalf("disabled service drifted: service=%#v snapshot=%#v err=%v", service, service.Snapshot(), err)
	}
	rendered := strings.Join([]string{fmtValue(config), fmtGoValue(config), fmtValue(service), fmtGoValue(service)}, "\n")
	for _, private := range []string{"private-invalid-cell", "/private/missing/input", "private-prefix"} {
		if strings.Contains(rendered, private) {
			t.Fatalf("disabled diagnostics exposed %q: %s", private, rendered)
		}
	}
}

func TestConfigEnablesOneExactBoundedCell(t *testing.T) {
	values := validEnvironment()
	config, err := materializerConfigFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("enabled config: %v", err)
	}
	if !config.Enabled || config.BindAddr != "[::1]:18093" || config.CellKey != testCellKey || config.RunID != testRunID ||
		config.Interval != 15*time.Second || config.AttemptTimeout != 18*time.Second ||
		config.ShutdownTimeout != 9*time.Second || !config.InputProjection.Enabled ||
		config.InputProjection.BaseURL != "https://api.example.test" ||
		config.InputProjection.ProjectionRoot != "/run/fugue/materializer/input" ||
		config.InputProjection.ExpectedCellKey != testCellKey || config.InputProjection.ExpectedRunID != testRunID ||
		config.InputProjection.RequestTimeout != 8*time.Second ||
		config.InputProjection.HandshakeTimeout != 4*time.Second || config.InputProjection.MaxResponseBytes != 32768 ||
		!config.SecretProjection.Enabled || config.SecretProjection.APIServerURL != "https://kubernetes.default.svc" ||
		config.SecretProjection.ProjectionRoot != "/run/fugue/materializer/kubernetes" ||
		config.SecretProjection.ExpectedCellKey != testCellKey || config.SecretProjection.RequestTimeout != 7*time.Second ||
		config.SecretProjection.HandshakeTimeout != 3*time.Second || config.SecretProjection.MaxResponseBytes != 65536 {
		t.Fatalf("enabled config drifted: %#v", config)
	}
}

func TestConfigRejectsAmbiguousOrUnboundedValues(t *testing.T) {
	for name, override := range map[string]map[string]string{
		"ambiguous enable": {"FUGUE_BACKUP_MATERIALIZER_ENABLED": "1"},
		"public bind":      {"FUGUE_BACKUP_MATERIALIZER_BIND_ADDR": "0.0.0.0:8093"},
		"hostname bind":    {"FUGUE_BACKUP_MATERIALIZER_BIND_ADDR": "localhost:8093"},
		"privileged port":  {"FUGUE_BACKUP_MATERIALIZER_BIND_ADDR": "127.0.0.1:443"},
		"zero shutdown":    {"FUGUE_BACKUP_MATERIALIZER_SHUTDOWN_TIMEOUT": "0s"},
		"long shutdown":    {"FUGUE_BACKUP_MATERIALIZER_SHUTDOWN_TIMEOUT": "61s"},
		"short interval":   {"FUGUE_BACKUP_MATERIALIZER_RECONCILE_INTERVAL": "999ms"},
		"long interval":    {"FUGUE_BACKUP_MATERIALIZER_RECONCILE_INTERVAL": "11m"},
		"fractional time":  {"FUGUE_BACKUP_MATERIALIZER_ATTEMPT_TIMEOUT": "1.000000001s"},
		"long attempt":     {"FUGUE_BACKUP_MATERIALIZER_ATTEMPT_TIMEOUT": "61s"},
		"long input":       {"FUGUE_BACKUP_MATERIALIZER_INPUT_REQUEST_TIMEOUT": "31s"},
		"input handshake":  {"FUGUE_BACKUP_MATERIALIZER_INPUT_HANDSHAKE_TIMEOUT": "9s"},
		"secret handshake": {"FUGUE_BACKUP_MATERIALIZER_SECRET_HANDSHAKE_TIMEOUT": "8s"},
		"request over attempt": {
			"FUGUE_BACKUP_MATERIALIZER_ATTEMPT_TIMEOUT":       "6s",
			"FUGUE_BACKUP_MATERIALIZER_INPUT_REQUEST_TIMEOUT": "7s",
		},
		"small input body":  {"FUGUE_BACKUP_MATERIALIZER_INPUT_MAX_RESPONSE_BYTES": "100"},
		"large input body":  {"FUGUE_BACKUP_MATERIALIZER_INPUT_MAX_RESPONSE_BYTES": "65537"},
		"small Secret body": {"FUGUE_BACKUP_MATERIALIZER_SECRET_MAX_RESPONSE_BYTES": "4095"},
		"large Secret body": {"FUGUE_BACKUP_MATERIALIZER_SECRET_MAX_RESPONSE_BYTES": "262145"},
	} {
		t.Run(name, func(t *testing.T) {
			values := validEnvironment()
			for key, value := range override {
				values[key] = value
			}
			if config, err := materializerConfigFromEnv(func(key string) string { return values[key] }); err == nil {
				t.Fatalf("unsafe configuration was accepted: %#v", config)
			}
		})
	}
}

func TestEnabledCompositionAcquiresOnlyTwoReadProjections(t *testing.T) {
	ca := testCAPEM(t)
	inputRoot := testProjection(t, ca)
	secretRoot := testProjection(t, ca)
	values := validEnvironment()
	values["FUGUE_BACKUP_MATERIALIZER_INPUT_PROJECTION_ROOT"] = inputRoot
	values["FUGUE_BACKUP_MATERIALIZER_KUBERNETES_PROJECTION_ROOT"] = secretRoot
	config, err := materializerConfigFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("parse composition: %v", err)
	}
	service, err := newMaterializerService(config, nil)
	if err != nil {
		t.Fatalf("construct composition: %v", err)
	}
	snapshot := service.Snapshot()
	if !service.Enabled() || snapshot.Mode != materializeragent.ModeShadow || snapshot.CellKey != testCellKey ||
		snapshot.Ready || snapshot.AttemptCount != 0 || materializeragent.ValidateSnapshot(snapshot) != nil {
		t.Fatalf("composed service drifted: %#v", snapshot)
	}

	privateRoot := "/private/missing/materializer-projection"
	config.InputProjection.ProjectionRoot = privateRoot
	if _, err := newMaterializerService(config, nil); err == nil || strings.Contains(err.Error(), privateRoot) {
		t.Fatalf("invalid input projection error was absent or exposed the path: %v", err)
	}
	config.InputProjection.ProjectionRoot = inputRoot
	config.SecretProjection.ProjectionRoot = privateRoot
	if _, err := newMaterializerService(config, nil); err == nil || strings.Contains(err.Error(), privateRoot) {
		t.Fatalf("invalid Secret projection error was absent or exposed the path: %v", err)
	}
	config.SecretProjection.ProjectionRoot = secretRoot
	for name, mutate := range map[string]func(*materializerConfig){
		"input disabled":  func(value *materializerConfig) { value.InputProjection.Enabled = false },
		"Secret disabled": func(value *materializerConfig) { value.SecretProjection.Enabled = false },
		"input cell": func(value *materializerConfig) {
			value.InputProjection.ExpectedCellKey = "backup/app-database/fedcba9876543210"
		},
		"Secret cell": func(value *materializerConfig) {
			value.SecretProjection.ExpectedCellKey = "backup/app-database/fedcba9876543210"
		},
		"input run": func(value *materializerConfig) { value.InputProjection.ExpectedRunID = "other-run" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := config
			mutate(&candidate)
			if _, err := newMaterializerService(candidate, nil); err == nil {
				t.Fatal("inconsistent projection identity was accepted")
			}
		})
	}
}

func TestServeOwnsLoopbackLifecycleAndDrainsAgent(t *testing.T) {
	service, err := materializeragent.New(materializeragent.Config{Enabled: false}, nil)
	if err != nil {
		t.Fatalf("construct disabled agent: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &http.Server{Handler: service.Handler()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, time.Second, service, server, listener) }()

	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + listener.Addr().String() + "/healthz")
	if err != nil {
		cancel()
		t.Fatalf("read health: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("health status = %d", response.StatusCode)
	}
	response, err = client.Get("http://" + listener.Addr().String() + "/readyz")
	if err != nil {
		cancel()
		t.Fatalf("read readiness: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		cancel()
		t.Fatalf("disabled readiness status = %d", response.StatusCode)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("drain lifecycle: %v", err)
	}
}

func TestLifecycleAndTopLevelRunFailClosed(t *testing.T) {
	service, err := materializeragent.New(materializeragent.Config{Enabled: false}, nil)
	if err != nil {
		t.Fatalf("construct disabled agent: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	server := &http.Server{Handler: service.Handler()}
	if err := serve(context.Background(), time.Second, service, server, listener); err == nil {
		t.Fatal("unexpected server stop was reported as clean")
	}
	if err := serve(nil, time.Second, service, server, listener); err == nil {
		t.Fatal("nil lifecycle context was accepted")
	}
	if err := serve(context.Background(), time.Second, service, &http.Server{}, listener); err == nil {
		t.Fatal("nil lifecycle handler was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	config := materializerConfig{BindAddr: defaultBindAddr, ShutdownTimeout: defaultShutdownTimeout}
	if err := run(canceled, config, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("already-canceled run error = %v", err)
	}
	if err := run(context.Background(), materializerConfig{BindAddr: "0.0.0.0:8093", ShutdownTimeout: defaultShutdownTimeout}, nil); err == nil {
		t.Fatal("top-level run accepted public bind")
	}
	if err := run(context.Background(), materializerConfig{BindAddr: defaultBindAddr, ShutdownTimeout: time.Nanosecond}, nil); err == nil {
		t.Fatal("top-level run accepted unsafe shutdown timeout")
	}
}

func TestProbeIsDirectLoopbackOnlyAndStatusExact(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/healthz" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		writer.WriteHeader(http.StatusServiceUnavailable)
	})}
	defer server.Close()
	go func() { _ = server.Serve(listener) }()
	getenv := func(key string) string {
		if key == "FUGUE_BACKUP_MATERIALIZER_BIND_ADDR" {
			return listener.Addr().String()
		}
		return ""
	}
	if err := runProbe([]string{"probe", "health"}, getenv); err != nil {
		t.Fatalf("health probe: %v", err)
	}
	if err := runProbe([]string{"probe", "ready"}, getenv); err == nil {
		t.Fatal("unready endpoint passed readiness probe")
	}
	if err := runProbe([]string{"probe", "other"}, getenv); err == nil {
		t.Fatal("unknown probe target was accepted")
	}
	if err := runProbe([]string{"probe", "health"}, nil); err == nil {
		t.Fatal("nil environment reader was accepted")
	}
	if err := runProbe([]string{"probe", "health"}, func(string) string { return "0.0.0.0:8093" }); err == nil {
		t.Fatal("probe accepted a non-loopback target")
	}
}

func TestBinaryDependencyBoundary(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list materializer binary dependencies: %v", err)
	}
	var local []string
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{
			"database/sql", "os/exec", "k8s.io/", "github.com/aws/", "github.com/google/go-containerregistry",
			"github.com/jackc/", "fugue/internal/api", "fugue/internal/model", "fugue/internal/store",
			"fugue/internal/backupidentity", "fugue/internal/backupmaterializerreview",
		} {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden) {
				t.Fatalf("materializer binary imported forbidden capability %q", dependency)
			}
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/cmd/fugue-backup-materializer",
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/agent",
		"fugue/internal/backupmaterializer/client",
		"fugue/internal/backupmaterializer/client/projected",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconcile",
		"fugue/internal/backupmaterializer/reconciler",
		"fugue/internal/backupmaterializer/secretreader",
		"fugue/internal/backupmaterializer/secretreader/projected",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("materializer binary dependency closure drifted: got=%v want=%v", local, want)
	}
}

func TestDockerfileHasExactReadOnlySourceAndScratchBoundary(t *testing.T) {
	document, err := os.ReadFile("../../Dockerfile.backup-materializer")
	if err != nil {
		t.Fatalf("read materializer Dockerfile: %v", err)
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
		"COPY cmd/fugue-backup-materializer ./cmd/fugue-backup-materializer",
		"COPY internal/backupcontrol ./internal/backupcontrol",
		"COPY internal/backupmaterializer/agent ./internal/backupmaterializer/agent",
		"COPY internal/backupmaterializer/client ./internal/backupmaterializer/client",
		"COPY internal/backupmaterializer/contract ./internal/backupmaterializer/contract",
		"COPY internal/backupmaterializer/materialization ./internal/backupmaterializer/materialization",
		"COPY internal/backupmaterializer/reconcile ./internal/backupmaterializer/reconcile",
		"COPY internal/backupmaterializer/reconciler ./internal/backupmaterializer/reconciler",
		"COPY internal/backupmaterializer/secretreader ./internal/backupmaterializer/secretreader",
	}
	if !reflect.DeepEqual(sourceCopies, wantCopies) {
		t.Fatalf("materializer Docker source closure drifted: got=%v want=%v", sourceCopies, wantCopies)
	}
	for _, required := range []string{
		"golang:1.25-alpine@sha256:56961d79ea8129efddcc0b8643fd8a5416b4e6228cfd477e3fd61deb2672c587",
		"CGO_ENABLED=0",
		"-trimpath",
		"-buildvcs=false",
		"FROM scratch",
		"USER 65532:65532",
		`ENTRYPOINT ["/usr/local/bin/fugue-backup-materializer"]`,
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("materializer Dockerfile is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"COPY internal/api", "COPY internal/auth", "COPY internal/backupidentity",
		"COPY internal/backupmaterializer/composition", "COPY internal/backupmaterializer/httpapi",
		"COPY internal/backupmaterializer/localissuer", "COPY internal/backupmaterializer/storesource",
		"COPY internal/backupmaterializeridentity", "COPY internal/backupmaterializerreview",
		"COPY internal/model", "COPY internal/store", "EXPOSE ", "apk add curl", "apk add bash",
		"/etc/ssl/certs/ca-certificates.crt",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("materializer Dockerfile widened runtime/source boundary through %q", forbidden)
		}
	}
}

func validEnvironment() map[string]string {
	return map[string]string{
		"FUGUE_BACKUP_MATERIALIZER_ENABLED":                    "true",
		"FUGUE_BACKUP_MATERIALIZER_BIND_ADDR":                  "[::1]:18093",
		"FUGUE_BACKUP_MATERIALIZER_CELL_KEY":                   testCellKey,
		"FUGUE_BACKUP_MATERIALIZER_RUN_ID":                     testRunID,
		"FUGUE_BACKUP_MATERIALIZER_INPUT_API_BASE_URL":         "https://api.example.test",
		"FUGUE_BACKUP_MATERIALIZER_INPUT_PROJECTION_ROOT":      "/run/fugue/materializer/input",
		"FUGUE_BACKUP_MATERIALIZER_KUBERNETES_API_URL":         "https://kubernetes.default.svc",
		"FUGUE_BACKUP_MATERIALIZER_KUBERNETES_PROJECTION_ROOT": "/run/fugue/materializer/kubernetes",
		"FUGUE_BACKUP_MATERIALIZER_RECONCILE_INTERVAL":         "15s",
		"FUGUE_BACKUP_MATERIALIZER_ATTEMPT_TIMEOUT":            "18s",
		"FUGUE_BACKUP_MATERIALIZER_INPUT_REQUEST_TIMEOUT":      "8s",
		"FUGUE_BACKUP_MATERIALIZER_INPUT_HANDSHAKE_TIMEOUT":    "4s",
		"FUGUE_BACKUP_MATERIALIZER_SECRET_REQUEST_TIMEOUT":     "7s",
		"FUGUE_BACKUP_MATERIALIZER_SECRET_HANDSHAKE_TIMEOUT":   "3s",
		"FUGUE_BACKUP_MATERIALIZER_SHUTDOWN_TIMEOUT":           "9s",
		"FUGUE_BACKUP_MATERIALIZER_INPUT_MAX_RESPONSE_BYTES":   "32768",
		"FUGUE_BACKUP_MATERIALIZER_SECRET_MAX_RESPONSE_BYTES":  "65536",
	}
}

func testProjection(t *testing.T, ca []byte) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "token"), []byte(testJWT), 0o600); err != nil {
		t.Fatalf("write projected token: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "ca.crt"), ca, 0o444); err != nil {
		t.Fatalf("write projected CA: %v", err)
	}
	return root
}

func testCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	now := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fugue materializer process test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	document, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: document})
}

func zeroInputProjection() clientprojected.Config { return clientprojected.Config{} }

func zeroSecretProjection() secretprojected.Config { return secretprojected.Config{} }

func fmtValue(value any) string { return fmt.Sprint(value) }

func fmtGoValue(value any) string { return fmt.Sprintf("%#v", value) }
