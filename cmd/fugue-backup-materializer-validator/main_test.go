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

	clientprojected "fugue/internal/backupmaterializer/client/projected"
	"fugue/internal/backupmaterializer/validationagent"
	"fugue/internal/backupmaterializer/validationcomposition"
)

const (
	testCellKey       = "backup/app-database/0123456789abcdef"
	testRunID         = "run-validator-1"
	testInputJWT      = "header.input-payload.signature-input"
	testReaderJWT     = "header.reader-payload.signature-reader"
	testValidationJWT = "header.validation-payload.signature-validation"
)

func TestValidatorConfigDefaultsToInertDisabledProcess(t *testing.T) {
	t.Parallel()
	values := map[string]string{
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_CELL_KEY":                "private-invalid-cell",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_RUN_ID":                  "private invalid run",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_API_BASE_URL":      "private invalid URL",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_PROJECTION_ROOT":   "/private/missing/input",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_KUBERNETES_API_URL":      "private invalid Kubernetes URL",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_PROJECTION_ROOT":  "/private/missing/reader",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_PROJECTION_ROOT": "/private/missing/writer",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_RECONCILE_INTERVAL":      "invalid",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ATTEMPT_TIMEOUT":         "invalid",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_REQUEST_TIMEOUT":   "invalid",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_REQUEST_TIMEOUT":  "invalid",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_REQUEST_TIMEOUT": "invalid",
	}
	config, err := validatorConfigFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	if config.Enabled || config.BindAddr != defaultBindAddr || config.ShutdownTimeout != defaultShutdownTimeout ||
		!reflect.DeepEqual(config.Composition, validationcomposition.Config{}) {
		t.Fatalf("disabled config retained capability: %#v", config)
	}
	service, err := newValidatorService(config, log.New(io.Discard, "private-prefix", 0))
	if err != nil || service.Enabled() || service.Snapshot().Mode != validationagent.ModeDisabled {
		t.Fatalf("disabled service drifted: service=%#v snapshot=%#v err=%v", service, service.Snapshot(), err)
	}
	rendered := strings.Join([]string{
		fmt.Sprint(config), fmt.Sprintf("%#v", config), fmt.Sprint(service), fmt.Sprintf("%#v", service),
	}, "\n")
	for _, private := range []string{"private-invalid-cell", "/private/missing/input", "private-prefix"} {
		if strings.Contains(rendered, private) {
			t.Fatalf("disabled diagnostics exposed %q: %s", private, rendered)
		}
	}
}

func TestValidatorConfigEnablesOneExactBoundedCell(t *testing.T) {
	t.Parallel()
	values := validEnvironment()
	config, err := validatorConfigFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("enabled config: %v", err)
	}
	composition := config.Composition
	if !config.Enabled || config.BindAddr != "[::1]:18094" || config.ShutdownTimeout != 9*time.Second ||
		!composition.Enabled || composition.CellKey != testCellKey || composition.RunID != testRunID ||
		composition.Interval != 15*time.Second || composition.AttemptTimeout != 18*time.Second ||
		!composition.InputProjection.Enabled || composition.InputProjection.BaseURL != "https://input.example.test" ||
		composition.InputProjection.ProjectionRoot != "/run/fugue/validator/input" ||
		composition.InputProjection.ExpectedCellKey != testCellKey || composition.InputProjection.ExpectedRunID != testRunID ||
		composition.InputProjection.RequestTimeout != 8*time.Second ||
		composition.InputProjection.HandshakeTimeout != 4*time.Second || composition.InputProjection.MaxResponseBytes != 32768 ||
		!composition.CurrentProjection.Enabled || composition.CurrentProjection.APIServerURL != "https://kubernetes.default.svc" ||
		composition.CurrentProjection.ProjectionRoot != "/run/fugue/validator/reader" ||
		composition.CurrentProjection.ExpectedCellKey != testCellKey ||
		composition.CurrentProjection.RequestTimeout != 7*time.Second ||
		composition.CurrentProjection.HandshakeTimeout != 3*time.Second ||
		composition.CurrentProjection.MaxResponseBytes != 65536 ||
		!composition.ValidationProjection.Enabled ||
		composition.ValidationProjection.APIServerURL != "https://kubernetes.default.svc" ||
		composition.ValidationProjection.ProjectionRoot != "/run/fugue/validator/dry-run" ||
		composition.ValidationProjection.ExpectedCellKey != testCellKey ||
		composition.ValidationProjection.RequestTimeout != 6*time.Second ||
		composition.ValidationProjection.HandshakeTimeout != 2*time.Second ||
		composition.ValidationProjection.MaxResponseBytes != 131072 {
		t.Fatalf("enabled config drifted: %#v", config)
	}
}

func TestValidatorConfigRejectsAmbiguousOrUnboundedValues(t *testing.T) {
	t.Parallel()
	for name, override := range map[string]map[string]string{
		"ambiguous enable":     {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ENABLED": "1"},
		"public bind":          {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_BIND_ADDR": "0.0.0.0:8094"},
		"hostname bind":        {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_BIND_ADDR": "localhost:8094"},
		"privileged port":      {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_BIND_ADDR": "127.0.0.1:443"},
		"zero shutdown":        {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_SHUTDOWN_TIMEOUT": "0s"},
		"long shutdown":        {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_SHUTDOWN_TIMEOUT": "61s"},
		"short interval":       {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_RECONCILE_INTERVAL": "999ms"},
		"long interval":        {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_RECONCILE_INTERVAL": "11m"},
		"fractional attempt":   {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ATTEMPT_TIMEOUT": "1.000000001s"},
		"long attempt":         {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ATTEMPT_TIMEOUT": "61s"},
		"long input request":   {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_REQUEST_TIMEOUT": "31s"},
		"input handshake":      {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_HANDSHAKE_TIMEOUT": "9s"},
		"reader handshake":     {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_HANDSHAKE_TIMEOUT": "8s"},
		"validation handshake": {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_HANDSHAKE_TIMEOUT": "7s"},
		"input over attempt": {
			"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ATTEMPT_TIMEOUT":       "6s",
			"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_REQUEST_TIMEOUT": "7s",
		},
		"reader over attempt": {
			"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ATTEMPT_TIMEOUT":        "6s",
			"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_REQUEST_TIMEOUT": "7s",
		},
		"validation over attempt": {
			"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ATTEMPT_TIMEOUT":         "6s",
			"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_REQUEST_TIMEOUT": "7s",
		},
		"small input body":         {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_MAX_RESPONSE_BYTES": "100"},
		"large input body":         {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_MAX_RESPONSE_BYTES": "65537"},
		"small reader body":        {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_MAX_RESPONSE_BYTES": "4095"},
		"large reader body":        {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_MAX_RESPONSE_BYTES": "262145"},
		"small validation body":    {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_MAX_RESPONSE_BYTES": "4095"},
		"large validation body":    {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_MAX_RESPONSE_BYTES": "1048577"},
		"invalid response integer": {"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_MAX_RESPONSE_BYTES": "private"},
	} {
		name, override := name, override
		t.Run(name, func(t *testing.T) {
			values := validEnvironment()
			for key, value := range override {
				values[key] = value
			}
			if config, err := validatorConfigFromEnv(func(key string) string { return values[key] }); err == nil {
				t.Fatalf("unsafe configuration accepted: %#v", config)
			}
		})
	}
	if _, err := validatorConfigFromEnv(nil); err == nil {
		t.Fatal("nil environment reader accepted")
	}
}

func TestEnabledValidatorCompositionAcquiresOnlyThreeSeparatedProjections(t *testing.T) {
	t.Parallel()
	ca := testCAPEM(t)
	inputRoot := testProjection(t, testInputJWT, ca)
	readerRoot := testProjection(t, testReaderJWT, ca)
	validationRoot := testProjection(t, testValidationJWT, ca)
	values := validEnvironment()
	values["FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_PROJECTION_ROOT"] = inputRoot
	values["FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_PROJECTION_ROOT"] = readerRoot
	values["FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_PROJECTION_ROOT"] = validationRoot
	config, err := validatorConfigFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("parse composition: %v", err)
	}
	service, err := newValidatorService(config, nil)
	if err != nil {
		t.Fatalf("construct composition: %v", err)
	}
	snapshot := service.Snapshot()
	if !service.Enabled() || snapshot.Mode != validationagent.ModeShadow || snapshot.CellKey != testCellKey ||
		snapshot.Ready || snapshot.AttemptCount != 0 || validationagent.ValidateSnapshot(snapshot) != nil {
		t.Fatalf("composed service drifted: %#v", snapshot)
	}

	privateRoot := "/private/missing/validator-projection"
	for name, mutate := range map[string]func(*validatorConfig){
		"missing input":      func(value *validatorConfig) { value.Composition.InputProjection.ProjectionRoot = privateRoot },
		"missing reader":     func(value *validatorConfig) { value.Composition.CurrentProjection.ProjectionRoot = privateRoot },
		"missing validation": func(value *validatorConfig) { value.Composition.ValidationProjection.ProjectionRoot = privateRoot },
		"shared projection": func(value *validatorConfig) {
			value.Composition.ValidationProjection.ProjectionRoot = value.Composition.CurrentProjection.ProjectionRoot
		},
		"shared authority": func(value *validatorConfig) {
			value.Composition.InputProjection.BaseURL = value.Composition.CurrentProjection.APIServerURL
		},
		"input cell":      func(value *validatorConfig) { value.Composition.InputProjection.ExpectedCellKey += "0" },
		"reader cell":     func(value *validatorConfig) { value.Composition.CurrentProjection.ExpectedCellKey += "0" },
		"validation cell": func(value *validatorConfig) { value.Composition.ValidationProjection.ExpectedCellKey += "0" },
		"input run":       func(value *validatorConfig) { value.Composition.InputProjection.ExpectedRunID += "-other" },
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			candidate := config
			mutate(&candidate)
			if _, err := newValidatorService(candidate, nil); err == nil || strings.Contains(err.Error(), privateRoot) {
				t.Fatalf("unsafe composition error absent or leaked input: %v", err)
			}
		})
	}
}

func TestValidatorServeOwnsLoopbackLifecycleAndDrainsAgent(t *testing.T) {
	t.Parallel()
	service, err := validationagent.New(validationagent.Config{Enabled: false}, nil)
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
	for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable} {
		response, err := client.Get("http://" + listener.Addr().String() + path)
		if err != nil {
			cancel()
			t.Fatalf("read %s: %v", path, err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != want {
			cancel()
			t.Fatalf("%s status = %d", path, response.StatusCode)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("drain lifecycle: %v", err)
	}
}

func TestValidatorTopLevelRunStartsDisabledAndShutsDownCleanly(t *testing.T) {
	t.Parallel()
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback address: %v", err)
	}
	address := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatalf("release loopback address: %v", err)
	}
	config := validatorConfig{
		BindAddr: address, ShutdownTimeout: time.Second,
		Composition: validationcomposition.Config{
			CellKey: "private-disabled-cell", RunID: "private-disabled-run",
		},
	}
	var logs strings.Builder
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, config, log.New(&logs, "validator ", 0)) }()
	client := &http.Client{Timeout: 100 * time.Millisecond}
	deadline := time.Now().Add(2 * time.Second)
	for {
		response, requestErr := client.Get("http://" + address + "/healthz")
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				cancel()
				t.Fatalf("disabled top-level health = %d", response.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("disabled top-level listener unavailable: %v", requestErr)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("disabled top-level shutdown: %v", err)
	}
	rendered := logs.String()
	if !strings.Contains(rendered, "enabled=false cell=disabled") ||
		strings.Contains(rendered, "private-disabled") {
		t.Fatalf("top-level log drifted: %q", rendered)
	}
}

func TestValidatorLifecycleAndTopLevelRunFailClosed(t *testing.T) {
	t.Parallel()
	service, err := validationagent.New(validationagent.Config{Enabled: false}, nil)
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
		t.Fatal("unexpected server stop reported clean")
	}
	if err := serve(nil, time.Second, service, server, listener); err == nil {
		t.Fatal("nil lifecycle context accepted")
	}
	if err := serve(context.Background(), time.Second, service, &http.Server{}, listener); err == nil {
		t.Fatal("nil lifecycle handler accepted")
	}
	if err := serve(context.Background(), time.Nanosecond, service, server, listener); err == nil {
		t.Fatal("unsafe lifecycle timeout accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	config := validatorConfig{BindAddr: defaultBindAddr, ShutdownTimeout: defaultShutdownTimeout}
	if err := run(canceled, config, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("already-canceled run error = %v", err)
	}
	if err := run(context.Background(), validatorConfig{
		BindAddr: "0.0.0.0:8094", ShutdownTimeout: defaultShutdownTimeout,
	}, nil); err == nil {
		t.Fatal("top-level run accepted public bind")
	}
	if err := run(context.Background(), validatorConfig{
		BindAddr: defaultBindAddr, ShutdownTimeout: time.Nanosecond,
	}, nil); err == nil {
		t.Fatal("top-level run accepted unsafe shutdown timeout")
	}
	if err := run(nil, config, nil); err == nil {
		t.Fatal("top-level run accepted nil context")
	}
}

func TestValidatorProbeIsDirectLoopbackOnlyAndStatusExact(t *testing.T) {
	t.Parallel()
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
		if key == "FUGUE_BACKUP_MATERIALIZER_VALIDATOR_BIND_ADDR" {
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
		t.Fatal("unknown probe target accepted")
	}
	if err := runProbe([]string{"probe", "health"}, nil); err == nil {
		t.Fatal("nil environment reader accepted")
	}
	if err := runProbe([]string{"probe", "health"}, func(string) string { return "0.0.0.0:8094" }); err == nil {
		t.Fatal("probe accepted non-loopback target")
	}
}

func TestValidatorHelpersRejectAmbiguity(t *testing.T) {
	t.Parallel()
	for raw, want := range map[string]bool{"": false, "false": false, "true": true} {
		got, err := strictBool(raw)
		if err != nil || got != want {
			t.Fatalf("strictBool(%q) = %t, %v", raw, got, err)
		}
	}
	if _, err := strictBool("TRUE"); err == nil {
		t.Fatal("ambiguous bool accepted")
	}
	if value, err := envDuration("", 2*time.Second, time.Second, 3*time.Second); err != nil || value != 2*time.Second {
		t.Fatalf("duration default = %s, %v", value, err)
	}
	if _, err := envDuration("private", time.Second, time.Second, 3*time.Second); err == nil {
		t.Fatal("invalid duration accepted")
	}
	if value, err := envInt64("", 2, 1, 3); err != nil || value != 2 {
		t.Fatalf("integer default = %d, %v", value, err)
	}
	if _, err := envInt64("4", 2, 1, 3); err == nil {
		t.Fatal("unbounded integer accepted")
	}
	for _, value := range []string{"", "localhost:8094", "127.0.0.1:1", "127.0.0.1:70000"} {
		if err := validateLoopbackBindAddr(value); err == nil {
			t.Fatalf("unsafe bind accepted: %q", value)
		}
	}
	if err := validateLoopbackBindAddr("[::1]:8094"); err != nil {
		t.Fatalf("IPv6 loopback rejected: %v", err)
	}
	if publicCell(validatorConfig{}) != "disabled" || publicCell(validatorConfig{
		Enabled: true, Composition: validationcomposition.Config{CellKey: testCellKey},
	}) != testCellKey {
		t.Fatal("public cell projection drifted")
	}
}

func TestValidatorBinaryDependencyBoundary(t *testing.T) {
	output, err := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", ".").Output()
	if err != nil {
		t.Fatalf("list validator binary dependencies: %v", err)
	}
	var local []string
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{
			"database/sql", "os/exec", "k8s.io/", "github.com/aws/", "github.com/google/go-containerregistry",
			"github.com/jackc/", "fugue/internal/api", "fugue/internal/auth", "fugue/internal/model",
			"fugue/internal/store", "fugue/internal/backupidentity", "fugue/internal/backupmaterializer/agent",
			"fugue/internal/backupmaterializer/composition", "fugue/internal/backupmaterializer/httpapi",
			"fugue/internal/backupmaterializer/legacysource", "fugue/internal/backupmaterializer/localissuer",
			"fugue/internal/backupmaterializer/storesource", "fugue/internal/backupmaterializeridentity",
			"fugue/internal/backupmaterializerreview",
		} {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden) {
				t.Fatalf("validator binary imported forbidden capability %q", dependency)
			}
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/cmd/fugue-backup-materializer-validator",
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/client",
		"fugue/internal/backupmaterializer/client/projected",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/dryrunreconciler",
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconcile",
		"fugue/internal/backupmaterializer/reconciler",
		"fugue/internal/backupmaterializer/secretreader",
		"fugue/internal/backupmaterializer/secretreader/projected",
		"fugue/internal/backupmaterializer/secretwriter",
		"fugue/internal/backupmaterializer/secretwriter/projected",
		"fugue/internal/backupmaterializer/validationagent",
		"fugue/internal/backupmaterializer/validationcomposition",
		"fugue/internal/backupmaterializer/validationcycle",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("validator binary dependency closure drifted: got=%v want=%v", local, want)
	}
}

func validEnvironment() map[string]string {
	return map[string]string{
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ENABLED":                    "true",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_BIND_ADDR":                  "[::1]:18094",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_CELL_KEY":                   testCellKey,
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_RUN_ID":                     testRunID,
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_API_BASE_URL":         "https://input.example.test",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_PROJECTION_ROOT":      "/run/fugue/validator/input",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_KUBERNETES_API_URL":         "https://kubernetes.default.svc",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_PROJECTION_ROOT":     "/run/fugue/validator/reader",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_PROJECTION_ROOT":    "/run/fugue/validator/dry-run",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_RECONCILE_INTERVAL":         "15s",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ATTEMPT_TIMEOUT":            "18s",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_REQUEST_TIMEOUT":      "8s",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_HANDSHAKE_TIMEOUT":    "4s",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_REQUEST_TIMEOUT":     "7s",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_HANDSHAKE_TIMEOUT":   "3s",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_REQUEST_TIMEOUT":    "6s",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_HANDSHAKE_TIMEOUT":  "2s",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_SHUTDOWN_TIMEOUT":           "9s",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_MAX_RESPONSE_BYTES":   "32768",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_MAX_RESPONSE_BYTES":  "65536",
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_MAX_RESPONSE_BYTES": "131072",
	}
}

func testProjection(t *testing.T, token string, ca []byte) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, clientprojected.TokenFileName), []byte(token), 0o600); err != nil {
		t.Fatalf("write projected token: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, clientprojected.CAFileName), ca, 0o444); err != nil {
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
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fugue validator process test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	document, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: document})
}
