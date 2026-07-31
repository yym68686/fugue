package main

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestReleaseControlConfigDefaultsToSafeDisabledMode(t *testing.T) {
	t.Parallel()

	cfg, err := releaseControlConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatalf("load defaults: %v", err)
	}
	if cfg.Service.Enabled || cfg.BindAddr != defaultReleaseControlBindAddr ||
		cfg.Service.Interval != defaultReleaseControlInterval ||
		cfg.Service.AttemptTimeout != defaultReleaseControlAttemptTimeout ||
		cfg.Service.RequestTimeout != defaultReleaseControlRequestTimeout ||
		cfg.Service.MaxResponseBytes != defaultReleaseControlResponseBytes ||
		cfg.ShutdownTimeout != defaultReleaseControlShutdown {
		t.Fatalf("unsafe defaults: %+v", cfg)
	}
}

func TestReleaseControlConfigEnablesOnlyAnExactBoundedHTTPBoundary(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"FUGUE_RELEASE_CONTROL_ENABLED":            "true",
		"FUGUE_RELEASE_CONTROL_BIND_ADDR":          ":19091",
		"FUGUE_RELEASE_CONTROL_SPEC_FILE":          "/run/fugue/component-plan.json",
		"FUGUE_RELEASE_CONTROL_TOKEN_FILE":         "/run/secrets/fugue-release-control/token",
		"FUGUE_RELEASE_CONTROL_API_BASE_URL":       "https://api.fugue.test/internal",
		"FUGUE_RELEASE_CONTROL_RECONCILE_INTERVAL": "15s",
		"FUGUE_RELEASE_CONTROL_ATTEMPT_TIMEOUT":    "40s",
		"FUGUE_RELEASE_CONTROL_REQUEST_TIMEOUT":    "8s",
		"FUGUE_RELEASE_CONTROL_SHUTDOWN_TIMEOUT":   "9s",
		"FUGUE_RELEASE_CONTROL_MAX_RESPONSE_BYTES": "1048576",
	}
	cfg, err := releaseControlConfigFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("load enabled config: %v", err)
	}
	if !cfg.Service.Enabled || cfg.BindAddr != ":19091" || cfg.Service.SpecPath != values["FUGUE_RELEASE_CONTROL_SPEC_FILE"] ||
		cfg.Service.TokenPath != values["FUGUE_RELEASE_CONTROL_TOKEN_FILE"] ||
		cfg.Service.APIBaseURL != values["FUGUE_RELEASE_CONTROL_API_BASE_URL"] ||
		cfg.Service.Interval != 15*time.Second || cfg.Service.AttemptTimeout != 40*time.Second ||
		cfg.Service.RequestTimeout != 8*time.Second || cfg.ShutdownTimeout != 9*time.Second ||
		cfg.Service.MaxResponseBytes != 1048576 {
		t.Fatalf("enabled config drifted: %+v", cfg)
	}
}

func TestReleaseControlConfigRejectsAmbiguousOrUnboundedValues(t *testing.T) {
	t.Parallel()

	base := map[string]string{
		"FUGUE_RELEASE_CONTROL_ENABLED":      "true",
		"FUGUE_RELEASE_CONTROL_SPEC_FILE":    "/run/fugue/component-plan.json",
		"FUGUE_RELEASE_CONTROL_TOKEN_FILE":   "/run/secrets/fugue-release-control/token",
		"FUGUE_RELEASE_CONTROL_API_BASE_URL": "https://api.fugue.test",
	}
	for name, mutation := range map[string]map[string]string{
		"ambiguous bool":      {"FUGUE_RELEASE_CONTROL_ENABLED": "1"},
		"relative spec":       {"FUGUE_RELEASE_CONTROL_SPEC_FILE": "component-plan.json"},
		"relative token":      {"FUGUE_RELEASE_CONTROL_TOKEN_FILE": "token"},
		"missing API":         {"FUGUE_RELEASE_CONTROL_API_BASE_URL": ""},
		"zero interval":       {"FUGUE_RELEASE_CONTROL_RECONCILE_INTERVAL": "0s"},
		"excess interval":     {"FUGUE_RELEASE_CONTROL_RECONCILE_INTERVAL": "11m"},
		"negative timeout":    {"FUGUE_RELEASE_CONTROL_ATTEMPT_TIMEOUT": "-1s"},
		"excess attempt":      {"FUGUE_RELEASE_CONTROL_ATTEMPT_TIMEOUT": "121s"},
		"invalid request":     {"FUGUE_RELEASE_CONTROL_REQUEST_TIMEOUT": "later"},
		"excess request":      {"FUGUE_RELEASE_CONTROL_REQUEST_TIMEOUT": "31s"},
		"zero response bound": {"FUGUE_RELEASE_CONTROL_MAX_RESPONSE_BYTES": "0"},
		"excess response":     {"FUGUE_RELEASE_CONTROL_MAX_RESPONSE_BYTES": "9000000"},
		"invalid bind":        {"FUGUE_RELEASE_CONTROL_BIND_ADDR": "localhost"},
	} {
		t.Run(name, func(t *testing.T) {
			values := make(map[string]string, len(base)+1)
			for key, value := range base {
				values[key] = value
			}
			for key, value := range mutation {
				values[key] = value
			}
			if _, err := releaseControlConfigFromEnv(func(key string) string { return values[key] }); err == nil {
				t.Fatal("invalid configuration unexpectedly passed")
			}
		})
	}
}

func TestReleaseControlDependencyBoundary(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", ".")
	output, err := command.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("go list dependencies: %v: %s", err, exitErr.Stderr)
		}
		t.Fatalf("go list dependencies: %v", err)
	}
	for _, dependency := range strings.Fields(string(output)) {
		if dependency == "database/sql" || dependency == "fugue/internal/store" || dependency == "fugue/internal/api" ||
			dependency == "fugue/internal/controller" || strings.HasPrefix(dependency, "k8s.io/") ||
			strings.HasPrefix(dependency, "github.com/jackc/pgx") {
			t.Fatalf("release-control imported forbidden dependency %q", dependency)
		}
	}
}

func TestRunRejectsUnsafeServerBoundaryBeforeStarting(t *testing.T) {
	t.Parallel()

	base := releaseControlConfig{BindAddr: "127.0.0.1:19091", ShutdownTimeout: time.Second}
	if err := run(nil, base, nil); err == nil {
		t.Fatal("nil context unexpectedly started")
	}
	invalidBind := base
	invalidBind.BindAddr = ""
	if err := run(context.Background(), invalidBind, nil); err == nil {
		t.Fatal("empty bind address unexpectedly started")
	}
	invalidShutdown := base
	invalidShutdown.ShutdownTimeout = 0
	if err := run(context.Background(), invalidShutdown, nil); err == nil {
		t.Fatal("zero shutdown timeout unexpectedly started")
	}
}
