package main

import (
	"context"
	"io"
	"log"
	"testing"
	"time"
)

func TestConfigDefaultsAreLocalAndNonAuthoritative(t *testing.T) {
	t.Parallel()

	cfg, err := configFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Enabled || cfg.BindAddr != defaultBindAddr || cfg.ShutdownTimeout != defaultShutdownTimeout {
		t.Fatalf("unsafe defaults: %+v", cfg)
	}
}

func TestConfigAcceptsExplicitShadowProcessSettings(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"FUGUE_EDGE_CONTROL_ENABLED":          "true",
		"FUGUE_EDGE_CONTROL_BIND_ADDR":        "0.0.0.0:8092",
		"FUGUE_EDGE_CONTROL_SHUTDOWN_TIMEOUT": "30s",
	}
	cfg, err := configFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Enabled || cfg.BindAddr != "0.0.0.0:8092" || cfg.ShutdownTimeout != 30*time.Second {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestConfigRejectsAmbiguousOrUnsafeValues(t *testing.T) {
	t.Parallel()

	for name, values := range map[string]map[string]string{
		"ambiguous bool": {"FUGUE_EDGE_CONTROL_ENABLED": "1"},
		"missing host":   {"FUGUE_EDGE_CONTROL_BIND_ADDR": ":8092"},
		"missing port":   {"FUGUE_EDGE_CONTROL_BIND_ADDR": "127.0.0.1"},
		"zero port":      {"FUGUE_EDGE_CONTROL_BIND_ADDR": "127.0.0.1:0"},
		"long shutdown":  {"FUGUE_EDGE_CONTROL_SHUTDOWN_TIMEOUT": "121s"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := configFromEnv(func(key string) string { return values[key] }); err == nil {
				t.Fatal("config unexpectedly accepted unsafe value")
			}
		})
	}
}

func TestRunHonorsCancelledContextWithoutSideEffects(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := run(ctx, config{BindAddr: "127.0.0.1:18092", ShutdownTimeout: time.Second}, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("run cancelled context: %v", err)
	}
}
