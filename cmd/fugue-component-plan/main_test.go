package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunEmitsDigestBoundShadowPlan(t *testing.T) {
	var stdout, stderr bytes.Buffer
	manifest := filepath.Join("..", "..", "docs", "architecture", "component-ownership-v1.yaml")
	err := run([]string{
		"--manifest", manifest,
		"--path", "cmd/fugue-image-cache/main.go",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() error = %v (stderr=%s)", err, stderr.String())
	}
	var plan struct {
		DispatchMode string `json:"dispatchMode"`
		PlanDigest   string `json:"planDigest"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &plan); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if plan.DispatchMode != "shadow-only" {
		t.Fatalf("dispatchMode = %q, want shadow-only", plan.DispatchMode)
	}
	if !strings.HasPrefix(plan.PlanDigest, "sha256:") {
		t.Fatalf("planDigest = %q, want sha256 digest", plan.PlanDigest)
	}
	if !strings.HasSuffix(stdout.String(), "\n") {
		t.Fatal("output is not newline terminated")
	}
}

func TestRunRejectsMissingOrUnexpectedPaths(t *testing.T) {
	manifest := filepath.Join("..", "..", "docs", "architecture", "component-ownership-v1.yaml")
	for name, args := range map[string][]string{
		"missing path": {"--manifest", manifest},
		"positional":   {"--manifest", manifest, "extra"},
		"empty path":   {"--manifest", manifest, "--path", ""},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if err := run(args, &stdout, &stderr); err == nil {
				t.Fatal("run() unexpectedly succeeded")
			}
		})
	}
}
