package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/componentmanifest"
	"fugue/internal/model"
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

func TestRunEmitsObservationOnlyCoordinationPlan(t *testing.T) {
	var stdout, stderr bytes.Buffer
	manifest := filepath.Join("..", "..", "docs", "architecture", "component-ownership-v1.yaml")
	err := run([]string{
		"--manifest", manifest,
		"--coordination",
		"--path", "cmd/fugue-image-cache/main.go",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() error = %v (stderr=%s)", err, stderr.String())
	}
	var plan struct {
		ObservationOnly           bool   `json:"observationOnly"`
		ProductionMutationAllowed bool   `json:"productionMutationAllowed"`
		CoordinationDigest        string `json:"coordinationDigest"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &plan); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if !plan.ObservationOnly || plan.ProductionMutationAllowed {
		t.Fatalf("coordination output can mutate production: %+v", plan)
	}
	if !strings.HasPrefix(plan.CoordinationDigest, "sha256:") {
		t.Fatalf("coordinationDigest = %q, want sha256 digest", plan.CoordinationDigest)
	}
}

func TestRunEmitsExactShadowArtifactCreateRequestWithoutSideEffects(t *testing.T) {
	var stdout, stderr bytes.Buffer
	manifest := filepath.Join("..", "..", "docs", "architecture", "component-ownership-v1.yaml")
	err := run([]string{
		"--manifest", manifest,
		"--artifact-request",
		"--base-commit", "1111111111111111111111111111111111111111",
		"--target-commit", "2222222222222222222222222222222222222222",
		"--path", "cmd/fugue-image-cache/main.go",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() error = %v (stderr=%s)", err, stderr.String())
	}
	var request model.PlatformArtifactCreateRequest
	if err := json.Unmarshal(stdout.Bytes(), &request); err != nil {
		t.Fatalf("decode artifact request: %v", err)
	}
	if request.ArtifactKind != model.PlatformArtifactKindComponentReleasePlan ||
		request.Generation != "git-2222222222222222222222222222222222222222" ||
		request.Scope.ScopeType != componentmanifest.ShadowArtifactScopeType ||
		request.Scope.Key == "" ||
		request.CompatibilityFloor != componentmanifest.ShadowArtifactSchemaVersionV1 {
		t.Fatalf("unexpected artifact request identity: %+v", request)
	}
	if err := componentmanifest.ValidateArtifactBinding(
		request.Content,
		request.Scope.ScopeType,
		request.Scope.Key,
		request.Scope.Key,
		request.Generation,
	); err != nil {
		t.Fatalf("artifact request content is not bound: %v", err)
	}
	if !strings.HasSuffix(stdout.String(), "\n") {
		t.Fatal("output is not newline terminated")
	}
}

func TestRunRejectsUnsafeArtifactRequestFlagCombinations(t *testing.T) {
	manifest := filepath.Join("..", "..", "docs", "architecture", "component-ownership-v1.yaml")
	for name, args := range map[string][]string{
		"missing commits":         {"--manifest", manifest, "--artifact-request", "--path", "cmd/fugue-image-cache/main.go"},
		"coordination conflict":   {"--manifest", manifest, "--coordination", "--artifact-request", "--base-commit", "1111111111111111111111111111111111111111", "--target-commit", "2222222222222222222222222222222222222222", "--path", "cmd/fugue-image-cache/main.go"},
		"commit without artifact": {"--manifest", manifest, "--base-commit", "1111111111111111111111111111111111111111", "--path", "cmd/fugue-image-cache/main.go"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if err := run(args, &stdout, &stderr); err == nil {
				t.Fatal("run() unexpectedly accepted unsafe flag combination")
			}
		})
	}
}
