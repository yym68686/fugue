package apispec

import (
	"context"
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestGeneratedArtifactsAreUpToDate(t *testing.T) {
	cmd := exec.Command(
		"go", "run", "../../cmd/fugue-openapi-gen",
		"-spec", "../../openapi/openapi.yaml",
		"-routes-out", "../api/routes_gen.go",
		"-spec-out", "./spec_gen.go",
		"-check",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("generated OpenAPI artifacts are stale: %v\n%s", err, output)
	}
}

func TestEmbeddedSpecIsValid(t *testing.T) {
	if !json.Valid(JSON()) {
		t.Fatal("embedded OpenAPI JSON is invalid")
	}

	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(YAML())
	if err != nil {
		t.Fatalf("load embedded OpenAPI YAML: %v", err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("validate embedded OpenAPI YAML: %v", err)
	}

	wantRoutes := 0
	for _, pathItem := range doc.Paths.Map() {
		wantRoutes += len(pathItem.Operations())
	}
	if got := len(Routes()); got != wantRoutes {
		t.Fatalf("expected %d generated routes, got %d", wantRoutes, got)
	}
}
