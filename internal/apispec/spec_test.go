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

func TestStreamingOperationsDeclareEveryHandlerParameter(t *testing.T) {
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(YAML())
	if err != nil {
		t.Fatalf("load embedded OpenAPI YAML: %v", err)
	}

	tests := map[string][]string{
		"/v1/apps/{id}/build-logs/stream": {
			"path:id", "query:operation_id", "query:tail_lines", "query:follow", "query:cursor", "header:Last-Event-ID",
		},
		"/v1/apps/{id}/runtime-logs/stream": {
			"path:id", "query:component", "query:pod", "query:tail_lines", "query:previous", "query:follow", "query:cursor", "header:Last-Event-ID",
		},
		"/v1/apps/{id}/observability/requests/stream": {
			"path:id", "query:since", "query:until", "query:limit", "query:trace_id", "query:request_id", "query:status_class", "query:status_code", "query:slow", "query:errors", "query:follow", "header:Last-Event-ID",
		},
	}

	for path, expected := range tests {
		pathItem := doc.Paths.Find(path)
		if pathItem == nil || pathItem.Get == nil {
			t.Fatalf("missing GET operation for %s", path)
		}
		actual := make(map[string]bool, len(pathItem.Get.Parameters))
		for _, parameterRef := range pathItem.Get.Parameters {
			if parameterRef == nil || parameterRef.Value == nil {
				t.Fatalf("unresolved parameter in %s", path)
			}
			parameter := parameterRef.Value
			actual[parameter.In+":"+parameter.Name] = true
		}
		for _, key := range expected {
			if !actual[key] {
				t.Errorf("%s does not declare %s", path, key)
			}
		}
	}
}

func TestEdgeTLSAskAndSourceArchiveContracts(t *testing.T) {
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(YAML())
	if err != nil {
		t.Fatalf("load embedded OpenAPI YAML: %v", err)
	}

	edgeTLSAsk := doc.Paths.Find("/v1/edge/tls/ask")
	if edgeTLSAsk == nil || edgeTLSAsk.Get == nil {
		t.Fatal("missing GET /v1/edge/tls/ask operation")
	}
	domain := edgeTLSAsk.Get.Parameters.GetByInAndName("query", "domain")
	if domain == nil || !domain.Required {
		t.Fatal("edge TLS ask domain query parameter must be required")
	}
	edgeSuccess := edgeTLSAsk.Get.Responses.Value("200")
	if edgeSuccess == nil || edgeSuccess.Value == nil || edgeSuccess.Value.Content.Get("text/plain") == nil {
		t.Fatal("edge TLS ask 200 response must declare text/plain")
	}
	for _, status := range []string{"400", "403", "404", "500", "502"} {
		response := edgeTLSAsk.Get.Responses.Value(status)
		if response == nil || response.Value == nil || response.Value.Content.Get("application/json") == nil {
			t.Errorf("edge TLS ask %s response must declare JSON ErrorResponse content", status)
		}
	}

	archive := doc.Paths.Find("/v1/source-uploads/{id}/archive")
	if archive == nil || archive.Get == nil {
		t.Fatal("missing GET /v1/source-uploads/{id}/archive operation")
	}
	archiveSuccess := archive.Get.Responses.Value("200")
	if archiveSuccess == nil || archiveSuccess.Value == nil {
		t.Fatal("missing source upload archive 200 response")
	}
	if archiveSuccess.Value.Content.Get("application/octet-stream") == nil {
		t.Fatal("source upload archive must declare application/octet-stream")
	}
	for _, header := range []string{"Content-Length", "Content-Disposition"} {
		if ref := archiveSuccess.Value.Headers[header]; ref == nil || ref.Value == nil {
			t.Errorf("source upload archive must declare %s", header)
		}
	}
}
