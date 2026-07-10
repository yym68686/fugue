package main

import (
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestInferAuthKindRecognizesPlatformComponentBearer(t *testing.T) {
	t.Parallel()

	security := openapi3.SecurityRequirements{
		openapi3.SecurityRequirement{"PlatformComponentBearerAuth": []string{}},
	}
	got, err := inferAuthKind(&openapi3.T{}, &openapi3.Operation{Security: &security})
	if err != nil {
		t.Fatalf("infer platform component auth: %v", err)
	}
	if got != "platform-component" {
		t.Fatalf("expected platform-component auth, got %q", got)
	}
}

func TestInferAuthKindRejectsCombinedBearerSchemes(t *testing.T) {
	t.Parallel()

	security := openapi3.SecurityRequirements{
		openapi3.SecurityRequirement{
			"BearerAuth":                  []string{},
			"PlatformComponentBearerAuth": []string{},
		},
	}
	if _, err := inferAuthKind(&openapi3.T{}, &openapi3.Operation{Security: &security}); err == nil {
		t.Fatal("combined bearer schemes must be rejected")
	}
}

func TestRenderRoutesUsesPlatformComponentMiddleware(t *testing.T) {
	t.Parallel()

	rendered, err := renderRoutesFile([]routeDefinition{{
		Method:      "POST",
		Path:        "/v1/platform-state/consumers/trusted-heartbeat",
		Pattern:     "POST /v1/platform-state/consumers/trusted-heartbeat",
		OperationID: "trustedPlatformConsumerHeartbeat",
		HandlerName: "handleTrustedPlatformConsumerHeartbeat",
		Auth:        "platform-component",
	}})
	if err != nil {
		t.Fatalf("render routes: %v", err)
	}
	if !strings.Contains(string(rendered), "s.auth.RequirePlatformComponent(http.HandlerFunc(s.handleTrustedPlatformConsumerHeartbeat))") {
		t.Fatalf("generated route did not use platform component middleware:\n%s", rendered)
	}
}
