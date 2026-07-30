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

func TestInferAuthKindRecognizesBackupObserverBearer(t *testing.T) {
	t.Parallel()

	security := openapi3.SecurityRequirements{
		openapi3.SecurityRequirement{"BackupObserverBearerAuth": []string{}},
	}
	got, err := inferAuthKind(&openapi3.T{}, &openapi3.Operation{Security: &security})
	if err != nil {
		t.Fatalf("infer backup observer auth: %v", err)
	}
	if got != "backup-observer" {
		t.Fatalf("expected backup-observer auth, got %q", got)
	}
}

func TestInferAuthKindRecognizesBackupMaterializerBearer(t *testing.T) {
	t.Parallel()

	security := openapi3.SecurityRequirements{
		openapi3.SecurityRequirement{"BackupMaterializerBearerAuth": []string{}},
	}
	got, err := inferAuthKind(&openapi3.T{}, &openapi3.Operation{Security: &security})
	if err != nil {
		t.Fatalf("infer backup materializer auth: %v", err)
	}
	if got != "backup-materializer" {
		t.Fatalf("expected backup-materializer auth, got %q", got)
	}
}

func TestInferAuthKindRejectsCombinedBearerSchemes(t *testing.T) {
	t.Parallel()

	security := openapi3.SecurityRequirements{
		openapi3.SecurityRequirement{
			"BearerAuth":                   []string{},
			"PlatformComponentBearerAuth":  []string{},
			"BackupObserverBearerAuth":     []string{},
			"BackupMaterializerBearerAuth": []string{},
		},
	}
	if _, err := inferAuthKind(&openapi3.T{}, &openapi3.Operation{Security: &security}); err == nil {
		t.Fatal("combined bearer schemes must be rejected")
	}
}

func TestRenderRoutesUsesBackupObserverMiddleware(t *testing.T) {
	t.Parallel()

	rendered, err := renderRoutesFile([]routeDefinition{{
		Method:      "GET",
		Path:        "/v1/backup-control/runs/{run}/observation",
		Pattern:     "GET /v1/backup-control/runs/{run}/observation",
		OperationID: "getBackupRunObservation",
		HandlerName: "handleGetBackupRunObservation",
		Auth:        "backup-observer",
	}})
	if err != nil {
		t.Fatalf("render routes: %v", err)
	}
	if !strings.Contains(string(rendered), "s.auth.RequireBackupObserver(http.HandlerFunc(s.handleGetBackupRunObservation))") {
		t.Fatalf("generated route did not use backup observer middleware:\n%s", rendered)
	}
}

func TestRenderRoutesUsesBackupMaterializerEndpointGate(t *testing.T) {
	t.Parallel()

	rendered, err := renderRoutesFile([]routeDefinition{{
		Method:      "GET",
		Path:        "/v1/backup-control/runs/{run}/observer-input-bundle",
		Pattern:     "GET /v1/backup-control/runs/{run}/observer-input-bundle",
		OperationID: "getBackupObserverInputBundle",
		HandlerName: "handleGetBackupObserverInputBundle",
		Auth:        "backup-materializer",
	}})
	if err != nil {
		t.Fatalf("render routes: %v", err)
	}
	if !strings.Contains(string(rendered), "s.requireBackupMaterializerEndpoint(http.HandlerFunc(s.handleGetBackupObserverInputBundle))") {
		t.Fatalf("generated route did not use backup materializer endpoint gate:\n%s", rendered)
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
