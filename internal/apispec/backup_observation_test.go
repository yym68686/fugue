package apispec

import (
	"reflect"
	"sort"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestBackupObservationOpenAPIContractIsPrivateStrictAndReadOnly(t *testing.T) {
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(YAML())
	if err != nil {
		t.Fatalf("load embedded OpenAPI YAML: %v", err)
	}
	path := "/v1/backup-control/runs/{run}/observation"
	item := doc.Paths.Find(path)
	if item == nil || item.Get == nil {
		t.Fatalf("missing GET %s", path)
	}
	if item.Post != nil || item.Put != nil || item.Patch != nil || item.Delete != nil {
		t.Fatal("backup observation contract contains a mutation method")
	}
	operation := item.Get
	if operation.OperationID != "getBackupRunObservation" || operation.Extensions["x-fugue-handler"] != "handleGetBackupRunObservation" {
		t.Fatalf("backup observation operation drifted: id=%q handler=%v", operation.OperationID, operation.Extensions["x-fugue-handler"])
	}
	if operation.Security == nil || len(*operation.Security) != 1 {
		t.Fatal("backup observation must have one dedicated security requirement")
	}
	if _, ok := (*operation.Security)[0]["BackupObserverBearerAuth"]; !ok || len((*operation.Security)[0]) != 1 {
		t.Fatalf("backup observation security drifted: %+v", *operation.Security)
	}
	for _, parameter := range []struct{ in, name string }{{"path", "run"}, {"query", "spec_digest"}} {
		value := operation.Parameters.GetByInAndName(parameter.in, parameter.name)
		if value == nil || !value.Required || value.Schema == nil || value.Schema.Value == nil || value.Schema.Value.Pattern == "" {
			t.Fatalf("backup observation parameter %s:%s is not required and bounded", parameter.in, parameter.name)
		}
	}
	success := operation.Responses.Value("200")
	if success == nil || success.Value == nil || success.Value.Content.Get("application/json") == nil ||
		success.Value.Content.Get("application/json").Schema == nil ||
		success.Value.Content.Get("application/json").Schema.Ref != "#/components/schemas/BackupRunObservationStatus" {
		t.Fatal("backup observation 200 response is not the strict status schema")
	}
	for _, header := range []string{"Cache-Control", "Pragma"} {
		if ref := success.Value.Headers[header]; ref == nil || ref.Value == nil {
			t.Fatalf("backup observation 200 response is missing %s", header)
		}
	}
	for _, status := range []string{"400", "401", "403", "404", "409", "503"} {
		response := operation.Responses.Value(status)
		if response == nil || response.Value == nil || response.Value.Content.Get("application/json") == nil {
			t.Fatalf("backup observation %s response is not JSON", status)
		}
	}
	serviceUnavailable := operation.Responses.Value("503")
	if retryAfter := serviceUnavailable.Value.Headers["Retry-After"]; retryAfter == nil || retryAfter.Value == nil ||
		retryAfter.Value.Schema == nil || retryAfter.Value.Schema.Value == nil ||
		retryAfter.Value.Schema.Value.Type == nil || !retryAfter.Value.Schema.Value.Type.Is("integer") {
		t.Fatal("backup observation 503 response is missing an integer Retry-After header")
	}
	scheme := doc.Components.SecuritySchemes["BackupObserverBearerAuth"]
	if scheme == nil || scheme.Value == nil || scheme.Value.Type != "http" || scheme.Value.Scheme != "bearer" || scheme.Value.BearerFormat != "fugue_bo_v1" {
		t.Fatalf("backup observer security scheme drifted: %+v", scheme)
	}

	statusSchema := requireStrictBackupObservationSchema(t, doc, "BackupRunObservationStatus")
	wantStatusProperties := []string{
		"apiVersion", "cellKey", "digest", "kind", "lastKnownGood", "observationOnly", "observedAt",
		"observedAttempt", "observedErrorCode", "observedErrorDigest", "observedFence", "observedState",
		"observedWorkerId", "policy", "productionWriteAllowed", "runId", "specDigest", "validUntil",
	}
	if got := sortedBackupObservationSchemaKeys(statusSchema.Properties); !reflect.DeepEqual(got, wantStatusProperties) {
		t.Fatalf("backup observation status properties=%v want=%v", got, wantStatusProperties)
	}
	artifactSchema := requireStrictBackupObservationSchema(t, doc, "BackupRunObservationArtifactRef")
	wantArtifactProperties := []string{"artifactId", "backendGeneration", "contentDigest", "kind", "manifestDigest", "runId"}
	if got := sortedBackupObservationSchemaKeys(artifactSchema.Properties); !reflect.DeepEqual(got, wantArtifactProperties) {
		t.Fatalf("backup observation artifact properties=%v want=%v", got, wantArtifactProperties)
	}
	for _, forbidden := range []string{
		"bucket", "endpoint", "baseUrl", "objectKey", "manifestObjectKey", "credentials", "accessKeyId",
		"secretAccessKey", "token", "ciphertext", "keyId", "credentialSecretId", "errorMessage",
	} {
		if _, ok := statusSchema.Properties[forbidden]; ok {
			t.Fatalf("status schema exposes forbidden property %q", forbidden)
		}
		if _, ok := artifactSchema.Properties[forbidden]; ok {
			t.Fatalf("artifact schema exposes forbidden property %q", forbidden)
		}
	}

	foundRoute := false
	for _, route := range Routes() {
		if route.Path != path {
			continue
		}
		foundRoute = true
		if route.Method != "GET" || route.HandlerName != "handleGetBackupRunObservation" || route.Auth != AuthBackupObserver {
			t.Fatalf("generated backup observation route drifted: %+v", route)
		}
	}
	if !foundRoute {
		t.Fatal("generated route table omits backup observation")
	}
}

func requireStrictBackupObservationSchema(t *testing.T, doc *openapi3.T, name string) *openapi3.Schema {
	t.Helper()
	ref := doc.Components.Schemas[name]
	if ref == nil || ref.Value == nil {
		t.Fatalf("missing %s schema", name)
	}
	if ref.Value.AdditionalProperties.Has == nil || *ref.Value.AdditionalProperties.Has {
		t.Fatalf("%s must reject additional properties", name)
	}
	return ref.Value
}

func sortedBackupObservationSchemaKeys(values openapi3.Schemas) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
