package apispec

import (
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupmaterializer"
	"fugue/internal/backupmaterializer/httpapi"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestBackupMaterializerOpenAPIContractIsPrivateStrictAndDefaultOff(t *testing.T) {
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(YAML())
	if err != nil {
		t.Fatalf("load embedded OpenAPI YAML: %v", err)
	}
	path := httpapi.RoutePath
	item := doc.Paths.Find(path)
	if item == nil || item.Get == nil {
		t.Fatalf("missing GET %s", path)
	}
	if item.Post != nil || item.Put != nil || item.Patch != nil || item.Delete != nil {
		t.Fatal("backup materializer contract contains a mutation method")
	}
	operation := item.Get
	if operation.OperationID != "getBackupObserverInputBundle" ||
		operation.Extensions["x-fugue-handler"] != "handleGetBackupObserverInputBundle" || operation.RequestBody != nil {
		t.Fatalf("backup materializer operation drifted: id=%q handler=%v body=%v", operation.OperationID, operation.Extensions["x-fugue-handler"], operation.RequestBody)
	}
	if operation.Security == nil || len(*operation.Security) != 1 {
		t.Fatal("backup materializer must have one dedicated security requirement")
	}
	if _, ok := (*operation.Security)[0]["BackupMaterializerBearerAuth"]; !ok || len((*operation.Security)[0]) != 1 {
		t.Fatalf("backup materializer security drifted: %+v", *operation.Security)
	}
	if len(operation.Parameters) != 1 {
		t.Fatalf("backup materializer parameters widened: %+v", operation.Parameters)
	}
	run := operation.Parameters.GetByInAndName("path", "run")
	if run == nil || !run.Required || run.Schema == nil || run.Schema.Value == nil || run.Schema.Value.Pattern == "" {
		t.Fatal("backup materializer run path is not required and bounded")
	}
	success := operation.Responses.Value("200")
	if success == nil || success.Value == nil || success.Value.Content.Get("application/json") == nil ||
		success.Value.Content.Get("application/json").Schema == nil ||
		success.Value.Content.Get("application/json").Schema.Ref != "#/components/schemas/BackupObserverInputBundle" {
		t.Fatal("backup materializer 200 response is not the strict bundle schema")
	}
	for _, header := range []string{"Cache-Control", "Pragma"} {
		if ref := success.Value.Headers[header]; ref == nil || ref.Value == nil {
			t.Fatalf("backup materializer 200 response is missing %s", header)
		}
	}
	cacheControl := success.Value.Headers["Cache-Control"].Value.Schema
	if cacheControl == nil || cacheControl.Value == nil {
		t.Fatal("backup materializer Cache-Control header is unbounded")
	}
	cachePattern, err := regexp.Compile(cacheControl.Value.Pattern)
	if err != nil || !cachePattern.MatchString("private, no-store, max-age=0") || cachePattern.MatchString("public, max-age=60") {
		t.Fatalf("backup materializer Cache-Control pattern drifted: pattern=%q err=%v", cacheControl.Value.Pattern, err)
	}
	for _, status := range []string{"400", "401", "404", "409", "503"} {
		response := operation.Responses.Value(status)
		if response == nil || response.Value == nil || response.Value.Content.Get("application/json") == nil {
			t.Fatalf("backup materializer %s response is not JSON", status)
		}
	}
	serviceUnavailable := operation.Responses.Value("503")
	if retryAfter := serviceUnavailable.Value.Headers["Retry-After"]; retryAfter == nil || retryAfter.Value == nil ||
		retryAfter.Value.Schema == nil || retryAfter.Value.Schema.Value == nil || retryAfter.Value.Schema.Value.Type == nil ||
		!retryAfter.Value.Schema.Value.Type.Is("integer") {
		t.Fatal("backup materializer 503 response is missing an integer Retry-After header")
	}
	scheme := doc.Components.SecuritySchemes["BackupMaterializerBearerAuth"]
	if scheme == nil || scheme.Value == nil || scheme.Value.Type != "http" || scheme.Value.Scheme != "bearer" ||
		scheme.Value.BearerFormat != "kubernetes-service-account-jwt" ||
		!strings.Contains(scheme.Value.Description, "fugue-backup-materializer.fugue.dev") {
		t.Fatalf("backup materializer security scheme drifted: %+v", scheme)
	}

	bundleSchema := requireStrictBackupMaterializerSchema(t, doc, "BackupObserverInputBundle")
	specSchema := requireStrictBackupMaterializerSchema(t, doc, "BackupRunDesiredSpec")
	targetSchema := requireStrictBackupMaterializerSchema(t, doc, "BackupRunDesiredTarget")
	assertBackupMaterializerSchemaMatchesGoType(t, bundleSchema, reflect.TypeOf(backupmaterializer.ObserverInputBundle{}))
	assertBackupMaterializerSchemaMatchesGoType(t, specSchema, reflect.TypeOf(backupcontrol.BackupRunSpec{}))
	assertBackupMaterializerSchemaMatchesGoType(t, targetSchema, reflect.TypeOf(backupcontrol.BackupTarget{}))
	observerToken := bundleSchema.Properties["observerToken"]
	if observerToken == nil || observerToken.Value == nil || observerToken.Value.Format != "password" ||
		observerToken.Value.Pattern == "" || observerToken.Value.Extensions["x-fugue-sensitive"] != true {
		t.Fatalf("observer token is not explicitly sensitive and bounded: %+v", observerToken)
	}
	tokenPattern, err := regexp.Compile(observerToken.Value.Pattern)
	if err != nil || !tokenPattern.MatchString("fugue_bo_v1.a.b.c") || tokenPattern.MatchString("fugue_bo_v1.a.b") {
		t.Fatalf("observer token pattern is not the exact v1 framing: pattern=%q err=%v", observerToken.Value.Pattern, err)
	}
	for _, forbidden := range []string{
		"bucket", "endpoint", "baseUrl", "objectKey", "manifestObjectKey", "credentials", "accessKeyId",
		"secretAccessKey", "ciphertext", "credentialSecretId", "databaseUrl", "dsn", "password",
	} {
		if _, ok := bundleSchema.Properties[forbidden]; ok {
			t.Fatalf("bundle schema exposes forbidden property %q", forbidden)
		}
		if _, ok := specSchema.Properties[forbidden]; ok {
			t.Fatalf("desired spec schema exposes forbidden property %q", forbidden)
		}
	}

	foundRoute := false
	for _, route := range Routes() {
		if route.Path != path {
			continue
		}
		foundRoute = true
		if route.Method != "GET" || route.HandlerName != "handleGetBackupObserverInputBundle" || route.Auth != AuthBackupMaterializer {
			t.Fatalf("generated backup materializer route drifted: %+v", route)
		}
	}
	if !foundRoute {
		t.Fatal("generated route table omits backup materializer")
	}
}

func requireStrictBackupMaterializerSchema(t *testing.T, doc *openapi3.T, name string) *openapi3.Schema {
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

func assertBackupMaterializerSchemaMatchesGoType(t *testing.T, schema *openapi3.Schema, valueType reflect.Type) {
	t.Helper()
	want := jsonFieldNames(valueType)
	got := sortedBackupMaterializerStrings(schemaPropertyNames(schema))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("schema properties=%v want Go JSON fields=%v", got, want)
	}
	required := sortedBackupMaterializerStrings(append([]string(nil), schema.Required...))
	if !reflect.DeepEqual(required, want) {
		t.Fatalf("schema required=%v want every Go JSON field=%v", required, want)
	}
}

func schemaPropertyNames(schema *openapi3.Schema) []string {
	values := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		values = append(values, name)
	}
	return values
}

func jsonFieldNames(valueType reflect.Type) []string {
	values := make([]string, 0, valueType.NumField())
	for index := 0; index < valueType.NumField(); index++ {
		field := valueType.Field(index)
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name != "" && name != "-" {
			values = append(values, name)
		}
	}
	return sortedBackupMaterializerStrings(values)
}

func sortedBackupMaterializerStrings(values []string) []string {
	sort.Strings(values)
	return values
}
