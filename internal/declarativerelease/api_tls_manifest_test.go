package declarativerelease

import (
	"bytes"
	"os"
	"reflect"
	"sort"
	"testing"
)

func TestDeclarativeAPIManifestOwnsTrustedRouteIntentTLS(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/releases/api/deployment.json")
	if err != nil {
		t.Fatal(err)
	}
	set, err := DecodeResourceSet(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}

	service, err := ResourceSetItem(mustCanonical(set), ResourceIdentity{
		APIVersion: "v1", Kind: "Service", Namespace: "fugue-system", Name: "fugue-api-tls",
	})
	if err != nil {
		t.Fatal(err)
	}
	serviceSpec := apiTLSObject(t, service, "spec")
	selector := apiTLSObject(t, serviceSpec, "selector")
	wantSelector := map[string]any{
		"app.kubernetes.io/component":            "api",
		"app.kubernetes.io/instance":             "fugue",
		"app.kubernetes.io/name":                 "fugue",
		"fugue.io/edge-control-route-intent-tls": "true",
	}
	if !reflect.DeepEqual(selector, wantSelector) {
		t.Fatalf("route-intent TLS selector drifted: %#v", selector)
	}
	servicePort := apiTLSNamedObject(t, apiTLSArray(t, serviceSpec, "ports"), "name", "https-route-intent")
	if port, ok := integerField(servicePort["port"]); !ok || port != 8443 ||
		stringField(servicePort, "targetPort") != "route-intent-tls" || stringField(servicePort, "protocol") != "TCP" {
		t.Fatalf("route-intent TLS service port drifted: %#v", servicePort)
	}

	deployment, err := ResourceSetItem(mustCanonical(set), ResourceIdentity{
		APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api",
	})
	if err != nil {
		t.Fatal(err)
	}
	template := apiTLSObject(t, apiTLSObject(t, deployment, "spec"), "template")
	podSpec := apiTLSObject(t, template, "spec")
	api := apiTLSNamedObject(t, apiTLSArray(t, podSpec, "containers"), "name", "api")
	env := apiTLSArray(t, api, "env")
	for name, value := range map[string]string{
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_BIND_ADDR":      ":8443",
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_PROJECTION_DIR": "/var/run/secrets/fugue-api-tls",
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_SERVER_NAME":    "fugue-api-tls.fugue-system.svc",
	} {
		entry := apiTLSNamedObject(t, env, "name", name)
		if stringField(entry, "value") != value || len(entry) != 2 {
			t.Fatalf("route-intent TLS env %s drifted: %#v", name, entry)
		}
	}
	containerPort := apiTLSNamedObject(t, apiTLSArray(t, api, "ports"), "name", "route-intent-tls")
	if port, ok := integerField(containerPort["containerPort"]); !ok || port != 8443 || stringField(containerPort, "protocol") != "TCP" {
		t.Fatalf("route-intent TLS container port drifted: %#v", containerPort)
	}
	mount := apiTLSNamedObject(t, apiTLSArray(t, api, "volumeMounts"), "name", "edge-control-route-intent-tls")
	if stringField(mount, "mountPath") != "/var/run/secrets/fugue-api-tls" || mount["readOnly"] != true {
		t.Fatalf("route-intent TLS mount drifted: %#v", mount)
	}
	volume := apiTLSNamedObject(t, apiTLSArray(t, podSpec, "volumes"), "name", "edge-control-route-intent-tls")
	secret := apiTLSObject(t, volume, "secret")
	if stringField(secret, "secretName") != "fugue-api-tls" {
		t.Fatalf("route-intent TLS Secret identity drifted: %#v", secret)
	}
	if mode, ok := integerField(secret["defaultMode"]); !ok || mode != 256 {
		t.Fatalf("route-intent TLS Secret mode drifted: %#v", secret["defaultMode"])
	}
	keys := make([]string, 0, 3)
	for _, rawItem := range apiTLSArray(t, secret, "items") {
		item, ok := rawItem.(map[string]any)
		if !ok || stringField(item, "key") != stringField(item, "path") {
			t.Fatalf("route-intent TLS Secret projection drifted: %#v", rawItem)
		}
		keys = append(keys, stringField(item, "key"))
	}
	sort.Strings(keys)
	if want := []string{"ca.crt", "tls.crt", "tls.key"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("route-intent TLS Secret keyset drifted: got=%v want=%v", keys, want)
	}
}

func apiTLSObject(t *testing.T, value map[string]any, key string) map[string]any {
	t.Helper()
	result, ok := value[key].(map[string]any)
	if !ok {
		t.Fatalf("%s is not an object: %#v", key, value[key])
	}
	return result
}

func apiTLSArray(t *testing.T, value map[string]any, key string) []any {
	t.Helper()
	result, ok := value[key].([]any)
	if !ok {
		t.Fatalf("%s is not an array: %#v", key, value[key])
	}
	return result
}

func apiTLSNamedObject(t *testing.T, values []any, key, name string) map[string]any {
	t.Helper()
	var found map[string]any
	for _, raw := range values {
		value, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("array member is not an object: %#v", raw)
		}
		if stringField(value, key) != name {
			continue
		}
		if found != nil {
			t.Fatalf("%s=%q is ambiguous", key, name)
		}
		found = value
	}
	if found == nil {
		t.Fatalf("%s=%q is absent", key, name)
	}
	return found
}
