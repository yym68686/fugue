package main

import (
	"errors"
	"reflect"
	"testing"

	"fugue/internal/declarativerelease"
)

func TestHelmProbePathTransferKeepsExactScalarAndCAS(t *testing.T) {
	pointer := "/spec/template/spec/containers[name=dns]/livenessProbe/httpGet/path"
	desired := map[string]any{"metadata": map[string]any{"uid": "workload-uid", "resourceVersion": "42"}, "spec": map[string]any{"replicas": 2, "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "dns", "livenessProbe": map[string]any{"httpGet": map[string]any{"path": "/livez", "port": "http"}, "timeoutSeconds": 3}}}}}}}
	r := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{Container: "dns"}}
	id := declarativerelease.ResourceIdentity{}
	allowed := ownershipConvergencePointers(r, id, desired)
	live := deepCopyJSONMap(t, desired)
	m := mapField(live, "metadata")
	m["managedFields"] = []any{map[string]any{"manager": "helm", "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{pointer, "/spec/replicas"})}}
	c := anySlice(mapField(mapField(mapField(live, "spec"), "template"), "spec")["containers"])[0].(map[string]any)
	mapField(mapField(c, "livenessProbe"), "httpGet")["path"] = "/healthz"
	failure := func(p string) error {
		return errors.New(`Apply failed with 1 conflict: conflict with "helm" using apps/v1: ` + ssaFieldForPointer(p))
	}
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, "dns-declarative", failure(pointer)); err != nil {
		t.Fatal(err)
	}
	patch, found, err := nextOwnershipTransferPatch(desired, live, allowed, "dns-declarative", failure(pointer))
	expected := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": "workload-uid"}, {"op": "test", "path": "/metadata/resourceVersion", "value": "42"}, {"op": "test", "path": "/spec/template/spec/containers/0/name", "value": "dns"}, {"op": "test", "path": "/spec/template/spec/containers/0/livenessProbe/httpGet/path", "value": "/healthz"}, {"op": "replace", "path": "/spec/template/spec/containers/0/livenessProbe/httpGet/path", "value": "/livez"}}
	if err != nil || !found || !reflect.DeepEqual(patch, expected) {
		t.Fatal("probe transfer not exact", patch, err)
	}
	for _, tail := range []string{"livenessProbe/httpGet/port", "livenessProbe/timeoutSeconds", "livenessProbe/exec/command", "livenessProbe", "image"} {
		outside := "/spec/template/spec/containers[name=dns]/" + tail
		if err := validateEmergencyOwnershipConflictEvidence(desired, live, append(allowed, outside), "dns-declarative", failure(outside)); err == nil {
			t.Fatal("probe path transfer admitted", tail)
		}
	}
	entry := m["managedFields"].([]any)[0].(map[string]any)
	entry["operation"] = "Apply"
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, "dns-declarative", failure(pointer)); err == nil {
		t.Fatal("missing Update witness accepted")
	}
}
