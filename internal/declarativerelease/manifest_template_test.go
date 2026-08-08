package declarativerelease

import (
	"bytes"
	"testing"
)

func TestManifestTemplateAddsAThirdGroupUsingOnlyRegistryData(t *testing.T) {
	template := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"@@CONTROL_NAME@@","namespace":"fugue-system"},"spec":{"replicas":1,"strategy":{"type":"Recreate"},"template":{"metadata":{"labels":{"fugue.io/edge-group-id":"@@GROUP_ID@@"}},"spec":{"containers":[{"args":["${RUNTIME_ENV}"],"image":"fugue-artifact://edge-control","name":"edge-control"}],"nodeSelector":{"fugue.io/edge-group":"@@GROUP@@"}}}}}],"kind":"ComponentResourceSet"}`)
	raw, err := MaterializeManifestTemplate(template, map[string]string{
		"CONTROL_NAME": "edge-control-gamma", "GROUP": "gamma", "GROUP_ID": "edge-group-metro-gamma",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"edge-control-gamma", "edge-group-metro-gamma", `"fugue.io/edge-group":"gamma"`} {
		if !bytes.Contains(raw, []byte(value)) {
			t.Fatalf("third group manifest lost %q: %s", value, raw)
		}
	}
	if !bytes.Contains(raw, []byte("${RUNTIME_ENV}")) {
		t.Fatalf("runtime shell variable was interpreted as a manifest placeholder: %s", raw)
	}
	if _, err := MaterializeManifestTemplate(template, map[string]string{"CONTROL_NAME": "edge-control-gamma"}); err == nil {
		t.Fatal("template with unresolved group variables was accepted")
	}
	if _, err := MaterializeManifestTemplate(template, map[string]string{
		"CONTROL_NAME": "edge-control-gamma", "GROUP": "gamma", "GROUP_ID": "edge-group-metro-gamma", "EXTRA": "unused",
	}); err == nil {
		t.Fatal("unused registry variable was accepted")
	}
}

func TestPredecessorTemplateAllowsOnlyNewUnusedRegistryVariables(t *testing.T) {
	predecessor := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"@@NAME@@","namespace":"fugue-system"},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"edge"}},"template":{"metadata":{"labels":{"app":"edge"}},"spec":{"containers":[{"image":"fugue-artifact://edge","name":"edge"}]}}}}],"kind":"ComponentResourceSet"}`)
	variables := map[string]string{"NAME": "edge-gamma", "NEW_COMPONENT": "edge-gamma-worker"}
	if _, err := MaterializePredecessorManifestTemplate(predecessor, variables); err != nil {
		t.Fatalf("new current registry variable rejected for predecessor: %v", err)
	}
	if _, err := MaterializeManifestTemplate(predecessor, variables); err == nil {
		t.Fatal("current forward template accepted an unused registry variable")
	}
	if _, err := MaterializePredecessorManifestTemplate(predecessor, map[string]string{"NEW_COMPONENT": "edge-gamma-worker"}); err == nil {
		t.Fatal("predecessor template accepted an unresolved historical placeholder")
	}
}
