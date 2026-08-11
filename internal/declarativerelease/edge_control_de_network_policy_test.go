package declarativerelease

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestEdgeControlDENetworkPolicyAddsOnlyExactAPIAuthorityReader(t *testing.T) {
	registryFile, err := os.Open("../../deploy/releases/edge-groups.json")
	if err != nil {
		t.Fatal(err)
	}
	defer registryFile.Close()
	registry, err := DecodeEdgeGroupRegistry(registryFile)
	if err != nil {
		t.Fatal(err)
	}
	var de, us *EdgeGroup
	for index := range registry.Groups {
		switch registry.Groups[index].ID {
		case "de":
			de = &registry.Groups[index]
		case "us":
			us = &registry.Groups[index]
		}
	}
	if de == nil || us == nil {
		t.Fatal("DE or US Edge group is absent")
	}
	if de.Control.ManifestPath != "internal/edgecontrol/component/resources.authority.de.json" {
		t.Fatalf("DE Edge Control manifest path=%q", de.Control.ManifestPath)
	}
	deIngress := materializedNetworkPolicyIngress(t, de.Control.ManifestPath, de.Control.ManifestVariables)
	sharedIngress := materializedNetworkPolicyIngress(t, "internal/edgecontrol/component/resources.authority.group.json", de.Control.ManifestVariables)
	importerRule := map[string]any{
		"from": []any{map[string]any{
			"namespaceSelector": map[string]any{"matchLabels": map[string]any{"kubernetes.io/metadata.name": "fugue-system"}},
			"podSelector":       map[string]any{"matchLabels": map[string]any{"fugue.io/edge-authority-importer": "true"}},
		}},
		"ports": []any{map[string]any{"port": float64(8092), "protocol": "TCP"}},
	}
	importerRules := 0
	for _, rawRule := range sharedIngress {
		if reflect.DeepEqual(rawRule, importerRule) {
			importerRules++
		}
	}
	if importerRules != 1 {
		t.Fatalf("shared Edge Control NetworkPolicy importer rules=%d want=1: %#v", importerRules, sharedIngress)
	}
	if len(deIngress) != len(sharedIngress)+1 || !reflect.DeepEqual(deIngress[:len(sharedIngress)], sharedIngress) {
		t.Fatalf("DE NetworkPolicy changed existing ingress rules: DE=%#v shared=%#v", deIngress, sharedIngress)
	}
	want := map[string]any{
		"from": []any{map[string]any{
			"namespaceSelector": map[string]any{"matchLabels": map[string]any{"kubernetes.io/metadata.name": "fugue-system"}},
			"podSelector":       map[string]any{"matchLabels": map[string]any{"fugue.io/edge-control-route-intent-tls": "true"}},
		}},
		"ports": []any{map[string]any{"port": float64(8092), "protocol": "TCP"}},
	}
	if !reflect.DeepEqual(deIngress[len(sharedIngress)], want) {
		t.Fatalf("DE API authority reader ingress=%#v, want %#v", deIngress[len(sharedIngress)], want)
	}
}

func materializedNetworkPolicyIngress(t *testing.T, path string, variables map[string]string) []any {
	t.Helper()
	raw, err := os.ReadFile("../../" + path)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = MaterializeManifestTemplate(raw, variables)
	if err != nil {
		t.Fatal(err)
	}
	var set struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(raw, &set); err != nil {
		t.Fatal(err)
	}
	for _, item := range set.Items {
		if item["kind"] != "NetworkPolicy" {
			continue
		}
		spec, _ := item["spec"].(map[string]any)
		ingress, _ := spec["ingress"].([]any)
		return ingress
	}
	t.Fatal("NetworkPolicy is absent")
	return nil
}
