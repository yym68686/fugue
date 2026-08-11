package declarativerelease

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestEdgeControlUSNetworkPolicyAddsOnlyExactAPIAuthorityReader(t *testing.T) {
	baseFile, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	base, err := DecodeRegistry(baseFile)
	closeErr := baseFile.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("decode base registry: %v close: %v", err, closeErr)
	}
	edgeFile, err := os.Open("../../deploy/releases/edge-groups.json")
	if err != nil {
		t.Fatal(err)
	}
	edge, err := DecodeEdgeGroupRegistry(edgeFile)
	closeErr = edgeFile.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("decode edge registry: %v close: %v", err, closeErr)
	}
	var us *EdgeGroup
	for index := range edge.Groups {
		if edge.Groups[index].ID == "us" {
			us = &edge.Groups[index]
			break
		}
	}
	if us == nil || us.Control.ManifestPath != "internal/edgecontrol/component/resources.authority.de.json" {
		t.Fatalf("US Edge Control does not use the production-verified API authority manifest: %+v", us)
	}

	sharedPath := "internal/edgecontrol/component/resources.authority.group.json"
	sharedItems := materializedResourceSetItems(t, sharedPath, us.Control.ManifestVariables)
	usItems := materializedResourceSetItems(t, us.Control.ManifestPath, us.Control.ManifestVariables)
	if len(sharedItems) != len(usItems) {
		t.Fatalf("US authority prerequisite changed resource count: shared=%d US=%d", len(sharedItems), len(usItems))
	}
	for index := range sharedItems {
		if sharedItems[index]["kind"] != "NetworkPolicy" {
			if !reflect.DeepEqual(sharedItems[index], usItems[index]) {
				t.Fatalf("US authority prerequisite changed non-NetworkPolicy resource %d", index)
			}
			continue
		}
		sharedSpec := sharedItems[index]["spec"].(map[string]any)
		usSpec := usItems[index]["spec"].(map[string]any)
		sharedIngress := sharedSpec["ingress"].([]any)
		usIngress := usSpec["ingress"].([]any)
		if len(usIngress) != len(sharedIngress)+1 || !reflect.DeepEqual(usIngress[:len(sharedIngress)], sharedIngress) {
			t.Fatalf("US NetworkPolicy changed existing ingress rules: shared=%#v US=%#v", sharedIngress, usIngress)
		}
		want := map[string]any{
			"from": []any{map[string]any{
				"namespaceSelector": map[string]any{"matchLabels": map[string]any{"kubernetes.io/metadata.name": "fugue-system"}},
				"podSelector":       map[string]any{"matchLabels": map[string]any{"fugue.io/edge-control-route-intent-tls": "true"}},
			}},
			"ports": []any{map[string]any{"port": json.Number("8092"), "protocol": "TCP"}},
		}
		if !reflect.DeepEqual(usIngress[len(sharedIngress)], want) {
			t.Fatalf("US API authority reader ingress=%#v, want %#v", usIngress[len(sharedIngress)], want)
		}
		copyUS := cloneJSONMap(t, usItems[index])
		copyUS["spec"].(map[string]any)["ingress"] = sharedIngress
		got, gotErr := CanonicalJSON(copyUS)
		wantRaw, wantErr := CanonicalJSON(sharedItems[index])
		if gotErr != nil || wantErr != nil || !bytes.Equal(got, wantRaw) {
			t.Fatalf("US NetworkPolicy changed fields outside the one API ingress rule: got=%s want=%s errors=%v/%v", got, wantRaw, gotErr, wantErr)
		}
	}

	intentFile, err := os.Open("../../" + us.Control.IntentPath)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := DecodeIntent(intentFile)
	closeErr = intentFile.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("decode US intent: %v close: %v", err, closeErr)
	}
	if intent.Generation != 18 || intent.ExpectedPreviousConfigSHA != "a4da8224f757869510fbf704fa15c5a1c17222cf" ||
		intent.ExpectedPreviousManifestSHA != intent.ExpectedPreviousConfigSHA || intent.ExpectedPreviousOCIRevision != intent.ExpectedPreviousConfigSHA ||
		intent.ExpectedPreviousImageDigest != "sha256:6d614215f6fe3f6b859e6a7ad212b4304326cfde3e47c401b37b0eef149b7070" ||
		intent.SupersedesFailedConfigSHA != "" || us.Control.Delivery.Writer != "guardian" || us.Control.Delivery.Group != "us" || us.Control.Delivery.DependencyService != "fugue-fugue" {
		t.Fatalf("US Edge Control intent does not bind the exact live predecessor: %+v", intent)
	}
	registry, err := MergeEdgeGroupRegistry(base, edge)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{"deploy/releases/edge-groups.json", us.Control.IntentPath})
	if err != nil {
		t.Fatal(err)
	}
	prior := intent
	prior.Generation = 17
	prior.ExpectedPreviousConfigSHA = "546958c18232758e30520115e511588081fd72fc"
	prior.ExpectedPreviousManifestSHA = prior.ExpectedPreviousConfigSHA
	prior.ExpectedPreviousOCIRevision = prior.ExpectedPreviousConfigSHA
	prior.ExpectedPreviousImageDigest = "sha256:94367a7a8f18b4b2fe0570e7dde15862856a5565b8fe4e28fae3c88e8f512c41"
	prior.SupersedesFailedConfigSHA = "a42ad747471d8a276c23c5763f84f746590dcbdb"
	bound, err := BindIntents(registry, plan, map[string]Intent{us.Control.ID: intent}, map[string]Intent{us.Control.ID: prior},
		map[string]string{})
	if err != nil || len(bound.Releases) != 1 || bound.Releases[0].ComponentID != "edge-control-us" {
		t.Fatalf("US Edge Control prerequisite planner expanded: releases=%+v err=%v", bound.Releases, err)
	}
}

func materializedResourceSetItems(t *testing.T, path string, variables map[string]string) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile("../../" + path)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = MaterializeManifestTemplate(raw, variables)
	if err != nil {
		t.Fatal(err)
	}
	set, err := DecodeResourceSet(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	return set.Items
}

func cloneJSONMap(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	raw, err := CanonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := decodeStrict(bytes.NewReader(raw), &result); err != nil {
		t.Fatal(err)
	}
	return result
}
