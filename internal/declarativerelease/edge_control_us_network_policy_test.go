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
	if intent.Generation != 12 || intent.ExpectedPreviousConfigSHA != "ea8f8255e2c56bf90c3a5c68855fcb0ca36efba7" ||
		intent.ExpectedPreviousManifestSHA != intent.ExpectedPreviousConfigSHA || intent.ExpectedPreviousOCIRevision != intent.ExpectedPreviousConfigSHA ||
		intent.ExpectedPreviousImageDigest != "sha256:039af788907e5f3037e242b42b1dd2921e214e2c72a81d8b88e045be6c667775" {
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
	prior.Generation = 11
	prior.ExpectedPreviousConfigSHA = "6d16e90ea1ebe5b37f46300b0e6967aa3fb0bed0"
	prior.ExpectedPreviousManifestSHA = prior.ExpectedPreviousConfigSHA
	prior.ExpectedPreviousOCIRevision = prior.ExpectedPreviousConfigSHA
	prior.ExpectedPreviousImageDigest = "sha256:9bfe284ea00b644c2434e3e3562b1ae326363c5f6eaafa79124d9c56a98c2ecb"
	bound, err := BindIntents(registry, plan, map[string]Intent{us.Control.ID: intent}, map[string]Intent{us.Control.ID: prior},
		map[string]string{us.Control.ID: intent.ExpectedPreviousConfigSHA})
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
