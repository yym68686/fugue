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
	if us == nil || us.Control.ManifestPath != "internal/edgecontrol/component/resources.authority.us.json" {
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
			if sharedItems[index]["kind"] != "Deployment" {
				if !reflect.DeepEqual(sharedItems[index], usItems[index]) {
					t.Fatalf("US authority prerequisite changed non-NetworkPolicy resource %d", index)
				}
				continue
			}
			sharedContainer := sharedItems[index]["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
			usContainer := usItems[index]["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
			sharedResources := sharedContainer["resources"].(map[string]any)
			usResources := usContainer["resources"].(map[string]any)
			if got := usResources["limits"].(map[string]any)["cpu"]; got != "2" {
				t.Fatalf("US Control CPU limit=%v, want 2", got)
			}
			if got := usResources["requests"].(map[string]any)["cpu"]; got != "100m" {
				t.Fatalf("US Control CPU request=%v, want 100m", got)
			}
			if got := usResources["limits"].(map[string]any)["memory"]; got != "2Gi" {
				t.Fatalf("US Control memory limit=%v, want 2Gi", got)
			}
			if got := usResources["requests"].(map[string]any)["memory"]; got != "512Mi" {
				t.Fatalf("US Control memory request=%v, want 512Mi", got)
			}
			copyUS := cloneJSONMap(t, usItems[index])
			copyContainer := copyUS["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
			copyResources := copyContainer["resources"].(map[string]any)
			copyResources["limits"].(map[string]any)["cpu"] = sharedResources["limits"].(map[string]any)["cpu"]
			copyResources["requests"].(map[string]any)["cpu"] = sharedResources["requests"].(map[string]any)["cpu"]
			copyResources["limits"].(map[string]any)["memory"] = sharedResources["limits"].(map[string]any)["memory"]
			copyResources["requests"].(map[string]any)["memory"] = sharedResources["requests"].(map[string]any)["memory"]
			got, gotErr := CanonicalJSON(copyUS)
			wantRaw, wantErr := CanonicalJSON(sharedItems[index])
			if gotErr != nil || wantErr != nil || !bytes.Equal(got, wantRaw) {
				t.Fatalf("US authority prerequisite changed non-NetworkPolicy resource %d: got=%s want=%s errors=%v/%v", index, got, wantRaw, gotErr, wantErr)
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
	const lkgSHA = "d0adfc530693687b8ad9cbe35fcc539e64c687c6"
	if intent.Generation != 50 || intent.ExpectedPreviousConfigSHA != lkgSHA ||
		intent.ExpectedPreviousManifestSHA != intent.ExpectedPreviousConfigSHA || intent.ExpectedPreviousOCIRevision != intent.ExpectedPreviousConfigSHA ||
		intent.ExpectedPreviousImageDigest != "sha256:b7c8cb4447f91e979371bf737d57d08f847acdf1f85bc4fcc7c5864da8f6633b" ||
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
	prior.Generation = intent.Generation - 1
	failed := intent
	failed.Generation = 32
	failed.SupersedesFailedConfigSHA = "4a5c93325d8c6aac734ba35d04932f1ed21f25fd"
	bound, err := BindIntents(registry, plan, map[string]Intent{us.Control.ID: intent}, map[string]Intent{us.Control.ID: prior}, map[string]string{us.Control.ID: lkgSHA}, SupersededIntents{us.Control.ID: {intent.SupersedesFailedConfigSHA: failed}})
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
