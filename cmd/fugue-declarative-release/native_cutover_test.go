package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestRenderNativeCutoverPolicyDoesNotTakeClusterCredentials(t *testing.T) {
	const fixture = `{
		"key":{"component":"demo","group":"cell-a"},
		"namespace":"isolated",
		"deployment":"demo",
		"monitorComponent":"demo",
		"componentLease":"demo-lease",
		"phase":"frozen",
		"publisher":"system:serviceaccount:isolated:publisher",
		"guardian":"system:serviceaccount:isolated:guardian",
		"canaryProber":"system:serviceaccount:isolated:canary",
		"deploymentStatusWriter":"system:kube-controller-manager"
	}`
	t.Setenv("KUBECONFIG", "/no-such-kubeconfig")
	var out bytes.Buffer
	if err := renderNativeCutoverPolicy(strings.NewReader(fixture), &out); err != nil {
		t.Fatal(err)
	}
	var objects struct {
		Kind  string `json:"kind"`
		Items []struct {
			Kind string `json:"kind"`
		} `json:"items"`
	}
	if err := json.Unmarshal(out.Bytes(), &objects); err != nil || objects.Kind != "List" || len(objects.Items) != 6 {
		t.Fatalf("renderer did not produce three Kubernetes policy/binding pairs: %+v, error=%v", objects, err)
	}
	for i := 0; i < len(objects.Items); i += 2 {
		if objects.Items[i].Kind != "ValidatingAdmissionPolicy" || objects.Items[i+1].Kind != "ValidatingAdmissionPolicyBinding" {
			t.Fatalf("unexpected object order: %+v", objects.Items)
		}
	}
	for _, invalid := range []string{fixture + "{}", strings.Replace(fixture, "\"phase\"", "\"unknown\"", 1), fixture + strings.Repeat(" ", 16<<10)} {
		out.Reset()
		if err := renderNativeCutoverPolicy(strings.NewReader(invalid), &out); err == nil || out.Len() != 0 {
			t.Fatalf("renderer accepted invalid policy config: %v", err)
		}
	}
}
