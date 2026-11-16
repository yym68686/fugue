package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestPreserveCloudNativePGDefaultedImage(t *testing.T) {
	for _, desiredImage := range []string{"", "registry.invalid/postgres:explicit"} {
		current := map[string]any{"spec": map[string]any{"imageName": "registry.invalid/postgres:existing"}}
		desired := map[string]any{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "spec": map[string]any{"instances": 1}}
		if desiredImage != "" {
			desired["spec"].(map[string]any)["imageName"] = desiredImage
		}
		preserveCloudNativePGDefaultedImage(current, desired)
		want := desiredImage
		if want == "" {
			want = "registry.invalid/postgres:existing"
		}
		if got := desired["spec"].(map[string]any)["imageName"]; got != want {
			t.Fatalf("image changed without explicit intent: got %v, want %s", got, want)
		}
	}
	current := map[string]any{"spec": map[string]any{"imageCatalogRef": map[string]any{"name": "versions", "major": 18}}}
	desired := map[string]any{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "spec": map[string]any{"instances": 1}}
	preserveCloudNativePGDefaultedImage(current, desired)
	if catalog := objectMapField(objectMapField(desired, "spec"), "imageCatalogRef"); catalog["name"] != "versions" {
		t.Fatalf("defaulted image catalog was lost: %#v", desired)
	}
}

func TestExistingPostgresDefaultImageIsNotResetByApplyOrSpecReplacement(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "apply", true: "replace"}[replace], func(t *testing.T) {
			writes := 0
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet {
					writes++
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","metadata":{"name":"sample"},"spec":{"instances":1,"imageName":"registry.invalid/postgres:existing","storage":{"size":"1Gi"}}}`))}, nil
			})
			client := &kubeClient{client: &http.Client{Transport: transport}, baseURL: "http://kube.test", namespace: "tenant"}
			var desired map[string]any
			_ = json.Unmarshal([]byte(`{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","metadata":{"name":"sample"},"spec":{"instances":1,"storage":{"size":"1Gi"}}}`), &desired)
			var err error
			if replace {
				err = client.replaceObjectSpec(context.Background(), desired)
			} else {
				err = client.applyObject(context.Background(), desired, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			if writes != 0 {
				t.Fatalf("unchanged image caused %d writes", writes)
			}
		})
	}
}
