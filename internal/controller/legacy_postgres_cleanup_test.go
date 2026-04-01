package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestTriggerLegacyPostgresCleanupCreatesJobPerNode(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		BackingServices: []model.BackingService{
			{
				OwnerAppID: "app_demo",
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						StoragePath: "/var/lib/fugue/tenant-data/fg-tenant-demo/demo/postgres",
					},
				},
			},
		},
	}

	createdJobs := make([]map[string]any, 0)
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/api/v1/nodes":
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(`{
					"items": [
						{"metadata": {"name": "node-b"}},
						{"metadata": {"name": "node-a"}}
					]
				}`)),
				Header: make(http.Header),
			}, nil
		case req.Method == http.MethodPost && req.URL.Path == "/apis/batch/v1/namespaces/fugue-system/jobs":
			defer req.Body.Close()
			var job map[string]any
			if err := json.NewDecoder(req.Body).Decode(&job); err != nil {
				t.Fatalf("decode cleanup job request: %v", err)
			}
			createdJobs = append(createdJobs, job)
			return &http.Response{
				StatusCode: http.StatusCreated,
				Body:       io.NopCloser(strings.NewReader(`{"kind":"Job"}`)),
				Header:     make(http.Header),
			}, nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
			return nil, nil
		}
	})

	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
		namespace:   "fugue-system",
	}

	svc := &Service{}
	if err := svc.triggerLegacyPostgresCleanup(context.Background(), client, app, "op_demo"); err != nil {
		t.Fatalf("trigger legacy postgres cleanup: %v", err)
	}

	if len(createdJobs) != 2 {
		t.Fatalf("expected 2 cleanup jobs, got %d", len(createdJobs))
	}

	nodeNames := make([]string, 0, len(createdJobs))
	for _, job := range createdJobs {
		spec := nestedMap(job, "spec", "template", "spec")
		nodeNames = append(nodeNames, nestedString(spec, "nodeName"))

		metadata := nestedMap(job, "metadata")
		if got := nestedString(metadata, "namespace"); got != "fugue-system" {
			t.Fatalf("expected cleanup job namespace fugue-system, got %q", got)
		}
		env := nestedSliceMap(spec, "containers")[0]["env"].([]any)
		if got := envValue(env, "LEGACY_PATHS"); got != "/var/lib/fugue/tenant-data/fg-tenant-demo/demo/postgres" {
			t.Fatalf("unexpected LEGACY_PATHS env value: %q", got)
		}
		if got := envValue(env, "APP_ID"); got != "app_demo" {
			t.Fatalf("unexpected APP_ID env value: %q", got)
		}
	}

	sort.Strings(nodeNames)
	if strings.Join(nodeNames, ",") != "node-a,node-b" {
		t.Fatalf("unexpected cleanup target nodes: %v", nodeNames)
	}
}

func nestedMap(root map[string]any, keys ...string) map[string]any {
	current := root
	for _, key := range keys {
		next, _ := current[key].(map[string]any)
		current = next
	}
	return current
}

func nestedSliceMap(root map[string]any, key string) []map[string]any {
	raw, _ := root[key].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		mapped, _ := item.(map[string]any)
		out = append(out, mapped)
	}
	return out
}

func nestedString(root map[string]any, key string) string {
	value, _ := root[key].(string)
	return value
}

func envValue(env []any, name string) string {
	for _, entry := range env {
		mapped, _ := entry.(map[string]any)
		if nestedString(mapped, "name") == name {
			return nestedString(mapped, "value")
		}
	}
	return ""
}
