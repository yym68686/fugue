package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestFormatKubeTimestampUsesMicrosecondPrecision(t *testing.T) {
	value := time.Date(2026, time.March, 24, 12, 34, 56, 123456789, time.UTC)

	formatted := formatKubeTimestamp(value)
	expected := "2026-03-24T12:34:56.123456Z"
	if formatted != expected {
		t.Fatalf("expected %q, got %q", expected, formatted)
	}

	parsed, err := time.Parse("2006-01-02T15:04:05.000000Z07:00", formatted)
	if err != nil {
		t.Fatalf("parse formatted timestamp: %v", err)
	}
	if !parsed.UTC().Equal(time.Date(2026, time.March, 24, 12, 34, 56, 123456000, time.UTC)) {
		t.Fatalf("unexpected parsed time: %s", parsed.UTC().Format(time.RFC3339Nano))
	}
}

func TestApplyObjectRecreatesDeploymentAfterImmutableSelectorError(t *testing.T) {
	t.Parallel()

	var requests []string
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests = append(requests, req.Method+" "+req.URL.Path)
		switch {
		case req.Method == http.MethodPatch && req.URL.Path == "/apis/apps/v1/namespaces/tenant-demo/deployments/uni-api-demo" && len(requests) == 1:
			return &http.Response{
				StatusCode: http.StatusUnprocessableEntity,
				Body:       io.NopCloser(strings.NewReader(`{"message":"Deployment.apps \"uni-api-demo\" is invalid: spec.selector: Invalid value: map[string]string{\"app.kubernetes.io/name\":\"uni-api-demo\"}: field is immutable"}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodDelete && req.URL.Path == "/apis/apps/v1/namespaces/tenant-demo/deployments/uni-api-demo":
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet && req.URL.Path == "/apis/apps/v1/namespaces/tenant-demo/deployments/uni-api-demo":
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"message":"not found"}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodPatch && req.URL.Path == "/apis/apps/v1/namespaces/tenant-demo/deployments/uni-api-demo" && len(requests) == 4:
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"kind":"Deployment"}`)),
				Header:     make(http.Header),
			}, nil
		default:
			t.Fatalf("unexpected request %s %s (sequence=%v)", req.Method, req.URL.Path, requests)
			return nil, nil
		}
	})

	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
		namespace:   "tenant-demo",
	}

	obj := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name": "uni-api-demo",
		},
	}

	if err := client.applyObject(context.Background(), obj, nil); err != nil {
		t.Fatalf("apply object: %v", err)
	}

	expected := []string{
		"PATCH /apis/apps/v1/namespaces/tenant-demo/deployments/uni-api-demo",
		"DELETE /apis/apps/v1/namespaces/tenant-demo/deployments/uni-api-demo",
		"GET /apis/apps/v1/namespaces/tenant-demo/deployments/uni-api-demo",
		"PATCH /apis/apps/v1/namespaces/tenant-demo/deployments/uni-api-demo",
	}
	if len(requests) != len(expected) {
		t.Fatalf("expected request sequence %v, got %v", expected, requests)
	}
	for i, want := range expected {
		if requests[i] != want {
			t.Fatalf("expected request %d to be %q, got %q", i, want, requests[i])
		}
	}
}

func TestApplyObjectRemovesStaleAppFileVolumeReferencesBeforeRetry(t *testing.T) {
	t.Parallel()

	var requests []string
	var cleanupPatch []map[string]string
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests = append(requests, req.Method+" "+req.URL.Path)
		switch {
		case req.Method == http.MethodPatch && req.URL.Path == "/apis/apps/v1/namespaces/tenant-demo/deployments/demo" && len(requests) == 1:
			return &http.Response{
				StatusCode: http.StatusUnprocessableEntity,
				Body:       io.NopCloser(strings.NewReader(`{"message":"Deployment.apps \"demo\" is invalid: spec.template.spec.containers[0].volumeMounts[0].name: Not found: \"app-files\""}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodGet && req.URL.Path == "/apis/apps/v1/namespaces/tenant-demo/deployments/demo":
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(`{
					"apiVersion":"apps/v1",
					"kind":"Deployment",
					"metadata":{"name":"demo","namespace":"tenant-demo"},
					"spec":{"template":{"spec":{
						"containers":[
							{"name":"demo","volumeMounts":[
								{"name":"app-files","mountPath":"/app/config.yaml"},
								{"name":"cache","mountPath":"/cache"},
								{"name":"app-files","mountPath":"/app/secret.yaml"}
							]},
							{"name":"sidecar","volumeMounts":[
								{"name":"app-files","mountPath":"/sidecar/config.yaml"}
							]}
						],
						"initContainers":[
							{"name":"init","volumeMounts":[
								{"name":"app-files","mountPath":"/init/config.yaml"}
							]}
						],
						"volumes":[
							{"name":"app-files","secret":{"secretName":"demo-files"}},
							{"name":"cache","emptyDir":{}}
						]
					}}}
				}`)),
				Header: make(http.Header),
			}, nil
		case req.Method == http.MethodPatch && req.URL.Path == "/apis/apps/v1/namespaces/tenant-demo/deployments/demo" && len(requests) == 3:
			if got := req.Header.Get("Content-Type"); got != "application/json-patch+json" {
				t.Fatalf("expected json patch content type, got %q", got)
			}
			if err := json.NewDecoder(req.Body).Decode(&cleanupPatch); err != nil {
				t.Fatalf("decode cleanup patch: %v", err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		case req.Method == http.MethodPatch && req.URL.Path == "/apis/apps/v1/namespaces/tenant-demo/deployments/demo" && len(requests) == 4:
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"kind":"Deployment"}`)),
				Header:     make(http.Header),
			}, nil
		default:
			t.Fatalf("unexpected request %s %s (sequence=%v)", req.Method, req.URL.Path, requests)
			return nil, nil
		}
	})

	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
		namespace:   "tenant-demo",
	}

	obj := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name": "demo",
		},
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []map[string]any{
						{
							"name":         "demo",
							"volumeMounts": []map[string]any{},
						},
					},
					"volumes": []map[string]any{},
				},
			},
		},
	}

	if err := client.applyObject(context.Background(), obj, nil); err != nil {
		t.Fatalf("apply object: %v", err)
	}

	expectedRequests := []string{
		"PATCH /apis/apps/v1/namespaces/tenant-demo/deployments/demo",
		"GET /apis/apps/v1/namespaces/tenant-demo/deployments/demo",
		"PATCH /apis/apps/v1/namespaces/tenant-demo/deployments/demo",
		"PATCH /apis/apps/v1/namespaces/tenant-demo/deployments/demo",
	}
	if len(requests) != len(expectedRequests) {
		t.Fatalf("expected request sequence %v, got %v", expectedRequests, requests)
	}
	for i, want := range expectedRequests {
		if requests[i] != want {
			t.Fatalf("expected request %d to be %q, got %q", i, want, requests[i])
		}
	}

	expectedPatch := []map[string]string{
		{"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts/2"},
		{"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts/0"},
		{"op": "remove", "path": "/spec/template/spec/containers/1/volumeMounts/0"},
		{"op": "remove", "path": "/spec/template/spec/initContainers/0/volumeMounts/0"},
		{"op": "remove", "path": "/spec/template/spec/volumes/0"},
	}
	if len(cleanupPatch) != len(expectedPatch) {
		t.Fatalf("expected cleanup patch %#v, got %#v", expectedPatch, cleanupPatch)
	}
	for index, want := range expectedPatch {
		if cleanupPatch[index]["op"] != want["op"] || cleanupPatch[index]["path"] != want["path"] {
			t.Fatalf("expected cleanup patch %d to be %#v, got %#v", index, want, cleanupPatch[index])
		}
	}
}

func TestCustomResourceListingsIgnoreMissingAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		apiPath string
		call    func(context.Context, *kubeClient) ([]string, error)
	}{
		{
			name:    "cnpg clusters",
			apiPath: "/apis/postgresql.cnpg.io/v1/namespaces/tenant-demo/clusters",
			call: func(ctx context.Context, client *kubeClient) ([]string, error) {
				return client.listCloudNativePGClusterNamesByLabel(ctx, "tenant-demo", "fugue.pro/owner-app-id=app_demo")
			},
		},
		{
			name:    "volsync destinations",
			apiPath: "/apis/volsync.backube/v1alpha1/namespaces/tenant-demo/replicationdestinations",
			call: func(ctx context.Context, client *kubeClient) ([]string, error) {
				return client.listVolSyncReplicationDestinationNamesByLabel(ctx, "tenant-demo", "fugue.pro/owner-app-id=app_demo")
			},
		},
		{
			name:    "volsync sources",
			apiPath: "/apis/volsync.backube/v1alpha1/namespaces/tenant-demo/replicationsources",
			call: func(ctx context.Context, client *kubeClient) ([]string, error) {
				return client.listVolSyncReplicationSourceNamesByLabel(ctx, "tenant-demo", "fugue.pro/owner-app-id=app_demo")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &kubeClient{
				client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if req.Method != http.MethodGet || req.URL.Path != tt.apiPath {
						t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
					}
					if got := req.URL.Query().Get("labelSelector"); got != "fugue.pro/owner-app-id=app_demo" {
						t.Fatalf("unexpected label selector %q", got)
					}
					return &http.Response{
						StatusCode: http.StatusNotFound,
						Body:       io.NopCloser(strings.NewReader(`{"kind":"Status","status":"Failure","message":"the server could not find the requested resource","reason":"NotFound","code":404}`)),
						Header:     make(http.Header),
					}, nil
				})},
				baseURL:     "http://kube.test",
				bearerToken: "token",
				namespace:   "tenant-demo",
			}

			names, err := tt.call(context.Background(), client)
			if err != nil {
				t.Fatalf("list resources: %v", err)
			}
			if len(names) != 0 {
				t.Fatalf("expected no resources, got %v", names)
			}
		})
	}
}
