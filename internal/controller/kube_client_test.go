package controller

import (
	"context"
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
