package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type observationRoundTripper func(*http.Request) (*http.Response, error)

func (f observationRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type interruptedObservationBody struct{ body string }

func (r *interruptedObservationBody) Read(p []byte) (int, error) {
	if r.body == "" {
		return 0, io.ErrUnexpectedEOF
	}
	n := copy(p, r.body)
	r.body = r.body[n:]
	return n, nil
}
func (*interruptedObservationBody) Close() error { return nil }

func TestRuntimeObservationRejectsIncompleteOrTrailingJSON(t *testing.T) {
	for _, body := range []string{"", `{"items":[]}`, `{"items":[]} {"items":[]}`, `{"items":[`} {
		t.Run(body, func(t *testing.T) {
			c := &managedAppStatusClient{baseURL: "https://cluster", client: &http.Client{Transport: observationRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: &interruptedObservationBody{body: body}}, nil
			})}}
			var out kubeTypedResourceList[kubeMetadataEvidence]
			if err := c.doJSON(context.Background(), "/api/v1/namespaces", &out); err == nil {
				t.Fatal("incomplete transport accepted as complete observation")
			}
		})
	}
	c := &managedAppStatusClient{baseURL: "https://cluster", client: &http.Client{Transport: observationRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"items":[]} {}`))}, nil
	})}}
	if err := c.doJSON(context.Background(), "/api/v1/namespaces", &kubeTypedResourceList[kubeMetadataEvidence]{}); err == nil {
		t.Fatal("trailing JSON accepted")
	}
}

func TestTypedObservationPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &managedAppStatusClient{baseURL: "https://cluster", client: &http.Client{Transport: observationRoundTripper(func(r *http.Request) (*http.Response, error) { return nil, r.Context().Err() })}}
	if err := c.doJSON(ctx, "/api/v1/namespaces", &kubeTypedResourceList[kubeMetadataEvidence]{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel lost: %v", err)
	}
}

func BenchmarkRuntimeObservationDecode(b *testing.B) {
	// Large fields such as managedFields and env are irrelevant to readiness.
	object := `{"metadata":{"name":"demo","namespace":"tenant-a","generation":3,"managedFields":[{"fieldsV1":{"blob":"` + strings.Repeat("x", 2048) + `"}}]},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"web","image":"example@sha256:abc","env":[{"name":"DATA","value":"` + strings.Repeat("y", 2048) + `"}]}]}}},"status":{"readyReplicas":1,"observedGeneration":3}}`
	payload := []byte(`{"items":[` + strings.TrimSuffix(strings.Repeat(object+",", 300), ",") + `]}`)
	b.Run("map-roundtrip", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var list struct{ Items []map[string]any }
			if err := json.Unmarshal(payload, &list); err != nil {
				b.Fatal(err)
			}
			for _, v := range list.Items {
				var out kubeDeploymentRuntimeEvidence
				if err := decodeKubeObject(v, &out); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("typed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var list kubeTypedResourceList[kubeDeploymentRuntimeEvidence]
			if err := json.Unmarshal(payload, &list); err != nil {
				b.Fatal(err)
			}
		}
	})
}
