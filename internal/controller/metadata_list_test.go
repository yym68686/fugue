package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNameOnlyListNegotiatesMetadataAndPreservesFallbackAndErrors(t *testing.T) {
	for _, kind := range []string{"PartialObjectMetadataList", "SecretList"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Accept") != metadataListAccept || r.URL.Query().Get("labelSelector") != "owner=sample" || r.URL.Query().Has("resourceVersion") || r.Method != "GET" {
					t.Errorf("metadata request changed semantics: %s %s %v", r.Method, r.URL, r.Header)
				}
				if strings.HasSuffix(r.URL.Path, "/missing") {
					http.Error(w, "missing", 404)
					return
				}
				fmt.Fprintf(w, `{"kind":%q,"items":[{"metadata":{"name":"first"},"data":{"unused":"content"}},{"metadata":{"name":"second"}}]}`, kind)
			}))
			defer server.Close()
			client := &kubeClient{client: server.Client(), baseURL: server.URL}
			names, err := client.listNamespacedResourceNames(context.Background(), "/api/v1/namespaces/sample/secrets", "owner=sample")
			if err != nil || !reflect.DeepEqual(names, []string{"first", "second"}) {
				t.Fatalf("metadata/fallback result changed: names=%v err=%v", names, err)
			}
			if _, err := client.listNamespacedResourceNames(context.Background(), "/missing", "owner=sample"); err == nil {
				t.Fatal("metadata request hid API failure")
			}
		})
	}
}

func TestNameOnlyListRetriesRejectedRepresentationWithoutChangingRead(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.String() != "/api/v1/namespaces/sample/secrets?labelSelector=owner%3Dsample" {
			t.Errorf("fallback changed read: %s", r.URL)
		}
		switch r.Header.Get("Accept") {
		case "application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1,application/json;q=0.9":
			http.Error(w, "unsupported representation", http.StatusNotAcceptable)
		case "application/json":
			fmt.Fprint(w, `{"items":[{"metadata":{"name":"kept"}}]}`)
		default:
			t.Error("unexpected representation", r.Header.Get("Accept"))
		}
	}))
	defer server.Close()
	c := &kubeClient{client: server.Client(), baseURL: server.URL}
	names, err := c.listNamespacedResourceNames(t.Context(), "/api/v1/namespaces/sample/secrets", "owner=sample")
	if err != nil || !reflect.DeepEqual(names, []string{"kept"}) || calls.Load() != 2 {
		t.Fatalf("fallback: %v %v calls=%d", names, err, calls.Load())
	}
}
