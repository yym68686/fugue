package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/registry"
)

func TestObservedRegistryPreservesLocalAndNetworkResponses(t *testing.T) {
	for _, peer := range []string{"", "192.0.2.1:1000"} {
		request := httptestRequest(http.MethodHead, "/v2/sample/blobs/example", "", nil)
		request.RemoteAddr = peer
		cache := &imageCache{registry: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r != request {
				t.Fatal("observer replaced the request")
			}
			w.Header().Set("Sample", "preserved")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "missing")
		})}
		response := httptest.NewRecorder()
		cache.serveObservedRegistry(response, request)
		if response.Code != http.StatusNotFound || response.Header().Get("Sample") != "preserved" || response.Body.String() != "missing" {
			t.Fatal("observer changed registry behavior")
		}
	}
}

func TestRegistryReadLogsRetainFailuresAndWrites(t *testing.T) {
	var output bytes.Buffer
	logger := registryRequestLogger(log.New(&output, "", 0))
	for _, line := range []string{"GET /v2/sample/manifests/tag", "HEAD /v2/sample/blobs/sha256:abc"} {
		logger.Print(line)
	}
	if output.Len() != 0 {
		t.Fatal("internal successful reads still produce logs")
	}
	for _, line := range []string{"GET /v2/sample/manifests/missing 404 MANIFEST_UNKNOWN Unknown manifest", "PUT /v2/sample/manifests/tag", "unexpected warning"} {
		logger.Print(line)
		if !strings.Contains(output.String(), line) {
			t.Fatalf("lost diagnostic %q", line)
		}
	}
}

func TestEmbeddedRegistryLoggerMatchesActualSuccessAndErrorShapes(t *testing.T) {
	var output bytes.Buffer
	handler := registry.New(registry.Logger(registryRequestLogger(log.New(&output, "", 0))))
	ok := httptest.NewRecorder()
	handler.ServeHTTP(ok, httptestRequest("GET", "/v2/", "", nil))
	if ok.Code != 200 || output.Len() != 0 {
		t.Fatal("successful local registry check logged or failed")
	}
	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, httptestRequest("HEAD", "/v2/sample/manifests/missing", "", nil))
	if missing.Code != 404 || !strings.Contains(output.String(), "404 NAME_UNKNOWN") {
		t.Fatalf("missing registry failure: %d %s", missing.Code, output.String())
	}
}
