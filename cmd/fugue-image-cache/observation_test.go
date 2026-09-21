package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
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
