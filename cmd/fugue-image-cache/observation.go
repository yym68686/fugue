package main

import (
	"net/http"
	"strings"
	"time"

	"fugue/internal/runtimeobservation"
)

var registryOperations = func() *runtimeobservation.Operations {
	names := []string{"inventory-graph", "inventory-blob-index"}
	for _, source := range []string{"local", "network"} {
		for _, method := range []string{"get", "head", "write", "other"} {
			names = append(names, "registry-"+source+"-"+method)
		}
	}
	o, err := runtimeobservation.NewOperations(names...)
	if err != nil {
		panic(err)
	}
	return o
}()

// Internal registry requests have no network peer. Observe the existing call
// without wrapping the ResponseWriter or changing request/response behavior.
func (c *imageCache) serveObservedRegistry(w http.ResponseWriter, r *http.Request) {
	source, method := "local", "other"
	if r.RemoteAddr != "" {
		source = "network"
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		method = strings.ToLower(r.Method)
	case http.MethodPut, http.MethodPost, http.MethodPatch, http.MethodDelete:
		method = "write"
	}
	started := time.Now()
	c.registry.ServeHTTP(w, r)
	registryOperations.Observe("registry-"+source+"-"+method, time.Since(started))
}
