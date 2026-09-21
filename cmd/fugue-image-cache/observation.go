package main

import (
	"log"
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
	if source == "network" && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		// The embedded registry also serves in-process graph checks. Emit
		// access facts only for real network reads; library errors stay logged.
		log.Printf("registry request method=%s path=%s source=network", r.Method, r.URL)
	}
}

func registryRequestLogger(destination *log.Logger) *log.Logger {
	return log.New(registryLogWriter{destination}, "", 0)
}

type registryLogWriter struct{ destination *log.Logger }

func (w registryLogWriter) Write(p []byte) (int, error) {
	line := strings.TrimSpace(string(p))
	fields := strings.Fields(line)
	// Upstream's successful read record has exactly METHOD URL; failures
	// include status/code/message. Never filter warnings, errors or writes.
	if len(fields) == 2 && (fields[0] == http.MethodGet || fields[0] == http.MethodHead) && strings.HasPrefix(fields[1], "/v2/") {
		return len(p), nil
	}
	w.destination.Print(line)
	return len(p), nil
}
