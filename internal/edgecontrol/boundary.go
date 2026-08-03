// Package edgecontrol defines the independently deployable Edge control-plane
// boundary. The initial boundary intentionally owns no production authority.
package edgecontrol

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const BoundarySchemaV1 = "edge-control-boundary/v1"

// BoundaryStatus is deliberately explicit about capabilities that have not
// moved out of the legacy Core API yet.
type BoundaryStatus struct {
	Schema                 string `json:"schema"`
	Status                 string `json:"status"`
	Mode                   string `json:"mode"`
	Authority              string `json:"authority"`
	Enabled                bool   `json:"enabled"`
	PublicationEnabled     bool   `json:"publication_enabled"`
	DataPlaneDependency    bool   `json:"data_plane_dependency"`
	DatabaseCapability     bool   `json:"database_capability"`
	KubernetesCapability   bool   `json:"kubernetes_capability"`
	BundleSignerCapability bool   `json:"bundle_signer_capability"`
}

// Boundary serves health and status only. It has no outbound client, store,
// signer, or Kubernetes capability by construction.
type Boundary struct {
	enabled bool
}

func NewBoundary(enabled bool) *Boundary {
	return &Boundary{enabled: enabled}
}

func (b *Boundary) Status(status string) BoundaryStatus {
	return BoundaryStatus{
		Schema:                 BoundarySchemaV1,
		Status:                 status,
		Mode:                   "boundary-only",
		Authority:              "none",
		Enabled:                b != nil && b.enabled,
		PublicationEnabled:     false,
		DataPlaneDependency:    false,
		DatabaseCapability:     false,
		KubernetesCapability:   false,
		BundleSignerCapability: false,
	}
}

func (b *Boundary) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, b.Status("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, b.Status("ready"))
	})
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, b.Status("ok"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = fmt.Fprintln(w, "# HELP fugue_edge_control_boundary_info Static identity of the non-authoritative Edge control boundary.")
		_, _ = fmt.Fprintln(w, "# TYPE fugue_edge_control_boundary_info gauge")
		_, _ = fmt.Fprintln(w, "fugue_edge_control_boundary_info{authority=\"none\",mode=\"boundary-only\"} 1")
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func Server(bindAddr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              bindAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}
