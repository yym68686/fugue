// Package edgecontrol defines the independently deployable, group-scoped Edge
// control-plane boundary.
package edgecontrol

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const BoundarySchemaV1 = "edge-control-boundary/v1"

// BoundaryStatus explicitly distinguishes the inert and shadow modes from the
// group-authority runtime.
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

// Boundary serves the process identity view; concrete runtime handlers own
// stores, signers, inventory, publication, and recovery endpoints.
type Boundary struct {
	enabled   bool
	mode      string
	authority bool
}

func NewBoundary(enabled bool) *Boundary {
	return &Boundary{enabled: enabled, mode: "boundary-only"}
}

func NewShadowBoundary(enabled bool) *Boundary {
	return &Boundary{enabled: enabled, mode: "shadow-only"}
}

func NewAuthorityBoundary(enabled bool) *Boundary {
	return &Boundary{enabled: enabled, mode: "group-authority", authority: true}
}

func (b *Boundary) Status(status string) BoundaryStatus {
	mode := "boundary-only"
	if b != nil && (b.mode == "shadow-only" || b.mode == "group-authority") {
		mode = b.mode
	}
	authority := "none"
	publication := false
	signer := false
	if b != nil && b.authority {
		authority = "edge-control"
		publication = true
		signer = true
	}
	return BoundaryStatus{
		Schema:                 BoundarySchemaV1,
		Status:                 status,
		Mode:                   mode,
		Authority:              authority,
		Enabled:                b != nil && b.enabled,
		PublicationEnabled:     publication,
		DataPlaneDependency:    false,
		DatabaseCapability:     false,
		KubernetesCapability:   false,
		BundleSignerCapability: signer,
	}
}

func (b *Boundary) Handler() http.Handler {
	boundaryStatus := b.Status("ok")
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
		_, _ = fmt.Fprintln(w, "# HELP fugue_edge_control_boundary_info Static identity of the Edge control boundary.")
		_, _ = fmt.Fprintln(w, "# TYPE fugue_edge_control_boundary_info gauge")
		_, _ = fmt.Fprintf(w, "fugue_edge_control_boundary_info{authority=%q,mode=%q} 1\n", boundaryStatus.Authority, boundaryStatus.Mode)
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
