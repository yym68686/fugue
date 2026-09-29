package staticedgemanager

import (
	"encoding/json"
	o "fugue/internal/staticedgeobserve"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadGrantQueriesIndependentCollectorWithoutRuntimeWrites(t *testing.T) {
	f := setup(t)
	dir, e := os.MkdirTemp("/tmp", "fso-manager-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "query.sock")
	ln, e := net.Listen("unix", socket)
	if e != nil {
		t.Fatal(e)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/status" {
			json.NewEncoder(w).Encode(map[string]any{"node_id": "test-edge", "ready": true})
			return
		}
		json.NewEncoder(w).Encode(o.Result{Schema: o.Schema, NodeID: "test-edge", Status: "not_observed", Records: []o.Record{}, Findings: []o.Finding{}, DiskScanComplete: true})
	})}
	go srv.Serve(ln)
	defer srv.Close()
	f.m.cfg.ObservationSocket = socket
	req := f.request("observability-status", "status", nil, "")
	result := f.m.Execute(req, "reader", "read")
	if !result.OK || result.ObservationStatus["ready"] != true {
		t.Fatal(result)
	}
	req = f.request("request-query", "query", nil, "")
	req.ObservationQuery = &o.Query{Since: time.Now().Add(-time.Minute), Until: time.Now(), Limit: 1}
	result = f.m.Execute(req, "reader", "read")
	if !result.OK || result.Observations == nil {
		t.Fatal(result)
	}
	srv.Close()
	result = f.m.Execute(req, "reader", "read")
	if result.Status != 503 {
		t.Fatal(result)
	}
	if f.r.writes != 0 {
		t.Fatal("observation query changed serving runtime")
	}
}
