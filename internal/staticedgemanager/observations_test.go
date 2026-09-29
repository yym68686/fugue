package staticedgemanager

import (
	"encoding/json"
	c "fugue/internal/staticedgecontract"
	o "fugue/internal/staticedgeobserve"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func blockedObservationCollector(t *testing.T, f *fixture) (<-chan struct{}, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "fso-blocked-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "query.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}, 4), make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_ = json.NewEncoder(w).Encode(o.Result{Schema: o.Schema, NodeID: "test-edge", Status: "not_observed", Records: []o.Record{}, Findings: []o.Finding{}, DiskScanComplete: true})
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	f.m.cfg.ObservationSocket = socket
	return entered, func() { close(release) }
}

func TestObservationQueryDoesNotHoldServingTransactionLock(t *testing.T) {
	f := setup(t)
	adopt(t, f)
	entered, release := blockedObservationCollector(t, f)
	req := f.request("request-query", "slow-query", nil, "")
	req.ObservationQuery = &o.Query{Since: time.Now().Add(-time.Minute), Until: time.Now(), Limit: 1}
	done := make(chan c.Response, 1)
	defer func() {
		release()
		select {
		case result := <-done:
			if !result.OK {
				t.Errorf("released collector query failed: %+v", result)
			}
		case <-time.After(2 * time.Second):
			t.Error("collector query did not finish")
		}
	}()
	go func() { done <- f.m.Execute(req, "reader", "read") }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("collector was not queried")
	}
	if result := f.call("status", "status-during-query", nil, ""); !result.OK || !result.Result.Ready {
		t.Fatalf("observation wait blocked independent management status: %+v", result)
	}
	bundle := f.bundle(2, "serving")
	if result := f.call("stage", "stage-during-query", &bundle, ""); !result.OK || result.Receipt.Outcome != "staged" {
		t.Fatalf("observation wait blocked signed configuration staging: %+v", result)
	}
	if f.r.writes != 0 {
		t.Fatal("observation/staging changed serving runtime")
	}
}

func TestObservationQueriesHaveIndependentBoundedCapacity(t *testing.T) {
	f := setup(t)
	adopt(t, f)
	entered, release := blockedObservationCollector(t, f)
	done := make(chan c.Response, 2)
	defer func() {
		release()
		for range 2 {
			select {
			case result := <-done:
				if !result.OK {
					t.Errorf("released collector query failed: %+v", result)
				}
			case <-time.After(2 * time.Second):
				t.Error("collector query did not finish")
			}
		}
	}()
	req := f.request("request-query", "bounded-query", nil, "")
	req.ObservationQuery = &o.Query{Since: time.Now().Add(-time.Minute), Until: time.Now(), Limit: 1}
	for range 2 {
		go func() { done <- f.m.Execute(req, "reader", "read") }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("two queries were not independently accepted")
		}
	}
	if result := f.m.Execute(req, "reader", "read"); result.Status != 429 {
		t.Fatalf("unbounded observation requests: %+v", result)
	}
	if result := f.m.Execute(req, "reader", "invalid"); result.Status != 403 {
		t.Fatalf("query capacity bypassed grant validation: %+v", result)
	}
	bad := req
	bad.EdgeID = "other-edge"
	if result := f.m.Execute(bad, "reader", "read"); result.Status != 400 {
		t.Fatalf("query capacity bypassed identity validation: %+v", result)
	}
	if result := f.call("status", "status-with-full-observation-pool", nil, ""); !result.OK || !result.Result.Ready {
		t.Fatalf("observation saturation blocked serving status: %+v", result)
	}
}

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
