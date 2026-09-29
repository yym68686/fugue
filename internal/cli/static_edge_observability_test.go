package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	c "fugue/internal/staticedgecontract"
	o "fugue/internal/staticedgeobserve"
)

func TestStaticEdgeObservationTransportOverrideIncludesPeers(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX SSH sentinel")
	}
	cfg, creds := tlsFixture(t)
	var calls atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, err := creds.Authorize(r); err != nil {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		var req c.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(c.Response{Schema: c.RPCSchema, EdgeID: req.EdgeID, RequestID: req.RequestID, OK: true, Status: 200,
			Observations: &o.Result{Schema: o.Schema, NodeID: req.EdgeID, Status: "not_observed", Records: []o.Record{}, Findings: []o.Finding{}, DiskScanComplete: true}})
	}))
	server.TLS = creds.Config()
	server.StartTLS()
	defer server.Close()
	cfg.ManagerURL = server.URL
	cfg.Transport, cfg.SSHHost, cfg.ManagerCommand = "ssh", "entry-alias", "/opt/manager"
	peer := cfg
	peer.Name, peer.EdgeID, peer.SSHHost = "peer-context", "peer-edge", "peer-alias"
	dir := t.TempDir()
	marker := filepath.Join(dir, "unexpected-ssh")
	if err := os.WriteFile(filepath.Join(dir, "ssh"), []byte("#!/bin/sh\nprintf called > \"$OBS_SSH_SENTINEL\"\nexit 73\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OBS_SSH_SENTINEL", marker)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FUGUE_STATIC_EDGE_CONTEXT_FILE", filepath.Join(dir, "contexts.json"))
	if err := saveStaticEdgeContexts(staticEdgeContextFile{SchemaVersion: 1, Contexts: []staticEdgeContext{cfg, peer}}); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	err := runWithStreams([]string{"--json", "static-edge", "requests", "explain", cfg.Name, "--request-id", "fixture-request", "--peer", peer.Name, "--transport", "mtls"}, &out, &stderr)
	if err != nil {
		t.Fatalf("explicit mTLS must apply to both independent contexts: %v; %s", err, stderr.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("expected two mTLS requests, got %d", calls.Load())
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("peer silently used SSH despite explicit mTLS")
	}
	var result struct {
		Results []o.Result `json:"results"`
	}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || len(result.Results) != 2 {
		t.Fatalf("missing peer evidence: %v, %s", err, out.String())
	}
	saved, err := loadStaticEdgeContext(peer.Name)
	if err != nil || saved.Transport != "ssh" {
		t.Fatal("one-command transport override persisted into context", err)
	}
}

func TestStaticEdgeExportReceiptCountsAllSavedPeerRecords(t *testing.T) {
	cfg, creds := tlsFixture(t)
	peer := cfg
	peer.Name, peer.EdgeID = "peer-context", "peer-edge"
	makeRecord := func(node, span string) o.Record {
		observer := o.New(o.Record{NodeID: node, ProcessID: "process-fixture", RequestID: "request-fixture", Hop: "ingress", Protocol: "HTTP/2.0", Build: "fixture", ConfigDigest: "fixture", Correlation: "entry"}, nil)
		observer.Finish(200, false, "")
		r, _ := observer.Snapshot()
		r.SpanID = span
		return r
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, err := creds.Authorize(r); err != nil {
			http.Error(w, "denied", 403)
			return
		}
		var request c.Request
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		records := []o.Record{makeRecord(request.EdgeID, request.EdgeID+"-first")}
		if request.EdgeID == cfg.EdgeID {
			records = append(records, makeRecord(request.EdgeID, request.EdgeID+"-second"))
		}
		json.NewEncoder(w).Encode(c.Response{Schema: c.RPCSchema, EdgeID: request.EdgeID, RequestID: request.RequestID, OK: true, Status: 200, Observations: &o.Result{Schema: o.Schema, NodeID: request.EdgeID, Records: records, Findings: []o.Finding{}, Status: "available", DiskScanComplete: true}})
	}))
	server.TLS = creds.Config()
	server.StartTLS()
	defer server.Close()
	cfg.ManagerURL, peer.ManagerURL = server.URL, server.URL
	dir := t.TempDir()
	t.Setenv("FUGUE_STATIC_EDGE_CONTEXT_FILE", filepath.Join(dir, "contexts.json"))
	if err := saveStaticEdgeContexts(staticEdgeContextFile{SchemaVersion: 1, Contexts: []staticEdgeContext{cfg, peer}}); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "evidence.json")
	var stdout, stderr bytes.Buffer
	if err := runWithStreams([]string{"--json", "static-edge", "requests", "export", cfg.Name, "--request-id", "request-fixture", "--peer", peer.Name, "--transport", "mtls", "--file", file}, &stdout, &stderr); err != nil {
		t.Fatal(err, stderr.String())
	}
	var receipt struct {
		Records int `json:"records"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var saved struct {
		Results []o.Result `json:"results"`
	}
	if err = json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, result := range saved.Results {
		count += len(result.Records)
	}
	if len(saved.Results) != 2 || count != 3 || receipt.Records != count {
		t.Fatalf("export receipt differs from saved peer evidence: nodes=%d saved=%d receipt=%d", len(saved.Results), count, receipt.Records)
	}
}
