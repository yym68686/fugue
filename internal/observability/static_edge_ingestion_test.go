package observability

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	o "fugue/internal/staticedgeobserve"
)

// Exercise the optional export against Fugue's actual structured receiver.
// The deployment's authenticated ingress still owns node/tenant authorization.
func TestStaticEdgeSummaryReachesAuthenticatedStructuredReceiver(t *testing.T) {
	pipeline := NewPipeline(Config{Enabled: true, QueueSize: 4, MaxPayloadBytes: 4096, MemoryLimitBytes: 4096}, nil)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture-export-credential" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		pipeline.HandleOTLPHTTP(w, r)
	}))
	defer server.Close()
	root := t.TempDir()
	ca, credential := filepath.Join(root, "ca.pem"), filepath.Join(root, "credential")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(credential, []byte("fixture-export-credential"), 0600); err != nil {
		t.Fatal(err)
	}
	sink, err := o.NewRemoteSink(o.RemoteConfig{URL: server.URL + "/v1/logs", CAFile: ca, TokenFile: credential})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); sink.Run(ctx) }()
	defer func() { cancel(); <-done }()
	first := 12.5
	record := o.Record{RequestID: "request-fixture", ApplicationRequestID: "application-fixture", SpanID: "span-fixture", ParentSpanID: "parent-fixture", NodeID: "edge-fixture", ProcessID: "process-fixture", Build: "build-fixture", ConfigDigest: "sha256:fixture", Hop: "ingress", Protocol: "HTTP/2.0", Finished: true, ElapsedMS: 38, Body: o.Body{Bytes: 4096, ReadBlockMS: 25, MaxReadBlockMS: 20, FirstByteMS: &first}}
	if !sink.Submit(record) {
		t.Fatal("summary not admitted")
	}
	var event Event
	select {
	case queued := <-pipeline.queue:
		if err := json.Unmarshal(queued.payload, &event); err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("receiver did not retain summary: %+v", sink.Status())
	}
	for key, want := range map[string]string{"event_type": "static_edge_observation", "request_id": record.RequestID, "application_request_id": record.ApplicationRequestID, "span_id": record.SpanID, "parent_span_id": record.ParentSpanID, "node_id": record.NodeID, "body_bytes": "4096", "read_block_ms": "25", "first_byte_ms": "12.5", "full_evidence_source": "independent_collector"} {
		if got := event.Attributes[key]; got != want {
			t.Fatalf("%s: got %q want %q", key, got, want)
		}
	}
	for _, forbidden := range []string{"authorization", "Authorization", "body", "headers", "events"} {
		if _, ok := event.Attributes[forbidden]; ok {
			t.Fatalf("summary retained %s", forbidden)
		}
	}
	deadline := time.Now().Add(time.Second)
	for sink.Status()["sent"].(uint64) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if sink.Status()["sent"].(uint64) != 1 {
		t.Fatal("receiver response not acknowledged")
	}
}
