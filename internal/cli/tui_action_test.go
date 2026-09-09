package cli

import (
	"context"
	"fmt"
	"fugue/internal/tui"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTUIUnknownSubmissionRecoversReadOnly(t *testing.T) {
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/apps/app-a/runtime-state":
			fmt.Fprintf(w, `{"schema_version":1,"app_id":"app-a","desired_spec_hash":%q}`, strings.Repeat("a", 64))
		case "/v1/apps/app-a/restart":
			writes.Add(1)
			if r.Header.Get("Idempotency-Key") != "plan-a" || r.Header.Get("If-Match") != `"`+strings.Repeat("a", 64)+`"` {
				t.Error("missing atomic request headers")
			}
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
		case "/v1/apps/app-a/action-requests/plan-a":
			if r.Method != http.MethodGet {
				t.Error("receipt lookup repeated a write")
			}
			fmt.Fprint(w, `{"operation":{"id":"op-a","type":"deploy","status":"running"}}`)
		default:
			t.Errorf("unexpected endpoint %s", r.URL)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	plan := tui.Plan{ID: "plan-a", Request: tui.ActionRequest{Target: tui.Target{Kind: "app", ID: "app-a", Name: "Sample"}, Action: "restart"}, Precondition: strings.Repeat("a", 64), ExpiresAt: time.Now().Add(time.Minute)}
	p := &tuiProvider{client: &Client{baseURL: server.URL, httpClient: server.Client()}, plans: map[string]tui.Plan{plan.ID: plan}}
	journal := t.TempDir()
	if err := p.loadReceipts(journal); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Execute(context.Background(), plan); err == nil {
		t.Fatal("disconnected response treated as success")
	}
	if _, err := p.Plan(context.Background(), plan.Request); err == nil {
		t.Fatal("new mutation allowed while previous result unknown")
	}
	reopened := &tuiProvider{client: p.client}
	if err := reopened.loadReceipts(journal); err != nil {
		t.Fatal(err)
	}
	p = reopened
	recovery := plan.Request
	recovery.Action = "recover"
	saved, err := p.Plan(context.Background(), recovery)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := p.Execute(context.Background(), saved)
	if err != nil || receipt.Operation.ID != "op-a" || writes.Load() != 1 {
		t.Fatalf("recovery = %+v err=%v writes=%d", receipt, err, writes.Load())
	}
	if len(p.uncertain) != 0 {
		t.Fatal("resolved receipt remained blocked")
	}
}
func TestTUIOldServerCannotEnableSafeActions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"paths":{}}`) }))
	defer server.Close()
	p := &tuiProvider{client: &Client{baseURL: server.URL, httpClient: server.Client()}}
	if p.supportsActionReceipts(p.client) {
		t.Fatal("old server passed capability negotiation")
	}
}
