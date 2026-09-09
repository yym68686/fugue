package cli

import (
	"context"
	"errors"
	"fmt"
	"fugue/internal/tui"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTUILogStreamResumesAndCancelsAtServer(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/apps/app-a/runtime-logs/stream" || r.Header.Get("Last-Event-ID") != "cursor-before" || r.URL.Query().Get("pod") != "pod-a" {
			t.Errorf("unexpected stream request: %s cursor=%q", r.URL, r.Header.Get("Last-Event-ID"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: log\nid: cursor-after\ndata: {\"line\":\"repeat\"}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &tuiProvider{client: &Client{baseURL: server.URL, token: "synthetic-token", httpClient: server.Client()}}
	done := make(chan error, 1)
	go func() {
		done <- p.Watch(ctx, tui.Target{Kind: "pod", ID: "pod-a", ProjectID: "app-a"}, "cursor-before", func(n tui.Notice) {
			if n.Cursor != "cursor-after" || len(n.Logs) != 1 || n.Logs[0] != "repeat" {
				t.Error("log payload lost")
			}
			cancel()
		})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream ignored cancellation")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("HTTP connection retained after cancellation")
	}
}
