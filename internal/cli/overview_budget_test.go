package cli

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestOverviewUsesBoundedParallelReadsAndOneDeadline(t *testing.T) {
	t.Setenv("FUGUE_CONTEXT", "none")
	var active, maximum atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/apps" {
			fmt.Fprint(w, `{"apps":[{"id":"app_test","name":"demo"}]}`)
			return
		}
		if r.URL.Path == "/v1/apps/app_test" {
			fmt.Fprint(w, `{"app":{"id":"app_test","name":"demo"}}`)
			return
		}
		current := active.Add(1)
		defer active.Add(-1)
		for previous := maximum.Load(); current > previous; previous = maximum.Load() {
			if maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(time.Second):
			fmt.Fprint(w, `{}`)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "test")
	if err != nil {
		t.Fatal(err)
	}
	c := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	c.root.BaseURL = server.URL
	start := time.Now()
	result, err := c.loadAppOverviewWithin(client, "demo", 80*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("deadline ignored: %v", elapsed)
	}
	if maximum.Load() < 2 || maximum.Load() > 4 {
		t.Fatalf("unbounded or serial fan-out: %d", maximum.Load())
	}
	if result.Completeness != "partial" || len(result.MissingEvidence) < 4 {
		t.Fatalf("lost source failures: %+v", result.Sources)
	}
}
