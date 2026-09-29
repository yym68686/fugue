package staticedgeobserve

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestAsyncSinkDrainFlushesQueuedTerminals(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "fugue-sink-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	path := filepath.Join(root, "sink.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	got := make(map[string]bool)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var v Record
		if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
			http.Error(w, "invalid", 400)
			return
		}
		mu.Lock()
		got[v.RequestID] = v.Finished
		mu.Unlock()
		w.WriteHeader(202)
	})}
	go srv.Serve(ln)
	defer srv.Close()
	sink, err := NewAsyncSink(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 12; i++ {
		if !sink.Submit(queryFixture(t, i, time.Now(), 0)) {
			t.Fatal("queue unexpectedly full")
		}
	}
	go sink.Run(ctx)
	drain, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err = sink.Drain(drain); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 12 {
		t.Fatal("queued terminals lost", len(got))
	}
	for _, finished := range got {
		if !finished {
			t.Fatal("terminal changed")
		}
	}
	if sink.Submit(queryFixture(t, 13, time.Now(), 0)) {
		t.Fatal("closed sink accepted more work")
	}
}

func TestAsyncSinkDrainDeadlineDoesNotRequireCollectorResponse(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "fugue-sink-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	path := filepath.Join(root, "sink.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})}
	go srv.Serve(ln)
	defer srv.Close()
	defer close(release)
	sink, err := NewAsyncSink(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sink.Run(ctx)
	sink.Submit(queryFixture(t, 1, time.Now(), 0))
	<-entered
	drain, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	if sink.Drain(drain) == nil {
		t.Fatal("blocked sink falsely reported drained")
	}
	cancel()
	select {
	case <-sink.done:
	case <-time.After(time.Second):
		t.Fatal("canceled telemetry worker did not exit")
	}
}
