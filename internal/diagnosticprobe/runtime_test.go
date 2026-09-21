package diagnosticprobe

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/livediagnostics"
)

func TestRuntimeJSONReadsOnlySelectedMetadataOnUnixSocket(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "runtime-reader-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "obs.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/snapshots/queue" || r.Header.Get("Authorization") != "" {
			t.Error("unexpected runtime request")
		}
		fmt.Fprint(w, `{"count":9007199254740993,"secret":"private","unselected":"private"}`)
	})}
	go server.Serve(ln)
	defer server.Close()
	v, err := runtimeJSONAt(context.Background(), socket, Collector{Path: "/v1/snapshots/queue", Fields: []string{"count", "secret"}})
	if err != nil || fmt.Sprint(v["count"]) != "9007199254740993" || v["secret"] != "[REDACTED]" || v["unselected"] != nil {
		t.Fatalf("incorrect runtime projection: %v %v", v, err)
	}
	for _, path := range []string{"/v1/snapshots/../mutate", "/v1/snapshots/queue?token=x", "/admin"} {
		if _, err := runtimeJSON(context.Background(), livediagnostics.ProbeRequest{}, Collector{Path: path, Fields: []string{"count"}}); err == nil {
			t.Fatalf("unsafe runtime path %s", path)
		}
	}
}
