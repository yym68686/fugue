package diagnosticprobe

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"fugue/internal/livediagnostics"
	"fugue/internal/runtimeobservation"
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

func TestRuntimeJSONDeduplicatesHelpersAndVerifiesActualProvider(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("requires Unix peer credentials")
	}
	dir, err := os.MkdirTemp("/tmp", "runtime-peers-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	root, proc := filepath.Join(dir, "shared"), filepath.Join(dir, "proc")
	socket := filepath.Join(root, runtimeobservation.SocketPath)
	if err := os.MkdirAll(filepath.Dir(socket), 0700); err != nil {
		t.Fatal(err)
	}
	pid := os.Getpid()
	before := map[int]string{pid: "42", pid + 1: "43", pid + 2: "44"}
	for _, n := range []int{pid, pid + 1} {
		path := filepath.Join(proc, strconv.Itoa(n))
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(root, filepath.Join(path, "root")); err != nil {
			t.Fatal(err)
		}
	}
	writeStat := func(start string) {
		t.Helper()
		fields := strings.Fields("S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 " + start)
		if err := os.WriteFile(filepath.Join(proc, strconv.Itoa(pid), "stat"), []byte(fmt.Sprintf("%d (runtime) %s", pid, strings.Join(fields, " "))), 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeStat("42")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"count":1}`) })}
	go server.Serve(ln)
	defer server.Close()
	c := Collector{Path: "/v1/snapshots/queue", Fields: []string{"count"}}
	v, err := runtimeJSONForProcesses(context.Background(), c, proc, before)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("helpers incorrectly degraded provider: %#v", v)
	}
	rows := result["processes"].([]any)
	if len(rows) != 1 || result["skipped_processes"] != 2 || rows[0].(map[string]any)["pid"] != pid {
		t.Fatalf("provider was not deduplicated and attributed to its actual peer: %#v", result)
	}
	writeStat("999")
	v, _ = runtimeJSONForProcesses(context.Background(), c, proc, before)
	if _, ok := v.(partialValue); !ok {
		t.Fatal("changed provider identity was accepted")
	}
	writeStat("42")
	v, _ = runtimeJSONForProcesses(context.Background(), c, proc, map[int]string{pid + 1: "43"})
	partial, ok := v.(partialValue)
	if !ok || len(partial.Value.(map[string]any)["processes"].([]any)) != 0 {
		t.Fatal("socket peer outside frozen process set was accepted")
	}
	v, _ = runtimeJSONForProcesses(context.Background(), c, proc, map[int]string{pid + 2: "44"})
	if _, ok := v.(partialValue); !ok {
		t.Fatal("missing observation socket was treated as complete")
	}
}
