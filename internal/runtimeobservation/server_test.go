package runtimeobservation

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPrivateObservationProvidersRejectMutationAndCleanUp(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "obs-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "obs.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	providers := map[string]Provider{"queue": func(context.Context) (any, error) { return map[string]int{"depth": 3}, nil }}
	if err := startAt(ctx, "worker", path, providers); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("bad socket permissions %v %v", info, err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	for _, row := range []struct {
		method, path string
		want         int
	}{{"GET", "/v1/status", 200}, {"GET", "/v1/snapshots/queue", 200}, {"POST", "/v1/snapshots/queue", 405}, {"GET", "/v1/snapshots/unknown", 404}, {"GET", "/v1/snapshots/queue?mutate=1", 400}} {
		req, _ := http.NewRequest(row.method, "http://runtime"+row.path, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != row.want {
			t.Fatalf("%s: %d", row.path, resp.StatusCode)
		}
	}
	if err := startAt(ctx, "other", path, providers); err == nil {
		t.Fatal("replaced an active socket")
	}
	cancel()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("socket remained after shutdown")
}
