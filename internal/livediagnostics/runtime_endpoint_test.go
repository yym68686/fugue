package livediagnostics

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestRuntimeEndpointUsesRootOnlyUnixSocketAndServesProfiles(t *testing.T) {
	temporary, err := os.CreateTemp("/tmp", "fugue-diagnostics-*.sock")
	if err != nil {
		t.Fatal(err)
	}
	socketPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(socketPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(socketPath) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := startRuntimeEndpointAt(ctx, "api", socketPath); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket permissions=%o, want 600", info.Mode().Perm())
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", socketPath)
	}}
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	defer transport.CloseIdleConnections()

	response, err := client.Get("http://runtime/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var status map[string]any
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status["component"] != "api" {
		t.Fatalf("unexpected status: %+v", status)
	}

	profileResponse, err := client.Get("http://runtime/v1/profile/heap")
	if err != nil {
		t.Fatal(err)
	}
	defer profileResponse.Body.Close()
	profile, err := io.ReadAll(io.LimitReader(profileResponse.Body, 4<<20))
	if err != nil {
		t.Fatal(err)
	}
	if profileResponse.StatusCode != http.StatusOK || len(profile) == 0 {
		t.Fatalf("heap profile status=%d bytes=%d", profileResponse.StatusCode, len(profile))
	}

	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socketPath); os.IsNotExist(err) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("runtime socket was not removed after shutdown")
}
