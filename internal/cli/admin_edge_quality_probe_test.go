package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestClientNetworkProbeDoesNotUseAPIOrCredentials(t *testing.T) {
	t.Setenv("FUGUE_CONFIG_DIR", t.TempDir())
	t.Setenv("FUGUE_CLI_UPDATE_CHECK", "on")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { calls.Add(1); writer.WriteHeader(401) }))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", server.URL, "--token", "invalid", "admin", "edge", "quality-probe", "app.example.test", "--target", "edge-a=127.0.0.1", "--vantage", "observer", "--rounds", "1"}, &stdout, &stderr)
	if err == nil || calls.Load() != 0 {
		t.Fatal("client probe used API credentials or probed a private address", err, calls.Load())
	}
}
