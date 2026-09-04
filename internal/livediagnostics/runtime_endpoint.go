package livediagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
	"strings"
	"time"
)

const RuntimeSocketPath = "/tmp/fugue-runtime-diagnostics.sock"

// StartRuntimeEndpoint exposes read-only Go runtime profiles on a root-only
// Unix socket inside the component container. It never listens on the network.
func StartRuntimeEndpoint(ctx context.Context, component string) error {
	return startRuntimeEndpointAt(ctx, component, RuntimeSocketPath)
}

func startRuntimeEndpointAt(ctx context.Context, component, socketPath string) error {
	if ctx == nil {
		return errors.New("diagnostic runtime context is required")
	}
	socketPath = strings.TrimSpace(socketPath)
	if socketPath == "" {
		return errors.New("diagnostic runtime socket path is required")
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(socketPath)
		return err
	}
	startedAt := time.Now().UTC()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"component": strings.TrimSpace(component), "pid": os.Getpid(), "started_at": startedAt,
		})
	})
	for _, name := range []string{"heap", "allocs", "goroutine", "mutex", "block"} {
		profileName := name
		mux.HandleFunc("GET /v1/profile/"+profileName, func(w http.ResponseWriter, _ *http.Request) {
			profile := pprof.Lookup(profileName)
			if profile == nil {
				http.Error(w, "runtime profile is unavailable", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			if err := profile.WriteTo(w, 0); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		})
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 4 << 10}
	go func() {
		<-ctx.Done()
		_ = server.Close()
		_ = listener.Close()
		_ = os.Remove(socketPath)
	}()
	go func() { _ = server.Serve(listener) }()
	return nil
}
