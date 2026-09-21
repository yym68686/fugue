// Package runtimeobservation exposes bounded, read-only component observations
// to local administrator diagnostics without a network listener.
package runtimeobservation

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"time"
)

const SocketPath = "/tmp/fugue-observation.sock"

type Provider func(context.Context) (any, error)

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

func Start(ctx context.Context, component string, providers map[string]Provider) error {
	return startAt(ctx, component, SocketPath, providers)
}

func startAt(ctx context.Context, component, path string, providers map[string]Provider) error {
	if ctx == nil || len(providers) == 0 || len(providers) > 32 {
		return errors.New("observation server requires a context and 1-32 providers")
	}
	names := []string{}
	mux := http.NewServeMux()
	for name, provider := range providers {
		if !namePattern.MatchString(name) || provider == nil {
			return errors.New("invalid observation provider")
		}
		names = append(names, name)
		mux.HandleFunc("GET /v1/snapshots/"+name, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.RawQuery != "" {
				http.Error(w, "query parameters are not supported", http.StatusBadRequest)
				return
			}
			value, err := provider(r.Context())
			if err != nil {
				http.Error(w, "observation is unavailable", http.StatusServiceUnavailable)
				return
			}
			data, err := json.Marshal(value)
			if err != nil || len(data) > 1<<20 {
				http.Error(w, "observation exceeded its response budget", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(data)
		})
	}
	sort.Strings(names)
	started := time.Now().UTC()
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"protocol": "fugue.runtime-observation/v1", "component": component, "pid": os.Getpid(), "started_at": started, "providers": names})
	})
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return errors.New("observation socket path is occupied by a non-socket")
		}
		if conn, err := net.DialTimeout("unix", path, time.Second); err == nil {
			conn.Close()
			return errors.New("observation socket is already active")
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	if err := os.Chmod(path, 0600); err != nil {
		listener.Close()
		os.Remove(path)
		return err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 4 << 10}
	go func() { <-ctx.Done(); server.Close(); listener.Close(); os.Remove(path) }()
	go func() { _ = server.Serve(listener) }()
	return nil
}
