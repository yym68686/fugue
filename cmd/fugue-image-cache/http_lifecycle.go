package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// runImageCacheHTTPServer owns the component's network and shutdown boundary.
// It stops accepting requests on cancellation, drains in-flight HTTP work and
// then waits for component-owned background jobs under the same deadline. A
// timeout force-closes the listener and returns an error instead of claiming a
// clean lane-local handoff.
func runImageCacheHTTPServer(
	ctx context.Context,
	server *http.Server,
	listener net.Listener,
	shutdownTimeout time.Duration,
	drain func(context.Context) error,
) error {
	if ctx == nil {
		return errors.New("image cache server context is nil")
	}
	if server == nil {
		return errors.New("image cache HTTP server is nil")
	}
	if listener == nil {
		return errors.New("image cache listener is nil")
	}
	if shutdownTimeout <= 0 {
		return errors.New("image cache shutdown timeout must be positive")
	}

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()

	select {
	case err := <-serveDone:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("HTTP server stopped unexpectedly: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = server.Close()
	}
	drainErr := error(nil)
	if drain != nil {
		drainErr = drain(shutdownCtx)
		if drainErr != nil {
			_ = server.Close()
		}
	}
	serveErr := <-serveDone

	var failures []error
	if shutdownErr != nil {
		failures = append(failures, fmt.Errorf("HTTP shutdown: %w", shutdownErr))
	}
	if drainErr != nil {
		failures = append(failures, fmt.Errorf("background drain: %w", drainErr))
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		failures = append(failures, fmt.Errorf("HTTP server shutdown: %w", serveErr))
	}
	return errors.Join(failures...)
}

func writeManagementJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
