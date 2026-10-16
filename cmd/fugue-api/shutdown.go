package main

import (
	"errors"
	"net/http"
)

// Shutdown closes the listener before it finishes draining active requests.
// Keep main (and therefore its store and workers) alive until the bounded
// shutdown sequence has finished, including the auxiliary HTTP listeners.
func serveUntilShutdownCompletes(serve func() error, shutdownDone <-chan struct{}) error {
	err := serve()
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-shutdownDone
	return nil
}
