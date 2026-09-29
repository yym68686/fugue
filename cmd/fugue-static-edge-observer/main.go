// The observer is an optional local evidence service. Its lifecycle never
// controls the manager, Caddy, DNS, or the origin.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"time"

	o "fugue/internal/staticedgeobserve"
)

type config struct {
	Socket string          `json:"socket"`
	Store  o.StoreConfig   `json:"store"`
	Remote *o.RemoteConfig `json:"remote,omitempty"`
}

func main() {
	if e := run(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(args []string) error {
	fs := flag.NewFlagSet("fugue-static-edge-observer", flag.ContinueOnError)
	file := fs.String("config", "", "private local collector configuration")
	if e := fs.Parse(args); e != nil {
		return e
	}
	if *file == "" || fs.NArg() != 0 {
		return errors.New("--config required")
	}
	f, e := os.Open(*file)
	if e != nil {
		return e
	}
	defer f.Close()
	var cfg config
	d := json.NewDecoder(io.LimitReader(f, 64<<10))
	d.DisallowUnknownFields()
	if d.Decode(&cfg) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid observer config")
	}
	if !filepath.IsAbs(cfg.Socket) {
		return errors.New("absolute socket required")
	}
	debug.SetMemoryLimit(128 << 20)
	store, e := o.NewStore(cfg.Store)
	if e != nil {
		return e
	}
	defer store.Close()
	if cfg.Remote != nil {
		store.Remote, e = o.NewRemoteSink(*cfg.Remote)
		if e != nil {
			return e
		}
	}
	if e = os.MkdirAll(filepath.Dir(cfg.Socket), 0700); e != nil {
		return e
	}
	// Never unlink a live collector. A stale socket can be removed only after
	// an explicit connection-refused result; ambiguous errors fail closed.
	if info, e := os.Lstat(cfg.Socket); e == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return errors.New("observer socket path is occupied")
		}
		conn, err := net.DialTimeout("unix", cfg.Socket, time.Second)
		if err == nil {
			conn.Close()
			return errors.New("collector already running")
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			return errors.New("collector socket state uncertain")
		}
		if e = os.Remove(cfg.Socket); e != nil {
			return e
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	ln, e := net.Listen("unix", cfg.Socket)
	if e != nil {
		return e
	}
	defer ln.Close()
	if e = os.Chmod(cfg.Socket, 0600); e != nil {
		return e
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	if store.Remote != nil {
		go store.Remote.Run(workerCtx)
	}
	go func() { store.Run(workerCtx); close(done) }()
	server := &http.Server{Handler: store.Handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 4096}
	errs := make(chan error, 1)
	go func() { errs <- server.Serve(ln) }()
	select {
	case <-ctx.Done():
	case e = <-errs:
	}
	shutdown, closeShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeShutdown()
	server.Shutdown(shutdown)
	cancel()
	select {
	case <-done:
	case <-shutdown.Done():
		return errors.New("collector stop exceeded bounded drain")
	}
	if errors.Is(e, http.ErrServerClosed) {
		return nil
	}
	return e
}
