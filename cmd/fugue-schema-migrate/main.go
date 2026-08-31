// Command fugue-schema-migrate applies the fixed Fugue platform-state schema
// migration before becoming ready. It then serves a bounded health endpoint
// so a standard Deployment can roll schema revisions while retaining its LKG.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"fugue/internal/schemamigrate"
)

const (
	schemaMigrationTimeout = 6 * time.Minute
	healthListenAddress    = ":8081"
	healthShutdownTimeout  = 5 * time.Second
)

func main() {
	signalContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(signalContext, os.Args, os.Getenv, migrateSchema, serveHealth); err != nil {
		log.Printf("fugue schema migrator failed: %v", err)
		os.Exit(1)
	}
}

type migrationRunner func(context.Context, string) error
type healthRunner func(context.Context, string) error

func run(ctx context.Context, args []string, getenv func(string) string, migrate migrationRunner, serve healthRunner) error {
	if ctx == nil {
		return errors.New("migration context is nil")
	}
	if len(args) != 1 {
		return errors.New("fugue-schema-migrate accepts zero arguments")
	}
	if getenv == nil || migrate == nil || serve == nil {
		return errors.New("migration dependency is nil")
	}
	databaseURL := strings.TrimSpace(getenv("FUGUE_DATABASE_URL"))
	if databaseURL == "" {
		return errors.New("FUGUE_DATABASE_URL is required")
	}
	migrateCtx, cancel := context.WithTimeout(ctx, schemaMigrationTimeout)
	err := migrate(migrateCtx, databaseURL)
	cancel()
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	log.Printf("fugue schema migration complete; health endpoint enabled")
	return serve(ctx, healthListenAddress)
}

func schemaHealthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			response.Header().Set("Allow", "GET, HEAD")
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		response.Header().Set("Cache-Control", "no-store")
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		response.Header().Set("X-Content-Type-Options", "nosniff")
		response.WriteHeader(http.StatusOK)
		if request.Method == http.MethodGet {
			_, _ = response.Write([]byte("ok\n"))
		}
	})
	return mux
}

func newHealthServer(address string) *http.Server {
	return &http.Server{
		Addr:              address,
		Handler:           schemaHealthHandler(),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       3 * time.Second,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       15 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}
}

func serveHealth(ctx context.Context, address string) error {
	if ctx == nil {
		return errors.New("health context is nil")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen for schema health: %w", err)
	}
	return serveHealthOnListener(ctx, listener, address)
}

func serveHealthOnListener(ctx context.Context, listener net.Listener, address string) error {
	if ctx == nil || listener == nil {
		return errors.New("health listener dependency is nil")
	}
	server := newHealthServer(address)
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- server.Serve(listener)
	}()
	select {
	case err := <-serveResult:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve schema health: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), healthShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return fmt.Errorf("shutdown schema health: %w", err)
		}
		err := <-serveResult
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("stop schema health: %w", err)
		}
		return nil
	}
}

func migrateSchema(ctx context.Context, databaseURL string) error {
	if err := schemamigrate.MigratePlatformState(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate platform-state schema: %w", err)
	}
	if err := schemamigrate.MigrateImageCacheManifestGraph(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate image-cache manifest graph schema: %w", err)
	}
	if err := schemamigrate.MigrateEdgeInstanceFencing(ctx, databaseURL); err != nil {
		return fmt.Errorf("migrate edge-instance fencing schema: %w", err)
	}
	return nil
}
