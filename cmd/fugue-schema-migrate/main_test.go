package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRunIsFixedZeroArgumentAndUsesExactDatabaseURL(t *testing.T) {
	called := 0
	migrate := func(_ context.Context, databaseURL string) error {
		called++
		if databaseURL != "postgres://schema.example/fugue" {
			t.Fatalf("database URL = %q", databaseURL)
		}
		return nil
	}
	getenv := func(key string) string {
		if key != "FUGUE_DATABASE_URL" {
			t.Fatalf("unexpected environment key %q", key)
		}
		return "  postgres://schema.example/fugue  "
	}
	served := 0
	serve := func(_ context.Context, address string) error {
		served++
		if address != healthListenAddress {
			t.Fatalf("health address = %q", address)
		}
		if called != 1 {
			t.Fatal("health became available before migration completed")
		}
		return nil
	}
	if err := run(context.Background(), []string{"fugue-schema-migrate"}, getenv, migrate, serve); err != nil {
		t.Fatalf("run: %v", err)
	}
	if called != 1 || served != 1 {
		t.Fatalf("migration calls = %d, health calls = %d", called, served)
	}
	for _, args := range [][]string{
		{},
		{"fugue-schema-migrate", "up"},
		{"fugue-schema-migrate", "--database-url=caller"},
	} {
		if err := run(context.Background(), args, getenv, migrate, serve); err == nil {
			t.Fatalf("arguments %q were accepted", args)
		}
	}
	if called != 1 || served != 1 {
		t.Fatalf("rejected arguments invoked dependencies; migration=%d health=%d", called, served)
	}
}

func TestRunFailsClosedBeforeMigration(t *testing.T) {
	migrate := func(context.Context, string) error {
		t.Fatal("migration must not run")
		return nil
	}
	serve := func(context.Context, string) error {
		t.Fatal("health must not run")
		return nil
	}
	if err := run(context.Background(), []string{"fugue-schema-migrate"}, func(string) string { return "" }, migrate, serve); err == nil {
		t.Fatal("missing database URL was accepted")
	}
	if err := run(nil, []string{"fugue-schema-migrate"}, func(string) string { return "postgres://example" }, migrate, serve); err == nil {
		t.Fatal("nil context was accepted")
	}
	if err := run(context.Background(), []string{"fugue-schema-migrate"}, nil, migrate, serve); err == nil {
		t.Fatal("nil environment reader was accepted")
	}
	if err := run(context.Background(), []string{"fugue-schema-migrate"}, func(string) string { return "postgres://example" }, nil, serve); err == nil {
		t.Fatal("nil migration runner was accepted")
	}
	if err := run(context.Background(), []string{"fugue-schema-migrate"}, func(string) string { return "postgres://example" }, migrate, nil); err == nil {
		t.Fatal("nil health runner was accepted")
	}
}

func TestRunReturnsMigrationFailureWithoutRetry(t *testing.T) {
	want := errors.New("migration failed")
	calls := 0
	served := 0
	err := run(context.Background(), []string{"fugue-schema-migrate"}, func(string) string { return "postgres://example" }, func(context.Context, string) error {
		calls++
		return want
	}, func(context.Context, string) error {
		served++
		return nil
	})
	if !errors.Is(err, want) || calls != 1 || served != 0 {
		t.Fatalf("run error = %v, migration calls = %d, health calls = %d", err, calls, served)
	}
}

func TestSchemaHealthEndpointIsExactAndBounded(t *testing.T) {
	for _, tc := range []struct {
		method string
		path   string
		status int
		body   string
	}{
		{method: http.MethodGet, path: "/healthz", status: http.StatusOK, body: "ok\n"},
		{method: http.MethodHead, path: "/healthz", status: http.StatusOK},
		{method: http.MethodPost, path: "/healthz", status: http.StatusMethodNotAllowed, body: "method not allowed\n"},
		{method: http.MethodGet, path: "/readyz", status: http.StatusNotFound, body: "404 page not found\n"},
	} {
		request := httptest.NewRequest(tc.method, tc.path, nil)
		response := httptest.NewRecorder()
		schemaHealthHandler().ServeHTTP(response, request)
		if response.Code != tc.status || response.Body.String() != tc.body {
			t.Fatalf("%s %s = status %d body %q", tc.method, tc.path, response.Code, response.Body.String())
		}
	}
	server := newHealthServer(healthListenAddress)
	if server.ReadHeaderTimeout != 2*time.Second || server.ReadTimeout != 3*time.Second ||
		server.WriteTimeout != 3*time.Second || server.IdleTimeout != 15*time.Second ||
		server.MaxHeaderBytes != 8<<10 {
		t.Fatalf("health server limits drifted: %+v", server)
	}
}

func TestSchemaHealthServerRunsUntilBoundedShutdown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- serveHealthOnListener(ctx, listener, listener.Addr().String())
	}()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + listener.Addr().String() + "/healthz")
	if err != nil {
		cancel()
		t.Fatalf("read schema health: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || string(body) != "ok\n" {
		cancel()
		t.Fatalf("schema health response = status %d body %q error %v", response.StatusCode, body, err)
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("schema health shutdown: %v", err)
		}
	case <-time.After(healthShutdownTimeout + time.Second):
		t.Fatal("schema health shutdown exceeded its bound")
	}
}
