package cli

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type retryRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn retryRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestClientRetriesTransientGetTransportErrors(t *testing.T) {
	t.Parallel()

	client, err := newClientWithOptions("https://api.example.com", "token", clientOptions{
		RequireToken:   true,
		ReadRetryCount: 2,
		ReadRetryDelay: time.Millisecond,
		RequestTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	attempts := 0
	client.httpClient = &http.Client{
		Transport: retryRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				return nil, io.EOF
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1,"updated_at":"2026-04-15T00:00:00Z"},"created_at":"2026-04-15T00:00:00Z","updated_at":"2026-04-15T00:00:00Z"}}`)),
			}, nil
		}),
		Timeout: 5 * time.Second,
	}

	app, err := client.GetApp("app_123")
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected one retry, got %d attempts", attempts)
	}
	if app.ID != "app_123" || app.Name != "demo" {
		t.Fatalf("unexpected app %+v", app)
	}
}

func TestDataObjectRequestsUseDedicatedClientWithoutAPITimeout(t *testing.T) {
	t.Parallel()

	client, err := newClientWithOptions("https://api.example.com", "token", clientOptions{
		RequireToken:   true,
		RequestTimeout: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if client.httpClient == nil || client.httpClient.Timeout != time.Nanosecond {
		t.Fatalf("expected API client timeout to remain configured, got %#v", client.httpClient)
	}
	if client.dataObjectHTTPClient == nil {
		t.Fatal("expected dedicated data object client")
	}
	if client.dataObjectHTTPClient.Timeout != 0 {
		t.Fatalf("data object client must not use a whole-request timeout, got %s", client.dataObjectHTTPClient.Timeout)
	}

	apiAttempts := 0
	objectAttempts := 0
	client.httpClient = &http.Client{
		Transport: retryRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			apiAttempts++
			return nil, fmt.Errorf("API client should not handle data object requests")
		}),
		Timeout: time.Nanosecond,
	}
	client.dataObjectHTTPClient = &http.Client{
		Transport: retryRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			objectAttempts++
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		}),
	}

	req, err := http.NewRequest(http.MethodPut, "https://r2.example.com/bucket/object", strings.NewReader("body"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if err := client.doDataObjectRequest(req); err != nil {
		t.Fatalf("data object request: %v", err)
	}
	if apiAttempts != 0 {
		t.Fatalf("expected API client to be skipped, got %d attempts", apiAttempts)
	}
	if objectAttempts != 1 {
		t.Fatalf("expected one data object attempt, got %d", objectAttempts)
	}
}
