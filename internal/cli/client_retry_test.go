package cli

import (
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
