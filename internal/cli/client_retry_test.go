package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
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

func TestSourceUploadRequestsUseDedicatedTimeout(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := newClientWithOptions(server.URL, "token", clientOptions{
		RequireToken:   true,
		RequestTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	if _, err := client.InspectUploadTemplate(importUploadRequest{Name: "demo"}, "demo.tgz", []byte("archive")); err != nil {
		t.Fatalf("inspect upload: %v", err)
	}
	if _, err := client.ImportUpload(importUploadRequest{Name: "demo"}, "demo.tgz", []byte("archive")); err != nil {
		t.Fatalf("import upload: %v", err)
	}
	if client.httpClient.Timeout != 10*time.Millisecond {
		t.Fatalf("source upload must not mutate the base client timeout, got %s", client.httpClient.Timeout)
	}
}

func TestSourceUploadChunkUsesConfigurableTimeout(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name           string
		requestTimeout time.Duration
		chunkTimeout   time.Duration
		wantTimeout    bool
	}{
		{name: "default", requestTimeout: 10 * time.Millisecond},
		{name: "longer override", requestTimeout: 10 * time.Millisecond, chunkTimeout: time.Second},
		{name: "shorter override", requestTimeout: time.Second, chunkTimeout: 10 * time.Millisecond, wantTimeout: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			archive := []byte("slow chunk")
			digest := fmt.Sprintf("%x", sha256.Sum256(archive))
			session := model.SourceUploadSession{
				ID:        "upload_123",
				SHA256:    digest,
				SizeBytes: int64(len(archive)),
				ChunkSize: 4 << 20,
			}
			var chunks atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/chunks/0") {
					chunks.Add(1)
					time.Sleep(50 * time.Millisecond)
				}
				if r.Context().Err() != nil {
					return
				}
				out := session
				out.Chunks = map[int]string{0: digest}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(uploadSessionResponse{Session: out}); err != nil {
					t.Errorf("encode response: %v", err)
				}
			}))
			defer server.Close()

			client, err := newClientWithOptions(server.URL, "token", clientOptions{
				RequireToken:             true,
				RequestTimeout:           tt.requestTimeout,
				SourceUploadChunkTimeout: tt.chunkTimeout,
			})
			if err != nil {
				t.Fatalf("new client: %v", err)
			}
			_, err = client.resumeUploadSession(session, archive)
			if tt.wantTimeout {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected chunk deadline, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("resume upload session: %v", err)
			}
			if chunks.Load() != 1 {
				t.Fatalf("expected one chunk request without retries, got %d", chunks.Load())
			}
			if client.httpClient.Timeout != tt.requestTimeout {
				t.Fatalf("source upload changed base timeout from %s to %s", tt.requestTimeout, client.httpClient.Timeout)
			}
		})
	}
}
