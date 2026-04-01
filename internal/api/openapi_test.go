package api

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/store"
)

func TestOpenAPIDocumentEndpoints(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	tests := []struct {
		name        string
		path        string
		contentType string
		contains    string
	}{
		{
			name:        "yaml",
			path:        "/openapi.yaml",
			contentType: "application/yaml; charset=utf-8",
			contains:    "openapi: 3.1.0",
		},
		{
			name:        "json",
			path:        "/openapi.json",
			contentType: "application/json; charset=utf-8",
			contains:    "\"openapi\": \"3.1.0\"",
		},
		{
			name:        "docs",
			path:        "/docs",
			contentType: "text/html; charset=utf-8",
			contains:    "/openapi.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			recorder := httptest.NewRecorder()

			server.Handler().ServeHTTP(recorder, req)

			if recorder.Code != http.StatusOK {
				t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
			}
			if got := recorder.Header().Get("Content-Type"); got != tt.contentType {
				t.Fatalf("expected content-type %q, got %q", tt.contentType, got)
			}
			if !strings.Contains(recorder.Body.String(), tt.contains) {
				t.Fatalf("expected response body to contain %q, got %s", tt.contains, recorder.Body.String())
			}
		})
	}
}
