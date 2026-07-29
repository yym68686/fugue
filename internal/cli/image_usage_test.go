package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunProjectImageUsagePrintsMeasurementReasons(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects/image-usage":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"registry_configured":false,"image_store_mode":"distributed","measurement_status":"partial","measurement_reasons":["digest_conflict","missing_blob_size_evidence"],"projects":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"projects":[]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"project", "images", "usage",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run project image usage: %v stderr=%s", err, stderr.String())
	}
	for _, want := range []string{
		"image_store_mode=distributed",
		"measurement_status=partial",
		"measurement_reasons=digest_conflict,missing_blob_size_evidence",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("expected output to contain %q, got %q", want, stdout.String())
		}
	}
}
