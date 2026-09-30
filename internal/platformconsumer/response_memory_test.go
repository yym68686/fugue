package platformconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type responseMemoryTransport func() io.Reader

func (f responseMemoryTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(f())}, nil
}

type responseReadFailure struct{}

func (responseReadFailure) Read([]byte) (int, error) { return 0, errors.New("body interrupted") }

func TestPlatformResponseRetainsWholeBodyBoundAndEOFValidation(t *testing.T) {
	const limit = 8 << 20
	for _, tc := range []struct {
		name  string
		body  func() io.Reader
		valid bool
	}{
		{"exact-limit", func() io.Reader {
			return strings.NewReader(`{"ok":true}` + strings.Repeat(" ", limit-len(`{"ok":true}`)))
		}, true},
		{"one-byte-over", func() io.Reader {
			return strings.NewReader(`{"ok":true}` + strings.Repeat(" ", limit+1-len(`{"ok":true}`)))
		}, false},
		{"oversized-value", func() io.Reader { return strings.NewReader(`{"value":"` + strings.Repeat("x", limit) + `"}`) }, false},
		{"trailing-value", func() io.Reader { return strings.NewReader(`{"ok":true} {}`) }, false},
		{"trailing-junk", func() io.Reader { return strings.NewReader(`{"ok":true}!`) }, false},
		{"truncated", func() io.Reader { return strings.NewReader(`{"ok":`) }, false},
		{"read-failure-after-value", func() io.Reader { return io.MultiReader(strings.NewReader(`{"ok":true}`), responseReadFailure{}) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := Client{HTTPClient: &http.Client{Transport: responseMemoryTransport(tc.body)}}
			var out map[string]any
			err := client.json(context.Background(), "http://control.example.test/artifact", "synthetic-token", http.MethodGet, nil, &out)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v: %v", tc.valid, err)
			}
		})
	}
}

func BenchmarkPlatformResponseMemory(b *testing.B) {
	// A neutral artifact-sized response exercises the transport buffers as well
	// as the decoded map. Payload creation is outside the measured work.
	routes := make([]map[string]any, 164)
	for i := range routes {
		routes[i] = map[string]any{"hostname": "app.example.test", "path_prefix": "/", "policy": strings.Repeat("p", 2048)}
	}
	payload, _ := json.Marshal(map[string]any{"routes": routes})
	text := string(payload)
	client := Client{HTTPClient: &http.Client{Transport: responseMemoryTransport(func() io.Reader { return strings.NewReader(text) })}}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for b.Loop() {
		var out map[string]any
		if err := client.json(context.Background(), "http://control.example.test/artifact", "synthetic-token", http.MethodGet, nil, &out); err != nil {
			b.Fatal(err)
		}
	}
}
