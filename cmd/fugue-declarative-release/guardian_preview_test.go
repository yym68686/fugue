package main

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

type previewRoundTripper func(*http.Request) (*http.Response, error)

func (f previewRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestGuardianPreviewTransportAllowsOnlyPlainGET(t *testing.T) {
	called := 0
	transport := guardianPreviewTransport{next: previewRoundTripper(func(*http.Request) (*http.Response, error) {
		called++
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	get, _ := http.NewRequest(http.MethodGet, "https://kubernetes.example/api", nil)
	if _, err := transport.RoundTrip(get); err != nil || called != 1 {
		t.Fatalf("read-only GET err=%v calls=%d", err, called)
	}
	for _, request := range []*http.Request{
		{Method: http.MethodPost},
		{Method: http.MethodGet, Header: http.Header{"Upgrade": []string{"websocket"}}},
	} {
		if _, err := transport.RoundTrip(request); err == nil {
			t.Errorf("accepted mutating or upgraded request: %+v", request)
		}
	}
	if called != 1 {
		t.Fatalf("denied requests reached Kubernetes transport: calls=%d", called)
	}
}
