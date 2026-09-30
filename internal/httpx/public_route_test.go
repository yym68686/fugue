package httpx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPublicRouteProbeDoesNotFollowRedirectOrSendCredentials(t *testing.T) {
	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.Method != "HEAD" || r.URL.Path != "/app/" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Location", "/private")
		w.WriteHeader(302)
	}))
	defer server.Close()
	got := ObservePublicRoute(context.Background(), server.URL+"/app/", server.Client())
	if !got.Reachable || hits != 1 {
		t.Fatalf("redirect was followed or rejected: %+v hits=%d", got, hits)
	}
}
func TestPublicRouteAvailabilityDistinguishesAuthFromMissingAndFailedRoutes(t *testing.T) {
	for _, code := range []int{200, 204, 301, 401, 403, 404, 405, 500, 502, 503} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) }))
		got := ObservePublicRoute(context.Background(), server.URL, server.Client())
		server.Close()
		want := code < 500 && code != 404
		if got.Reachable != want || got.StatusCode != code {
			t.Errorf("code=%d got=%+v", code, got)
		}
	}
	for _, target := range []string{"file:///etc/passwd", "https://user:secret@example.test/", "https://example.test/?token=secret"} {
		if got := ObservePublicRoute(context.Background(), target, nil); got.Reachable || got.Reason != "invalid public route URL" {
			t.Fatal(got)
		}
	}
}
func TestPublicRouteDefaultTransportRejectsPrivateTargets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("private target was contacted") }))
	defer server.Close()
	if got := ObservePublicRoute(context.Background(), server.URL, nil); got.Reachable || got.StatusCode != 0 {
		t.Fatal(got)
	}
}
