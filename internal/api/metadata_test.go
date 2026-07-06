package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLookupCountryCodeForPublicIPFallsBackAcrossProviders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/primary":
			http.Error(w, "rate limited", http.StatusTooManyRequests)
		case "/secondary":
			_, _ = w.Write([]byte("US\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	oldClient := geoIPHTTPClient
	oldEndpoints := geoIPLookupEndpoints
	defer func() {
		geoIPHTTPClient = oldClient
		geoIPLookupEndpoints = oldEndpoints
	}()

	geoIPHTTPClient = server.Client()
	geoIPLookupEndpoints = []geoIPLookupEndpoint{
		{
			Name:   "primary",
			URL:    func(string) string { return server.URL + "/primary" },
			Decode: decodeIPAPICountryCode,
		},
		{
			Name:   "secondary",
			URL:    func(string) string { return server.URL + "/secondary" },
			Decode: decodePlainCountryCode,
		},
	}

	country, source, err := lookupCountryCodeForPublicIP(context.Background(), "203.0.113.10")
	if err != nil {
		t.Fatalf("lookup country: %v", err)
	}
	if country != "us" || source != "secondary" {
		t.Fatalf("expected secondary fallback to return us, got country=%q source=%q", country, source)
	}
}
