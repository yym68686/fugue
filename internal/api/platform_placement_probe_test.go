package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/routeproof"
)

func TestParsePlacementRouteProofBindsNonceIdentityExpiryAndNoStore(t *testing.T) {
	now := time.Now().UTC()
	nonce := "0123456789abcdef0123456789abcdef"
	request := httptest.NewRequest(http.MethodHead, "https://app.example.test/api", nil)
	request.Header.Set(routeproof.NonceHeader, nonce)
	response := httptest.NewRecorder()
	response.Header().Set(routeproof.DigestHeader, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	response.Header().Set(routeproof.NonceHeader, nonce)
	response.Header().Set(routeproof.VersionHeader, "bundle-1")
	response.Header().Set(routeproof.ExpiryHeader, now.Add(time.Minute).Format(time.RFC3339Nano))
	response.Header().Set(routeproof.EdgeHeader, "edge-a")
	response.Header().Set(routeproof.GroupHeader, "group-a")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(http.StatusNoContent)
	proof, err := parsePlacementRouteProof(&http.Response{StatusCode: response.Code, Header: response.Header()}, nonce, now)
	if err != nil || proof.Digest == "" || proof.EdgeID != "edge-a" || proof.GroupID != "group-a" || !proof.ValidUntil.After(now) {
		t.Fatalf("proof=%+v err=%v", proof, err)
	}
	for name, mutate := range map[string]func(*http.Response){
		"status":    func(r *http.Response) { r.StatusCode = 200 },
		"nonce":     func(r *http.Response) { r.Header.Set(routeproof.NonceHeader, "ffffffffffffffffffffffffffffffff") },
		"duplicate": func(r *http.Response) { r.Header.Add(routeproof.DigestHeader, r.Header.Get(routeproof.DigestHeader)) },
		"expired": func(r *http.Response) {
			r.Header.Set(routeproof.ExpiryHeader, now.Add(-time.Second).Format(time.RFC3339Nano))
		},
		"cache":         func(r *http.Response) { r.Header.Set("Cache-Control", "max-age=30") },
		"unknown state": func(r *http.Response) { r.Header.Set(routeproof.StateHeader, "any") },
		"ambiguous state": func(r *http.Response) {
			r.Header.Add(routeproof.StateHeader, "disabled")
			r.Header.Add(routeproof.StateHeader, "unavailable")
		},
		"empty state": func(r *http.Response) { r.Header.Set(routeproof.StateHeader, "") },
	} {
		t.Run(name, func(t *testing.T) {
			r := &http.Response{StatusCode: response.Code, Header: response.Header().Clone()}
			mutate(r)
			if _, err := parsePlacementRouteProof(r, nonce, now); err == nil {
				t.Fatal("invalid proof accepted")
			}
		})
	}
}

func TestProbePlacementRouteRejectsPrivateAddressAndMalformedHost(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "10.0.0.1", "not-an-ip"} {
		if _, err := probePlacementRoute(nil, "app.example.test", "/", address); err == nil {
			t.Fatalf("accepted %s", address)
		}
	}
	for _, host := range []string{"", "app.example.test/path", "app:example.test"} {
		if _, err := probePlacementRoute(nil, host, "/", "93.184.216.34"); err == nil {
			t.Fatalf("accepted %q", host)
		}
	}
}
