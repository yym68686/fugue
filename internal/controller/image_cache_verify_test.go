package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

const (
	imageCacheVerifyTestRef       = "registry.fugue.internal:5000/fugue-apps/demo:image-abc123"
	imageCacheVerifyTestDigest    = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	imageCacheVerifyTestBlob      = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	imageCacheVerifyTestAuthToken = "management-secret"
)

func TestDestinationImageCacheVerifierUsesAuthenticatedLocalGraphEndpoint(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/fugue/cache/v1/verify" || r.URL.RawQuery != "" {
			t.Fatalf("verification request = %s %s", r.Method, r.URL.String())
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+imageCacheVerifyTestAuthToken {
			t.Fatalf("authorization = %q", got)
		}
		var request map[string]string
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(request) != 1 || request["image_ref"] != imageCacheVerifyTestRef {
			t.Fatalf("verification request body = %+v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"repo":"fugue-apps/demo","target":"image-abc123","available":true,"canonical_digest":"` + imageCacheVerifyTestDigest + `","referenced_blobs":["` + imageCacheVerifyTestBlob + `"],"referenced_manifests":[],"referenced_blob_bytes":128,"error":""}`))
	}))
	t.Cleanup(server.Close)

	result, err := newDestinationImageCacheVerifier(imageCacheVerifyTestAuthToken)(context.Background(), server.URL, imageCacheVerifyTestRef)
	if err != nil {
		t.Fatalf("verify destination cache: %v", err)
	}
	if !result.Available || result.CanonicalDigest != imageCacheVerifyTestDigest || len(result.ReferencedBlobs) != 1 {
		t.Fatalf("verification result = %+v", result)
	}
}

func TestDestinationImageCacheVerifierRejectsLegacyManifestOnlyResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"repo":"fugue-apps/demo","target":"image-abc123","available":true}`))
	}))
	t.Cleanup(server.Close)

	_, err := newDestinationImageCacheVerifier(imageCacheVerifyTestAuthToken)(context.Background(), server.URL, imageCacheVerifyTestRef)
	if err == nil || !strings.Contains(err.Error(), "no canonical digest") {
		t.Fatalf("legacy manifest-only response error = %v", err)
	}
}

func TestDestinationImageCacheVerifierRejectsMissingManagementCredential(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hits.Add(1)
	}))
	t.Cleanup(server.Close)
	for _, token := range []string{"", "   "} {
		_, err := newDestinationImageCacheVerifier(token)(context.Background(), server.URL, imageCacheVerifyTestRef)
		if err == nil || !strings.Contains(err.Error(), "management token is unavailable") {
			t.Fatalf("missing management credential error = %v", err)
		}
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("missing management credential made %d HTTP request(s)", got)
	}
}

func TestImageCacheManagementTokenFromEnvFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  string
	}{
		{name: "unset equivalent", value: "", want: ""},
		{name: "blank", value: "   ", want: ""},
		{name: "configured", value: " image-cache-management-secret ", want: "image-cache-management-secret"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("FUGUE_BOOTSTRAP_ADMIN_KEY", test.value)
			if got := imageCacheManagementTokenFromEnv(); got != test.want {
				t.Fatalf("image-cache management token = %q, want %q", got, test.want)
			}
		})
	}
}

func TestDestinationImageCacheVerifierRejectsAbsoluteCrossHostRedirect(t *testing.T) {
	t.Parallel()
	testDestinationImageCacheVerifierRejectsCrossHostRedirect(t, false)
}

func TestDestinationImageCacheVerifierRejectsSchemeRelativeCrossHostRedirect(t *testing.T) {
	t.Parallel()
	testDestinationImageCacheVerifierRejectsCrossHostRedirect(t, true)
}

func testDestinationImageCacheVerifierRejectsCrossHostRedirect(t *testing.T, schemeRelative bool) {
	t.Helper()
	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	location := upstream.URL + "/fugue/cache/v1/verify"
	if schemeRelative {
		parsed, err := url.Parse(location)
		if err != nil {
			t.Fatalf("parse upstream URL: %v", err)
		}
		location = "//" + parsed.Host + parsed.Path
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, location, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirect.Close)

	_, err := newDestinationImageCacheVerifier(imageCacheVerifyTestAuthToken)(context.Background(), redirect.URL, imageCacheVerifyTestRef)
	if err == nil || !strings.Contains(err.Error(), "redirect rejected") {
		t.Fatalf("redirect verification error = %v", err)
	}
	if got := upstreamHits.Load(); got != 0 {
		t.Fatalf("cross-host redirect reached upstream %d times", got)
	}
}

func TestDestinationImageCacheVerifierRejectsNonOriginEndpoint(t *testing.T) {
	t.Parallel()
	for _, endpoint := range []string{
		"http://127.0.0.1:5000/prefix",
		"http://127.0.0.1:5000?redirect=http://other.example",
		"http://127.0.0.1:5000?",
		"ftp://127.0.0.1:5000",
		"http://user@127.0.0.1:5000",
	} {
		if _, err := destinationImageCacheVerifyURL(endpoint); err == nil {
			t.Fatalf("non-origin endpoint was accepted: %q", endpoint)
		}
	}
}
