package routeprobe

import (
	"context"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"fugue/internal/routeproof"
)

func TestOnlyTransportTimeoutsAllowBoundedProofRetention(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		transient bool
	}{
		{"deadline", context.DeadlineExceeded, true},
		{"wrapped timeout", &url.Error{Op: "Head", URL: "https://app.example.test/", Err: &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}}, true},
		{"cancelled", context.Canceled, false},
		{"certificate", &url.Error{Op: "Head", URL: "https://app.example.test/", Err: x509.UnknownAuthorityError{}}, false},
		{"explicit failure", errors.New("invalid route proof"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := errors.Is(classifyTransportError(tc.err), ErrUnavailable); got != tc.transient {
				t.Fatalf("transient=%v, want %v", got, tc.transient)
			}
		})
	}
}

func TestParseResponseAppTrafficProofIsOptionalButStrict(t *testing.T) {
	now := time.Now().UTC()
	nonce := "0123456789abcdef0123456789abcdef"
	digest := "sha256:" + strings.Repeat("a", 64)
	for name, values := range map[string][]string{
		"legacy": nil, "valid": {digest}, "empty": {""}, "duplicate": {digest, digest},
		"malformed": {"sha256:aa"}, "uppercase": {"sha256:" + strings.Repeat("A", 64)}, "whitespace": {digest + " "},
	} {
		t.Run(name, func(t *testing.T) {
			h := http.Header{}
			for key, value := range map[string]string{routeproof.NonceHeader: nonce, routeproof.DigestHeader: digest, routeproof.VersionHeader: "bundle", routeproof.ExpiryHeader: now.Add(time.Minute).Format(time.RFC3339Nano), routeproof.EdgeHeader: "edge", routeproof.GroupHeader: "group", "Cache-Control": "no-store"} {
				h.Set(key, value)
			}
			if values != nil {
				h[http.CanonicalHeaderKey(routeproof.AppTrafficHeader)] = values
			}
			proof, err := ParseResponse(&http.Response{StatusCode: 204, Header: h}, nonce, now)
			valid := name == "valid" || name == "legacy"
			if (err == nil) != valid {
				t.Fatalf("unexpected acceptance: %v", err)
			}
			if name == "valid" && proof.AppTrafficDigest != digest {
				t.Fatal("missing application digest")
			}
			if name == "legacy" && proof.AppTrafficDigest != "" {
				t.Fatal("legacy response invented application evidence")
			}
		})
	}
}
