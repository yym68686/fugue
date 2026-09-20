package routeprobe

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/routeproof"
)

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
