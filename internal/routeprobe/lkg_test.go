package routeprobe

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/routeproof"
)

func TestRetainedProofRequiresExplicitModeAndOriginalExpiredLease(t *testing.T) {
	now := time.Now().UTC()
	nonce := "0123456789abcdef0123456789abcdef"
	for _, scenario := range []string{"valid", "unsolicited", "missing original", "future original", "duplicate mode", "unknown mode", "application proof", "past offline deadline"} {
		t.Run(scenario, func(t *testing.T) {
			h := http.Header{}
			for k, v := range map[string]string{routeproof.NonceHeader: nonce, routeproof.DigestHeader: "sha256:" + strings.Repeat("a", 64),
				routeproof.VersionHeader: "applied", routeproof.EdgeHeader: "edge-a", routeproof.GroupHeader: "group-a", "Cache-Control": "no-store",
				routeproof.ModeHeader: routeproof.RetainedLKGMode, routeproof.BundleExpiryHeader: now.Add(-time.Minute).Format(time.RFC3339Nano),
				routeproof.ExpiryHeader: now.Add(time.Hour).Format(time.RFC3339Nano)} {
				h.Set(k, v)
			}
			switch scenario {
			case "missing original":
				h.Del(routeproof.BundleExpiryHeader)
			case "future original":
				h.Set(routeproof.BundleExpiryHeader, now.Add(time.Minute).Format(time.RFC3339Nano))
			case "duplicate mode":
				h.Add(routeproof.ModeHeader, routeproof.RetainedLKGMode)
			case "unknown mode":
				h.Set(routeproof.ModeHeader, "other")
			case "application proof":
				h.Set(routeproof.AppTrafficHeader, "sha256:"+strings.Repeat("a", 64))
			case "past offline deadline":
				h.Set(routeproof.ExpiryHeader, now.Add(-time.Second).Format(time.RFC3339Nano))
			}
			proof, err := parseResponse(&http.Response{StatusCode: 204, Header: h}, nonce, now, scenario != "unsolicited")
			if (err == nil) != (scenario == "valid") {
				t.Fatal(proof, err)
			}
			if scenario == "valid" && !proof.BundleValidUntil.Equal(now.Add(-time.Minute)) {
				t.Fatal("original expiry lost", proof)
			}
		})
	}
}
