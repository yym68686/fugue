package model

import "testing"

func TestPlatformErrorClassRollbackContract(t *testing.T) {
	t.Parallel()
	for _, class := range []string{
		PlatformErrorClassRouteUnavailable,
		PlatformErrorClassNoHealthy,
		PlatformErrorClassBundleSignature,
		PlatformErrorClassInvariant,
		PlatformErrorClassOriginDNS,
		PlatformErrorClassOriginConnect,
		PlatformErrorClassOriginUnavailable,
		PlatformErrorClassDecisionMissing,
		PlatformErrorClassEvidenceUnknown,
		PlatformErrorClassLatencyRegression,
	} {
		if !PlatformErrorClassBlocksRelease(class) {
			t.Fatalf("platform class %q did not block release", class)
		}
	}
	if PlatformErrorClassBlocksRelease(PlatformErrorClassOriginConnectedApp5xx) {
		t.Fatal("origin-connected application 5xx must not trigger platform rollback")
	}
	if PlatformErrorClassBlocksRelease(PlatformErrorClassNone) {
		t.Fatal("empty platform class unexpectedly blocked release")
	}
	if !PlatformErrorClassBlocksRelease("future-unknown-class") {
		t.Fatal("unknown platform class must fail closed")
	}
}

func TestPlatformErrorClassForRouteStatus(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"no healthy edge groups":                              PlatformErrorClassNoHealthy,
		"bundle signature verification failed":                PlatformErrorClassBundleSignature,
		"runtime invariant violation: current_image_mismatch": PlatformErrorClassInvariant,
		"edge route is administratively unavailable":          PlatformErrorClassRouteUnavailable,
		"": PlatformErrorClassEvidenceUnknown,
	}
	for reason, want := range tests {
		if got := PlatformErrorClassForRouteStatus(EdgeRouteStatusUnavailable, reason); got != want {
			t.Fatalf("reason=%q class=%q want=%q", reason, got, want)
		}
	}
	if got := PlatformErrorClassForRouteStatus(EdgeRouteStatusActive, "runtime invariant violation"); got != PlatformErrorClassNone {
		t.Fatalf("active route produced platform class %q", got)
	}
}
