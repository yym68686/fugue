package model

import "strings"

const (
	PlatformErrorClassNone                  = ""
	PlatformErrorClassRouteUnavailable      = "route_unavailable"
	PlatformErrorClassNoHealthy             = "no_healthy"
	PlatformErrorClassBundleSignature       = "bundle_signature"
	PlatformErrorClassInvariant             = "invariant"
	PlatformErrorClassOriginDNS             = "origin_dns"
	PlatformErrorClassOriginConnect         = "origin_connect"
	PlatformErrorClassOriginUnavailable     = "origin_unavailable"
	PlatformErrorClassDecisionMissing       = "decision_missing"
	PlatformErrorClassEvidenceUnknown       = "evidence_unknown"
	PlatformErrorClassLatencyRegression     = "latency_regression"
	PlatformErrorClassOriginConnectedApp5xx = "origin_connected_application_5xx"
)

var platformErrorClasses = map[string]struct{}{
	PlatformErrorClassNone:                  {},
	PlatformErrorClassRouteUnavailable:      {},
	PlatformErrorClassNoHealthy:             {},
	PlatformErrorClassBundleSignature:       {},
	PlatformErrorClassInvariant:             {},
	PlatformErrorClassOriginDNS:             {},
	PlatformErrorClassOriginConnect:         {},
	PlatformErrorClassOriginUnavailable:     {},
	PlatformErrorClassDecisionMissing:       {},
	PlatformErrorClassEvidenceUnknown:       {},
	PlatformErrorClassLatencyRegression:     {},
	PlatformErrorClassOriginConnectedApp5xx: {},
}

func NormalizePlatformErrorClass(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if _, ok := platformErrorClasses[value]; ok {
		return value
	}
	return PlatformErrorClassEvidenceUnknown
}

// PlatformErrorClassBlocksRelease deliberately excludes an application 5xx
// that was received over an established origin connection. Unknown or missing
// platform evidence remains fail-closed.
func PlatformErrorClassBlocksRelease(value string) bool {
	switch NormalizePlatformErrorClass(value) {
	case PlatformErrorClassNone, PlatformErrorClassOriginConnectedApp5xx:
		return false
	default:
		return true
	}
}

func PlatformErrorClassRequiresDecision(value string) bool {
	switch NormalizePlatformErrorClass(value) {
	case PlatformErrorClassRouteUnavailable, PlatformErrorClassNoHealthy,
		PlatformErrorClassBundleSignature, PlatformErrorClassInvariant:
		return true
	default:
		return false
	}
}

func PlatformErrorClassForRouteStatus(status, reason string) string {
	if strings.EqualFold(strings.TrimSpace(status), EdgeRouteStatusActive) {
		return PlatformErrorClassNone
	}
	reason = strings.ToLower(strings.TrimSpace(reason))
	switch {
	case strings.Contains(reason, "no healthy"):
		return PlatformErrorClassNoHealthy
	case strings.Contains(reason, "signature"):
		return PlatformErrorClassBundleSignature
	case strings.Contains(reason, "invariant"):
		return PlatformErrorClassInvariant
	case reason != "":
		return PlatformErrorClassRouteUnavailable
	default:
		return PlatformErrorClassEvidenceUnknown
	}
}
