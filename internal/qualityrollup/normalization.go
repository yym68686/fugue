package qualityrollup

import (
	"strings"

	"fugue/internal/edgequality"
	"fugue/internal/model"
)

func normalizedHostname(raw string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(raw)), ".")
}

func NormalizeTrafficClass(value string) string {
	return edgequality.NormalizeTrafficClass(value)
}

func PathPrefixBucket(pathPrefix string) string {
	pathPrefix = model.NormalizeAppRoutePathPrefix(pathPrefix)
	switch {
	case pathPrefix == "" || pathPrefix == "/":
		return ""
	case strings.HasPrefix(pathPrefix, "/_next/static"):
		return "/_next/static/*"
	case strings.HasPrefix(pathPrefix, "/assets"):
		return "/assets/*"
	case strings.HasPrefix(pathPrefix, "/api"):
		return "/api/*"
	case strings.HasPrefix(pathPrefix, "/upload"):
		return "/upload/*"
	case strings.HasPrefix(pathPrefix, "/stream"):
		return "/stream/*"
	default:
		return pathPrefix
	}
}
