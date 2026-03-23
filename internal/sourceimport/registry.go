package sourceimport

import (
	"net"
	"strings"
)

func registryHostFromImageRef(imageRef string) string {
	host := strings.TrimSpace(imageRef)
	if idx := strings.Index(host, "/"); idx >= 0 {
		host = host[:idx]
	}
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	}
	return strings.Trim(strings.TrimSpace(host), "[]")
}

func isInsecureRegistryHost(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	if host == "" {
		return false
	}
	if host == "localhost" || net.ParseIP(host) != nil {
		return true
	}
	return strings.HasSuffix(host, ".svc") ||
		strings.HasSuffix(host, ".svc.cluster.local") ||
		strings.HasSuffix(host, ".cluster.local")
}

func kanikoDestinationArgs(imageRef string, baseArgs ...string) []string {
	args := append([]string(nil), baseArgs...)
	args = append(args, "--destination="+imageRef, "--cleanup")
	if registryHost := registryHostFromImageRef(imageRef); isInsecureRegistryHost(registryHost) {
		args = append(args,
			"--insecure",
			"--insecure-registry="+registryHost,
		)
	}
	return args
}
