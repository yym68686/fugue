package sourceimport

import (
	"net"
	"os"
	"strings"
)

const defaultKanikoRegistryMirror = "mirror.gcr.io"
const defaultKanikoSnapshotMode = "redo"

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

func isLoopbackRegistryHost(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func configuredBuilderRegistryPushBase() string {
	return trimRegistryBase(os.Getenv("FUGUE_BUILDER_REGISTRY_PUSH_BASE"))
}

func builderDestinationImageRef(imageRef, registryPushBase string) string {
	imageRef = strings.TrimSpace(imageRef)
	registryPushBase = trimRegistryBase(registryPushBase)
	builderPushBase := configuredBuilderRegistryPushBase()
	if imageRef == "" || registryPushBase == "" || builderPushBase == "" || builderPushBase == registryPushBase {
		return imageRef
	}
	return imageRefWithRegistryBase(imageRef, registryPushBase, builderPushBase)
}

func trimRegistryBase(raw string) string {
	raw = strings.TrimRight(strings.TrimSpace(raw), "/")
	raw = strings.TrimPrefix(raw, "http://")
	raw = strings.TrimPrefix(raw, "https://")
	return raw
}

func imageRefWithRegistryBase(imageRef, fromBase, toBase string) string {
	imageRef = strings.TrimSpace(imageRef)
	fromBase = trimRegistryBase(fromBase)
	toBase = trimRegistryBase(toBase)
	if imageRef == "" || fromBase == "" || toBase == "" || toBase == fromBase {
		return imageRef
	}
	prefix := fromBase + "/"
	if !strings.HasPrefix(imageRef, prefix) {
		return imageRef
	}
	return toBase + "/" + strings.TrimPrefix(imageRef, prefix)
}

func effectiveDestinationImageRef(imageRef, destinationImageRef string) string {
	if destination := strings.TrimSpace(destinationImageRef); destination != "" {
		return destination
	}
	return strings.TrimSpace(imageRef)
}

func applyBuilderRegistryNetwork(podSpec map[string]any, destinationImageRef string) {
	if podSpec == nil {
		return
	}
	if !isLoopbackRegistryHost(registryHostFromImageRef(destinationImageRef)) {
		return
	}
	podSpec["hostNetwork"] = true
	podSpec["dnsPolicy"] = "ClusterFirstWithHostNet"
}

func kanikoDestinationArgs(imageRef string, baseArgs ...string) []string {
	args := append([]string(nil), baseArgs...)
	args = append(args, configuredKanikoSnapshotArgs()...)
	args = append(args, "--destination="+imageRef, "--cleanup")
	args = append(args, configuredKanikoExtraArgs()...)
	for _, mirror := range configuredKanikoRegistryMirrors() {
		args = append(args, "--registry-mirror="+mirror)
	}
	if registryHost := registryHostFromImageRef(imageRef); isInsecureRegistryHost(registryHost) {
		args = append(args,
			"--insecure",
			"--insecure-registry="+registryHost,
		)
	}
	return args
}

func configuredKanikoSnapshotArgs() []string {
	mode := strings.TrimSpace(os.Getenv("FUGUE_KANIKO_SNAPSHOT_MODE"))
	if mode == "" {
		mode = defaultKanikoSnapshotMode
	}
	switch strings.ToLower(mode) {
	case "none", "off", "disabled":
		return nil
	default:
		return []string{"--snapshot-mode=" + mode}
	}
}

func configuredKanikoExtraArgs() []string {
	raw := strings.TrimSpace(os.Getenv("FUGUE_KANIKO_EXTRA_ARGS"))
	if raw == "" {
		return nil
	}
	return strings.Fields(raw)
}

func configuredKanikoRegistryMirrors() []string {
	raw := strings.TrimSpace(os.Getenv("FUGUE_KANIKO_REGISTRY_MIRROR"))
	if raw == "" {
		return []string{defaultKanikoRegistryMirror}
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		return []string{defaultKanikoRegistryMirror}
	}
	return out
}
