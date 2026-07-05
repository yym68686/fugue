package imagecachekeys

import (
	"strings"
)

// ImageReferenceKeys returns stable matching keys for image references that may
// be expressed with a registry prefix, digest form, Fugue live tag, or comma/
// whitespace separated values.
func ImageReferenceKeys(ref, digest string) []string {
	out := []string{}
	for _, value := range splitReferenceList(ref) {
		out = append(out, value)
		withoutRegistry := StripRegistry(value)
		out = append(out, withoutRegistry)
		if repo, target, ok := SplitRepoTarget(withoutRegistry); ok {
			out = appendManifestKeys(out, repo, target, digest)
		}
	}
	if normalized := NormalizeDigest(digest); normalized != "" {
		out = append(out, normalized)
	}
	return uniqueKeys(out)
}

// ExactImageReferenceKeys returns only generation-exact matching keys for an
// image reference. Unlike ImageReferenceKeys it intentionally omits bare repo
// and bare tag keys, because those keys are suitable for grouping but too broad
// for deletion protection decisions.
func ExactImageReferenceKeys(ref, digest string) []string {
	out := []string{}
	for _, value := range splitReferenceList(ref) {
		out = append(out, value)
		withoutRegistry := StripRegistry(value)
		out = append(out, withoutRegistry)
		if repo, target, ok := SplitRepoTarget(withoutRegistry); ok {
			out = appendExactManifestKeys(out, repo, target, digest)
		}
	}
	if normalized := NormalizeDigest(digest); normalized != "" {
		out = append(out, normalized)
	}
	return uniqueKeys(out)
}

// ManifestReferenceKeys returns the key universe for a registry manifest row.
// It intentionally includes both repo:tag and repo@digest forms so inventory
// rows can match current workload refs regardless of which syntax each caller
// observed.
func ManifestReferenceKeys(repo, target, digest, imageRef string) []string {
	keys := ImageReferenceKeys(imageRef, digest)
	keys = appendManifestKeys(keys, repo, target, digest)
	return uniqueKeys(keys)
}

// ExactManifestReferenceKeys returns generation-exact keys for a manifest row.
// It is the deletion-protection counterpart to ManifestReferenceKeys.
func ExactManifestReferenceKeys(repo, target, digest, imageRef string) []string {
	keys := ExactImageReferenceKeys(imageRef, digest)
	keys = appendExactManifestKeys(keys, repo, target, digest)
	return uniqueKeys(keys)
}

func appendManifestKeys(keys []string, repo, target, digest string) []string {
	repo = strings.Trim(strings.TrimSpace(repo), "/")
	target = strings.TrimSpace(target)
	digest = firstNonEmptyString(NormalizeDigest(digest), NormalizeDigest(target))
	if repo != "" {
		keys = append(keys, repo)
		if target != "" {
			keys = append(keys, repo+":"+target, repo+"@"+target)
		}
		if digest != "" {
			keys = append(keys, repo+"@"+digest)
			keys = append(keys, repo+":sha256:"+strings.TrimPrefix(digest, "sha256:"))
		}
	}
	if target != "" {
		keys = append(keys, target)
	}
	if digest != "" {
		keys = append(keys, digest)
	}
	return keys
}

func appendExactManifestKeys(keys []string, repo, target, digest string) []string {
	repo = strings.Trim(strings.TrimSpace(repo), "/")
	target = strings.TrimSpace(target)
	digest = firstNonEmptyString(NormalizeDigest(digest), NormalizeDigest(target))
	if repo != "" {
		if target != "" {
			keys = append(keys, repo+":"+target)
			if NormalizeDigest(target) != "" {
				keys = append(keys, repo+"@"+target)
			}
		}
		if digest != "" {
			keys = append(keys, repo+"@"+digest)
			keys = append(keys, repo+":sha256:"+strings.TrimPrefix(digest, "sha256:"))
		}
	}
	if digest != "" {
		keys = append(keys, digest)
	}
	return keys
}

func splitReferenceList(ref string) []string {
	ref = strings.NewReplacer(",", " ").Replace(strings.TrimSpace(ref))
	if ref == "" {
		return nil
	}
	values := strings.Fields(ref)
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		value = strings.TrimPrefix(value, "http://")
		value = strings.TrimPrefix(value, "https://")
		out = append(out, value)
	}
	return out
}

// StripRegistry removes the registry host from an image reference when one is
// present. It preserves escaped cache repository names after the first slash.
func StripRegistry(ref string) string {
	ref = strings.Trim(strings.TrimSpace(ref), "/")
	if ref == "" {
		return ""
	}
	firstSegment, rest, hasSlash := strings.Cut(ref, "/")
	if !hasSlash {
		return ref
	}
	if strings.Contains(firstSegment, ".") || strings.Contains(firstSegment, ":") || firstSegment == "localhost" {
		return strings.Trim(rest, "/")
	}
	return ref
}

// SplitRepoTarget splits an image reference into repo and tag/digest target.
// It handles the uncommon repo:sha256:digest form used by Fugue cache reports.
func SplitRepoTarget(ref string) (string, string, bool) {
	ref = strings.Trim(strings.TrimSpace(ref), "/")
	if ref == "" {
		return "", "", false
	}
	if repo, target, ok := strings.Cut(ref, "@"); ok {
		return strings.Trim(repo, "/"), strings.TrimSpace(target), true
	}
	if idx := strings.LastIndex(ref, ":sha256:"); idx > 0 {
		return strings.Trim(ref[:idx], "/"), strings.TrimSpace(ref[idx+1:]), true
	}
	lastSlash := strings.LastIndex(ref, "/")
	lastColon := strings.LastIndex(ref, ":")
	if lastColon > lastSlash && lastColon+1 < len(ref) {
		return strings.Trim(ref[:lastColon], "/"), strings.TrimSpace(ref[lastColon+1:]), true
	}
	return ref, "latest", true
}

func NormalizeDigest(digest string) string {
	digest = strings.ToLower(strings.TrimSpace(digest))
	digest = strings.TrimPrefix(digest, "@")
	if strings.HasPrefix(digest, "sha256:") && len(digest) > len("sha256:") {
		return digest
	}
	return ""
}

func uniqueKeys(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
