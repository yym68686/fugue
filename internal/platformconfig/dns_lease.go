package platformconfig

import "fugue/internal/model"

// DNSArtifactRequiresTrafficRelease detects semantics that require the bound
// traffic executor, including live-readiness candidates and content expiration.
func DNSArtifactRequiresTrafficRelease(a model.PlatformArtifact) bool {
	if a.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle {
		return false
	}
	if p, ok := a.Content["policy"].(map[string]any); ok && p["dns_placement_mode"] == DNSPlacementConsumerReadiness {
		return true
	}
	has := func(rows any) bool {
		items, _ := rows.([]any)
		for _, item := range items {
			r, _ := item.(map[string]any)
			if expirations, ok := r["value_expirations"].(map[string]any); ok && len(expirations) > 0 {
				return true
			}
		}
		return false
	}
	if has(a.Content["records"]) {
		return true
	}
	for _, key := range []string{"consumer_views", "query_views"} {
		views, _ := a.Content[key].([]any)
		for _, item := range views {
			view, _ := item.(map[string]any)
			if has(view["records"]) {
				return true
			}
		}
	}
	return false
}
