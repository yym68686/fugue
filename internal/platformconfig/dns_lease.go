package platformconfig

import "fugue/internal/model"

// DNSArtifactHasValueExpirations detects expiration semantics in both global
// records and per-consumer views before they enter serving.
func DNSArtifactHasValueExpirations(a model.PlatformArtifact) bool {
	if a.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle {
		return false
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
