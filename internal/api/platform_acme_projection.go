package api

import (
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func projectACMEChallengeIntents(result *platformIntentProjectionResponse, challenges []model.DNSACMEChallenge) error {
	for _, c := range challenges {
		result.Intent.ACMEChallenges = append(result.Intent.ACMEChallenges, platformconfig.ACMEChallengeIntent{ID: c.ID, Zone: c.Zone, Hostname: c.Name, Value: c.Value, TTL: c.TTL, ExpiresAt: c.ExpiresAt})
	}
	if err := platformconfig.ValidateACMEChallenges(result.Intent.ACMEChallenges); err != nil {
		return err
	}
	result.Intent = platformconfig.NormalizePlatformIntent(result.Intent)
	gen, err := platformconfig.PlatformIntentGeneration(result.Intent)
	if err != nil {
		return err
	}
	result.Intent.Generation = gen
	result.RuntimeSnapshot.IntentGeneration = gen
	issues := result.Issues[:0]
	for _, issue := range result.Issues {
		if issue.Code != "dns_acme_not_projected" && issue.Code != "dns_value_expiration_consumer_not_ready" {
			issues = append(issues, issue)
		}
	}
	result.Issues = issues
	// Runtime capability belongs to transactional admission. Capturing desired
	// ACME input cannot assert that every executor is permanently unsupported.
	return nil
}

// Consumers must enforce per-value expiration before these artifacts enter
// traffic. Standalone DNS remains blocked; ReleaseSet publication also checks
// fresh executor capabilities within the store transaction.
func (s *Server) platformArtifactHasDNSLeases(artifact model.PlatformArtifact) (bool, error) {
	if platformconfig.DNSArtifactHasValueExpirations(artifact) {
		return true, nil
	}
	if artifact.ArtifactKind == model.PlatformArtifactKindReleaseSet {
		if ids, ok := artifact.Content["artifact_ids"].([]any); ok {
			for _, id := range ids {
				value, ok := id.(string)
				if !ok {
					continue
				}
				child, err := s.store.GetPlatformArtifact(value)
				if err != nil {
					return false, err
				}
				if child.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle {
					continue
				}
				has, err := s.platformArtifactHasDNSLeases(child)
				if err != nil || has {
					return has, err
				}
			}
		}
	}
	return false, nil
}
