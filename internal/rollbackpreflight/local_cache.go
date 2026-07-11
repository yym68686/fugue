package rollbackpreflight

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

const localDNSCacheFileVersion = 1

type LocalCacheCandidate struct {
	Kind        string
	ScopeKey    string
	Reference   string
	CandidateID string
	Data        []byte
	MaxStale    time.Duration
}

type LocalCacheInventory struct {
	Candidates       []LocalCacheCandidate
	SourceID         string
	ObservedAt       time.Time
	ValidFor         time.Duration
	ClockUncertainty time.Duration
}

type LocalCacheObservation struct {
	Evidence platformsafety.RollbackAssetEvidence
	Detail   string
}

type localCacheCandidateResult struct {
	Generation string
	State      string
	ExpiresAt  time.Time
	Detail     string
}

type localEdgeRouteCacheFile struct {
	Version  int                   `json:"version"`
	ETag     string                `json:"etag,omitempty"`
	CachedAt time.Time             `json:"cached_at"`
	Bundle   model.EdgeRouteBundle `json:"bundle"`
}

type localDNSCacheFile struct {
	Version  int                 `json:"version"`
	ETag     string              `json:"etag,omitempty"`
	CachedAt time.Time           `json:"cached_at"`
	Bundle   model.EdgeDNSBundle `json:"bundle"`
}

// CollectLocalCacheObservations verifies exact rollback generations against
// cache bytes already loaded by a caller. It deliberately performs no
// filesystem, network, store, or runtime I/O.
func CollectLocalCacheObservations(
	requirements []platformsafety.RollbackAssetRequirement,
	inventory LocalCacheInventory,
	keyring bundleauth.Keyring,
) []LocalCacheObservation {
	observedAt := inventory.ObservedAt.UTC()
	defaultExpiresAt := time.Time{}
	if !observedAt.IsZero() && inventory.ValidFor > 0 {
		defaultExpiresAt = observedAt.Add(inventory.ValidFor)
	}
	observations := make([]LocalCacheObservation, 0, len(requirements))
	for _, requirement := range requirements {
		kind := normalizeKind(requirement.Kind)
		if !localCacheKind(kind) {
			continue
		}
		observation := LocalCacheObservation{Evidence: platformsafety.RollbackAssetEvidence{
			Kind:       kind,
			ScopeKey:   strings.TrimSpace(requirement.ScopeKey),
			Reference:  strings.TrimSpace(requirement.Reference),
			Identity:   strings.TrimSpace(requirement.Identity),
			State:      model.InvariantEvidenceStateUnknown,
			SourceID:   strings.TrimSpace(inventory.SourceID),
			ObservedAt: observedAt,
			ExpiresAt:  defaultExpiresAt,
		}}
		if observation.Evidence.SourceID == "" {
			observation.Detail = "local cache source identity is missing"
			observations = append(observations, observation)
			continue
		}
		if observedAt.IsZero() {
			observation.Detail = "local cache observation time is missing"
			observations = append(observations, observation)
			continue
		}
		if inventory.ValidFor <= 0 {
			observation.Detail = "local cache evidence validity must be positive"
			observations = append(observations, observation)
			continue
		}
		if inventory.ClockUncertainty < 0 {
			observation.Detail = "local cache clock uncertainty must not be negative"
			observations = append(observations, observation)
			continue
		}
		if !localCacheKeyringConfigured(keyring) {
			observation.Detail = "local cache bundle verification keyring is missing"
			observations = append(observations, observation)
			continue
		}

		candidates := matchingLocalCacheCandidates(inventory.Candidates, requirement)
		if len(candidates) == 0 {
			observation.Detail = "local cache inventory has no candidates for the pinned kind, scope, and reference"
			observations = append(observations, observation)
			continue
		}
		if detail := validateLocalCacheCandidateIDs(candidates); detail != "" {
			observation.Evidence.State = model.InvariantEvidenceStateFail
			observation.Detail = detail
			observations = append(observations, observation)
			continue
		}

		expectedGeneration := strings.TrimSpace(requirement.Identity)
		matching := 0
		passing := 0
		failing := 0
		stale := 0
		unattributedFailures := 0
		latestPassingExpiry := time.Time{}
		latestStaleExpiry := time.Time{}
		for _, candidate := range candidates {
			result := inspectLocalCacheCandidate(candidate, expectedGeneration, observedAt, inventory.ClockUncertainty, keyring)
			if result.Generation == "" {
				if result.State == model.InvariantEvidenceStateFail {
					unattributedFailures++
				}
				continue
			}
			if result.Generation != expectedGeneration {
				continue
			}
			matching++
			switch result.State {
			case model.InvariantEvidenceStatePass:
				passing++
				if result.ExpiresAt.After(latestPassingExpiry) {
					latestPassingExpiry = result.ExpiresAt
				}
			case model.InvariantEvidenceStateStale:
				stale++
				if result.ExpiresAt.After(latestStaleExpiry) {
					latestStaleExpiry = result.ExpiresAt
				}
			default:
				failing++
			}
		}

		switch {
		case passing > 0:
			observation.Evidence.State = model.InvariantEvidenceStatePass
			observation.Evidence.ExpiresAt = earlierTime(defaultExpiresAt, latestPassingExpiry)
			observation.Detail = fmt.Sprintf(
				"pinned generation is available in %d verified local cache candidate(s); matching_fail=%d matching_stale=%d unattributed_corrupt=%d",
				passing,
				failing,
				stale,
				unattributedFailures,
			)
		case failing > 0 || unattributedFailures > 0:
			observation.Evidence.State = model.InvariantEvidenceStateFail
			observation.Detail = fmt.Sprintf(
				"pinned generation has no verified candidate; matching_fail=%d unattributed_corrupt=%d matching_stale=%d",
				failing,
				unattributedFailures,
				stale,
			)
		case stale > 0:
			observation.Evidence.State = model.InvariantEvidenceStateStale
			observation.Evidence.ExpiresAt = latestStaleExpiry
			observation.Detail = fmt.Sprintf("all %d candidate(s) for the pinned generation are stale", stale)
		case matching == 0:
			observation.Detail = "pinned generation is absent from current, previous, and archived local cache candidates"
		default:
			observation.Detail = "pinned generation has no usable local cache candidate"
		}
		observations = append(observations, observation)
	}
	return observations
}

func LocalCacheEvidence(observations []LocalCacheObservation) []platformsafety.RollbackAssetEvidence {
	evidence := make([]platformsafety.RollbackAssetEvidence, 0, len(observations))
	for _, observation := range observations {
		evidence = append(evidence, observation.Evidence)
	}
	return evidence
}

func localCacheKind(kind string) bool {
	switch normalizeKind(kind) {
	case platformsafety.RollbackAssetKindDNSBundle,
		platformsafety.RollbackAssetKindEdgeRouteBundle:
		return true
	default:
		return false
	}
}

func matchingLocalCacheCandidates(
	candidates []LocalCacheCandidate,
	requirement platformsafety.RollbackAssetRequirement,
) []LocalCacheCandidate {
	kind := normalizeKind(requirement.Kind)
	scopeKey := strings.TrimSpace(requirement.ScopeKey)
	reference := strings.TrimSpace(requirement.Reference)
	matches := make([]LocalCacheCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if normalizeKind(candidate.Kind) == kind &&
			strings.TrimSpace(candidate.ScopeKey) == scopeKey &&
			strings.TrimSpace(candidate.Reference) == reference {
			matches = append(matches, candidate)
		}
	}
	return matches
}

func validateLocalCacheCandidateIDs(candidates []LocalCacheCandidate) string {
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		candidateID := strings.TrimSpace(candidate.CandidateID)
		if candidateID == "" {
			return "local cache candidate identity is missing"
		}
		if _, duplicate := seen[candidateID]; duplicate {
			return "local cache candidate identity is duplicated"
		}
		seen[candidateID] = struct{}{}
		if candidate.MaxStale < 0 {
			return "local cache candidate max stale duration must not be negative"
		}
	}
	return ""
}

func inspectLocalCacheCandidate(
	candidate LocalCacheCandidate,
	expectedGeneration string,
	observedAt time.Time,
	clockUncertainty time.Duration,
	keyring bundleauth.Keyring,
) localCacheCandidateResult {
	switch normalizeKind(candidate.Kind) {
	case platformsafety.RollbackAssetKindEdgeRouteBundle:
		return inspectEdgeRouteCacheCandidate(candidate, expectedGeneration, observedAt, clockUncertainty, keyring)
	case platformsafety.RollbackAssetKindDNSBundle:
		return inspectDNSCacheCandidate(candidate, expectedGeneration, observedAt, clockUncertainty, keyring)
	default:
		return localCacheCandidateResult{State: model.InvariantEvidenceStateFail, Detail: "unsupported local cache kind"}
	}
}

func inspectEdgeRouteCacheCandidate(
	candidate LocalCacheCandidate,
	expectedGeneration string,
	observedAt time.Time,
	clockUncertainty time.Duration,
	keyring bundleauth.Keyring,
) localCacheCandidateResult {
	var cached localEdgeRouteCacheFile
	if err := json.Unmarshal(candidate.Data, &cached); err != nil {
		return localCacheCandidateResult{State: model.InvariantEvidenceStateFail, Detail: "edge route cache is not valid JSON"}
	}
	generation, err := edgeRouteCacheGeneration(cached.Bundle)
	if err != nil {
		return localCacheCandidateResult{State: model.InvariantEvidenceStateFail, Detail: err.Error()}
	}
	result := localCacheCandidateResult{Generation: generation, State: model.InvariantEvidenceStateUnknown}
	if generation != expectedGeneration {
		result.Detail = "edge route cache contains another generation"
		return result
	}
	state, expiresAt, err := verifySignedCacheBundle(
		cached.Bundle.GeneratedAt,
		cached.Bundle.ValidUntil,
		candidate.MaxStale,
		observedAt,
		clockUncertainty,
		func(verifyAt time.Time) error {
			return bundleauth.VerifyEdgeRouteBundleWithKeyring(cached.Bundle, keyring, verifyAt)
		},
	)
	result.State = state
	result.ExpiresAt = expiresAt
	if err != nil {
		result.Detail = "edge route cache bundle verification failed: " + err.Error()
		return result
	}
	result.Detail = "edge route cache bundle is signed and available"
	return result
}

func inspectDNSCacheCandidate(
	candidate LocalCacheCandidate,
	expectedGeneration string,
	observedAt time.Time,
	clockUncertainty time.Duration,
	keyring bundleauth.Keyring,
) localCacheCandidateResult {
	data := candidate.Data
	envelopeGeneration := ""
	envelopeExpiresAt := time.Time{}
	if localDNSCacheLooksLikeEnvelope(data) {
		var marker lkgcache.Envelope
		if err := json.Unmarshal(data, &marker); err != nil {
			return localCacheCandidateResult{State: model.InvariantEvidenceStateFail, Detail: "dns cache envelope is not valid JSON"}
		}
		generation := strings.TrimSpace(marker.Generation)
		if generation == "" {
			return localCacheCandidateResult{State: model.InvariantEvidenceStateFail, Detail: "dns cache envelope generation is missing"}
		}
		if generation != expectedGeneration {
			return localCacheCandidateResult{Generation: generation, State: model.InvariantEvidenceStateUnknown, Detail: "dns cache envelope contains another generation"}
		}
		envelopeGeneration = generation
		if marker.CreatedAt.IsZero() || marker.CreatedAt.After(observedAt.Add(clockUncertainty)) {
			return localCacheCandidateResult{Generation: generation, State: model.InvariantEvidenceStateFail, Detail: "dns cache envelope creation time is missing or in the future"}
		}
		decodeAt := observedAt
		if !marker.ExpiresAt.IsZero() && !decodeAt.Before(marker.ExpiresAt) {
			decodeAt = marker.ExpiresAt.Add(-time.Nanosecond)
		}
		envelope, err := lkgcache.DecodeEnvelope(data, lkgcache.ReadEnvelopeOptions{
			Now:          decodeAt,
			ExpectedKind: model.PlatformArtifactKindDNSAnswerBundle,
		})
		if err != nil {
			return localCacheCandidateResult{Generation: generation, State: model.InvariantEvidenceStateFail, Detail: "dns cache envelope verification failed: " + err.Error()}
		}
		envelopeExpiresAt = envelope.ExpiresAt.UTC()
		data = envelope.Payload
	}

	var cached localDNSCacheFile
	if err := json.Unmarshal(data, &cached); err != nil {
		return localCacheCandidateResult{Generation: envelopeGeneration, State: model.InvariantEvidenceStateFail, Detail: "dns cache payload is not valid JSON"}
	}
	generation, err := edgeDNSCacheGeneration(cached.Bundle)
	if err != nil {
		if envelopeGeneration != "" {
			generation = envelopeGeneration
		}
		return localCacheCandidateResult{Generation: generation, State: model.InvariantEvidenceStateFail, Detail: err.Error()}
	}
	if envelopeGeneration != "" && generation != envelopeGeneration {
		return localCacheCandidateResult{
			Generation: envelopeGeneration,
			State:      model.InvariantEvidenceStateFail,
			Detail:     "dns cache envelope generation does not match the signed bundle generation",
		}
	}
	result := localCacheCandidateResult{Generation: generation, State: model.InvariantEvidenceStateUnknown}
	if generation != expectedGeneration {
		result.Detail = "dns cache contains another generation"
		return result
	}
	if cached.Version != localDNSCacheFileVersion {
		result.State = model.InvariantEvidenceStateFail
		result.Detail = fmt.Sprintf("dns cache file version %d is unsupported", cached.Version)
		return result
	}
	state, expiresAt, err := verifySignedCacheBundle(
		cached.Bundle.GeneratedAt,
		cached.Bundle.ValidUntil,
		candidate.MaxStale,
		observedAt,
		clockUncertainty,
		func(verifyAt time.Time) error {
			return bundleauth.VerifyEdgeDNSBundleWithKeyring(cached.Bundle, keyring, verifyAt)
		},
	)
	result.State = state
	result.ExpiresAt = expiresAt
	if err != nil {
		result.Detail = "dns cache bundle verification failed: " + err.Error()
		return result
	}
	if !envelopeExpiresAt.IsZero() {
		if envelopeExpiresAt.Before(result.ExpiresAt) {
			result.ExpiresAt = envelopeExpiresAt
		}
		if !result.ExpiresAt.After(observedAt) {
			result.State = model.InvariantEvidenceStateStale
			result.Detail = "dns cache envelope is stale"
			return result
		}
	}
	result.Detail = "dns cache envelope and signed bundle are available"
	return result
}

func verifySignedCacheBundle(
	generatedAt time.Time,
	validUntil time.Time,
	maxStale time.Duration,
	observedAt time.Time,
	clockUncertainty time.Duration,
	verify func(time.Time) error,
) (string, time.Time, error) {
	if generatedAt.IsZero() || generatedAt.After(observedAt.Add(clockUncertainty)) {
		return model.InvariantEvidenceStateFail, time.Time{}, fmt.Errorf("bundle generation time is missing or in the future")
	}
	if validUntil.IsZero() || !validUntil.After(generatedAt) {
		return model.InvariantEvidenceStateFail, time.Time{}, fmt.Errorf("bundle validity window is missing or invalid")
	}
	if maxStale < 0 {
		return model.InvariantEvidenceStateFail, time.Time{}, fmt.Errorf("bundle max stale duration must not be negative")
	}
	verifyAt := observedAt
	if verifyAt.After(validUntil) {
		verifyAt = validUntil
	}
	if err := verify(verifyAt); err != nil {
		return model.InvariantEvidenceStateFail, time.Time{}, err
	}
	expiresAt := validUntil.UTC()
	if maxStale > 0 {
		expiresAt = expiresAt.Add(maxStale)
	}
	if !expiresAt.After(observedAt) {
		return model.InvariantEvidenceStateStale, expiresAt, nil
	}
	return model.InvariantEvidenceStatePass, expiresAt, nil
}

func edgeRouteCacheGeneration(bundle model.EdgeRouteBundle) (string, error) {
	version := strings.TrimSpace(bundle.Version)
	generation := strings.TrimSpace(bundle.Generation)
	if version == "" {
		return "", fmt.Errorf("edge route cache bundle version is missing")
	}
	if generation == "" {
		generation = version
	}
	if generation != version {
		return generation, fmt.Errorf("edge route cache generation does not match bundle version")
	}
	return generation, nil
}

func edgeDNSCacheGeneration(bundle model.EdgeDNSBundle) (string, error) {
	version := strings.TrimSpace(bundle.Version)
	generation := strings.TrimSpace(bundle.Generation)
	if version == "" {
		return "", fmt.Errorf("dns cache bundle version is missing")
	}
	if generation == "" {
		generation = version
	}
	if generation != version {
		return generation, fmt.Errorf("dns cache generation does not match bundle version")
	}
	return generation, nil
}

func localDNSCacheLooksLikeEnvelope(data []byte) bool {
	var marker struct {
		SchemaVersion string          `json:"schema_version"`
		Kind          string          `json:"kind"`
		ContentHash   string          `json:"content_hash"`
		Payload       json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(data, &marker); err != nil {
		return false
	}
	return strings.TrimSpace(marker.SchemaVersion) != "" ||
		strings.TrimSpace(marker.Kind) != "" ||
		strings.TrimSpace(marker.ContentHash) != "" ||
		len(marker.Payload) > 0
}

func localCacheKeyringConfigured(keyring bundleauth.Keyring) bool {
	return strings.TrimSpace(keyring.PrimaryKey) != "" || strings.TrimSpace(keyring.PreviousKey) != ""
}

func earlierTime(left, right time.Time) time.Time {
	if left.IsZero() {
		return right
	}
	if right.IsZero() || left.Before(right) {
		return left
	}
	return right
}
