package platformsafety

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	RollbackAssetKindImageDigest      = "platform_image_digest"
	RollbackAssetKindCaddyConfig      = model.PlatformArtifactKindCaddyRouteConfig
	RollbackAssetKindDNSBundle        = model.PlatformArtifactKindDNSAnswerBundle
	RollbackAssetKindEdgeRouteBundle  = model.PlatformArtifactKindEdgeRouteBundle
	RollbackAssetKindNodeDesiredState = model.PlatformArtifactKindNodeDesiredState
)

type RollbackAssetRequirement struct {
	Kind      string
	ScopeKey  string
	Reference string
	Identity  string
}

type RollbackAssetEvidence struct {
	Kind       string
	ScopeKey   string
	Reference  string
	Identity   string
	State      string
	SourceID   string
	ObservedAt time.Time
	ExpiresAt  time.Time
}

type RollbackAssetCheck struct {
	Kind             string
	ScopeKey         string
	Reference        string
	ExpectedIdentity string
	ObservedIdentity string
	State            string
	Pass             bool
	Message          string
}

type RollbackAssetPreflightResult struct {
	Pass   bool
	Checks []RollbackAssetCheck
}

type rollbackAssetLocatorKey struct {
	Kind      string
	ScopeKey  string
	Reference string
}

// EvaluateRollbackAssetPreflight combines already-collected observations. It
// deliberately performs no network, registry, filesystem, or store access.
func EvaluateRollbackAssetPreflight(
	requirements []RollbackAssetRequirement,
	evidence []RollbackAssetEvidence,
	now time.Time,
) RollbackAssetPreflightResult {
	now = now.UTC()
	result := RollbackAssetPreflightResult{
		Pass:   len(requirements) > 0,
		Checks: make([]RollbackAssetCheck, 0, len(requirements)),
	}
	if len(requirements) == 0 {
		result.Checks = append(result.Checks, RollbackAssetCheck{
			State:   model.InvariantEvidenceStateUnknown,
			Message: "rollback asset requirements are empty",
		})
		return result
	}

	evidenceByLocator := make(map[rollbackAssetLocatorKey][]RollbackAssetEvidence, len(evidence))
	for _, observation := range evidence {
		key := rollbackAssetLocator(
			observation.Kind,
			observation.ScopeKey,
			observation.Reference,
		)
		evidenceByLocator[key] = append(evidenceByLocator[key], observation)
	}

	seenRequirements := make(map[rollbackAssetLocatorKey]struct{}, len(requirements))
	for _, requirement := range requirements {
		check := RollbackAssetCheck{
			Kind:             normalizeRollbackAssetKind(requirement.Kind),
			ScopeKey:         strings.TrimSpace(requirement.ScopeKey),
			Reference:        strings.TrimSpace(requirement.Reference),
			ExpectedIdentity: strings.TrimSpace(requirement.Identity),
			State:            model.InvariantEvidenceStateUnknown,
		}
		locator := rollbackAssetLocator(check.Kind, check.ScopeKey, check.Reference)
		if _, duplicate := seenRequirements[locator]; duplicate {
			check.State = model.InvariantEvidenceStateFail
			check.Message = "rollback asset requirement is duplicated"
			result.Checks = append(result.Checks, check)
			result.Pass = false
			continue
		}
		seenRequirements[locator] = struct{}{}

		if err := validateRollbackAssetRequirement(requirement); err != nil {
			check.State = model.InvariantEvidenceStateFail
			check.Message = err.Error()
			result.Checks = append(result.Checks, check)
			result.Pass = false
			continue
		}

		observations := evidenceByLocator[locator]
		if len(observations) == 0 {
			check.Message = "rollback asset availability evidence is missing"
			result.Checks = append(result.Checks, check)
			result.Pass = false
			continue
		}
		if len(observations) != 1 {
			check.State = model.InvariantEvidenceStateFail
			check.Message = "rollback asset has conflicting duplicate evidence"
			result.Checks = append(result.Checks, check)
			result.Pass = false
			continue
		}

		observation := observations[0]
		check.ObservedIdentity = strings.TrimSpace(observation.Identity)
		check.State = strings.TrimSpace(observation.State)
		if !rollbackAssetIdentitiesEqual(check.Kind, check.ExpectedIdentity, check.ObservedIdentity) {
			check.State = model.InvariantEvidenceStateFail
			check.Message = "rollback asset evidence identity does not match the pinned target"
			result.Checks = append(result.Checks, check)
			result.Pass = false
			continue
		}
		if !validInvariantEvidenceState(check.State) {
			check.State = model.InvariantEvidenceStateUnknown
			check.Message = "rollback asset evidence state is missing or unsupported"
			result.Checks = append(result.Checks, check)
			result.Pass = false
			continue
		}
		if strings.TrimSpace(observation.SourceID) == "" {
			check.State = model.InvariantEvidenceStateUnknown
			check.Message = "rollback asset evidence source identity is missing"
			result.Checks = append(result.Checks, check)
			result.Pass = false
			continue
		}
		observedAt := observation.ObservedAt.UTC()
		expiresAt := observation.ExpiresAt.UTC()
		if observedAt.IsZero() || observedAt.After(now) {
			check.State = model.InvariantEvidenceStateUnknown
			check.Message = "rollback asset evidence observation time is missing or in the future"
			result.Checks = append(result.Checks, check)
			result.Pass = false
			continue
		}
		if expiresAt.IsZero() || !expiresAt.After(now) || expiresAt.Before(observedAt) {
			check.State = model.InvariantEvidenceStateStale
			check.Message = "rollback asset evidence is stale or has an invalid validity window"
			result.Checks = append(result.Checks, check)
			result.Pass = false
			continue
		}
		if check.State != model.InvariantEvidenceStatePass {
			check.Message = fmt.Sprintf("rollback asset evidence state is %s", check.State)
			result.Checks = append(result.Checks, check)
			result.Pass = false
			continue
		}

		check.Pass = true
		check.Message = "rollback asset is available with fresh matching evidence"
		result.Checks = append(result.Checks, check)
	}
	return result
}

func validateRollbackAssetRequirement(requirement RollbackAssetRequirement) error {
	kind := normalizeRollbackAssetKind(requirement.Kind)
	switch kind {
	case RollbackAssetKindImageDigest,
		RollbackAssetKindCaddyConfig,
		RollbackAssetKindDNSBundle,
		RollbackAssetKindEdgeRouteBundle,
		RollbackAssetKindNodeDesiredState:
	default:
		return fmt.Errorf("unsupported rollback asset kind %q", kind)
	}
	if strings.TrimSpace(requirement.ScopeKey) == "" {
		return fmt.Errorf("rollback asset scope is required")
	}
	if strings.TrimSpace(requirement.Reference) == "" {
		return fmt.Errorf("rollback asset reference is required")
	}
	identity := strings.TrimSpace(requirement.Identity)
	if kind == RollbackAssetKindImageDigest {
		if !validSHA256Digest(identity) {
			return fmt.Errorf("rollback image identity must be a full sha256 digest")
		}
		return nil
	}
	if identity == "" {
		return fmt.Errorf("rollback config generation is required")
	}
	return nil
}

func rollbackAssetLocator(kind, scopeKey, reference string) rollbackAssetLocatorKey {
	return rollbackAssetLocatorKey{
		Kind:      normalizeRollbackAssetKind(kind),
		ScopeKey:  strings.TrimSpace(scopeKey),
		Reference: strings.TrimSpace(reference),
	}
}

func normalizeRollbackAssetKind(kind string) string {
	return strings.ToLower(strings.TrimSpace(kind))
}

func rollbackAssetIdentitiesEqual(kind, expected, observed string) bool {
	expected = strings.TrimSpace(expected)
	observed = strings.TrimSpace(observed)
	if normalizeRollbackAssetKind(kind) == RollbackAssetKindImageDigest {
		return strings.EqualFold(expected, observed)
	}
	return expected == observed
}

func validInvariantEvidenceState(state string) bool {
	switch strings.TrimSpace(state) {
	case model.InvariantEvidenceStatePass,
		model.InvariantEvidenceStateFail,
		model.InvariantEvidenceStateUnknown,
		model.InvariantEvidenceStateStale:
		return true
	default:
		return false
	}
}

func validSHA256Digest(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != len("sha256:")+64 || !strings.EqualFold(value[:len("sha256:")], "sha256:") {
		return false
	}
	_, err := hex.DecodeString(value[len("sha256:"):])
	return err == nil
}
