package platformsafety

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"fugue/internal/model"
)

const (
	InvariantArtifactValidated          = "artifact.validated"
	InvariantArtifactContentHash        = "artifact.content_hash"
	InvariantGenerationMonotonic        = "generation.monotonic"
	InvariantShadowNoProductionImpact   = "shadow.no_production_impact"
	InvariantCanaryScopeIsolation       = "canary.scope_isolation"
	InvariantBlastRadiusHardCap         = "action.blast_radius_hard_cap"
	InvariantKillSwitchPrecedence       = "action.kill_switch_precedence"
	InvariantFullPinnedRollback         = "full.pinned_rollback"
	InvariantFencingTokenCurrent        = "release.fencing_token_current"
	InvariantVerificationEvidencePassed = "lkg.verification_evidence_passed"
)

var immutableInvariantIDs = []string{
	InvariantArtifactValidated,
	InvariantArtifactContentHash,
	InvariantGenerationMonotonic,
	InvariantShadowNoProductionImpact,
	InvariantCanaryScopeIsolation,
	InvariantBlastRadiusHardCap,
	InvariantKillSwitchPrecedence,
	InvariantFullPinnedRollback,
	InvariantFencingTokenCurrent,
	InvariantVerificationEvidencePassed,
}

type Violation struct {
	Invariant string
	Message   string
}

type Decision struct {
	Pass       bool
	Violations []Violation
}

func ImmutableInvariantIDs() []string {
	return append([]string(nil), immutableInvariantIDs...)
}

func ReleaseLaneKey(kind, scopeKey, channel string) string {
	return strings.Join([]string{
		strings.TrimSpace(strings.ToLower(kind)),
		strings.TrimSpace(strings.ToLower(scopeKey)),
		strings.TrimSpace(strings.ToLower(channel)),
	}, "|")
}

func EvaluateArtifactRelease(artifact model.PlatformArtifact, channel, pinnedRollbackGeneration, canaryRuleRef string) Decision {
	violations := []Violation{}
	if artifact.Status != model.PlatformArtifactStatusValidated {
		violations = append(violations, Violation{
			Invariant: InvariantArtifactValidated,
			Message:   "artifact must be validated before release",
		})
	}
	computedHash := artifactContentHash(artifact.Content)
	if !strings.HasPrefix(strings.TrimSpace(artifact.ContentHash), "sha256:") ||
		computedHash == "" ||
		!strings.EqualFold(strings.TrimSpace(artifact.ContentHash), computedHash) {
		violations = append(violations, Violation{
			Invariant: InvariantArtifactContentHash,
			Message:   "artifact content hash must use sha256 and match canonical content",
		})
	}
	if channel == model.PlatformArtifactReleaseChannelFull && strings.TrimSpace(pinnedRollbackGeneration) == "" {
		violations = append(violations, Violation{
			Invariant: InvariantFullPinnedRollback,
			Message:   "full release requires a pinned verified rollback generation",
		})
	}
	if channel == model.PlatformArtifactReleaseChannelGray && !CanaryScopeRefValid(canaryRuleRef) {
		violations = append(violations, Violation{
			Invariant: InvariantCanaryScopeIsolation,
			Message:   "gray release requires one bounded canary scope",
		})
	}
	return Decision{Pass: len(violations) == 0, Violations: violations}
}

func CanaryScopeRefValid(raw string) bool {
	raw = strings.TrimSpace(strings.ToLower(raw))
	switch raw {
	case "", "*", "all", "global", "scope=global", "scope:global":
		return false
	}
	var key, value string
	if left, right, ok := strings.Cut(raw, "="); ok {
		key, value = left, right
	} else if left, right, ok := strings.Cut(raw, ":"); ok {
		key, value = left, right
	} else {
		return false
	}
	switch strings.TrimSpace(key) {
	case "node", "edge", "edge_group", "failure_domain", "region", "country", "cohort", "consumer", "hostname", "service", "app":
	default:
		return false
	}
	value = strings.TrimSpace(value)
	if value == "" || value == "*" || value == "all" || value == "global" || strings.ContainsAny(value, ",;") {
		return false
	}
	return true
}

func EvaluateLKGPromotion(release model.PlatformArtifactRelease, req model.PlatformArtifactVerifyLKGRequest, hasExistingLKG bool) Decision {
	violations := []Violation{}
	initialShadowSeed := !hasExistingLKG &&
		req.AllowInitialLKG &&
		release.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow
	if hasExistingLKG && release.ReleaseChannel != model.PlatformArtifactReleaseChannelFull {
		violations = append(violations, Violation{
			Invariant: InvariantFullPinnedRollback,
			Message:   "only a full release can replace an existing verified LKG",
		})
	}
	if !hasExistingLKG && !initialShadowSeed {
		violations = append(violations, Violation{
			Invariant: InvariantFullPinnedRollback,
			Message:   "initial verified LKG requires an explicit shadow seed",
		})
	}
	if req.FencingToken <= 0 || req.FencingToken != release.FencingToken {
		violations = append(violations, Violation{
			Invariant: InvariantFencingTokenCurrent,
			Message:   "verification fencing token does not match the active release",
		})
	}
	if strings.TrimSpace(req.Reason) == "" {
		violations = append(violations, Violation{
			Invariant: InvariantVerificationEvidencePassed,
			Message:   "verification reason is required",
		})
	}
	if !hasExistingLKG && !req.AllowInitialLKG {
		violations = append(violations, Violation{
			Invariant: InvariantFullPinnedRollback,
			Message:   "initial verified LKG requires explicit allow_initial_lkg",
		})
	}
	evidence := req.Evidence
	missing := []string{}
	if !evidence.ConsumerConvergence {
		missing = append(missing, "consumer_convergence")
	}
	if !evidence.LocalProbe {
		missing = append(missing, "local_probe")
	}
	if !evidence.PublicSynthetic {
		missing = append(missing, "public_synthetic")
	}
	if !evidence.WatchWindow {
		missing = append(missing, "watch_window")
	}
	if !evidence.BaselineMonotonic {
		missing = append(missing, "baseline_monotonic")
	}
	if !evidence.DatabaseRollbackCompatible {
		missing = append(missing, "database_rollback_compatible")
	}
	if len(missing) > 0 {
		violations = append(violations, Violation{
			Invariant: InvariantVerificationEvidencePassed,
			Message:   fmt.Sprintf("verification evidence did not pass: %s", strings.Join(missing, ",")),
		})
	}
	return Decision{Pass: len(violations) == 0, Violations: violations}
}

func artifactContentHash(content map[string]any) string {
	if content == nil {
		return ""
	}
	payload, err := json.Marshal(content)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func VerificationEvidenceMap(req model.PlatformArtifactVerifyLKGRequest) map[string]string {
	return map[string]string{
		"baseline_monotonic":           fmt.Sprintf("%t", req.Evidence.BaselineMonotonic),
		"consumer_convergence":         fmt.Sprintf("%t", req.Evidence.ConsumerConvergence),
		"database_rollback_compatible": fmt.Sprintf("%t", req.Evidence.DatabaseRollbackCompatible),
		"evidence_refs":                strings.Join(normalizeStrings(req.Evidence.EvidenceRefs), ","),
		"expected_consumer_set_id":     strings.TrimSpace(req.Evidence.ExpectedConsumerSetID),
		"local_probe":                  fmt.Sprintf("%t", req.Evidence.LocalProbe),
		"public_synthetic":             fmt.Sprintf("%t", req.Evidence.PublicSynthetic),
		"reason":                       strings.TrimSpace(req.Reason),
		"watch_window":                 fmt.Sprintf("%t", req.Evidence.WatchWindow),
	}
}

func VerificationEvidenceHash(req model.PlatformArtifactVerifyLKGRequest) string {
	payload, _ := json.Marshal(VerificationEvidenceMap(req))
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func normalizeStrings(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
