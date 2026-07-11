package platformsafety

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEvaluateRollbackAssetPreflightPassesCompleteFreshInventory(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	requirements := rollbackAssetRequirementsForTest()
	evidence := make([]RollbackAssetEvidence, 0, len(requirements))
	for _, requirement := range requirements {
		evidence = append(evidence, passingRollbackAssetEvidenceForTest(requirement, now))
	}

	result := EvaluateRollbackAssetPreflight(requirements, evidence, now)
	if !result.Pass || len(result.Checks) != len(requirements) {
		t.Fatalf("complete rollback preflight = %+v", result)
	}
	for _, check := range result.Checks {
		if !check.Pass || check.State != model.InvariantEvidenceStatePass {
			t.Fatalf("expected every rollback check to pass: %+v", check)
		}
	}
}

func TestEvaluateRollbackAssetPreflightFailsClosed(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	base := rollbackAssetRequirementsForTest()[0]
	pass := passingRollbackAssetEvidenceForTest(base, now)

	tests := []struct {
		name         string
		requirements []RollbackAssetRequirement
		evidence     []RollbackAssetEvidence
		wantState    string
		wantMessage  string
	}{
		{
			name:        "empty requirements",
			wantState:   model.InvariantEvidenceStateUnknown,
			wantMessage: "requirements are empty",
		},
		{
			name:         "missing evidence",
			requirements: []RollbackAssetRequirement{base},
			wantState:    model.InvariantEvidenceStateUnknown,
			wantMessage:  "evidence is missing",
		},
		{
			name: "malformed image digest",
			requirements: []RollbackAssetRequirement{{
				Kind: RollbackAssetKindImageDigest, ScopeKey: "control-plane", Reference: "ghcr.io/acme/api", Identity: "sha256:short",
			}},
			wantState:   model.InvariantEvidenceStateFail,
			wantMessage: "full sha256 digest",
		},
		{
			name:         "identity mismatch",
			requirements: []RollbackAssetRequirement{base},
			evidence: []RollbackAssetEvidence{func() RollbackAssetEvidence {
				observation := pass
				observation.Identity = "sha256:" + strings.Repeat("b", 64)
				return observation
			}()},
			wantState:   model.InvariantEvidenceStateFail,
			wantMessage: "does not match",
		},
		{
			name:         "duplicate evidence",
			requirements: []RollbackAssetRequirement{base},
			evidence:     []RollbackAssetEvidence{pass, pass},
			wantState:    model.InvariantEvidenceStateFail,
			wantMessage:  "duplicate evidence",
		},
		{
			name:         "unknown evidence state",
			requirements: []RollbackAssetRequirement{base},
			evidence: []RollbackAssetEvidence{func() RollbackAssetEvidence {
				observation := pass
				observation.State = model.InvariantEvidenceStateUnknown
				return observation
			}()},
			wantState:   model.InvariantEvidenceStateUnknown,
			wantMessage: "state is unknown",
		},
		{
			name:         "failed evidence state",
			requirements: []RollbackAssetRequirement{base},
			evidence: []RollbackAssetEvidence{func() RollbackAssetEvidence {
				observation := pass
				observation.State = model.InvariantEvidenceStateFail
				return observation
			}()},
			wantState:   model.InvariantEvidenceStateFail,
			wantMessage: "state is fail",
		},
		{
			name:         "stale evidence state",
			requirements: []RollbackAssetRequirement{base},
			evidence: []RollbackAssetEvidence{func() RollbackAssetEvidence {
				observation := pass
				observation.State = model.InvariantEvidenceStateStale
				return observation
			}()},
			wantState:   model.InvariantEvidenceStateStale,
			wantMessage: "state is stale",
		},
		{
			name:         "unsupported evidence state",
			requirements: []RollbackAssetRequirement{base},
			evidence: []RollbackAssetEvidence{func() RollbackAssetEvidence {
				observation := pass
				observation.State = "degraded"
				return observation
			}()},
			wantState:   model.InvariantEvidenceStateUnknown,
			wantMessage: "missing or unsupported",
		},
		{
			name:         "expired evidence",
			requirements: []RollbackAssetRequirement{base},
			evidence: []RollbackAssetEvidence{func() RollbackAssetEvidence {
				observation := pass
				observation.ExpiresAt = now
				return observation
			}()},
			wantState:   model.InvariantEvidenceStateStale,
			wantMessage: "evidence is stale",
		},
		{
			name:         "future observation",
			requirements: []RollbackAssetRequirement{base},
			evidence: []RollbackAssetEvidence{func() RollbackAssetEvidence {
				observation := pass
				observation.ObservedAt = now.Add(time.Second)
				return observation
			}()},
			wantState:   model.InvariantEvidenceStateUnknown,
			wantMessage: "in the future",
		},
		{
			name:         "missing source identity",
			requirements: []RollbackAssetRequirement{base},
			evidence: []RollbackAssetEvidence{func() RollbackAssetEvidence {
				observation := pass
				observation.SourceID = ""
				return observation
			}()},
			wantState:   model.InvariantEvidenceStateUnknown,
			wantMessage: "source identity is missing",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result := EvaluateRollbackAssetPreflight(test.requirements, test.evidence, now)
			if result.Pass || len(result.Checks) != 1 {
				t.Fatalf("expected one fail-closed check, got %+v", result)
			}
			check := result.Checks[0]
			if check.Pass || check.State != test.wantState || !strings.Contains(check.Message, test.wantMessage) {
				t.Fatalf("check = %+v, want state=%q message containing %q", check, test.wantState, test.wantMessage)
			}
		})
	}
}

func TestEvaluateRollbackAssetPreflightRejectsDuplicateRequirements(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	requirement := rollbackAssetRequirementsForTest()[1]
	evidence := passingRollbackAssetEvidenceForTest(requirement, now)
	result := EvaluateRollbackAssetPreflight(
		[]RollbackAssetRequirement{requirement, requirement},
		[]RollbackAssetEvidence{evidence},
		now,
	)
	if result.Pass || len(result.Checks) != 2 || !result.Checks[0].Pass || result.Checks[1].Pass {
		t.Fatalf("duplicate requirement must fail closed without hiding the first result: %+v", result)
	}
	if result.Checks[1].State != model.InvariantEvidenceStateFail ||
		!strings.Contains(result.Checks[1].Message, "duplicated") {
		t.Fatalf("duplicate requirement check = %+v", result.Checks[1])
	}
}

func rollbackAssetRequirementsForTest() []RollbackAssetRequirement {
	return []RollbackAssetRequirement{
		{
			Kind: RollbackAssetKindImageDigest, ScopeKey: "control-plane", Reference: "ghcr.io/acme/fugue-api", Identity: "sha256:" + strings.Repeat("a", 64),
		},
		{
			Kind: RollbackAssetKindCaddyConfig, ScopeKey: "edge:global", Reference: "caddy-routes", Identity: "caddy_gen_7",
		},
		{
			Kind: RollbackAssetKindDNSBundle, ScopeKey: "dns:global", Reference: "authoritative-answers", Identity: "dns_gen_7",
		},
		{
			Kind: RollbackAssetKindEdgeRouteBundle, ScopeKey: "edge:global", Reference: "http-routes", Identity: "routes_gen_7",
		},
		{
			Kind: RollbackAssetKindNodeDesiredState, ScopeKey: "node:edge-a", Reference: "node-updater", Identity: "node_gen_7",
		},
	}
}

func passingRollbackAssetEvidenceForTest(requirement RollbackAssetRequirement, now time.Time) RollbackAssetEvidence {
	return RollbackAssetEvidence{
		Kind:       requirement.Kind,
		ScopeKey:   requirement.ScopeKey,
		Reference:  requirement.Reference,
		Identity:   requirement.Identity,
		State:      model.InvariantEvidenceStatePass,
		SourceID:   "preflight-probe-a",
		ObservedAt: now.Add(-time.Minute),
		ExpiresAt:  now.Add(5 * time.Minute),
	}
}
