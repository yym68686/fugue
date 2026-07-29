package componentmanifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	shadowCoordinationAPIVersion = "component-coordination.fugue.dev/v1"
	shadowCoordinationKind       = "ShadowCoordinationPlan"
)

// CoordinationScope is one fenced key that the future release-control service
// must acquire.  Keys are globally sorted so two multi-resource releases
// cannot acquire the same set in opposite orders and deadlock.
type CoordinationScope struct {
	Key          string `json:"key"`
	ScopeType    string `json:"scopeType"`
	Owner        string `json:"owner"`
	ConflictMode string `json:"conflictMode"`
}

// ShadowCoordinationPlan translates a component change plan into the exact
// lane and shared-resource scopes that would be fenced.  ObservationOnly and
// ProductionMutationAllowed deliberately prevent this migration stage from
// becoming an accidental second release entrypoint.
type ShadowCoordinationPlan struct {
	APIVersion                string              `json:"apiVersion"`
	Kind                      string              `json:"kind"`
	ChangePlanDigest          string              `json:"changePlanDigest"`
	DispatchMode              DispatchMode        `json:"dispatchMode"`
	Scopes                    []CoordinationScope `json:"scopes"`
	RecoveryLanes             []string            `json:"recoveryLanes"`
	IdempotencyKey            string              `json:"idempotencyKey"`
	ObservationOnly           bool                `json:"observationOnly"`
	ProductionMutationAllowed bool                `json:"productionMutationAllowed"`
	Blockers                  []string            `json:"blockers"`
	CoordinationDigest        string              `json:"coordinationDigest"`
}

// BuildShadowCoordinationPlan verifies the immutable input plan and emits a
// deadlock-safe, observation-only coordination record.  It does not acquire a
// Lease, persist state, dispatch a workflow, or authorize mutation.
func BuildShadowCoordinationPlan(changePlan ChangePlan) (ShadowCoordinationPlan, error) {
	if changePlan.APIVersion != changePlanAPIVersion || changePlan.Kind != changePlanKind {
		return ShadowCoordinationPlan{}, fmt.Errorf("unsupported component change plan identity %q/%q", changePlan.APIVersion, changePlan.Kind)
	}
	if err := changePlan.VerifyDigest(); err != nil {
		return ShadowCoordinationPlan{}, err
	}

	plan := ShadowCoordinationPlan{
		APIVersion:                shadowCoordinationAPIVersion,
		Kind:                      shadowCoordinationKind,
		ChangePlanDigest:          changePlan.PlanDigest,
		DispatchMode:              changePlan.DispatchMode,
		ObservationOnly:           true,
		ProductionMutationAllowed: false,
		IdempotencyKey:            "component-shadow/" + strings.TrimPrefix(changePlan.PlanDigest, "sha256:"),
		Blockers:                  []string{"shadow coordination cannot authorize a production mutation"},
	}

	scopes := make(map[string]CoordinationScope)
	for _, component := range changePlan.ImpactedComponents {
		key := "lane/" + component.ReleaseLane
		scopes[key] = CoordinationScope{
			Key:          key,
			ScopeType:    "lane",
			Owner:        component.ID,
			ConflictMode: "exclusive",
		}
		plan.RecoveryLanes = append(plan.RecoveryLanes, component.ReleaseLane)
	}
	for _, resource := range changePlan.SharedResources {
		if !resource.RequiresCoordinator {
			continue
		}
		key := "resource/" + resource.ID
		scopes[key] = CoordinationScope{
			Key:          key,
			ScopeType:    "resource",
			Owner:        resource.Owner,
			ConflictMode: resource.ConflictMode,
		}
	}
	if changePlan.RequiresLegacyRelease {
		key := "legacy-release/" + changePlan.LegacyRelease
		scopes[key] = CoordinationScope{
			Key:          key,
			ScopeType:    "legacy-release",
			Owner:        "release-control",
			ConflictMode: "exclusive",
		}
	}

	keys := make([]string, 0, len(scopes))
	for key := range scopes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		plan.Scopes = append(plan.Scopes, scopes[key])
	}
	plan.RecoveryLanes = uniqueSorted(plan.RecoveryLanes)

	switch changePlan.DispatchMode {
	case DispatchModeLegacyShared:
		plan.Blockers = append(plan.Blockers, "a shared source path still requires the legacy release boundary")
	case DispatchModeShadow:
		plan.Blockers = append(plan.Blockers, "at least one affected component still has transitional-shared ownership")
	case DispatchModeCoordinated:
		plan.Blockers = append(plan.Blockers, "shared resources require an explicit fenced coordinator")
	case DispatchModeIndependent:
		plan.Blockers = append(plan.Blockers, "independent lane evidence has not been promoted through the production cutover gate")
	default:
		return ShadowCoordinationPlan{}, fmt.Errorf("unsupported component dispatch mode %q", changePlan.DispatchMode)
	}
	sort.Strings(plan.Blockers)
	plan.CoordinationDigest = plan.Digest()
	return plan, nil
}

// VerifyDigest rejects mutation of a shadow coordination record after it was
// derived from the component plan.
func (plan ShadowCoordinationPlan) VerifyDigest() error {
	if plan.CoordinationDigest == "" {
		return fmt.Errorf("shadow coordination digest is empty")
	}
	if got := plan.Digest(); got != plan.CoordinationDigest {
		return fmt.Errorf("shadow coordination digest mismatch: got %s, want %s", plan.CoordinationDigest, got)
	}
	return nil
}

// Digest returns the canonical SHA-256 digest with the self-referential field
// omitted.
func (plan ShadowCoordinationPlan) Digest() string {
	plan.CoordinationDigest = ""
	encoded, err := json.Marshal(plan)
	if err != nil {
		panic(fmt.Sprintf("encode shadow coordination plan: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}
