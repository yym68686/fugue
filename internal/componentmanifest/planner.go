package componentmanifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
)

// DispatchMode is the most permissive release mode that a change plan may
// use.  A planner may only return independent when every directly affected
// component is independent and no mediated/exclusive shared resource needs a
// coordinator.
type DispatchMode string

const (
	DispatchModeLegacyShared DispatchMode = "legacy-shared"
	DispatchModeShadow       DispatchMode = "shadow-only"
	DispatchModeCoordinated  DispatchMode = "coordinated"
	DispatchModeIndependent  DispatchMode = "independent"
)

// ChangedPath is deterministic evidence for why a component was selected.
// Shared is true when the path belongs to a repository-wide shared source
// root; such a path cannot be released through an independent lane.
type ChangedPath struct {
	Path       string   `json:"path"`
	Components []string `json:"components"`
	Shared     bool     `json:"shared"`
}

// ComponentImpact identifies a component directly selected by one or more
// changed paths.
type ComponentImpact struct {
	ID            string `json:"id"`
	ReleaseLane   string `json:"releaseLane"`
	OwnershipMode string `json:"ownershipMode"`
}

// ResourceImpact is the explicit conflict graph edge that a release
// coordinator must inspect before mutating a shared resource.
type ResourceImpact struct {
	ID                  string   `json:"id"`
	Owner               string   `json:"owner"`
	ConflictMode        string   `json:"conflictMode"`
	Participants        []string `json:"participants"`
	AffectedComponents  []string `json:"affectedComponents"`
	RequiresCoordinator bool     `json:"requiresCoordinator"`
}

// ChangePlan is a side-effect-free, revision-local release decision.  It is
// intentionally a plan rather than an authorization: callers must still bind
// it to trusted commits/evidence and obtain the release-control fence before
// performing any mutation.
type ChangePlan struct {
	APIVersion               string            `json:"apiVersion"`
	Kind                     string            `json:"kind"`
	ManifestAPIVersion       string            `json:"manifestApiVersion"`
	MigrationPhase           string            `json:"migrationPhase"`
	LegacyRelease            string            `json:"legacyRelease"`
	ChangedPaths             []ChangedPath     `json:"changedPaths"`
	ImpactedComponents       []ComponentImpact `json:"impactedComponents"`
	ValidationOnlyComponents []string          `json:"validationOnlyComponents,omitempty"`
	SharedResources          []ResourceImpact  `json:"sharedResources,omitempty"`
	DispatchMode             DispatchMode      `json:"dispatchMode"`
	RequiresLegacyRelease    bool              `json:"requiresLegacyRelease"`
	PlanDigest               string            `json:"planDigest"`
}

const (
	changePlanAPIVersion = "component-plan.fugue.dev/v1"
	changePlanKind       = "ComponentChangePlan"
)

// PlanChanges validates the manifest and maps repository-relative changed
// paths to components, downstream validation consumers, and shared-resource
// conflicts.  It never reads the working tree and never performs a mutation.
func PlanChanges(manifest Manifest, changedPaths []string) (ChangePlan, error) {
	if err := manifest.Validate(); err != nil {
		return ChangePlan{}, fmt.Errorf("validate component manifest: %w", err)
	}
	if len(changedPaths) == 0 {
		return ChangePlan{}, fmt.Errorf("component change plan requires at least one changed path")
	}

	sharedRoots, err := normalizePathSet(manifest.SharedSourceRoots, "shared source root")
	if err != nil {
		return ChangePlan{}, err
	}
	componentRoots := make(map[string]map[string]struct{}, len(manifest.Components))
	componentsByID := make(map[string]Component, len(manifest.Components))
	for _, component := range manifest.Components {
		roots, rootErr := normalizePathSet(component.SourceRoots, "component source root")
		if rootErr != nil {
			return ChangePlan{}, fmt.Errorf("component %q: %w", component.ID, rootErr)
		}
		componentRoots[component.ID] = roots
		componentsByID[component.ID] = component
	}

	seenPaths := make(map[string]struct{}, len(changedPaths))
	plan := ChangePlan{
		APIVersion:         changePlanAPIVersion,
		Kind:               changePlanKind,
		ManifestAPIVersion: manifest.APIVersion,
		MigrationPhase:     manifest.MigrationPhase,
		LegacyRelease:      manifest.LegacyRelease,
		ChangedPaths:       make([]ChangedPath, 0, len(changedPaths)),
	}
	impactedIDs := make(map[string]struct{})
	for _, rawPath := range changedPaths {
		changedPath, pathErr := normalizeChangedPath(rawPath)
		if pathErr != nil {
			return ChangePlan{}, pathErr
		}
		if _, exists := seenPaths[changedPath]; exists {
			return ChangePlan{}, fmt.Errorf("component change plan repeats changed path %q", changedPath)
		}
		seenPaths[changedPath] = struct{}{}

		matched := make([]string, 0)
		shared := false
		for _, root := range sortedSetKeys(sharedRoots) {
			if changePathMatchesRoot(changedPath, root) {
				shared = true
				break
			}
		}
		for _, component := range manifest.Components {
			for _, root := range sortedSetKeys(componentRoots[component.ID]) {
				if changePathMatchesRoot(changedPath, root) {
					matched = append(matched, component.ID)
					break
				}
			}
		}
		if shared {
			// A shared root is intentionally a fail-safe fan-out.  The
			// component roots cannot overlap it (Validate enforces that), so
			// no component-specific match is expected here.
			matched = make([]string, 0, len(manifest.Components))
			for _, component := range manifest.Components {
				matched = append(matched, component.ID)
			}
		}
		if len(matched) == 0 {
			return ChangePlan{}, fmt.Errorf("component change path %q is not covered by a component or shared source root", changedPath)
		}
		sort.Strings(matched)
		for _, componentID := range matched {
			impactedIDs[componentID] = struct{}{}
		}
		plan.ChangedPaths = append(plan.ChangedPaths, ChangedPath{
			Path:       changedPath,
			Components: matched,
			Shared:     shared,
		})
	}
	sort.Slice(plan.ChangedPaths, func(i, j int) bool { return plan.ChangedPaths[i].Path < plan.ChangedPaths[j].Path })

	impactedSorted := sortedStringSet(impactedIDs)
	for _, componentID := range impactedSorted {
		component := componentsByID[componentID]
		plan.ImpactedComponents = append(plan.ImpactedComponents, ComponentImpact{
			ID:            component.ID,
			ReleaseLane:   component.ReleaseLane,
			OwnershipMode: component.OwnershipMode,
		})
	}

	// A component change must also validate every downstream consumer, but
	// those consumers are not implicitly released.  This is the key distinction
	// between independent artifact lanes and compatibility verification.
	reverseDependencies := make(map[string][]string, len(manifest.Components))
	for _, component := range manifest.Components {
		for _, dependency := range component.Dependencies {
			reverseDependencies[dependency] = append(reverseDependencies[dependency], component.ID)
		}
	}
	for dependency := range reverseDependencies {
		sort.Strings(reverseDependencies[dependency])
	}
	validationOnly := make(map[string]struct{})
	queue := append([]string(nil), impactedSorted...)
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, dependent := range reverseDependencies[current] {
			if _, direct := impactedIDs[dependent]; direct {
				continue
			}
			if _, alreadyQueued := validationOnly[dependent]; alreadyQueued {
				continue
			}
			validationOnly[dependent] = struct{}{}
			queue = append(queue, dependent)
		}
	}
	plan.ValidationOnlyComponents = sortedStringSet(validationOnly)

	for _, resource := range manifest.SharedResources {
		participants := uniqueSorted(append([]string{resource.Owner}, resource.Consumers...))
		affected := intersectSorted(participants, impactedSorted)
		if len(affected) == 0 {
			continue
		}
		requiresCoordinator := resource.ConflictMode != "read-only" && len(participants) > 1
		plan.SharedResources = append(plan.SharedResources, ResourceImpact{
			ID:                  resource.ID,
			Owner:               resource.Owner,
			ConflictMode:        resource.ConflictMode,
			Participants:        participants,
			AffectedComponents:  affected,
			RequiresCoordinator: requiresCoordinator,
		})
	}
	sort.Slice(plan.SharedResources, func(i, j int) bool { return plan.SharedResources[i].ID < plan.SharedResources[j].ID })

	plan.DispatchMode = dispatchMode(plan, componentsByID)
	// A shadow plan is deliberately not dispatchable yet, but its affected
	// component still lives inside the legacy release.  Keep this bit explicit
	// so callers cannot mistake shadow analysis for an independent production
	// authorization.
	plan.RequiresLegacyRelease = plan.DispatchMode == DispatchModeLegacyShared || plan.DispatchMode == DispatchModeShadow
	plan.PlanDigest = plan.Digest()
	return plan, nil
}

// VerifyDigest checks the immutable digest that binds a plan's complete
// changed-path and conflict graph.  It is safe to call on a copied plan.
func (plan ChangePlan) VerifyDigest() error {
	if plan.PlanDigest == "" {
		return fmt.Errorf("component change plan digest is empty")
	}
	if got := plan.Digest(); got != plan.PlanDigest {
		return fmt.Errorf("component change plan digest mismatch: got %s, want %s", plan.PlanDigest, got)
	}
	return nil
}

// Digest returns the canonical SHA-256 digest of the plan with its own digest
// field omitted.  All collections are sorted before this function is called
// by PlanChanges, making the result stable across process and map iteration.
func (plan ChangePlan) Digest() string {
	plan.PlanDigest = ""
	encoded, err := json.Marshal(plan)
	if err != nil {
		// ChangePlan contains only finite strings, booleans and slices, so this
		// is unreachable unless the type is changed without updating the plan.
		panic(fmt.Sprintf("encode component change plan: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func dispatchMode(plan ChangePlan, components map[string]Component) DispatchMode {
	for _, changed := range plan.ChangedPaths {
		if changed.Shared {
			return DispatchModeLegacyShared
		}
	}
	for _, component := range plan.ImpactedComponents {
		if components[component.ID].OwnershipMode != "independent" {
			return DispatchModeShadow
		}
	}
	for _, resource := range plan.SharedResources {
		if resource.RequiresCoordinator {
			return DispatchModeCoordinated
		}
	}
	return DispatchModeIndependent
}

func normalizeChangedPath(raw string) (string, error) {
	if raw == "" || path.IsAbs(raw) || strings.Contains(raw, "\\") {
		return "", fmt.Errorf("component change path %q must be a relative POSIX path", raw)
	}
	clean := path.Clean(raw)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("component change path %q escapes the repository", raw)
	}
	if clean != raw {
		return "", fmt.Errorf("component change path %q is not normalized; use %q", raw, clean)
	}
	return clean, nil
}

func changePathMatchesRoot(changedPath, root string) bool {
	return changedPath == root || strings.HasPrefix(changedPath, root+"/")
}

func sortedStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	return sortedStringSet(seen)
}

func intersectSorted(left, right []string) []string {
	allowed := make(map[string]struct{}, len(right))
	for _, value := range right {
		allowed[value] = struct{}{}
	}
	result := make(map[string]struct{})
	for _, value := range left {
		if _, ok := allowed[value]; ok {
			result[value] = struct{}{}
		}
	}
	return sortedStringSet(result)
}
