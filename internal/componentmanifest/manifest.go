// Package componentmanifest defines the machine-readable ownership contract
// used by Fugue's staged component and release-lane migration.
//
// The manifest is deliberately declarative and side-effect free.  It describes
// the target boundaries while the repository is still in a transitional,
// shared-release state; later release tooling can use the same validated
// document to compute affected components and resource conflicts.
package componentmanifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	APIVersion = "component.fugue.dev/v1"
	Kind       = "ComponentOwnershipManifest"
)

var (
	identifierPattern      = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
	contractPattern        = regexp.MustCompile(`^[a-z][a-z0-9.-]*@v[1-9][0-9]*$`)
	canonicalSHA256Pattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// Manifest is the root ownership and release-boundary document.
type Manifest struct {
	APIVersion        string           `yaml:"apiVersion" json:"apiVersion"`
	Kind              string           `yaml:"kind" json:"kind"`
	MigrationPhase    string           `yaml:"migrationPhase" json:"migrationPhase"`
	LegacyRelease     string           `yaml:"legacyRelease" json:"legacyRelease"`
	SharedSourceRoots []string         `yaml:"sharedSourceRoots" json:"sharedSourceRoots"`
	Components        []Component      `yaml:"components" json:"components"`
	SharedResources   []SharedResource `yaml:"sharedResources" json:"sharedResources"`
}

// Component describes one target service/domain boundary.  ownershipMode is
// transitional-shared until the component has its own data and release
// ownership; this prevents the manifest from claiming a migration that has
// not happened yet.
type Component struct {
	ID              string   `yaml:"id" json:"id"`
	Description     string   `yaml:"description" json:"description"`
	RuntimeKinds    []string `yaml:"runtimeKinds" json:"runtimeKinds"`
	OwnershipMode   string   `yaml:"ownershipMode" json:"ownershipMode"`
	SourceRoots     []string `yaml:"sourceRoots" json:"sourceRoots"`
	ArtifactKinds   []string `yaml:"artifactKinds" json:"artifactKinds"`
	ReleaseLane     string   `yaml:"releaseLane" json:"releaseLane"`
	Coordinator     string   `yaml:"coordinator" json:"coordinator"`
	OwnedState      []string `yaml:"ownedState" json:"ownedState"`
	Contracts       []string `yaml:"contracts" json:"contracts"`
	Dependencies    []string `yaml:"dependencies" json:"dependencies"`
	FailureBoundary string   `yaml:"failureBoundary" json:"failureBoundary"`
	LKGPolicy       string   `yaml:"lkgPolicy" json:"lkgPolicy"`
}

// SharedResource records a resource that deliberately remains shared during
// migration.  A mediated resource requires an explicit coordinator/adapter;
// an exclusive resource must never be mutated by two lanes concurrently.
type SharedResource struct {
	ID           string   `yaml:"id" json:"id"`
	Owner        string   `yaml:"owner" json:"owner"`
	ConflictMode string   `yaml:"conflictMode" json:"conflictMode"`
	Consumers    []string `yaml:"consumers" json:"consumers"`
}

// Digest binds a plan or persisted observation to the exact validated
// ownership manifest. Slice order is retained deliberately: the digest names
// the reviewed document, not merely an equivalent set of fields.
func (manifest Manifest) Digest() string {
	encoded, err := json.Marshal(manifest)
	if err != nil {
		panic(fmt.Sprintf("encode component manifest: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// Load parses exactly one strict YAML document and validates it.
func Load(reader io.Reader) (Manifest, error) {
	if reader == nil {
		return Manifest{}, errors.New("component manifest reader is nil")
	}
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode component manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return Manifest{}, fmt.Errorf("decode trailing component manifest: %w", err)
		}
		return Manifest{}, errors.New("component manifest must contain exactly one YAML document")
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// Validate verifies that ownership, release lanes, contracts and resource
// conflicts are deterministic and fail closed.
func (manifest Manifest) Validate() error {
	if manifest.APIVersion != APIVersion {
		return fmt.Errorf("component manifest apiVersion must be %q", APIVersion)
	}
	if manifest.Kind != Kind {
		return fmt.Errorf("component manifest kind must be %q", Kind)
	}
	if err := validateIdentifier(manifest.MigrationPhase, "migration phase"); err != nil {
		return err
	}
	if err := validateIdentifier(manifest.LegacyRelease, "legacy release"); err != nil {
		return err
	}
	if len(manifest.Components) == 0 {
		return errors.New("component manifest must define at least one component")
	}

	sharedRoots, err := normalizePathSet(manifest.SharedSourceRoots, "shared source root")
	if err != nil {
		return err
	}

	components := make(map[string]Component, len(manifest.Components))
	lanes := make(map[string]string, len(manifest.Components))
	artifacts := make(map[string]string)
	ownedState := make(map[string]string)
	type sourceClaim struct {
		component string
		root      string
	}
	sourceClaims := make([]sourceClaim, 0)
	orderedSharedRoots := sortedSetKeys(sharedRoots)
	for _, component := range manifest.Components {
		if err := validateIdentifier(component.ID, "component id"); err != nil {
			return err
		}
		if _, exists := components[component.ID]; exists {
			return fmt.Errorf("duplicate component id %q", component.ID)
		}
		if strings.TrimSpace(component.Description) == "" {
			return fmt.Errorf("component %q description must not be empty", component.ID)
		}
		if err := validateRuntimeKinds(component.RuntimeKinds, component.ID); err != nil {
			return err
		}
		if component.OwnershipMode != "transitional-shared" && component.OwnershipMode != "independent" {
			return fmt.Errorf("component %q ownershipMode must be transitional-shared or independent", component.ID)
		}
		if err := validateIdentifier(component.ReleaseLane, "release lane"); err != nil {
			return fmt.Errorf("component %q: %w", component.ID, err)
		}
		if previous, exists := lanes[component.ReleaseLane]; exists {
			return fmt.Errorf("release lane %q is claimed by both %q and %q", component.ReleaseLane, previous, component.ID)
		}
		lanes[component.ReleaseLane] = component.ID
		if err := validateIdentifier(component.Coordinator, "coordinator"); err != nil {
			return fmt.Errorf("component %q: %w", component.ID, err)
		}
		if err := validateIdentifier(component.FailureBoundary, "failure boundary"); err != nil {
			return fmt.Errorf("component %q: %w", component.ID, err)
		}
		if component.LKGPolicy != "required" && component.LKGPolicy != "optional" && component.LKGPolicy != "not-applicable" {
			return fmt.Errorf("component %q lkgPolicy must be required, optional, or not-applicable", component.ID)
		}
		roots, err := normalizePathSet(component.SourceRoots, "component source root")
		if err != nil {
			return fmt.Errorf("component %q: %w", component.ID, err)
		}
		for _, root := range sortedSetKeys(roots) {
			for _, shared := range orderedSharedRoots {
				if pathOverlaps(root, shared) {
					return fmt.Errorf("component %q source root %q overlaps shared source root %q; classify the path in only one place", component.ID, root, shared)
				}
			}
			for _, claim := range sourceClaims {
				if pathOverlaps(root, claim.root) {
					return fmt.Errorf("component %q source root %q overlaps component %q source root %q", component.ID, root, claim.component, claim.root)
				}
			}
			sourceClaims = append(sourceClaims, sourceClaim{component: component.ID, root: root})
		}
		for _, artifact := range component.ArtifactKinds {
			if err := validateIdentifier(artifact, "artifact kind"); err != nil {
				return fmt.Errorf("component %q: %w", component.ID, err)
			}
			if previous, exists := artifacts[artifact]; exists {
				return fmt.Errorf("artifact kind %q is claimed by both %q and %q", artifact, previous, component.ID)
			}
			artifacts[artifact] = component.ID
		}
		for _, state := range component.OwnedState {
			if err := validateIdentifier(state, "owned state"); err != nil {
				return fmt.Errorf("component %q: %w", component.ID, err)
			}
			if previous, exists := ownedState[state]; exists {
				return fmt.Errorf("owned state %q is claimed by both %q and %q", state, previous, component.ID)
			}
			ownedState[state] = component.ID
		}
		for _, contract := range component.Contracts {
			if !contractPattern.MatchString(contract) {
				return fmt.Errorf("component %q contract %q must use name@vN form", component.ID, contract)
			}
		}
		components[component.ID] = component
	}

	coordinator, exists := components["release-control"]
	if !exists || coordinator.Coordinator != "release-control" {
		return errors.New("manifest must define release-control as its own coordinator")
	}
	for _, component := range manifest.Components {
		if _, exists := components[component.Coordinator]; !exists {
			return fmt.Errorf("component %q references unknown coordinator %q", component.ID, component.Coordinator)
		}
		seenDependencies := make(map[string]struct{}, len(component.Dependencies))
		for _, dependency := range component.Dependencies {
			if err := validateIdentifier(dependency, "dependency"); err != nil {
				return fmt.Errorf("component %q: %w", component.ID, err)
			}
			if _, exists := seenDependencies[dependency]; exists {
				return fmt.Errorf("component %q repeats dependency %q", component.ID, dependency)
			}
			seenDependencies[dependency] = struct{}{}
			if dependency == component.ID {
				return fmt.Errorf("component %q cannot depend on itself", component.ID)
			}
			if _, exists := components[dependency]; !exists {
				return fmt.Errorf("component %q references unknown dependency %q", component.ID, dependency)
			}
		}
	}
	if err := validateDependencyAcyclic(components); err != nil {
		return err
	}

	resources := make(map[string]SharedResource, len(manifest.SharedResources))
	for _, resource := range manifest.SharedResources {
		if err := validateIdentifier(resource.ID, "shared resource id"); err != nil {
			return err
		}
		if _, exists := resources[resource.ID]; exists {
			return fmt.Errorf("duplicate shared resource id %q", resource.ID)
		}
		if resource.ConflictMode != "exclusive" && resource.ConflictMode != "mediated" && resource.ConflictMode != "read-only" {
			return fmt.Errorf("shared resource %q conflictMode must be exclusive, mediated, or read-only", resource.ID)
		}
		if _, exists := components[resource.Owner]; !exists {
			return fmt.Errorf("shared resource %q references unknown owner %q", resource.ID, resource.Owner)
		}
		seenConsumers := make(map[string]struct{}, len(resource.Consumers))
		for _, consumer := range resource.Consumers {
			if _, exists := components[consumer]; !exists {
				return fmt.Errorf("shared resource %q references unknown consumer %q", resource.ID, consumer)
			}
			if _, exists := seenConsumers[consumer]; exists {
				return fmt.Errorf("shared resource %q repeats consumer %q", resource.ID, consumer)
			}
			seenConsumers[consumer] = struct{}{}
		}
		resources[resource.ID] = resource
	}
	return nil
}

func validateIdentifier(value, label string) error {
	if !identifierPattern.MatchString(value) {
		return fmt.Errorf("%s %q must be lowercase kebab-case", label, value)
	}
	return nil
}

func validateRuntimeKinds(kinds []string, component string) error {
	if len(kinds) == 0 {
		return fmt.Errorf("component %q runtimeKinds must not be empty", component)
	}
	allowed := map[string]struct{}{"deployment": {}, "daemonset": {}, "job": {}, "cronjob": {}, "binary": {}, "coordinator": {}}
	seen := make(map[string]struct{}, len(kinds))
	for _, kind := range kinds {
		if _, ok := allowed[kind]; !ok {
			return fmt.Errorf("component %q has unknown runtime kind %q", component, kind)
		}
		if _, ok := seen[kind]; ok {
			return fmt.Errorf("component %q repeats runtime kind %q", component, kind)
		}
		seen[kind] = struct{}{}
	}
	return nil
}

func normalizePathSet(values []string, label string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(values))
	for _, raw := range values {
		if raw == "" || path.IsAbs(raw) || strings.Contains(raw, "\\") {
			return nil, fmt.Errorf("%s %q must be a relative POSIX path", label, raw)
		}
		clean := path.Clean(raw)
		if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
			return nil, fmt.Errorf("%s %q escapes the repository", label, raw)
		}
		if clean != raw {
			return nil, fmt.Errorf("%s %q is not normalized; use %q", label, raw, clean)
		}
		if _, exists := result[clean]; exists {
			return nil, fmt.Errorf("duplicate %s %q", label, clean)
		}
		result[clean] = struct{}{}
	}
	return result, nil
}

func pathOverlaps(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}

func sortedSetKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for value := range values {
		keys = append(keys, value)
	}
	sort.Strings(keys)
	return keys
}

func validateDependencyAcyclic(components map[string]Component) error {
	state := make(map[string]uint8, len(components))
	var visit func(string) error
	visit = func(id string) error {
		switch state[id] {
		case 1:
			return fmt.Errorf("component dependency cycle includes %q", id)
		case 2:
			return nil
		}
		state[id] = 1
		dependencies := append([]string(nil), components[id].Dependencies...)
		sort.Strings(dependencies)
		for _, dependency := range dependencies {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[id] = 2
		return nil
	}
	ids := make([]string, 0, len(components))
	for id := range components {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}
