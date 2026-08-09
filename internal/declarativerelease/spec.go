// Package declarativerelease defines the single production release contract.
//
// The package is intentionally side-effect free. It validates the component
// registry and immutable production intents, then computes the exact set of
// independently deployable lanes selected by a Git change. Build and cluster
// mutation code consume this plan; they do not reimplement component impact
// rules.
package declarativerelease

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strings"
)

const (
	RegistryAPIVersion = "release.fugue.dev/v2"
	RegistryKind       = "ProductionComponentRegistry"
	IntentAPIVersion   = "release.fugue.dev/v2"
	IntentKind         = "ProductionComponentIntent"
)

var (
	componentIDPattern           = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)
	edgeGroupIDPattern           = regexp.MustCompile(`^edge-group-[a-z0-9]+(?:-[a-z0-9]+)*$`)
	shaPattern                   = regexp.MustCompile(`^[0-9a-f]{40}$`)
	digestPattern                = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	repositoryPattern            = regexp.MustCompile(`^[a-z0-9.-]+(?::[0-9]+)?/[a-z0-9._/-]+$`)
	manifestVariableNamePattern  = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)
	manifestVariableValuePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$`)
	fieldManagerPattern          = regexp.MustCompile(
		`^[a-z0-9](?:[a-z0-9.-]{0,126}[a-z0-9])?$`,
	)
)

// Registry is the reviewed, repository-local inventory of all independently
// releasable production components. Components are ordered by ID so that the
// registry has one canonical representation.
type Registry struct {
	APIVersion            string      `json:"apiVersion"`
	Kind                  string      `json:"kind"`
	EdgeGroupRegistryPath string      `json:"edgeGroupRegistryPath,omitempty"`
	Components            []Component `json:"components"`
}

// Component contains only static release mechanics. Runtime version choices
// belong to Intent and immutable build receipts.
type Component struct {
	ID                  string             `json:"id"`
	Family              string             `json:"family"`
	IntentPath          string             `json:"intentPath"`
	ManifestPath        string             `json:"manifestPath"`
	ManifestVariables   map[string]string  `json:"manifestVariables,omitempty"`
	BootstrapLKGPath    string             `json:"bootstrapLkgPath,omitempty"`
	BootstrapRuntime    *BootstrapRuntime  `json:"bootstrapRuntime,omitempty"`
	SourceRoots         []string           `json:"sourceRoots"`
	Artifact            Artifact           `json:"artifact"`
	ArtifactTargets     []ArtifactTarget   `json:"artifactTargets,omitempty"`
	Workload            Workload           `json:"workload"`
	Transition          *Transition        `json:"transition,omitempty"`
	Health              []HealthProbe      `json:"health"`
	Concurrency         string             `json:"concurrency"`
	MigrationState      string             `json:"migrationState"`
	OwnershipAdoption   *OwnershipAdoption `json:"ownershipAdoption,omitempty"`
	AdoptionReceiptPath string             `json:"adoptionReceiptPath,omitempty"`
}

// OwnershipAdoption is the reviewed, one-time compatibility boundary for a
// workload that is still owned by a legacy Kubernetes field manager. It is
// copied into the immutable release plan; independent components must not
// carry it.
type OwnershipAdoption struct {
	LegacyFieldManager  string                   `json:"legacyFieldManager"`
	LegacyFieldManagers []string                 `json:"legacyFieldManagers,omitempty"`
	Resources           []OwnershipAdoptionScope `json:"resources"`
}

// OwnershipAdoptionScope limits force-conflicts to named resources and
// explicit JSON-pointer subtrees. The adoption adapter materializes a
// separate minimal SSA document from this allowlist.
type OwnershipAdoptionScope struct {
	Identity ResourceIdentity `json:"identity"`
	Fields   []string         `json:"fields"`
}

// ArtifactTarget identifies every container in the component resource set
// that must run the component's one immutable OCI artifact. This keeps image
// materialization declarative for multi-workload components without allowing
// arbitrary JSON patches or executable hooks.
type ArtifactTarget struct {
	APIVersion    string `json:"apiVersion"`
	Kind          string `json:"kind"`
	Namespace     string `json:"namespace"`
	Name          string `json:"name"`
	Container     string `json:"container"`
	ContainerType string `json:"containerType"`
}

type Artifact struct {
	Repository   string `json:"repository"`
	Dockerfile   string `json:"dockerfile"`
	Context      string `json:"context"`
	BuildPackage string `json:"buildPackage"`
}

// BootstrapRuntime binds a healthy legacy OnDelete Pod cohort whose running
// image differs from the workload's desired LKG template. It exists only for
// the explicit adopting phase and is removed once ownership converges.
type BootstrapRuntime struct {
	Resource    ResourceIdentity `json:"resource"`
	Container   string           `json:"container"`
	ImageDigest string           `json:"imageDigest"`
	OCIRevision string           `json:"ociRevision"`
}

type Workload struct {
	APIVersion           string `json:"apiVersion"`
	Kind                 string `json:"kind"`
	Namespace            string `json:"namespace"`
	Name                 string `json:"name"`
	Container            string `json:"container"`
	FieldManager         string `json:"fieldManager"`
	Replicas             int    `json:"replicas"`
	PreservedUnavailable int    `json:"preservedUnavailable,omitempty"`
	RolloutMode          string `json:"rolloutMode"`
}

// Transition declares the one non-standard rollout primitive that cannot be
// delegated to a Kubernetes workload controller. It is data, not an
// executable hook: the production entrypoint implements this exact allowlist
// and rejects arbitrary commands or component-name based behavior.
type Transition struct {
	Type        string                 `json:"type"`
	EdgeGroupAB *EdgeGroupABTransition `json:"edgeGroupAB,omitempty"`
}

// EdgeGroupABTransition binds one edge group to its front and worker slots.
// Each node owns its activation file locally, so the transition is performed
// and verified independently on every declared group node.
type EdgeGroupABTransition struct {
	GroupID             string `json:"groupId"`
	FrontName           string `json:"frontName"`
	WorkerAName         string `json:"workerAName"`
	WorkerBName         string `json:"workerBName"`
	WorkerContainer     string `json:"workerContainer"`
	ActivationStatePath string `json:"activationStatePath"`
	CASBinary           string `json:"casBinary"`
	ExpectedNodes       int    `json:"expectedNodes"`
	SoakSeconds         int    `json:"soakSeconds"`
}

// HealthProbe is data, not an executable hook. The production entrypoint
// supports this fixed allowlist and rejects arbitrary commands.
type HealthProbe struct {
	Type     string `json:"type"`
	Name     string `json:"name"`
	Port     string `json:"port,omitempty"`
	Path     string `json:"path,omitempty"`
	Expected string `json:"expected,omitempty"`
}

// Intent is changed in the same final commit as runtime code. Desired source
// identity is therefore the immutable push SHA and is deliberately absent
// from this document; self-referential Git commits are forbidden.
type Intent struct {
	APIVersion                  string `json:"apiVersion"`
	Kind                        string `json:"kind"`
	Component                   string `json:"component"`
	Generation                  int    `json:"generation"`
	ExpectedPreviousPresent     bool   `json:"expectedPreviousPresent"`
	ExpectedPreviousConfigSHA   string `json:"expectedPreviousConfigSha"`
	ExpectedPreviousManifestSHA string `json:"expectedPreviousManifestSha"`
	ExpectedPreviousOCIRevision string `json:"expectedPreviousOciRevision"`
	ExpectedPreviousImageDigest string `json:"expectedPreviousImageDigest"`
	SupersedesFailedConfigSHA   string `json:"supersedesFailedConfigSha,omitempty"`
	Rollback                    string `json:"rollback"`
}

type Plan struct {
	APIVersion string        `json:"apiVersion"`
	Kind       string        `json:"kind"`
	BaseSHA    string        `json:"baseSha"`
	HeadSHA    string        `json:"headSha"`
	Releases   []PlanRelease `json:"releases"`
	PlanDigest string        `json:"planDigest"`
}

type PlanRelease struct {
	ComponentID                 string             `json:"component"`
	ChangedPaths                []string           `json:"changedPaths"`
	IntentPath                  string             `json:"intentPath"`
	IntentDigest                string             `json:"intentDigest"`
	IntentGeneration            int                `json:"intentGeneration"`
	ExpectedPreviousPresent     bool               `json:"expectedPreviousPresent"`
	ExpectedPreviousConfigSHA   string             `json:"expectedPreviousConfigSha"`
	ExpectedPreviousManifestSHA string             `json:"expectedPreviousManifestSha"`
	ExpectedPreviousOCIRevision string             `json:"expectedPreviousOciRevision"`
	ExpectedPreviousImageDigest string             `json:"expectedPreviousImageDigest"`
	SupersedesFailedConfigSHA   string             `json:"supersedesFailedConfigSha,omitempty"`
	ManifestPath                string             `json:"manifestPath"`
	ManifestVariables           map[string]string  `json:"manifestVariables,omitempty"`
	AdoptionReceiptPath         string             `json:"adoptionReceiptPath,omitempty"`
	BootstrapLKGPath            string             `json:"bootstrapLkgPath,omitempty"`
	BootstrapRuntime            *BootstrapRuntime  `json:"bootstrapRuntime,omitempty"`
	RetrySameLKG                bool               `json:"retrySameLkg,omitempty"`
	Artifact                    Artifact           `json:"artifact"`
	ArtifactTargets             []ArtifactTarget   `json:"artifactTargets,omitempty"`
	Workload                    Workload           `json:"workload"`
	Transition                  *Transition        `json:"transition,omitempty"`
	Health                      []HealthProbe      `json:"health"`
	Concurrency                 string             `json:"concurrency"`
	MigrationState              string             `json:"migrationState"`
	OwnershipAdoption           *OwnershipAdoption `json:"ownershipAdoption,omitempty"`
}

func (release PlanRelease) receiptBoundHistoricalLKG() bool {
	expectedReceiptPath := "deploy/releases/" + release.ComponentID + "/adoption-receipt.json"
	return release.MigrationState == "independent" && release.RetrySameLKG && release.ExpectedPreviousPresent &&
		release.AdoptionReceiptPath == expectedReceiptPath && release.BootstrapLKGPath == "" && release.OwnershipAdoption == nil
}

// InitialExplicitBootstrapFailedAtomSuccessor is the only non-retry adopting
// successor allowed to resume a failed first production atom. The planner
// separately proves that SupersedesFailedConfigSHA is the immediately prior
// component intent and that both intents name this same bootstrap LKG.
func InitialExplicitBootstrapFailedAtomSuccessor(release PlanRelease) bool {
	bootstrap := release.BootstrapRuntime
	return release.MigrationState == "adopting" && release.IntentGeneration == 2 && !release.RetrySameLKG &&
		shaPattern.MatchString(release.SupersedesFailedConfigSHA) && release.ExpectedPreviousPresent &&
		shaPattern.MatchString(release.ExpectedPreviousConfigSHA) && release.ExpectedPreviousManifestSHA == release.ExpectedPreviousConfigSHA &&
		release.ExpectedPreviousOCIRevision == release.ExpectedPreviousConfigSHA && digestPattern.MatchString(release.ExpectedPreviousImageDigest) &&
		release.BootstrapLKGPath != "" && release.OwnershipAdoption != nil && len(release.OwnershipAdoption.Resources) > 0 &&
		bootstrap != nil && bootstrap.Resource.APIVersion == release.Workload.APIVersion && bootstrap.Resource.Kind == release.Workload.Kind &&
		bootstrap.Resource.Namespace == release.Workload.Namespace && bootstrap.Resource.Name == release.Workload.Name &&
		bootstrap.Container == release.Workload.Container && digestPattern.MatchString(bootstrap.ImageDigest) &&
		shaPattern.MatchString(bootstrap.OCIRevision)
}

// DecodeRegistry accepts exactly one strict JSON document.
func DecodeRegistry(reader io.Reader) (Registry, error) {
	var registry Registry
	if err := decodeStrict(reader, &registry); err != nil {
		return Registry{}, fmt.Errorf("decode production component registry: %w", err)
	}
	if err := registry.Validate(); err != nil {
		return Registry{}, err
	}
	return registry, nil
}

func DecodeIntent(reader io.Reader) (Intent, error) {
	if reader == nil {
		return Intent{}, errors.New("production intent reader is nil")
	}
	raw, err := io.ReadAll(io.LimitReader(reader, 1<<20))
	if err != nil {
		return Intent{}, fmt.Errorf("read production intent: %w", err)
	}
	var intent Intent
	if err := decodeStrict(bytes.NewReader(raw), &intent); err != nil {
		return Intent{}, fmt.Errorf("decode production component intent: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Intent{}, fmt.Errorf("decode production intent fields: %w", err)
	}
	if _, exists := fields["expectedPreviousPresent"]; !exists {
		return Intent{}, errors.New("production intent must explicitly declare expectedPreviousPresent")
	}
	if err := intent.Validate(); err != nil {
		return Intent{}, err
	}
	return intent, nil
}

func decodeStrict(reader io.Reader, target any) error {
	if reader == nil {
		return errors.New("reader is nil")
	}
	decoder := json.NewDecoder(io.LimitReader(reader, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON documents")
		}
		return err
	}
	return nil
}

func (registry Registry) Validate() error {
	if registry.APIVersion != RegistryAPIVersion || registry.Kind != RegistryKind {
		return fmt.Errorf("unsupported registry identity %q/%q", registry.APIVersion, registry.Kind)
	}
	if len(registry.Components) == 0 {
		return errors.New("production component registry is empty")
	}
	if registry.EdgeGroupRegistryPath != "" {
		if normalized, err := normalizeRepositoryPath(registry.EdgeGroupRegistryPath); err != nil || normalized != registry.EdgeGroupRegistryPath {
			return errors.New("edge group registry path is invalid")
		}
	}
	seenIDs := make(map[string]struct{}, len(registry.Components))
	seenIntents := make(map[string]string, len(registry.Components))
	seenWorkloads := make(map[string]string, len(registry.Components))
	for index, component := range registry.Components {
		if err := component.Validate(); err != nil {
			return fmt.Errorf("component %d: %w", index, err)
		}
		if index > 0 && registry.Components[index-1].ID >= component.ID {
			return errors.New("production components must be strictly ordered by id")
		}
		if _, exists := seenIDs[component.ID]; exists {
			return fmt.Errorf("duplicate component %q", component.ID)
		}
		seenIDs[component.ID] = struct{}{}
		if previous, exists := seenIntents[component.IntentPath]; exists {
			return fmt.Errorf("components %q and %q share intent path %q", previous, component.ID, component.IntentPath)
		}
		seenIntents[component.IntentPath] = component.ID
		workloadKey := component.Workload.Namespace + "/" + component.Workload.Kind + "/" + component.Workload.Name
		if previous, exists := seenWorkloads[workloadKey]; exists {
			return fmt.Errorf("components %q and %q share workload %q", previous, component.ID, workloadKey)
		}
		seenWorkloads[workloadKey] = component.ID
	}
	return nil
}

func (component Component) Validate() error {
	if !componentIDPattern.MatchString(component.ID) || !componentIDPattern.MatchString(component.Family) {
		return fmt.Errorf("component id/family is invalid: %q/%q", component.ID, component.Family)
	}
	for label, value := range map[string]string{
		"intentPath":   component.IntentPath,
		"manifestPath": component.ManifestPath,
		"dockerfile":   component.Artifact.Dockerfile,
	} {
		if _, err := normalizeRepositoryPath(value); err != nil {
			return fmt.Errorf("component %q %s: %w", component.ID, label, err)
		}
	}
	if component.BootstrapLKGPath != "" {
		if normalized, err := normalizeRepositoryPath(component.BootstrapLKGPath); err != nil || normalized != component.BootstrapLKGPath {
			return fmt.Errorf("component %q bootstrapLkgPath is invalid", component.ID)
		}
	}
	for key, value := range component.ManifestVariables {
		if !manifestVariableNamePattern.MatchString(key) || !manifestVariableValuePattern.MatchString(value) {
			return fmt.Errorf("component %q manifest variable %q is invalid", component.ID, key)
		}
	}
	if component.Artifact.Context != "." {
		if _, err := normalizeRepositoryPath(component.Artifact.Context); err != nil {
			return fmt.Errorf("component %q context: %w", component.ID, err)
		}
	}
	if !repositoryPattern.MatchString(component.Artifact.Repository) {
		return fmt.Errorf("component %q repository is invalid", component.ID)
	}
	if !strings.HasPrefix(component.Artifact.BuildPackage, "./cmd/") {
		return fmt.Errorf("component %q buildPackage must be a repository command package", component.ID)
	}
	if len(component.SourceRoots) == 0 {
		return fmt.Errorf("component %q has no source roots", component.ID)
	}
	for index, root := range component.SourceRoots {
		normalized, err := normalizeRepositoryPath(root)
		if err != nil || normalized != root {
			return fmt.Errorf("component %q source root %q is invalid", component.ID, root)
		}
		if index > 0 && component.SourceRoots[index-1] >= root {
			return fmt.Errorf("component %q source roots must be strictly ordered", component.ID)
		}
	}
	if component.Concurrency != "fugue-production-"+component.ID {
		return fmt.Errorf("component %q concurrency must be component-scoped", component.ID)
	}
	if component.MigrationState != "pending" && component.MigrationState != "adopting" && component.MigrationState != "independent" {
		return fmt.Errorf("component %q migrationState must be pending, adopting, or independent", component.ID)
	}
	if component.MigrationState == "independent" && component.OwnershipAdoption != nil {
		return fmt.Errorf("component %q independent lane retains ownership adoption", component.ID)
	}
	if component.BootstrapRuntime != nil {
		bootstrap := component.BootstrapRuntime
		if component.MigrationState != "adopting" || component.OwnershipAdoption == nil || component.BootstrapLKGPath == "" ||
			bootstrap.Resource.APIVersion != component.Workload.APIVersion || bootstrap.Resource.Kind != component.Workload.Kind ||
			bootstrap.Resource.Namespace != component.Workload.Namespace || bootstrap.Resource.Name != component.Workload.Name ||
			bootstrap.Container != component.Workload.Container || !digestPattern.MatchString(bootstrap.ImageDigest) ||
			!shaPattern.MatchString(bootstrap.OCIRevision) {
			return fmt.Errorf("component %q bootstrap runtime identity is invalid", component.ID)
		}
	}
	if component.AdoptionReceiptPath != "" {
		expected := "deploy/releases/" + component.ID + "/adoption-receipt.json"
		if component.MigrationState != "independent" || component.AdoptionReceiptPath != expected {
			return fmt.Errorf("component %q adoption receipt path is invalid", component.ID)
		}
	}
	if component.OwnershipAdoption != nil {
		if err := component.OwnershipAdoption.validate(component); err != nil {
			return err
		}
	}
	if err := component.Workload.validate(component.ID); err != nil {
		return err
	}
	if err := validateArtifactTargets(component); err != nil {
		return err
	}
	if err := component.Transition.validate(component); err != nil {
		return err
	}
	if len(component.Health) == 0 {
		return fmt.Errorf("component %q has no health probes", component.ID)
	}
	for index, probe := range component.Health {
		if err := probe.validate(); err != nil {
			return fmt.Errorf("component %q health probe %d: %w", component.ID, index, err)
		}
	}
	return nil
}

func (adoption OwnershipAdoption) validate(component Component) error {
	if !fieldManagerPattern.MatchString(adoption.LegacyFieldManager) || adoption.LegacyFieldManager == component.Workload.FieldManager {
		return fmt.Errorf("component %q ownership adoption legacy manager is invalid", component.ID)
	}
	if len(adoption.Resources) == 0 || len(adoption.Resources) > 16 {
		return fmt.Errorf("component %q ownership adoption resource count is invalid", component.ID)
	}
	managers := adoption.legacyManagers()
	if len(managers) == 0 || len(managers) > 8 {
		return fmt.Errorf("component %q ownership adoption legacy manager count is invalid", component.ID)
	}
	for index, manager := range managers {
		if !fieldManagerPattern.MatchString(manager) || manager == component.Workload.FieldManager ||
			(index > 0 && managers[index-1] >= manager) {
			return fmt.Errorf("component %q ownership adoption legacy managers are invalid or unordered", component.ID)
		}
	}
	if !stringSliceContains(managers, adoption.LegacyFieldManager) {
		return fmt.Errorf("component %q ownership adoption primary legacy manager is absent", component.ID)
	}
	previous := ""
	for index, scope := range adoption.Resources {
		key := scope.Identity.key()
		if !componentIDPattern.MatchString(scope.Identity.Namespace) || !componentIDPattern.MatchString(scope.Identity.Name) ||
			scope.Identity.APIVersion == "" || scope.Identity.Kind == "" || (previous != "" && previous >= key) {
			return fmt.Errorf("component %q ownership adoption resource %d is invalid or unordered", component.ID, index)
		}
		if len(scope.Fields) == 0 || len(scope.Fields) > 128 {
			return fmt.Errorf("component %q ownership adoption resource %d field count is invalid", component.ID, index)
		}
		for fieldIndex, field := range scope.Fields {
			if !strings.HasPrefix(field, "/") || strings.Contains(field, "//") ||
				(fieldIndex > 0 && scope.Fields[fieldIndex-1] >= field) {
				return fmt.Errorf("component %q ownership adoption fields must be strict ordered JSON pointers", component.ID)
			}
			if _, err := parseAdoptionPointer(field); err != nil {
				return fmt.Errorf("component %q ownership adoption field %q: %w", component.ID, field, err)
			}
		}
		previous = key
	}
	return nil
}

func (adoption OwnershipAdoption) legacyManagers() []string {
	if len(adoption.LegacyFieldManagers) == 0 {
		return []string{adoption.LegacyFieldManager}
	}
	return adoption.LegacyFieldManagers
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (transition *Transition) validate(component Component) error {
	if transition == nil {
		return nil
	}
	if transition.Type != "edge-group-ab" || transition.EdgeGroupAB == nil {
		return fmt.Errorf("component %q transition is unsupported", component.ID)
	}
	edge := transition.EdgeGroupAB
	if !strings.HasPrefix(component.ID, "edge-worker-") || component.Workload.Kind != "DaemonSet" || component.Workload.RolloutMode != "on-delete" {
		return fmt.Errorf("component %q edge-group-ab transition requires an on-delete edge worker", component.ID)
	}
	if !edgeGroupIDPattern.MatchString(edge.GroupID) ||
		!componentIDPattern.MatchString(edge.FrontName) || !componentIDPattern.MatchString(edge.WorkerAName) ||
		!componentIDPattern.MatchString(edge.WorkerBName) || !componentIDPattern.MatchString(edge.WorkerContainer) ||
		edge.ExpectedNodes < 1 || edge.ExpectedNodes > 100 || edge.CASBinary != "/usr/local/bin/fugue-edge-front-cas" ||
		edge.ActivationStatePath != "/var/lib/fugue-edge-front/activation.json" || edge.SoakSeconds != 180 {
		return fmt.Errorf("component %q edge-group-ab transition identity is invalid", component.ID)
	}
	if component.Workload.Name != edge.FrontName || component.Workload.Container != "edge-front" || edge.WorkerAName == edge.WorkerBName {
		return fmt.Errorf("component %q edge-group-ab workload binding is invalid", component.ID)
	}
	wanted := map[string]bool{edge.FrontName + "\x00edge-front": false, edge.WorkerAName + "\x00" + edge.WorkerContainer: false, edge.WorkerBName + "\x00" + edge.WorkerContainer: false}
	for _, target := range component.ArtifactTargets {
		key := target.Name + "\x00" + target.Container
		if _, exists := wanted[key]; exists && target.ContainerType == "container" {
			wanted[key] = true
		}
	}
	for key, found := range wanted {
		if !found {
			return fmt.Errorf("component %q edge-group-ab target %q is not artifact-bound", component.ID, key)
		}
	}
	return nil
}

func validateArtifactTargets(component Component) error {
	if len(component.ArtifactTargets) == 0 {
		return nil
	}
	primaryFound := false
	previous := ""
	for index, target := range component.ArtifactTargets {
		if err := target.validate(); err != nil {
			return fmt.Errorf("component %q artifact target %d: %w", component.ID, index, err)
		}
		key := target.key()
		if previous != "" && previous >= key {
			return fmt.Errorf("component %q artifact targets must be strictly identity ordered", component.ID)
		}
		if target.APIVersion == component.Workload.APIVersion && target.Kind == component.Workload.Kind &&
			target.Namespace == component.Workload.Namespace && target.Name == component.Workload.Name &&
			target.Container == component.Workload.Container && target.ContainerType == "container" {
			primaryFound = true
		}
		previous = key
	}
	if !primaryFound {
		return fmt.Errorf("component %q artifact targets omit the primary workload container", component.ID)
	}
	return nil
}

func (target ArtifactTarget) validate() error {
	if target.Kind != "Deployment" && target.Kind != "DaemonSet" && target.Kind != "Job" {
		return errors.New("artifact target kind is unsupported")
	}
	if (target.Kind == "Job" && target.APIVersion != "batch/v1") ||
		(target.Kind != "Job" && target.APIVersion != "apps/v1") {
		return errors.New("artifact target apiVersion does not match kind")
	}
	for _, value := range []string{target.Namespace, target.Name, target.Container} {
		if !componentIDPattern.MatchString(value) {
			return errors.New("artifact target identity is invalid")
		}
	}
	if target.ContainerType != "container" && target.ContainerType != "init-container" {
		return errors.New("artifact target containerType is invalid")
	}
	return nil
}

func (target ArtifactTarget) key() string {
	return target.APIVersion + "\x00" + target.Kind + "\x00" + target.Namespace + "\x00" + target.Name + "\x00" + target.ContainerType + "\x00" + target.Container
}

func (workload Workload) validate(componentID string) error {
	if workload.Kind != "Deployment" && workload.Kind != "DaemonSet" && workload.Kind != "Job" {
		return fmt.Errorf("component %q workload kind is unsupported", componentID)
	}
	if (workload.Kind == "Job" && workload.APIVersion != "batch/v1") ||
		(workload.Kind != "Job" && workload.APIVersion != "apps/v1") {
		return fmt.Errorf("component %q workload apiVersion does not match kind", componentID)
	}
	for label, value := range map[string]string{
		"namespace": workload.Namespace,
		"name":      workload.Name,
		"container": workload.Container,
	} {
		if !componentIDPattern.MatchString(value) {
			return fmt.Errorf("component %q workload %s is invalid", componentID, label)
		}
	}
	if !fieldManagerPattern.MatchString(workload.FieldManager) {
		return fmt.Errorf("component %q field manager is invalid", componentID)
	}
	if workload.Replicas < 0 || workload.Replicas > 10000 {
		return fmt.Errorf("component %q replicas is invalid", componentID)
	}
	allowedMode := map[string]bool{"rolling": true, "recreate": true, "on-delete": true, "job": true}
	if !allowedMode[workload.RolloutMode] {
		return fmt.Errorf("component %q rollout mode is invalid", componentID)
	}
	if workload.Kind == "Job" && workload.RolloutMode != "job" {
		return fmt.Errorf("component %q Job requires job rollout mode", componentID)
	}
	if workload.Kind == "Deployment" && workload.Replicas < 1 {
		return fmt.Errorf("component %q Deployment requires positive replicas", componentID)
	}
	if workload.Kind == "Deployment" && workload.RolloutMode != "rolling" && workload.RolloutMode != "recreate" {
		return fmt.Errorf("component %q Deployment rollout mode is invalid", componentID)
	}
	if workload.Kind != "Deployment" && workload.Replicas != 0 {
		return fmt.Errorf("component %q non-Deployment replicas must be zero", componentID)
	}
	if workload.PreservedUnavailable < 0 || (workload.Kind != "DaemonSet" && workload.PreservedUnavailable != 0) {
		return fmt.Errorf("component %q preservedUnavailable is invalid", componentID)
	}
	return nil
}

func (probe HealthProbe) validate() error {
	allowed := map[string]bool{
		"deployment":           true,
		"daemonset":            true,
		"job":                  true,
		"service-http":         true,
		"pod-http":             true,
		"leader-lease":         true,
		"edge-group-authority": true,
	}
	if !allowed[probe.Type] || !componentIDPattern.MatchString(probe.Name) {
		return errors.New("unsupported health probe")
	}
	if strings.ContainsAny(probe.Port+probe.Path+probe.Expected, "\r\n\x00") {
		return errors.New("health probe contains control characters")
	}
	if (probe.Type == "service-http" || probe.Type == "pod-http") &&
		(probe.Port == "" || !strings.HasPrefix(probe.Path, "/")) {
		return errors.New("HTTP health probe requires port and absolute path")
	}
	if probe.Type == "edge-group-authority" && (probe.Port != "" || probe.Path != "" || probe.Expected != "") {
		return errors.New("edge group authority probe does not accept HTTP fields")
	}
	return nil
}

func (intent Intent) Validate() error {
	if intent.APIVersion != IntentAPIVersion || intent.Kind != IntentKind {
		return fmt.Errorf("unsupported intent identity %q/%q", intent.APIVersion, intent.Kind)
	}
	if !componentIDPattern.MatchString(intent.Component) || intent.Generation < 1 {
		return errors.New("production intent component/generation is invalid")
	}
	if intent.ExpectedPreviousPresent {
		if !shaPattern.MatchString(intent.ExpectedPreviousConfigSHA) ||
			!shaPattern.MatchString(intent.ExpectedPreviousManifestSHA) ||
			!shaPattern.MatchString(intent.ExpectedPreviousOCIRevision) ||
			!digestPattern.MatchString(intent.ExpectedPreviousImageDigest) {
			return errors.New("production intent predecessor identity is invalid")
		}
	} else if intent.ExpectedPreviousConfigSHA != "" || intent.ExpectedPreviousManifestSHA != "" ||
		intent.ExpectedPreviousOCIRevision != "" || intent.ExpectedPreviousImageDigest != "" {
		return errors.New("absent production predecessor must be the first empty identity")
	}
	if intent.SupersedesFailedConfigSHA != "" {
		if !intent.ExpectedPreviousPresent || !shaPattern.MatchString(intent.SupersedesFailedConfigSHA) ||
			intent.SupersedesFailedConfigSHA == intent.ExpectedPreviousConfigSHA {
			return errors.New("superseded failed production atom identity is invalid")
		}
	}
	if intent.Rollback != "previous-git-lkg" {
		return errors.New("production intent rollback must be previous-git-lkg")
	}
	return nil
}

// BuildPlan maps changed paths to components and enforces that every runtime
// component selected by the change has an intent changed in the same commit.
// Unclassified paths are validation-only; callers decide whether their normal
// CI checks are sufficient for those paths.
func BuildPlan(registry Registry, baseSHA, headSHA string, changedPaths []string) (Plan, error) {
	if err := registry.Validate(); err != nil {
		return Plan{}, err
	}
	if !shaPattern.MatchString(baseSHA) || !shaPattern.MatchString(headSHA) || baseSHA == headSHA {
		return Plan{}, errors.New("release plan base/head identity is invalid")
	}
	if len(changedPaths) == 0 {
		return Plan{}, errors.New("release plan changed path set is empty")
	}
	changed := make(map[string]struct{}, len(changedPaths))
	for _, raw := range changedPaths {
		normalized, err := normalizeRepositoryPath(raw)
		if err != nil || normalized != raw {
			return Plan{}, fmt.Errorf("invalid changed path %q", raw)
		}
		if _, exists := changed[raw]; exists {
			return Plan{}, fmt.Errorf("duplicate changed path %q", raw)
		}
		changed[raw] = struct{}{}
	}

	type candidate struct {
		component     Component
		selectedPaths []string
		intentChanged bool
	}
	candidates := make([]candidate, 0, len(registry.Components))
	for _, component := range registry.Components {
		selectedPaths := make([]string, 0)
		for changedPath := range changed {
			if strings.HasSuffix(changedPath, "_test.go") {
				continue
			}
			if changedPath == component.ManifestPath {
				// Pending components have no production writer yet. Their shared
				// manifest template is configuration-only until the same atom
				// advances the component to adopting and changes its intent.
				if component.MigrationState == "pending" {
					continue
				}
				selectedPaths = append(selectedPaths, changedPath)
				continue
			}
			for _, root := range component.SourceRoots {
				if pathMatchesRoot(changedPath, root) {
					selectedPaths = append(selectedPaths, changedPath)
					break
				}
			}
		}
		_, intentChanged := changed[component.IntentPath]
		if len(selectedPaths) == 0 && !intentChanged {
			continue
		}
		sort.Strings(selectedPaths)
		candidates = append(candidates, candidate{component: component, selectedPaths: selectedPaths, intentChanged: intentChanged})
	}

	plan := Plan{APIVersion: IntentAPIVersion, Kind: "ProductionReleasePlan", BaseSHA: baseSHA, HeadSHA: headSHA}
	if len(candidates) == 0 {
		return plan, nil
	}
	intentCount := 0
	selectedIndex := -1
	for index, item := range candidates {
		if item.intentChanged {
			intentCount++
			selectedIndex = index
		}
	}
	if intentCount == 0 {
		return Plan{}, fmt.Errorf("component %q runtime change is missing same-commit production intent", candidates[0].component.ID)
	}
	if intentCount > 1 {
		return Plan{}, errors.New("runtime commit contains multiple production intents; split it into independent production atoms")
	}
	selected := candidates[selectedIndex]
	if selected.component.MigrationState != "adopting" && selected.component.MigrationState != "independent" {
		return Plan{}, fmt.Errorf("component %q is not migrated to the declarative release entrypoint", selected.component.ID)
	}
	selectedPathSet := make(map[string]struct{}, len(selected.selectedPaths))
	for _, changedPath := range selected.selectedPaths {
		selectedPathSet[changedPath] = struct{}{}
	}
	for index, item := range candidates {
		if index == selectedIndex || len(item.selectedPaths) == 0 {
			continue
		}
		if item.component.Artifact != selected.component.Artifact {
			return Plan{}, fmt.Errorf("runtime commit affects distinct component artifacts %q and %q; split the shared product boundary first", selected.component.ID, item.component.ID)
		}
		for _, changedPath := range item.selectedPaths {
			if changedPath == item.component.ManifestPath && changedPath != selected.component.ManifestPath {
				return Plan{}, fmt.Errorf("runtime commit changes multiple component manifests %q and %q", selected.component.ID, item.component.ID)
			}
			if _, shared := selectedPathSet[changedPath]; !shared {
				return Plan{}, fmt.Errorf("runtime path %q is not shared by selected component %q", changedPath, selected.component.ID)
			}
		}
	}
	plan.Releases = append(plan.Releases, PlanRelease{
		ComponentID:  selected.component.ID,
		ChangedPaths: selected.selectedPaths,
		IntentPath:   selected.component.IntentPath,
	})
	return plan, nil
}

// BindIntents turns a path-only plan into the immutable server-side plan used
// by build and deploy jobs. current contains intents from HeadSHA. previous
// contains intents from BaseSHA after the first v2 production atom.
func BindIntents(registry Registry, plan Plan, current, previous map[string]Intent, previousConfigSHA map[string]string) (Plan, error) {
	if plan.PlanDigest != "" {
		return Plan{}, errors.New("release plan is already bound")
	}
	components := make(map[string]Component, len(registry.Components))
	for _, component := range registry.Components {
		components[component.ID] = component
	}
	for index := range plan.Releases {
		release := &plan.Releases[index]
		component, exists := components[release.ComponentID]
		if !exists {
			return Plan{}, fmt.Errorf("release plan references unknown component %q", release.ComponentID)
		}
		intent, exists := current[release.ComponentID]
		if !exists {
			return Plan{}, fmt.Errorf("component %q intent was not loaded", release.ComponentID)
		}
		if err := intent.Validate(); err != nil {
			return Plan{}, fmt.Errorf("component %q intent: %w", release.ComponentID, err)
		}
		if intent.Component != component.ID {
			return Plan{}, fmt.Errorf("component %q intent identity mismatch", component.ID)
		}
		prior, hasPrior := previous[component.ID]
		retrySameLKG := false
		if hasPrior {
			if err := prior.Validate(); err != nil {
				return Plan{}, fmt.Errorf("component %q previous intent: %w", component.ID, err)
			}
			if prior.Component != component.ID || intent.Generation != prior.Generation+1 {
				return Plan{}, fmt.Errorf("component %q intent generation is not consecutive", component.ID)
			}
			priorConfigSHA := previousConfigSHA[component.ID]
			normalSuccessor := intent.ExpectedPreviousPresent && shaPattern.MatchString(priorConfigSHA) &&
				intent.ExpectedPreviousConfigSHA == priorConfigSHA &&
				intent.ExpectedPreviousManifestSHA == priorConfigSHA && intent.ExpectedPreviousOCIRevision == priorConfigSHA
			failedAtomSuccessor := intent.SupersedesFailedConfigSHA == priorConfigSHA &&
				intent.ExpectedPreviousPresent && intent.ExpectedPreviousConfigSHA == intent.ExpectedPreviousManifestSHA &&
				intent.ExpectedPreviousConfigSHA == intent.ExpectedPreviousOCIRevision
			retrySameLKG = intent.ExpectedPreviousPresent == prior.ExpectedPreviousPresent &&
				intent.ExpectedPreviousConfigSHA == prior.ExpectedPreviousConfigSHA &&
				intent.ExpectedPreviousManifestSHA == prior.ExpectedPreviousManifestSHA &&
				intent.ExpectedPreviousOCIRevision == prior.ExpectedPreviousOCIRevision &&
				intent.ExpectedPreviousImageDigest == prior.ExpectedPreviousImageDigest && intent.Rollback == prior.Rollback &&
				intent.SupersedesFailedConfigSHA == ""
			if !normalSuccessor && !failedAtomSuccessor && !retrySameLKG {
				return Plan{}, fmt.Errorf("component %q predecessor is not the prior production atom", component.ID)
			}
		} else if intent.Generation != 1 {
			return Plan{}, fmt.Errorf("component %q first v2 intent must use generation 1", component.ID)
		}
		intentBytes, err := CanonicalJSON(intent)
		if err != nil {
			return Plan{}, err
		}
		intentHash := sha256.Sum256(intentBytes)
		release.IntentDigest = fmt.Sprintf("sha256:%x", intentHash)
		release.IntentGeneration = intent.Generation
		release.ExpectedPreviousPresent = intent.ExpectedPreviousPresent
		release.ExpectedPreviousConfigSHA = intent.ExpectedPreviousConfigSHA
		release.ExpectedPreviousManifestSHA = intent.ExpectedPreviousManifestSHA
		release.ExpectedPreviousOCIRevision = intent.ExpectedPreviousOCIRevision
		release.ExpectedPreviousImageDigest = intent.ExpectedPreviousImageDigest
		release.SupersedesFailedConfigSHA = intent.SupersedesFailedConfigSHA
		if component.MigrationState == "adopting" && intent.ExpectedPreviousPresent && component.OwnershipAdoption == nil {
			return Plan{}, fmt.Errorf("component %q adopting predecessor has no explicit ownership adoption", component.ID)
		}
		release.ManifestPath = component.ManifestPath
		release.AdoptionReceiptPath = component.AdoptionReceiptPath
		release.ManifestVariables = make(map[string]string, len(component.ManifestVariables))
		for key, value := range component.ManifestVariables {
			release.ManifestVariables[key] = value
		}
		release.BootstrapLKGPath = component.BootstrapLKGPath
		if component.BootstrapRuntime != nil {
			copyBootstrap := *component.BootstrapRuntime
			release.BootstrapRuntime = &copyBootstrap
		}
		release.RetrySameLKG = retrySameLKG
		release.Artifact = component.Artifact
		release.ArtifactTargets = append([]ArtifactTarget(nil), component.ArtifactTargets...)
		if component.Transition != nil {
			copyTransition := *component.Transition
			if component.Transition.EdgeGroupAB != nil {
				copyEdge := *component.Transition.EdgeGroupAB
				copyTransition.EdgeGroupAB = &copyEdge
			}
			release.Transition = &copyTransition
		}
		release.Workload = component.Workload
		release.Health = append([]HealthProbe(nil), component.Health...)
		release.Concurrency = component.Concurrency
		release.MigrationState = component.MigrationState
		if component.OwnershipAdoption != nil {
			copyAdoption := *component.OwnershipAdoption
			copyAdoption.LegacyFieldManagers = append([]string(nil), component.OwnershipAdoption.LegacyFieldManagers...)
			copyAdoption.Resources = append([]OwnershipAdoptionScope(nil), component.OwnershipAdoption.Resources...)
			for index := range copyAdoption.Resources {
				copyAdoption.Resources[index].Fields = append([]string(nil), component.OwnershipAdoption.Resources[index].Fields...)
			}
			release.OwnershipAdoption = &copyAdoption
		}
	}
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		return Plan{}, err
	}
	digest := sha256.Sum256(unsigned)
	plan.PlanDigest = fmt.Sprintf("sha256:%x", digest)
	return plan, nil
}

func normalizeRepositoryPath(value string) (string, error) {
	if value == "" || strings.Contains(value, "\\") || strings.HasPrefix(value, "/") || strings.ContainsRune(value, '\x00') {
		return "", errors.New("repository path is invalid")
	}
	normalized := path.Clean(value)
	if normalized == "." || normalized == ".." || strings.HasPrefix(normalized, "../") {
		return "", errors.New("repository path escapes root")
	}
	return normalized, nil
}

func pathMatchesRoot(changedPath, root string) bool {
	return changedPath == root || strings.HasPrefix(changedPath, root+"/")
}

// CanonicalJSON returns the byte-exact receipt representation.
func CanonicalJSON(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var generic any
	if err := decoder.Decode(&generic); err != nil {
		return nil, err
	}
	return json.Marshal(generic)
}
