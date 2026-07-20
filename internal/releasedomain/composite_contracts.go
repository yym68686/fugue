package releasedomain

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	MultiDomainContractAPIVersion = "release-domain.fugue.dev/v2"

	BuildArtifactPlanKind   = "BuildArtifactPlan"
	BuildArtifactPlanPolicy = "artifact-build-plan-v1"

	ImageActivationPlanKind   = "ImageActivationPlan"
	ImageActivationPlanPolicy = "live-relative-image-activation-v1"

	CompositeReleasePlanKind   = "CompositeReleasePlan"
	CompositeReleasePlanPolicy = "evidence-derived-composite-saga-v1"
)

// BuildArtifactPlan records verified build outputs without implying that any
// artifact will be written into a production workload.
type BuildArtifactPlan struct {
	APIVersion         string          `json:"apiVersion"`
	Kind               string          `json:"kind"`
	Policy             string          `json:"policy"`
	BaseCommit         string          `json:"baseCommit"`
	TargetCommit       string          `json:"targetCommit"`
	ChangedFilesDigest string          `json:"changedFilesDigest"`
	Artifacts          []BuildArtifact `json:"artifacts"`
	Digest             string          `json:"digest"`
}

type BuildArtifact struct {
	Name             string `json:"name"`
	SourceBaseCommit string `json:"sourceBaseCommit"`
	ArtifactDigest   string `json:"artifactDigest"`
	ProvenanceDigest string `json:"provenanceDigest"`
}

// ImageActivationPlan contains only the image changes that a later adapter
// would write relative to the attested live state. An artifact absent from
// Activations remains build-only.
type ImageActivationPlan struct {
	APIVersion              string            `json:"apiVersion"`
	Kind                    string            `json:"kind"`
	Policy                  string            `json:"policy"`
	BaseCommit              string            `json:"baseCommit"`
	TargetCommit            string            `json:"targetCommit"`
	BuildArtifactPlanDigest string            `json:"buildArtifactPlanDigest"`
	LiveStateDigest         string            `json:"liveStateDigest"`
	Activations             []ImageActivation `json:"activations"`
	Digest                  string            `json:"digest"`
}

type ImageActivation struct {
	ID                    string             `json:"id"`
	ArtifactName          string             `json:"artifactName"`
	ArtifactDigest        string             `json:"artifactDigest"`
	Workload              ActivationWorkload `json:"workload"`
	Domain                Domain             `json:"domain"`
	Adapter               string             `json:"adapter"`
	LiveImageRef          string             `json:"liveImageRef"`
	TargetImageRef        string             `json:"targetImageRef"`
	ForwardRenderedDigest string             `json:"forwardRenderedDigest"`
	ReverseRenderedDigest string             `json:"reverseRenderedDigest"`
}

type ActivationWorkload struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	Container  string `json:"container"`
}

// CompositeReleasePlan is a dormant, evidence-only contract. It does not
// authorize or execute a mutation. A future TransactionEnvelope v2 consumer
// must independently bind ImageActivationPlanDigest before the first write.
type CompositeReleasePlan struct {
	APIVersion                string                 `json:"apiVersion"`
	Kind                      string                 `json:"kind"`
	Policy                    string                 `json:"policy"`
	BaseCommit                string                 `json:"baseCommit"`
	TargetCommit              string                 `json:"targetCommit"`
	ImageActivationPlanDigest string                 `json:"imageActivationPlanDigest"`
	Generation                string                 `json:"generation"`
	FencingEpoch              string                 `json:"fencingEpoch"`
	BaseVersions              []DomainVersion        `json:"baseVersions"`
	TargetVersions            []DomainVersion        `json:"targetVersions"`
	Steps                     []CompositeReleaseStep `json:"steps"`
	Digest                    string                 `json:"digest"`
}

type DomainVersion struct {
	Domain  Domain `json:"domain"`
	Version string `json:"version"`
}

type CompositeReleaseStep struct {
	ID                    string                     `json:"id"`
	Domain                Domain                     `json:"domain"`
	Adapter               string                     `json:"adapter"`
	DependsOn             []string                   `json:"dependsOn"`
	ActivationIDs         []string                   `json:"activationIds"`
	BaseVersion           string                     `json:"baseVersion"`
	TargetVersion         string                     `json:"targetVersion"`
	ForwardRenderedDigest string                     `json:"forwardRenderedDigest"`
	ReverseRenderedDigest string                     `json:"reverseRenderedDigest"`
	Observation           CompositeObservationPolicy `json:"observation"`
	RollbackBudgetSeconds string                     `json:"rollbackBudgetSeconds"`
}

type CompositeObservationPolicy struct {
	HealthEvidenceDigest string `json:"healthEvidenceDigest"`
	MinimumSamples       string `json:"minimumSamples"`
	WindowSeconds        string `json:"windowSeconds"`
}

func NewBuildArtifactPlan(baseCommit, targetCommit, changedFilesDigest string, artifacts []BuildArtifact) (BuildArtifactPlan, error) {
	plan := BuildArtifactPlan{
		APIVersion: MultiDomainContractAPIVersion, Kind: BuildArtifactPlanKind,
		Policy: BuildArtifactPlanPolicy, BaseCommit: baseCommit, TargetCommit: targetCommit,
		ChangedFilesDigest: changedFilesDigest, Artifacts: canonicalBuildArtifacts(artifacts),
	}
	plan.Digest = buildArtifactPlanDigest(plan)
	if err := VerifyBuildArtifactPlan(plan); err != nil {
		return BuildArtifactPlan{}, err
	}
	return plan, nil
}

func VerifyBuildArtifactPlan(plan BuildArtifactPlan) error {
	if plan.APIVersion != MultiDomainContractAPIVersion || plan.Kind != BuildArtifactPlanKind || plan.Policy != BuildArtifactPlanPolicy {
		return fmt.Errorf("build artifact plan identity is unsupported")
	}
	if err := validateRevisionPair(plan.BaseCommit, plan.TargetCommit, "build artifact plan"); err != nil {
		return err
	}
	if err := validateCanonicalSHA256Digest(plan.ChangedFilesDigest, "build artifact plan changed-files digest"); err != nil {
		return err
	}
	canonical := canonicalBuildArtifacts(plan.Artifacts)
	if !reflect.DeepEqual(canonical, plan.Artifacts) {
		return fmt.Errorf("build artifacts are not uniquely canonical")
	}
	for index, artifact := range plan.Artifacts {
		if !validContractIdentifier(artifact.Name) || (index > 0 && plan.Artifacts[index-1].Name == artifact.Name) {
			return fmt.Errorf("build artifact name is invalid or duplicated")
		}
		if err := validateTrustedGitCommit(artifact.SourceBaseCommit, "build artifact source base commit"); err != nil {
			return err
		}
		if err := validateCanonicalSHA256Digest(artifact.ArtifactDigest, "build artifact digest"); err != nil {
			return err
		}
		if err := validateCanonicalSHA256Digest(artifact.ProvenanceDigest, "build artifact provenance digest"); err != nil {
			return err
		}
	}
	return verifyContractDigest(plan.Digest, buildArtifactPlanDigest(plan), "build artifact plan")
}

func MarshalBuildArtifactPlan(plan BuildArtifactPlan) ([]byte, error) {
	if err := VerifyBuildArtifactPlan(plan); err != nil {
		return nil, err
	}
	return marshalOperationalJSON(plan)
}

func DecodeAndVerifyBuildArtifactPlan(reader io.Reader, expectedDigest string) (BuildArtifactPlan, error) {
	var plan BuildArtifactPlan
	if err := decodeStrictContract(reader, expectedDigest, reflect.TypeOf(plan), &plan, "build artifact plan"); err != nil {
		return BuildArtifactPlan{}, err
	}
	if err := VerifyBuildArtifactPlan(plan); err != nil {
		return BuildArtifactPlan{}, err
	}
	return plan, nil
}

func NewImageActivationPlan(baseCommit, targetCommit, buildPlanDigest, liveStateDigest string, activations []ImageActivation) (ImageActivationPlan, error) {
	plan := ImageActivationPlan{
		APIVersion: MultiDomainContractAPIVersion, Kind: ImageActivationPlanKind,
		Policy: ImageActivationPlanPolicy, BaseCommit: baseCommit, TargetCommit: targetCommit,
		BuildArtifactPlanDigest: buildPlanDigest, LiveStateDigest: liveStateDigest,
		Activations: canonicalImageActivations(activations),
	}
	plan.Digest = imageActivationPlanDigest(plan)
	if err := VerifyImageActivationPlan(plan); err != nil {
		return ImageActivationPlan{}, err
	}
	return plan, nil
}

func VerifyImageActivationPlan(plan ImageActivationPlan) error {
	if plan.APIVersion != MultiDomainContractAPIVersion || plan.Kind != ImageActivationPlanKind || plan.Policy != ImageActivationPlanPolicy {
		return fmt.Errorf("image activation plan identity is unsupported")
	}
	if err := validateRevisionPair(plan.BaseCommit, plan.TargetCommit, "image activation plan"); err != nil {
		return err
	}
	for label, digest := range map[string]string{
		"build artifact plan": plan.BuildArtifactPlanDigest,
		"live state":          plan.LiveStateDigest,
	} {
		if err := validateCanonicalSHA256Digest(digest, "image activation plan "+label+" digest"); err != nil {
			return err
		}
	}
	canonical := canonicalImageActivations(plan.Activations)
	if !reflect.DeepEqual(canonical, plan.Activations) {
		return fmt.Errorf("image activations are not uniquely canonical")
	}
	workloads := map[string]struct{}{}
	for index, activation := range plan.Activations {
		if !validContractIdentifier(activation.ID) || !validContractIdentifier(activation.ArtifactName) ||
			(index > 0 && plan.Activations[index-1].ID == activation.ID) {
			return fmt.Errorf("image activation identity is invalid or duplicated")
		}
		if err := validateCanonicalSHA256Digest(activation.ArtifactDigest, "image activation artifact digest"); err != nil {
			return err
		}
		if _, err := ParseDomain(string(activation.Domain)); err != nil {
			return fmt.Errorf("image activation domain: %w", err)
		}
		if expected, ok := fixedAdapterForDomain(activation.Domain); !ok || activation.Adapter != expected {
			return fmt.Errorf("image activation adapter does not match its fixed domain")
		}
		if err := validateActivationWorkload(activation.Workload); err != nil {
			return err
		}
		workloadKey := strings.Join([]string{activation.Workload.APIVersion, activation.Workload.Kind, activation.Workload.Namespace, activation.Workload.Name, activation.Workload.Container}, "\x00")
		if _, duplicate := workloads[workloadKey]; duplicate {
			return fmt.Errorf("image activation workload is duplicated")
		}
		workloads[workloadKey] = struct{}{}
		if !validContractText(activation.LiveImageRef, 1024) || !validContractText(activation.TargetImageRef, 1024) || activation.LiveImageRef == activation.TargetImageRef ||
			!strings.HasSuffix(activation.TargetImageRef, "@"+activation.ArtifactDigest) {
			return fmt.Errorf("image activation live/target reference binding is invalid")
		}
		for label, digest := range map[string]string{
			"forward rendered": activation.ForwardRenderedDigest,
			"reverse rendered": activation.ReverseRenderedDigest,
		} {
			if err := validateCanonicalSHA256Digest(digest, "image activation "+label+" digest"); err != nil {
				return err
			}
		}
	}
	return verifyContractDigest(plan.Digest, imageActivationPlanDigest(plan), "image activation plan")
}

func MarshalImageActivationPlan(plan ImageActivationPlan) ([]byte, error) {
	if err := VerifyImageActivationPlan(plan); err != nil {
		return nil, err
	}
	return marshalOperationalJSON(plan)
}

func DecodeAndVerifyImageActivationPlan(reader io.Reader, expectedDigest string) (ImageActivationPlan, error) {
	var plan ImageActivationPlan
	if err := decodeStrictContract(reader, expectedDigest, reflect.TypeOf(plan), &plan, "image activation plan"); err != nil {
		return ImageActivationPlan{}, err
	}
	if err := VerifyImageActivationPlan(plan); err != nil {
		return ImageActivationPlan{}, err
	}
	return plan, nil
}

func NewCompositeReleasePlan(plan CompositeReleasePlan) (CompositeReleasePlan, error) {
	plan.APIVersion = MultiDomainContractAPIVersion
	plan.Kind = CompositeReleasePlanKind
	plan.Policy = CompositeReleasePlanPolicy
	plan.BaseVersions = canonicalDomainVersions(plan.BaseVersions)
	plan.TargetVersions = canonicalDomainVersions(plan.TargetVersions)
	plan.Steps = cloneCompositeSteps(plan.Steps)
	for index := range plan.Steps {
		dependencies := canonicalContractStrings(plan.Steps[index].DependsOn)
		activations := canonicalContractStrings(plan.Steps[index].ActivationIDs)
		if len(dependencies) != len(plan.Steps[index].DependsOn) || len(activations) != len(plan.Steps[index].ActivationIDs) {
			return CompositeReleasePlan{}, fmt.Errorf("composite step dependencies or activations are duplicated")
		}
		plan.Steps[index].DependsOn = dependencies
		plan.Steps[index].ActivationIDs = activations
	}
	plan.Digest = compositeReleasePlanDigest(plan)
	if err := VerifyCompositeReleasePlan(plan); err != nil {
		return CompositeReleasePlan{}, err
	}
	return plan, nil
}

func VerifyCompositeReleasePlan(plan CompositeReleasePlan) error {
	if plan.APIVersion != MultiDomainContractAPIVersion || plan.Kind != CompositeReleasePlanKind || plan.Policy != CompositeReleasePlanPolicy {
		return fmt.Errorf("composite release plan identity is unsupported")
	}
	if err := validateRevisionPair(plan.BaseCommit, plan.TargetCommit, "composite release plan"); err != nil {
		return err
	}
	if err := validateCanonicalSHA256Digest(plan.ImageActivationPlanDigest, "composite image activation plan digest"); err != nil {
		return err
	}
	if !validPositiveDecimal(plan.Generation) || !validPositiveDecimal(plan.FencingEpoch) {
		return fmt.Errorf("composite generation and fencing epoch must be positive")
	}
	baseDomains, err := verifyDomainVersionVector(plan.BaseVersions, "base")
	if err != nil {
		return err
	}
	targetDomains, err := verifyDomainVersionVector(plan.TargetVersions, "target")
	if err != nil {
		return err
	}
	if len(baseDomains) < 2 || !equalDomains(baseDomains, targetDomains) {
		return fmt.Errorf("composite version vectors must contain the same two or more domains")
	}
	if len(plan.Steps) < 2 {
		return fmt.Errorf("composite release plan requires at least two ordered steps")
	}
	knownIDs := map[string]struct{}{}
	activationIDs := map[string]struct{}{}
	stepDomains := map[Domain]struct{}{}
	vectorDomains := map[Domain]struct{}{}
	baseVersionByDomain := map[Domain]string{}
	targetVersionByDomain := map[Domain]string{}
	for index, domain := range baseDomains {
		vectorDomains[domain] = struct{}{}
		baseVersionByDomain[domain] = plan.BaseVersions[index].Version
		targetVersionByDomain[domain] = plan.TargetVersions[index].Version
	}
	for _, step := range plan.Steps {
		if !validContractIdentifier(step.ID) {
			return fmt.Errorf("composite step identity is invalid")
		}
		if _, duplicate := knownIDs[step.ID]; duplicate {
			return fmt.Errorf("composite step identity is duplicated")
		}
		if _, ok := vectorDomains[step.Domain]; !ok {
			return fmt.Errorf("composite step domain is absent from the version vector")
		}
		if expected, ok := fixedAdapterForDomain(step.Domain); !ok || step.Adapter != expected {
			return fmt.Errorf("composite step adapter does not match its fixed domain")
		}
		if !reflect.DeepEqual(step.DependsOn, canonicalContractStrings(step.DependsOn)) ||
			!reflect.DeepEqual(step.ActivationIDs, canonicalContractStrings(step.ActivationIDs)) {
			return fmt.Errorf("composite step dependencies or activations are not canonical")
		}
		for _, dependency := range step.DependsOn {
			if _, ok := knownIDs[dependency]; !ok {
				return fmt.Errorf("composite step dependency must reference an earlier step")
			}
		}
		for _, activationID := range step.ActivationIDs {
			if !validContractIdentifier(activationID) {
				return fmt.Errorf("composite activation identity is invalid")
			}
			if _, duplicate := activationIDs[activationID]; duplicate {
				return fmt.Errorf("composite activation identity is assigned more than once")
			}
			activationIDs[activationID] = struct{}{}
		}
		for label, digest := range map[string]string{
			"base version":     step.BaseVersion,
			"target version":   step.TargetVersion,
			"forward rendered": step.ForwardRenderedDigest,
			"reverse rendered": step.ReverseRenderedDigest,
			"health evidence":  step.Observation.HealthEvidenceDigest,
		} {
			if err := validateCanonicalSHA256Digest(digest, "composite step "+label+" digest"); err != nil {
				return err
			}
		}
		if step.BaseVersion != baseVersionByDomain[step.Domain] || step.TargetVersion != targetVersionByDomain[step.Domain] {
			return fmt.Errorf("composite step versions do not match the domain-version vectors")
		}
		if !validPositiveDecimal(step.Observation.MinimumSamples) || !validPositiveDecimal(step.Observation.WindowSeconds) ||
			!validPositiveDecimal(step.RollbackBudgetSeconds) {
			return fmt.Errorf("composite observation and rollback budgets must be positive")
		}
		knownIDs[step.ID] = struct{}{}
		stepDomains[step.Domain] = struct{}{}
	}
	if len(stepDomains) < 2 {
		return fmt.Errorf("composite steps must cover at least two domains")
	}
	for domain := range vectorDomains {
		if _, ok := stepDomains[domain]; !ok {
			return fmt.Errorf("composite version-vector domain has no step")
		}
	}
	return verifyContractDigest(plan.Digest, compositeReleasePlanDigest(plan), "composite release plan")
}

func MarshalCompositeReleasePlan(plan CompositeReleasePlan) ([]byte, error) {
	if err := VerifyCompositeReleasePlan(plan); err != nil {
		return nil, err
	}
	return marshalOperationalJSON(plan)
}

func DecodeAndVerifyCompositeReleasePlan(reader io.Reader, expectedDigest string) (CompositeReleasePlan, error) {
	var plan CompositeReleasePlan
	if err := decodeStrictContract(reader, expectedDigest, reflect.TypeOf(plan), &plan, "composite release plan"); err != nil {
		return CompositeReleasePlan{}, err
	}
	if err := VerifyCompositeReleasePlan(plan); err != nil {
		return CompositeReleasePlan{}, err
	}
	return plan, nil
}

func canonicalBuildArtifacts(values []BuildArtifact) []BuildArtifact {
	result := append([]BuildArtifact(nil), values...)
	sort.Slice(result, func(left, right int) bool { return result[left].Name < result[right].Name })
	if result == nil {
		return []BuildArtifact{}
	}
	return result
}

func canonicalImageActivations(values []ImageActivation) []ImageActivation {
	result := append([]ImageActivation(nil), values...)
	sort.Slice(result, func(left, right int) bool { return result[left].ID < result[right].ID })
	if result == nil {
		return []ImageActivation{}
	}
	return result
}

func canonicalDomainVersions(values []DomainVersion) []DomainVersion {
	result := append([]DomainVersion(nil), values...)
	sort.Slice(result, func(left, right int) bool { return domainRank[result[left].Domain] < domainRank[result[right].Domain] })
	if result == nil {
		return []DomainVersion{}
	}
	return result
}

func verifyDomainVersionVector(values []DomainVersion, label string) ([]Domain, error) {
	if !reflect.DeepEqual(values, canonicalDomainVersions(values)) {
		return nil, fmt.Errorf("composite %s version vector is not canonical", label)
	}
	domains := make([]Domain, 0, len(values))
	for index, value := range values {
		if _, err := ParseDomain(string(value.Domain)); err != nil {
			return nil, fmt.Errorf("composite %s version domain: %w", label, err)
		}
		if index > 0 && values[index-1].Domain == value.Domain {
			return nil, fmt.Errorf("composite %s version domain is duplicated", label)
		}
		if err := validateCanonicalSHA256Digest(value.Version, "composite "+label+" version digest"); err != nil {
			return nil, err
		}
		domains = append(domains, value.Domain)
	}
	return domains, nil
}

func cloneCompositeSteps(values []CompositeReleaseStep) []CompositeReleaseStep {
	result := append([]CompositeReleaseStep(nil), values...)
	for index := range result {
		result[index].DependsOn = append([]string(nil), result[index].DependsOn...)
		result[index].ActivationIDs = append([]string(nil), result[index].ActivationIDs...)
	}
	if result == nil {
		return []CompositeReleaseStep{}
	}
	return result
}

func canonicalContractStrings(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	compacted := result[:0]
	for _, value := range result {
		if len(compacted) == 0 || compacted[len(compacted)-1] != value {
			compacted = append(compacted, value)
		}
	}
	if compacted == nil {
		return []string{}
	}
	return compacted
}

func fixedAdapterForDomain(domain Domain) (string, bool) {
	for _, binding := range fixedOperationalBindings() {
		if binding.Domain == domain {
			return binding.Adapter, true
		}
	}
	return "", false
}

func validateActivationWorkload(workload ActivationWorkload) error {
	for label, value := range map[string]string{
		"apiVersion": workload.APIVersion,
		"kind":       workload.Kind,
		"namespace":  workload.Namespace,
		"name":       workload.Name,
		"container":  workload.Container,
	} {
		if !validContractText(value, 253) {
			return fmt.Errorf("activation workload %s is invalid", label)
		}
	}
	return nil
}

func validateRevisionPair(baseCommit, targetCommit, label string) error {
	if err := validateTrustedGitCommit(baseCommit, label+" base commit"); err != nil {
		return err
	}
	if err := validateTrustedGitCommit(targetCommit, label+" target commit"); err != nil {
		return err
	}
	if baseCommit == targetCommit {
		return fmt.Errorf("%s base and target commits must differ", label)
	}
	return nil
}

func verifyContractDigest(actual, expected, label string) error {
	if err := validateCanonicalSHA256Digest(actual, label+" digest"); err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("%s digest mismatch", label)
	}
	return nil
}

func decodeStrictContract(reader io.Reader, expectedDigest string, valueType reflect.Type, output any, label string) error {
	if err := validateCanonicalSHA256Digest(expectedDigest, "expected "+label+" digest"); err != nil {
		return err
	}
	data, err := readOperationalEvidence(reader, label)
	if err != nil {
		return err
	}
	if err := validateStrictOperationalJSON(data, valueType, label); err != nil {
		return err
	}
	if err := json.Unmarshal(data, output); err != nil {
		return fmt.Errorf("decode %s: %w", label, err)
	}
	actual := ""
	switch value := output.(type) {
	case *BuildArtifactPlan:
		actual = value.Digest
	case *ImageActivationPlan:
		actual = value.Digest
	case *CompositeReleasePlan:
		actual = value.Digest
	default:
		return fmt.Errorf("decode %s into unsupported contract", label)
	}
	if actual != expectedDigest {
		return fmt.Errorf("%s external digest mismatch", label)
	}
	return nil
}

func validContractIdentifier(value string) bool {
	if !validContractText(value, 128) {
		return false
	}
	for index, char := range value {
		if index == 0 && (char < 'a' || char > 'z') {
			return false
		}
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func validContractText(value string, maximum int) bool {
	if !utf8.ValidString(value) || value == "" || len(value) > maximum || strings.TrimSpace(value) != value {
		return false
	}
	return strings.IndexFunc(value, unicode.IsSpace) == -1
}

func validPositiveDecimal(value string) bool {
	if len(value) == 0 || len(value) > 20 || value[0] < '1' || value[0] > '9' {
		return false
	}
	for _, digit := range value[1:] {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && parsed > 0
}

func buildArtifactPlanDigest(plan BuildArtifactPlan) string {
	plan.Digest = ""
	encoded, err := json.Marshal(plan)
	if err != nil {
		panic(fmt.Sprintf("marshal build artifact plan: %v", err))
	}
	return digestOperationalBytes(encoded)
}

func imageActivationPlanDigest(plan ImageActivationPlan) string {
	plan.Digest = ""
	encoded, err := json.Marshal(plan)
	if err != nil {
		panic(fmt.Sprintf("marshal image activation plan: %v", err))
	}
	return digestOperationalBytes(encoded)
}

func compositeReleasePlanDigest(plan CompositeReleasePlan) string {
	plan.Digest = ""
	encoded, err := json.Marshal(plan)
	if err != nil {
		panic(fmt.Sprintf("marshal composite release plan: %v", err))
	}
	return digestOperationalBytes(encoded)
}
