package releasedomain

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"fugue/internal/releasecontract"
)

const (
	MultiDomainContractAPIVersion = releasecontract.MultiDomainContractAPIVersion

	BuildArtifactPlanKind   = "BuildArtifactPlan"
	BuildArtifactPlanPolicy = "artifact-build-plan-v1"

	ImageActivationPlanKind   = "ImageActivationPlan"
	ImageActivationPlanPolicy = "live-relative-image-activation-v1"

	CompositeReleasePlanKind   = releasecontract.CompositeReleasePlanKind
	CompositeReleasePlanPolicy = releasecontract.CompositeReleasePlanPolicy
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

type CompositeReleasePlan = releasecontract.CompositeReleasePlan
type DomainVersion = releasecontract.DomainVersion
type CompositeReleaseStep = releasecontract.CompositeReleaseStep
type CompositeObservationPolicy = releasecontract.CompositeObservationPolicy

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
	return releasecontract.NewCompositeReleasePlan(plan)
}

func VerifyCompositeReleasePlan(plan CompositeReleasePlan) error {
	return releasecontract.VerifyCompositeReleasePlan(plan)
}

func MarshalCompositeReleasePlan(plan CompositeReleasePlan) ([]byte, error) {
	return releasecontract.MarshalCompositeReleasePlan(plan)
}

func DecodeAndVerifyCompositeReleasePlan(reader io.Reader, expectedDigest string) (CompositeReleasePlan, error) {
	return releasecontract.DecodeAndVerifyCompositeReleasePlan(reader, expectedDigest)
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

func cloneCompositeSteps(values []CompositeReleaseStep) []CompositeReleaseStep {
	return releasecontract.CloneCompositeSteps(values)
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
	return releasecontract.AdapterForDomain(domain)
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
	return releasecontract.DigestCompositeReleasePlan(plan)
}
