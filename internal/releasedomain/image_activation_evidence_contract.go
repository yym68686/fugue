package releasedomain

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
)

const (
	ImageActivationEvidenceKind   = "ImageActivationEvidence"
	ImageActivationEvidencePolicy = "resolved-built-only-unresolved-v1"

	ImageActivationGapAbsentCreate       = "absent-create"
	ImageActivationGapTargetNotImmutable = "target-image-not-immutable"
	ImageActivationGapArtifactNotBuilt   = "artifact-not-built"
	ImageActivationGapArtifactAmbiguous  = "artifact-ambiguous"
	ImageActivationGapOwnershipMissing   = "ownership-missing"
	ImageActivationGapOwnershipAmbiguous = "ownership-ambiguous"
	ImageActivationGapAdapterMissing     = "adapter-missing"
)

// ImageActivationEvidence binds a resolved ImageActivationPlan while keeping
// every actual rendered image change that could not be resolved explicit.
// Complete is false whenever Unresolved is non-empty; consumers must never
// treat a partial resolved plan as authorization.
type ImageActivationEvidence struct {
	APIVersion                        string               `json:"apiVersion"`
	Kind                              string               `json:"kind"`
	Policy                            string               `json:"policy"`
	BaseCommit                        string               `json:"baseCommit"`
	TargetCommit                      string               `json:"targetCommit"`
	BuildArtifactPlanDigest           string               `json:"buildArtifactPlanDigest"`
	ResolvedImageActivationPlanDigest string               `json:"resolvedImageActivationPlanDigest"`
	BuiltOnlyArtifacts                []string             `json:"builtOnlyArtifacts"`
	Unresolved                        []ImageActivationGap `json:"unresolved"`
	Complete                          bool                 `json:"complete"`
	Digest                            string               `json:"digest"`
}

// ImageActivationGap is one observed rendered workload/container image change
// that cannot enter the resolved activation plan without inventing evidence.
type ImageActivationGap struct {
	ID                     string             `json:"id"`
	Workload               ActivationWorkload `json:"workload"`
	LiveImageRef           string             `json:"liveImageRef"`
	TargetImageRef         string             `json:"targetImageRef"`
	ArtifactDigest         string             `json:"artifactDigest"`
	MatchingBuildArtifacts []string           `json:"matchingBuildArtifacts"`
	OwnershipDomains       []Domain           `json:"ownershipDomains"`
	Reason                 string             `json:"reason"`
	ForwardRenderedDigest  string             `json:"forwardRenderedDigest"`
	ReverseRenderedDigest  string             `json:"reverseRenderedDigest"`
}

func NewImageActivationEvidence(evidence ImageActivationEvidence) (ImageActivationEvidence, error) {
	evidence.APIVersion = MultiDomainContractAPIVersion
	evidence.Kind = ImageActivationEvidenceKind
	evidence.Policy = ImageActivationEvidencePolicy
	builtOnly := canonicalContractStrings(evidence.BuiltOnlyArtifacts)
	if len(builtOnly) != len(evidence.BuiltOnlyArtifacts) {
		return ImageActivationEvidence{}, fmt.Errorf("built-only artifacts are duplicated")
	}
	evidence.BuiltOnlyArtifacts = builtOnly
	evidence.Unresolved = append([]ImageActivationGap(nil), evidence.Unresolved...)
	if evidence.Unresolved == nil {
		evidence.Unresolved = []ImageActivationGap{}
	}
	for index := range evidence.Unresolved {
		gap := &evidence.Unresolved[index]
		artifacts := canonicalContractStrings(gap.MatchingBuildArtifacts)
		if len(artifacts) != len(gap.MatchingBuildArtifacts) {
			return ImageActivationEvidence{}, fmt.Errorf("activation gap build artifacts are duplicated")
		}
		gap.MatchingBuildArtifacts = artifacts
		domains := canonicalDomains(gap.OwnershipDomains)
		if len(domains) != len(gap.OwnershipDomains) {
			return ImageActivationEvidence{}, fmt.Errorf("activation gap ownership domains are duplicated or unknown")
		}
		gap.OwnershipDomains = domains
	}
	sort.Slice(evidence.Unresolved, func(left, right int) bool {
		return evidence.Unresolved[left].ID < evidence.Unresolved[right].ID
	})
	evidence.Complete = len(evidence.Unresolved) == 0
	evidence.Digest = imageActivationEvidenceDigest(evidence)
	if err := VerifyImageActivationEvidence(evidence); err != nil {
		return ImageActivationEvidence{}, err
	}
	return evidence, nil
}

func VerifyImageActivationEvidence(evidence ImageActivationEvidence) error {
	if evidence.APIVersion != MultiDomainContractAPIVersion || evidence.Kind != ImageActivationEvidenceKind || evidence.Policy != ImageActivationEvidencePolicy {
		return fmt.Errorf("image activation evidence identity is unsupported")
	}
	if err := validateRevisionPair(evidence.BaseCommit, evidence.TargetCommit, "image activation evidence"); err != nil {
		return err
	}
	for label, digest := range map[string]string{
		"build artifact plan":            evidence.BuildArtifactPlanDigest,
		"resolved image activation plan": evidence.ResolvedImageActivationPlanDigest,
	} {
		if err := validateCanonicalSHA256Digest(digest, "image activation evidence "+label+" digest"); err != nil {
			return err
		}
	}
	if !reflect.DeepEqual(evidence.BuiltOnlyArtifacts, canonicalContractStrings(evidence.BuiltOnlyArtifacts)) {
		return fmt.Errorf("built-only artifacts are not uniquely canonical")
	}
	for _, artifact := range evidence.BuiltOnlyArtifacts {
		if !validContractIdentifier(artifact) {
			return fmt.Errorf("built-only artifact identity is invalid")
		}
	}
	if evidence.Unresolved == nil || evidence.Complete != (len(evidence.Unresolved) == 0) {
		return fmt.Errorf("image activation evidence completeness mismatch")
	}
	previousID := ""
	for index, gap := range evidence.Unresolved {
		if !validContractIdentifier(gap.ID) || (index > 0 && gap.ID <= previousID) {
			return fmt.Errorf("activation gaps are not uniquely canonical")
		}
		previousID = gap.ID
		if err := verifyImageActivationGap(gap); err != nil {
			return err
		}
	}
	return verifyContractDigest(evidence.Digest, imageActivationEvidenceDigest(evidence), "image activation evidence")
}

func MarshalImageActivationEvidence(evidence ImageActivationEvidence) ([]byte, error) {
	if err := VerifyImageActivationEvidence(evidence); err != nil {
		return nil, err
	}
	return marshalOperationalJSON(evidence)
}

func DecodeAndVerifyImageActivationEvidence(reader io.Reader, expectedDigest string) (ImageActivationEvidence, error) {
	if err := validateCanonicalSHA256Digest(expectedDigest, "expected image activation evidence digest"); err != nil {
		return ImageActivationEvidence{}, err
	}
	data, err := readOperationalEvidence(reader, "image activation evidence")
	if err != nil {
		return ImageActivationEvidence{}, err
	}
	if err := validateStrictOperationalJSON(data, reflect.TypeOf(ImageActivationEvidence{}), "imageActivationEvidence"); err != nil {
		return ImageActivationEvidence{}, err
	}
	var evidence ImageActivationEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		return ImageActivationEvidence{}, fmt.Errorf("decode image activation evidence: %w", err)
	}
	if evidence.Digest != expectedDigest {
		return ImageActivationEvidence{}, fmt.Errorf("image activation evidence external digest mismatch")
	}
	if err := VerifyImageActivationEvidence(evidence); err != nil {
		return ImageActivationEvidence{}, err
	}
	return evidence, nil
}

func verifyImageActivationGap(gap ImageActivationGap) error {
	if err := validateActivationWorkload(gap.Workload); err != nil {
		return fmt.Errorf("activation gap workload: %w", err)
	}
	if !validContractText(gap.TargetImageRef, 1024) {
		return fmt.Errorf("activation gap target image reference is invalid")
	}
	if !reflect.DeepEqual(gap.MatchingBuildArtifacts, canonicalContractStrings(gap.MatchingBuildArtifacts)) {
		return fmt.Errorf("activation gap build artifacts are not uniquely canonical")
	}
	for _, artifact := range gap.MatchingBuildArtifacts {
		if !validContractIdentifier(artifact) {
			return fmt.Errorf("activation gap build artifact identity is invalid")
		}
	}
	if !reflect.DeepEqual(gap.OwnershipDomains, canonicalDomains(gap.OwnershipDomains)) {
		return fmt.Errorf("activation gap ownership domains are not uniquely canonical")
	}
	if err := validateCanonicalSHA256Digest(gap.ForwardRenderedDigest, "activation gap forward rendered digest"); err != nil {
		return err
	}

	requiresExisting := gap.Reason != ImageActivationGapAbsentCreate
	if requiresExisting {
		if !validContractText(gap.LiveImageRef, 1024) {
			return fmt.Errorf("activation gap live image reference is invalid")
		}
		if gap.LiveImageRef == gap.TargetImageRef {
			return fmt.Errorf("activation gap must represent an actual image change")
		}
		if err := validateCanonicalSHA256Digest(gap.ReverseRenderedDigest, "activation gap reverse rendered digest"); err != nil {
			return err
		}
	} else if gap.LiveImageRef != "" || gap.ReverseRenderedDigest != "" {
		return fmt.Errorf("absent-create gap must not invent live or reverse evidence")
	}

	if gap.Reason == ImageActivationGapTargetNotImmutable {
		if gap.ArtifactDigest != "" || len(gap.MatchingBuildArtifacts) != 0 {
			return fmt.Errorf("non-immutable target gap must not invent artifact evidence")
		}
	} else {
		if err := validateCanonicalSHA256Digest(gap.ArtifactDigest, "activation gap artifact digest"); err != nil {
			return err
		}
		if !strings.HasSuffix(gap.TargetImageRef, "@"+gap.ArtifactDigest) {
			return fmt.Errorf("activation gap target image does not bind its artifact digest")
		}
	}

	switch gap.Reason {
	case ImageActivationGapAbsentCreate:
		if len(gap.MatchingBuildArtifacts) != 1 {
			return fmt.Errorf("absent-create gap must bind one build artifact")
		}
	case ImageActivationGapTargetNotImmutable:
	case ImageActivationGapArtifactNotBuilt:
		if len(gap.MatchingBuildArtifacts) != 0 {
			return fmt.Errorf("artifact-not-built gap must have no matching build artifact")
		}
	case ImageActivationGapArtifactAmbiguous:
		if len(gap.MatchingBuildArtifacts) < 2 {
			return fmt.Errorf("artifact-ambiguous gap must bind two or more build artifacts")
		}
	case ImageActivationGapOwnershipMissing:
		if len(gap.MatchingBuildArtifacts) != 1 || len(gap.OwnershipDomains) != 0 {
			return fmt.Errorf("ownership-missing gap evidence is inconsistent")
		}
	case ImageActivationGapOwnershipAmbiguous:
		if len(gap.MatchingBuildArtifacts) != 1 || len(gap.OwnershipDomains) < 2 {
			return fmt.Errorf("ownership-ambiguous gap evidence is inconsistent")
		}
	case ImageActivationGapAdapterMissing:
		if len(gap.MatchingBuildArtifacts) != 1 || len(gap.OwnershipDomains) != 1 {
			return fmt.Errorf("adapter-missing gap evidence is inconsistent")
		}
	default:
		return fmt.Errorf("activation gap reason is unsupported")
	}
	return nil
}

func imageActivationEvidenceDigest(evidence ImageActivationEvidence) string {
	evidence.Digest = ""
	encoded, err := json.Marshal(evidence)
	if err != nil {
		panic(fmt.Sprintf("marshal image activation evidence: %v", err))
	}
	return digestOperationalBytes(encoded)
}
