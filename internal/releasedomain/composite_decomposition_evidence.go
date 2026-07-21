package releasedomain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
)

const (
	CompositeDecompositionEvidenceKind   = "CompositeDecompositionEvidence"
	CompositeDecompositionEvidencePolicy = "evidence-derived-serial-domain-dag-v1"

	CompositeDecompositionIssueActivationIncomplete = "image-activation-evidence-incomplete"
	CompositeDecompositionIssueTooFewDomains        = "composite-domain-count-below-two"
)

// CompositeDecompositionEvidence is a dormant, non-authorizing report
// contract. It preserves a partial evidence-derived domain decomposition when
// the stricter CompositeReleasePlan cannot yet be constructed. Dependencies
// form a canonical serial chain so no consumer can interpret the report as
// permission for parallel mutation.
type CompositeDecompositionEvidence struct {
	APIVersion                    string                       `json:"apiVersion"`
	Kind                          string                       `json:"kind"`
	Policy                        string                       `json:"policy"`
	BaseCommit                    string                       `json:"baseCommit"`
	TargetCommit                  string                       `json:"targetCommit"`
	ImageActivationPlanDigest     string                       `json:"imageActivationPlanDigest"`
	ImageActivationEvidenceDigest string                       `json:"imageActivationEvidenceDigest"`
	Steps                         []CompositeDecompositionStep `json:"steps"`
	UnresolvedActivationIDs       []string                     `json:"unresolvedActivationIds"`
	Issues                        []string                     `json:"issues"`
	Complete                      bool                         `json:"complete"`
	Digest                        string                       `json:"digest"`
}

// CompositeDecompositionStep binds every resolved activation in one domain to
// its fixed adapter and aggregate forward/reverse rendered evidence.
type CompositeDecompositionStep struct {
	ID                    string   `json:"id"`
	Domain                Domain   `json:"domain"`
	Adapter               string   `json:"adapter"`
	DependsOn             []string `json:"dependsOn"`
	ActivationIDs         []string `json:"activationIds"`
	ForwardRenderedDigest string   `json:"forwardRenderedDigest"`
	ReverseRenderedDigest string   `json:"reverseRenderedDigest"`
}

func NewCompositeDecompositionEvidence(evidence CompositeDecompositionEvidence) (CompositeDecompositionEvidence, error) {
	evidence.APIVersion = MultiDomainContractAPIVersion
	evidence.Kind = CompositeDecompositionEvidenceKind
	evidence.Policy = CompositeDecompositionEvidencePolicy
	evidence.Steps = canonicalCompositeDecompositionSteps(evidence.Steps)
	for index := range evidence.Steps {
		dependencies := canonicalContractStrings(evidence.Steps[index].DependsOn)
		activations := canonicalContractStrings(evidence.Steps[index].ActivationIDs)
		if len(dependencies) != len(evidence.Steps[index].DependsOn) || len(activations) != len(evidence.Steps[index].ActivationIDs) {
			return CompositeDecompositionEvidence{}, fmt.Errorf("composite decomposition dependencies or activations are duplicated")
		}
		evidence.Steps[index].DependsOn = dependencies
		evidence.Steps[index].ActivationIDs = activations
	}
	unresolved := canonicalContractStrings(evidence.UnresolvedActivationIDs)
	if len(unresolved) != len(evidence.UnresolvedActivationIDs) {
		return CompositeDecompositionEvidence{}, fmt.Errorf("composite decomposition unresolved activations are duplicated")
	}
	evidence.UnresolvedActivationIDs = unresolved
	evidence.Issues = derivedCompositeDecompositionIssues(evidence.Steps, evidence.UnresolvedActivationIDs)
	evidence.Complete = len(evidence.Issues) == 0
	evidence.Digest = compositeDecompositionEvidenceDigest(evidence)
	if err := VerifyCompositeDecompositionEvidence(evidence); err != nil {
		return CompositeDecompositionEvidence{}, err
	}
	return evidence, nil
}

func VerifyCompositeDecompositionEvidence(evidence CompositeDecompositionEvidence) error {
	if evidence.APIVersion != MultiDomainContractAPIVersion || evidence.Kind != CompositeDecompositionEvidenceKind || evidence.Policy != CompositeDecompositionEvidencePolicy {
		return fmt.Errorf("composite decomposition evidence identity is unsupported")
	}
	if err := validateRevisionPair(evidence.BaseCommit, evidence.TargetCommit, "composite decomposition evidence"); err != nil {
		return err
	}
	for label, digest := range map[string]string{
		"image activation plan":     evidence.ImageActivationPlanDigest,
		"image activation evidence": evidence.ImageActivationEvidenceDigest,
	} {
		if err := validateCanonicalSHA256Digest(digest, "composite decomposition "+label+" digest"); err != nil {
			return err
		}
	}
	if evidence.Steps == nil || !reflect.DeepEqual(evidence.Steps, canonicalCompositeDecompositionSteps(evidence.Steps)) {
		return fmt.Errorf("composite decomposition steps are not canonical")
	}
	activationIDs := map[string]struct{}{}
	for index, step := range evidence.Steps {
		if _, err := ParseDomain(string(step.Domain)); err != nil || step.ID != string(step.Domain) {
			return fmt.Errorf("composite decomposition step identity is not its fixed domain")
		}
		if index > 0 && evidence.Steps[index-1].Domain == step.Domain {
			return fmt.Errorf("composite decomposition step domain is duplicated")
		}
		if adapter, ok := fixedAdapterForDomain(step.Domain); !ok || step.Adapter != adapter {
			return fmt.Errorf("composite decomposition step adapter does not match its fixed domain")
		}
		expectedDependencies := []string{}
		if index > 0 {
			expectedDependencies = []string{evidence.Steps[index-1].ID}
		}
		if !reflect.DeepEqual(step.DependsOn, expectedDependencies) {
			return fmt.Errorf("composite decomposition steps are not a strict serial DAG")
		}
		if len(step.ActivationIDs) == 0 || !reflect.DeepEqual(step.ActivationIDs, canonicalContractStrings(step.ActivationIDs)) {
			return fmt.Errorf("composite decomposition activation identities are empty or non-canonical")
		}
		for _, activationID := range step.ActivationIDs {
			if !validContractIdentifier(activationID) {
				return fmt.Errorf("composite decomposition activation identity is invalid")
			}
			if _, duplicate := activationIDs[activationID]; duplicate {
				return fmt.Errorf("composite decomposition activation identity is assigned more than once")
			}
			activationIDs[activationID] = struct{}{}
		}
		for label, digest := range map[string]string{
			"forward rendered": step.ForwardRenderedDigest,
			"reverse rendered": step.ReverseRenderedDigest,
		} {
			if err := validateCanonicalSHA256Digest(digest, "composite decomposition "+label+" digest"); err != nil {
				return err
			}
		}
	}
	if evidence.UnresolvedActivationIDs == nil || !reflect.DeepEqual(evidence.UnresolvedActivationIDs, canonicalContractStrings(evidence.UnresolvedActivationIDs)) {
		return fmt.Errorf("composite decomposition unresolved activations are not canonical")
	}
	for _, activationID := range evidence.UnresolvedActivationIDs {
		if !validContractIdentifier(activationID) {
			return fmt.Errorf("composite decomposition unresolved activation identity is invalid")
		}
		if _, resolved := activationIDs[activationID]; resolved {
			return fmt.Errorf("composite decomposition activation cannot be both resolved and unresolved")
		}
	}
	expectedIssues := derivedCompositeDecompositionIssues(evidence.Steps, evidence.UnresolvedActivationIDs)
	if !reflect.DeepEqual(evidence.Issues, expectedIssues) || evidence.Complete != (len(expectedIssues) == 0) {
		return fmt.Errorf("composite decomposition completeness or issues mismatch")
	}
	return verifyContractDigest(evidence.Digest, compositeDecompositionEvidenceDigest(evidence), "composite decomposition evidence")
}

func MarshalCompositeDecompositionEvidence(evidence CompositeDecompositionEvidence) ([]byte, error) {
	if err := VerifyCompositeDecompositionEvidence(evidence); err != nil {
		return nil, err
	}
	return marshalOperationalJSON(evidence)
}

func DecodeAndVerifyCompositeDecompositionEvidence(reader io.Reader, expectedDigest string) (CompositeDecompositionEvidence, error) {
	if err := validateCanonicalSHA256Digest(expectedDigest, "expected composite decomposition evidence digest"); err != nil {
		return CompositeDecompositionEvidence{}, err
	}
	data, err := readOperationalEvidence(reader, "composite decomposition evidence")
	if err != nil {
		return CompositeDecompositionEvidence{}, err
	}
	if err := validateStrictOperationalJSON(data, reflect.TypeOf(CompositeDecompositionEvidence{}), "compositeDecompositionEvidence"); err != nil {
		return CompositeDecompositionEvidence{}, err
	}
	var evidence CompositeDecompositionEvidence
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		return CompositeDecompositionEvidence{}, fmt.Errorf("decode composite decomposition evidence: %w", err)
	}
	if evidence.Digest != expectedDigest {
		return CompositeDecompositionEvidence{}, fmt.Errorf("composite decomposition evidence external digest mismatch")
	}
	if err := VerifyCompositeDecompositionEvidence(evidence); err != nil {
		return CompositeDecompositionEvidence{}, err
	}
	return evidence, nil
}

func canonicalCompositeDecompositionSteps(values []CompositeDecompositionStep) []CompositeDecompositionStep {
	result := append([]CompositeDecompositionStep(nil), values...)
	for index := range result {
		result[index].DependsOn = append([]string(nil), result[index].DependsOn...)
		result[index].ActivationIDs = append([]string(nil), result[index].ActivationIDs...)
		if result[index].DependsOn == nil {
			result[index].DependsOn = []string{}
		}
		if result[index].ActivationIDs == nil {
			result[index].ActivationIDs = []string{}
		}
	}
	sort.Slice(result, func(left, right int) bool {
		return domainRank[result[left].Domain] < domainRank[result[right].Domain]
	})
	if result == nil {
		return []CompositeDecompositionStep{}
	}
	return result
}

func derivedCompositeDecompositionIssues(steps []CompositeDecompositionStep, unresolved []string) []string {
	issues := []string{}
	if len(unresolved) != 0 {
		issues = append(issues, CompositeDecompositionIssueActivationIncomplete)
	}
	if len(steps) < 2 {
		issues = append(issues, CompositeDecompositionIssueTooFewDomains)
	}
	return issues
}

func compositeDecompositionEvidenceDigest(evidence CompositeDecompositionEvidence) string {
	evidence.Digest = ""
	encoded, err := json.Marshal(evidence)
	if err != nil {
		panic(fmt.Sprintf("marshal composite decomposition evidence: %v", err))
	}
	return digestOperationalBytes(encoded)
}
