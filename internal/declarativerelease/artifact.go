package declarativerelease

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	ArtifactReceiptAPIVersion = "release.fugue.dev/v2"
	ArtifactReceiptKind       = "ImmutableComponentArtifactReceipt"
)

type RegistryVerification struct {
	Image           string `json:"image"`
	IndexDigest     string `json:"index_digest"`
	ManifestDigest  string `json:"manifest_digest"`
	ConfigDigest    string `json:"config_digest"`
	OCIRevision     string `json:"oci_revision"`
	Platform        string `json:"platform"`
	Verification    string `json:"verification"`
	BlobCount       int    `json:"blob_count"`
	LayerProbeCount int    `json:"layer_get_probe_count"`
	RequestCount    int    `json:"request_count"`
	TotalLayerBytes int64  `json:"total_layer_bytes"`
}

type ArtifactReceipt struct {
	APIVersion             string `json:"apiVersion"`
	Kind                   string `json:"kind"`
	Component              string `json:"component"`
	ConfigSHA              string `json:"configSha"`
	SourceSHA              string `json:"sourceSha"`
	SourceTag              string `json:"sourceTag"`
	Repository             string `json:"repository"`
	ImmutableRef           string `json:"immutableRef"`
	TopDigest              string `json:"topDigest"`
	PlatformManifestDigest string `json:"platformManifestDigest"`
	ConfigDigest           string `json:"configDigest"`
	OCIRevision            string `json:"ociRevision"`
	Platform               string `json:"platform"`
	Verification           string `json:"verification"`
	PlanDigest             string `json:"planDigest"`
	IntentDigest           string `json:"intentDigest"`
	ReceiptDigest          string `json:"receiptDigest"`
}

func DecodePlan(reader io.Reader) (Plan, error) {
	var plan Plan
	if err := decodeStrict(reader, &plan); err != nil {
		return Plan{}, fmt.Errorf("decode production release plan: %w", err)
	}
	if err := plan.ValidateBound(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func (plan Plan) ValidateBound() error {
	if plan.APIVersion != IntentAPIVersion || plan.Kind != "ProductionReleasePlan" ||
		!shaPattern.MatchString(plan.BaseSHA) || !shaPattern.MatchString(plan.HeadSHA) ||
		plan.BaseSHA == plan.HeadSHA || !digestPattern.MatchString(plan.PlanDigest) {
		return errors.New("bound production release plan identity is invalid")
	}
	if len(plan.Releases) > 1 {
		return errors.New("bound production release plan contains more than one component atom")
	}
	seen := make(map[string]struct{}, len(plan.Releases))
	for index, release := range plan.Releases {
		if !componentIDPattern.MatchString(release.ComponentID) || !digestPattern.MatchString(release.IntentDigest) ||
			release.IntentGeneration < 1 || release.Concurrency == "" {
			return fmt.Errorf("bound release %d is invalid", index)
		}
		if err := release.Workload.validate(release.ComponentID); err != nil {
			return fmt.Errorf("bound release %d workload: %w", index, err)
		}
		if err := validateArtifactTargets(Component{ID: release.ComponentID, Workload: release.Workload, ArtifactTargets: release.ArtifactTargets}); err != nil {
			return fmt.Errorf("bound release %d artifact targets: %w", index, err)
		}
		if err := release.Transition.validate(Component{ID: release.ComponentID, Workload: release.Workload, ArtifactTargets: release.ArtifactTargets}); err != nil {
			return fmt.Errorf("bound release %d transition: %w", index, err)
		}
		if release.ExpectedPreviousPresent {
			if !shaPattern.MatchString(release.ExpectedPreviousConfigSHA) ||
				!shaPattern.MatchString(release.ExpectedPreviousManifestSHA) ||
				!shaPattern.MatchString(release.ExpectedPreviousOCIRevision) ||
				!digestPattern.MatchString(release.ExpectedPreviousImageDigest) {
				return fmt.Errorf("bound release %d predecessor is invalid", index)
			}
		} else if (!release.RetrySameLKG && release.IntentGeneration != 1) || release.ExpectedPreviousConfigSHA != "" ||
			release.ExpectedPreviousManifestSHA != "" || release.ExpectedPreviousOCIRevision != "" ||
			release.ExpectedPreviousImageDigest != "" {
			return fmt.Errorf("bound release %d absent predecessor is invalid", index)
		}
		if release.RetrySameLKG && release.IntentGeneration < 2 {
			return fmt.Errorf("bound release %d same-LKG attempt is invalid", index)
		}
		if _, exists := seen[release.ComponentID]; exists {
			return fmt.Errorf("bound plan repeats component %q", release.ComponentID)
		}
		seen[release.ComponentID] = struct{}{}
	}
	copy := plan
	copy.PlanDigest = ""
	unsigned, err := CanonicalJSON(copy)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(unsigned)
	if plan.PlanDigest != fmt.Sprintf("sha256:%x", digest) {
		return errors.New("bound production release plan digest is invalid")
	}
	return nil
}

func DecodeRegistryVerification(reader io.Reader) (RegistryVerification, error) {
	var verification RegistryVerification
	if err := decodeStrict(reader, &verification); err != nil {
		return RegistryVerification{}, fmt.Errorf("decode registry verification: %w", err)
	}
	return verification, nil
}

func MaterializeArtifactReceipt(plan Plan, componentID string, verification RegistryVerification) (ArtifactReceipt, error) {
	if err := plan.ValidateBound(); err != nil {
		return ArtifactReceipt{}, err
	}
	var release *PlanRelease
	for index := range plan.Releases {
		if plan.Releases[index].ComponentID == componentID {
			release = &plan.Releases[index]
			break
		}
	}
	if release == nil {
		return ArtifactReceipt{}, fmt.Errorf("component %q is not in the release plan", componentID)
	}
	prefix := release.Artifact.Repository + "@"
	if !strings.HasPrefix(verification.Image, prefix) {
		return ArtifactReceipt{}, errors.New("registry verification repository mismatch")
	}
	topDigest := strings.TrimPrefix(verification.Image, prefix)
	for label, value := range map[string]string{
		"top":      topDigest,
		"manifest": verification.ManifestDigest,
		"config":   verification.ConfigDigest,
	} {
		if !digestPattern.MatchString(value) {
			return ArtifactReceipt{}, fmt.Errorf("registry verification %s digest is invalid", label)
		}
	}
	if verification.IndexDigest != "" && verification.IndexDigest != topDigest {
		return ArtifactReceipt{}, errors.New("registry verification index/top digest mismatch")
	}
	if verification.OCIRevision != plan.HeadSHA || verification.Platform != "linux/amd64" ||
		verification.Verification != "registry_manifest_config_and_layer_get" ||
		verification.BlobCount < 1 || verification.LayerProbeCount < 1 ||
		verification.RequestCount < 1 || verification.TotalLayerBytes < 1 {
		return ArtifactReceipt{}, errors.New("registry verification provenance or completeness is invalid")
	}
	receipt := ArtifactReceipt{
		APIVersion: ArtifactReceiptAPIVersion, Kind: ArtifactReceiptKind,
		Component: componentID, ConfigSHA: plan.HeadSHA, SourceSHA: plan.HeadSHA,
		SourceTag: plan.HeadSHA, Repository: release.Artifact.Repository,
		ImmutableRef: verification.Image, TopDigest: topDigest,
		PlatformManifestDigest: verification.ManifestDigest, ConfigDigest: verification.ConfigDigest,
		OCIRevision: verification.OCIRevision, Platform: verification.Platform,
		Verification: verification.Verification, PlanDigest: plan.PlanDigest, IntentDigest: release.IntentDigest,
	}
	unsigned, err := CanonicalJSON(receipt)
	if err != nil {
		return ArtifactReceipt{}, err
	}
	digest := sha256.Sum256(unsigned)
	receipt.ReceiptDigest = fmt.Sprintf("sha256:%x", digest)
	return receipt, nil
}

func DecodeArtifactReceipt(reader io.Reader) (ArtifactReceipt, error) {
	var receipt ArtifactReceipt
	if err := decodeStrict(reader, &receipt); err != nil {
		return ArtifactReceipt{}, fmt.Errorf("decode artifact receipt: %w", err)
	}
	if receipt.APIVersion != ArtifactReceiptAPIVersion || receipt.Kind != ArtifactReceiptKind ||
		!componentIDPattern.MatchString(receipt.Component) || !shaPattern.MatchString(receipt.ConfigSHA) ||
		receipt.SourceSHA != receipt.ConfigSHA || receipt.SourceTag != receipt.ConfigSHA ||
		receipt.OCIRevision != receipt.ConfigSHA || !repositoryPattern.MatchString(receipt.Repository) ||
		receipt.ImmutableRef != receipt.Repository+"@"+receipt.TopDigest ||
		!digestPattern.MatchString(receipt.TopDigest) || !digestPattern.MatchString(receipt.PlatformManifestDigest) ||
		!digestPattern.MatchString(receipt.ConfigDigest) || !digestPattern.MatchString(receipt.PlanDigest) ||
		!digestPattern.MatchString(receipt.IntentDigest) || !digestPattern.MatchString(receipt.ReceiptDigest) ||
		receipt.Platform != "linux/amd64" || receipt.Verification != "registry_manifest_config_and_layer_get" {
		return ArtifactReceipt{}, errors.New("artifact receipt identity is invalid")
	}
	copy := receipt
	copy.ReceiptDigest = ""
	unsigned, err := CanonicalJSON(copy)
	if err != nil {
		return ArtifactReceipt{}, err
	}
	digest := sha256.Sum256(unsigned)
	if receipt.ReceiptDigest != fmt.Sprintf("sha256:%x", digest) {
		return ArtifactReceipt{}, errors.New("artifact receipt digest is invalid")
	}
	return receipt, nil
}

// MarshalRegistryVerification is only used by tests and fixture generators.
func MarshalRegistryVerification(value RegistryVerification) ([]byte, error) {
	return json.Marshal(value)
}
