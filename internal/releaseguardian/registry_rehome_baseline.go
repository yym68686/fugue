package releaseguardian

import (
	"bytes"
	"context"
	"errors"
	"math"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
)

const (
	registryRehomeBaselineSchema   = "fugue.production.registry-rehome-baseline.v1"
	MaxRegistryRehomeBaselineBytes = 16 << 10
)

// RegistryRehomeBaseline is the content identity that a successful handoff
// must verify. The proposal, historical monitor, and relocated baseline
// have different identities even when the underlying OCI bytes are the same.
// This record does not claim that the handoff has taken place.
type RegistryRehomeBaseline struct {
	Schema         string                              `json:"schema"`
	Scope          productioncontrol.Scope             `json:"scope"`
	Key            Key                                 `json:"key"`
	Workload       declarativerelease.ResourceIdentity `json:"workload"`
	Predecessor    productioncontrol.Version           `json:"predecessor"`
	IntentDigest   string                              `json:"intentDigest"`
	RecoveryBundle string                              `json:"recoveryBundleDigest"`
	SourceArchive  string                              `json:"sourceArchiveDigest"`
	Current        RegistryRehomeBaselineArtifact      `json:"current"`
	Rollback       RegistryRehomeBaselineArtifact      `json:"rollback"`
	BaselineDigest string                              `json:"baselineDigest,omitempty"`
}

type RegistryRehomeBaselineArtifact struct {
	Target               declarativerelease.TargetIdentity `json:"target"`
	SourceMonitorDigest  string                            `json:"sourceMonitorDigest"`
	HealthContractDigest string                            `json:"healthContractDigest"`
	Artifact             RegistryRecoveryReadback          `json:"artifact"`
}

// Version identifies the proposed terminal journal head, not permission to
// settle it. A separate provider/serving closure must prove this exact head.
func (b RegistryRehomeBaseline) Version() productioncontrol.Version {
	return productioncontrol.Version{Generation: b.Predecessor.Generation + 1, Digest: b.BaselineDigest}
}

// BuildRegistryRehomeBaseline derives immutable metadata for both relocated
// releases without changing the source receipts or simulating an execution.
// The recovery bundle must remain available in independent custody.
func BuildRegistryRehomeBaseline(bundle []byte, pin string) ([]byte, RegistryRehomeBaseline, error) {
	verified, err := verifyRegistryRehomeRecoveryBundle(bundle, pin)
	if err != nil {
		return nil, RegistryRehomeBaseline{}, err
	}
	return buildRegistryRehomeBaseline(verified)
}

func buildRegistryRehomeBaseline(verified verifiedRegistryRehomeRecoveryBundle) ([]byte, RegistryRehomeBaseline, error) {
	var none RegistryRehomeBaseline
	submission, report := verified.bundle.Review, verified.report
	review, err := InspectRegistryRehomeSubmission(submission)
	if err != nil {
		return nil, none, err
	}
	if review.Source.Version.Generation == math.MaxInt64 {
		return nil, none, errors.New("registry rehome baseline cannot advance an exhausted generation")
	}
	makeVersion := func(role string, sealed sealedRecoveryRecord, mirror RegistryRecoveryReadback) (RegistryRehomeBaselineArtifact, error) {
		_, input, err := registryRehomeReleaseInput(verified, role)
		if err != nil {
			return RegistryRehomeBaselineArtifact{}, err
		}
		health, err := declarativerelease.CanonicalJSON(sealed.release.Health)
		if err != nil {
			return RegistryRehomeBaselineArtifact{}, err
		}
		return RegistryRehomeBaselineArtifact{input.Target, input.SourceRecordDigest, digest(health), mirror}, nil
	}
	current, err := makeVersion("current", verified.archive.forward, review.Draft.Mirror.Forward)
	if err != nil {
		return nil, none, err
	}
	rollback, err := makeVersion("rollback", verified.archive.rollback, review.Draft.Mirror.Rollback)
	if err != nil {
		return nil, none, err
	}
	intent := submission.Intent
	out := RegistryRehomeBaseline{
		Schema: registryRehomeBaselineSchema, Scope: productioncontrol.Scope{ClusterID: intent.ClusterID, ResourceID: intent.ResourceID},
		Key: review.Source.Key, Workload: report.Workload, Predecessor: review.Source.Version,
		IntentDigest: report.IntentDigest, RecoveryBundle: report.BundleDigest, SourceArchive: report.ArchiveDigest,
		Current: current, Rollback: rollback,
	}
	unsigned, err := declarativerelease.CanonicalJSON(out)
	if err != nil {
		return nil, none, err
	}
	out.BaselineDigest = digest(unsigned)
	raw, err := declarativerelease.CanonicalJSON(out)
	if err != nil || len(raw) > MaxRegistryRehomeBaselineBytes {
		return nil, none, errors.New("registry rehome baseline exceeds its canonical size bound")
	}
	return raw, out, nil
}

// VerifyRegistryRehomeBaseline checks both the independently pinned
// baseline identity and its byte-for-byte derivation from the pinned bundle.
// A rehashed but changed predecessor, version or role is still rejected.
func VerifyRegistryRehomeBaseline(raw []byte, expectedPin string, bundle []byte, bundlePin string) (RegistryRehomeBaseline, error) {
	return VerifyRegistryRehomeRecoveryBaseline(context.Background(), raw, expectedPin, bundle, bundlePin, nil)
}

// VerifyRegistryRehomeRecoveryBaseline reconstructs the handoff and, when
// supplied, re-reads both retained images from the independent registry.
// A verified backup is neither a live serving receipt nor a journal head.
func VerifyRegistryRehomeRecoveryBaseline(ctx context.Context, raw []byte, expectedPin string, bundle []byte, bundlePin string, registry *IndependentRegistry) (RegistryRehomeBaseline, error) {
	var none RegistryRehomeBaseline
	if len(raw) == 0 || len(raw) > MaxRegistryRehomeBaselineBytes || !digestPattern.MatchString(expectedPin) {
		return none, errors.New("registry rehome baseline has invalid size or pin")
	}
	verified, err := verifyRegistryRehomeRecoveryBundle(bundle, bundlePin)
	if err != nil {
		return none, err
	}
	canonical, record, err := buildRegistryRehomeBaseline(verified)
	if err != nil {
		return none, err
	}
	if record.BaselineDigest != expectedPin || !bytes.Equal(raw, canonical) {
		return none, errors.New("registry rehome baseline differs from the pinned canonical handoff")
	}
	if registry != nil {
		if err := verifyRegistryRehomeRecoveryMirror(ctx, verified, registry); err != nil {
			return none, err
		}
	}
	return record, nil
}
