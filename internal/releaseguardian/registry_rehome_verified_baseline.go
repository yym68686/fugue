package releaseguardian

import (
	"context"
	"crypto/ed25519"
	"errors"
	"time"

	"fugue/internal/productioncontrol"
	"k8s.io/client-go/kubernetes"
)

// VerifiedRegistryRehomeBaseline reconstructs the retained releases without
// issuing synthetic legacy MonitorRecords or ArtifactReceipts. The manifests
// are the exact bytes to use when comparing a successor or planning recovery.
// This read does not re-observe live serving health or confer write authority.
type VerifiedRegistryRehomeBaseline struct {
	Baseline         RegistryRehomeBaseline
	EvidenceDigest   string
	CurrentManifest  []byte
	RollbackManifest []byte
}

// VerifyRegistryRehomeAppliedReadback repeats the independently enrolled
// provider reads used by the dedicated applied-settlement journal. It does
// not write a result. The signed custody and write-closure claims still
// require independent evidence; they cannot be inferred from pod health.
func VerifyRegistryRehomeAppliedReadback(ctx context.Context, client kubernetes.Interface, journal *productioncontrol.Journal,
	cutover KubeDeploymentCutover, provider productioncontrol.ParticipationKubernetesBinding, observer ed25519.PublicKey,
	recoveryRaw []byte, bundlePin string, registry *IndependentRegistry, route RegistryRehomeRoute, retained []byte,
	proof productioncontrol.RegistryRehomeAppliedEvidence) error {
	return verifyRegistryRehomeAppliedReadback(ctx, client, journal, cutover, provider, observer, recoveryRaw, bundlePin,
		registry, route, retained, proof, readRegistryRehomeHTTP)
}

func verifyRegistryRehomeAppliedReadback(ctx context.Context, client kubernetes.Interface, journal *productioncontrol.Journal,
	cutover KubeDeploymentCutover, provider productioncontrol.ParticipationKubernetesBinding, observer ed25519.PublicKey,
	recoveryRaw []byte, bundlePin string, registry *IndependentRegistry, route RegistryRehomeRoute, retained []byte,
	proof productioncontrol.RegistryRehomeAppliedEvidence, httpReader registryRehomeHTTPReader) error {
	old, end, err := readRegistryRehomeServingReport(retained)
	if err != nil {
		return err
	}
	scope := productioncontrol.Scope{ClusterID: proof.ClusterID, ResourceID: proof.ResourceID}
	claim := productioncontrol.Claim{Scope: scope, Fence: proof.Fence, IntentDigest: proof.IntentDigest, ApprovalDigest: proof.ApprovalDigest}
	baseline, placement := old.Cutover.Baseline, old.Cutover.Placement
	if bundlePin != proof.BundleDigest || baseline.RecoveryBundle != bundlePin || baseline.BaselineDigest != proof.BaselineDigest ||
		baseline.Scope != scope || baseline.IntentDigest != proof.IntentDigest || placement.Claim != claim ||
		placement.Placement != registryRehomeIndependentPlacement || placement.AdmissionDigest != proof.AdmissionDigest ||
		placement.ReadbackDigest != proof.ReadbackDigest || old.EvidenceDigest != proof.ServingEvent || old.StateDigest != proof.ServingState ||
		end.Unix() > proof.ObservedAt || old.Route != route {
		return errors.New("registry rehome applied evidence differs from the independently retained serving event and recovery pins")
	}
	reader, err := productioncontrol.NewRegistryRehomeAdmissionReader(journal, scope, observer)
	if err != nil {
		return err
	}
	admission, pin, err := reader.ReadAdmission(ctx, claim)
	start, startErr := time.Parse(time.RFC3339Nano, old.StartedAt)
	if err != nil || startErr != nil || pin != proof.AdmissionDigest || start.Unix() < admission.ExpiresAt {
		return errors.Join(errors.New("registry rehome applied evidence must follow expiry of its exact temporary write admission"), err)
	}
	fresh, err := observeRegistryRehomeServing(ctx, client, journal, scope.ClusterID, claim.IntentDigest, cutover, provider, observer,
		recoveryRaw, bundlePin, registry, route, httpReader)
	if err != nil {
		return err
	}
	return VerifyRetainedRegistryRehomeServing(retained, proof.ServingEvent, fresh)
}

// LoadVerifiedRegistryRehomeBaseline binds the recovered bytes to the exact
// applied receipt and the current durable head. A subsequent pending claim
// does not erase the last verified version. An unavailable journal or stale
// head is an error; callers must not fall back to a Git/legacy monitor version.
func LoadVerifiedRegistryRehomeBaseline(ctx context.Context, journal *productioncontrol.Journal, scope productioncontrol.Scope,
	observer ed25519.PublicKey, recoveryRaw []byte, independentlyPinnedBundle string) (VerifiedRegistryRehomeBaseline, error) {
	var none VerifiedRegistryRehomeBaseline
	reader, err := productioncontrol.NewRegistryRehomeAdmissionReader(journal, scope, observer)
	if err != nil {
		return none, err
	}
	verified, err := verifyRegistryRehomeRecoveryBundle(recoveryRaw, independentlyPinnedBundle)
	if err != nil {
		return none, err
	}
	_, baseline, err := buildRegistryRehomeBaseline(verified)
	if err != nil || baseline.Scope != scope {
		return none, errors.Join(errors.New("registry rehome recovery bundle belongs to another scope"), err)
	}
	before, err := journal.ReadResource(ctx, scope)
	if err != nil || before.LastVerified != baseline.Version() {
		return none, errors.Join(errors.New("registry rehome baseline is not the current verified journal version"), err)
	}
	closure, result, err := reader.ReadApplied(ctx, baseline.IntentDigest)
	if err != nil || result.Verified != baseline.Version() || closure.BundleDigest != baseline.RecoveryBundle {
		return none, errors.Join(errors.New("registry rehome applied receipt does not bind the pinned recovery bundle"), err)
	}
	current, _, err := registryRehomeReleaseInput(verified, "current")
	if err != nil {
		return none, err
	}
	rollback, _, err := registryRehomeReleaseInput(verified, "rollback")
	if err != nil {
		return none, err
	}
	after, err := journal.ReadResource(ctx, scope)
	if err != nil || after != before {
		return none, errors.Join(errors.New("registry rehome verified head changed during recovery"), err)
	}
	return VerifiedRegistryRehomeBaseline{Baseline: baseline, EvidenceDigest: result.EvidenceDigest, CurrentManifest: current, RollbackManifest: rollback}, nil
}
