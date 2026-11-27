package releaseguardian

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"time"

	"fugue/internal/declarativerelease"
)

const (
	MaxCutoverAttestationBytes = 8 << 10
	maxCutoverAttestationAge   = 5 * time.Minute
)

// DeploymentCutoverAttestation is signed by an independently provisioned
// provider observer. All referenced records must be collected and retained
// outside the publisher before it is signed. The publisher authenticates the
// attestation; it cannot mint it or substitute policy readback for drainage.
type DeploymentCutoverAttestation struct {
	PolicyScopeDigest    string `json:"policyScopeDigest"`
	ReleaseBindingDigest string `json:"releaseBindingDigest"`
	EndpointAdmission    string `json:"endpointAdmissionDigest"`
	LegacyRevocation     string `json:"legacyRevocationDigest"`
	LegacyRequestDrain   string `json:"legacyRequestDrainDigest"`
	NativeGuardian       string `json:"nativeGuardianDigest"`
	NodeArtifactPulls    string `json:"nodeArtifactPullsDigest"`
	RegistryRetention    string `json:"registryRetentionDigest"`
	ObservedAt           int64  `json:"observedAtUnix"`
	ExpiresAt            int64  `json:"expiresAtUnix"`
	Signature            []byte `json:"signature"`
}

// DeploymentCutoverDigests binds attestation to the exact identities,
// workload UID/version, pointer, predecessor, candidate and OCI receipts.
func DeploymentCutoverDigests(config KubeDeploymentCutover, binding NativePublicationBoundary) (string, string, error) {
	if config.Phase != KubeCutoverNative || config.validate() != nil ||
		binding.Scope.ClusterID == "" || binding.Scope.ResourceID != nativeKubernetesResourceID(binding.Workload) ||
		binding.Workload.APIVersion != "apps/v1" || binding.Workload.Kind != "Deployment" ||
		binding.Workload.Namespace != config.Namespace || binding.Workload.Name != config.Deployment ||
		binding.Namespace != config.Namespace || binding.MonitorComponent != config.MonitorComponent ||
		binding.ComponentLease != config.ComponentLease || binding.Predecessor.Key() != config.Key ||
		binding.Candidate.Key() != config.Key || binding.Predecessor.Validate() != nil || binding.Candidate.Validate() != nil ||
		binding.WorkloadUID == "" || binding.WorkloadVersion == "" ||
		binding.DesiredObject != desiredName(config.Key) || binding.RecordObject != releaseRecordName(config.Key, binding.Candidate.RecordDigest) ||
		binding.ExecutionObject != executionSnapshotName(config.Key, binding.Candidate.Generation) {
		return "", "", errors.New("native publication does not identify this cutover")
	}
	policy, err := declarativerelease.CanonicalJSON(config)
	if err != nil {
		return "", "", err
	}
	bound, err := declarativerelease.CanonicalJSON(binding)
	if err != nil {
		return "", "", err
	}
	return digest(append([]byte("fugue.cutover.policy.v1\x00"), policy...)),
		digest(append([]byte("fugue.cutover.publication.v1\x00"), bound...)), nil
}

func (a DeploymentCutoverAttestation) SigningInput() ([]byte, error) {
	seen := make(map[string]bool, 8)
	for _, d := range []string{a.PolicyScopeDigest, a.ReleaseBindingDigest, a.EndpointAdmission, a.LegacyRevocation,
		a.LegacyRequestDrain, a.NativeGuardian, a.NodeArtifactPulls, a.RegistryRetention} {
		if !digestPattern.MatchString(d) || seen[d] {
			return nil, errors.New("native cutover requires distinct, complete provider evidence")
		}
		seen[d] = true
	}
	if a.ObservedAt <= 0 || a.ExpiresAt <= a.ObservedAt ||
		a.ExpiresAt-a.ObservedAt > int64(maxCutoverAttestationAge/time.Second) {
		return nil, errors.New("invalid native cutover evidence lifetime")
	}
	a.Signature = nil
	raw, err := declarativerelease.CanonicalJSON(a)
	if err != nil {
		return nil, err
	}
	return append([]byte("fugue.cutover.attestation.v1\x00"), raw...), nil
}

type signedDeploymentCutoverEvidence struct {
	attestation DeploymentCutoverAttestation
	now         func() time.Time
}

func NewSignedDeploymentCutoverEvidence(raw []byte, observer ed25519.PublicKey) (DeploymentCutoverEvidence, error) {
	var a DeploymentCutoverAttestation
	raw = bytes.TrimSpace(raw)
	if len(observer) != ed25519.PublicKeySize || len(raw) == 0 || len(raw) > MaxCutoverAttestationBytes ||
		decodeStrict(raw, &a) != nil {
		return nil, errors.New("invalid native cutover evidence or observer key")
	}
	canonical, err := declarativerelease.CanonicalJSON(a)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, errors.New("native cutover evidence is not canonical")
	}
	message, err := a.SigningInput()
	if err != nil || !ed25519.Verify(observer, message, a.Signature) {
		return nil, errors.New("native cutover evidence was not signed by the trusted provider observer")
	}
	return &signedDeploymentCutoverEvidence{a, time.Now}, nil
}

func (v *signedDeploymentCutoverEvidence) VerifyDeploymentCutoverEvidence(ctx context.Context, config KubeDeploymentCutover, b NativePublicationBoundary) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	policy, boundary, err := DeploymentCutoverDigests(config, b)
	a := v.attestation
	if err != nil || a.PolicyScopeDigest != policy || a.ReleaseBindingDigest != boundary {
		return errors.New("native cutover evidence does not cover the exact provider and release boundary")
	}
	at := v.now().Unix()
	if at < a.ObservedAt || at >= a.ExpiresAt {
		return errors.New("native cutover evidence is outside its validity window")
	}
	return nil
}
