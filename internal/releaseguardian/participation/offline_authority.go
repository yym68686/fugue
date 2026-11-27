package participation

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/productioncontrol"
)

const (
	maxOfflineAuthorityBytes = 8 << 10
	maxOfflineAuthorityAge   = 5 * time.Minute
)

// OfflineHoldAuthority is a short-lived, independently signed statement
// about the exact installed guard and intended dispatch. Referenced records
// must be retained by the provider observer. This statement cannot be
// derived from a policy GET, a Node GET or an owner approval.
type OfflineHoldAuthority struct {
	BoundaryDigest     string `json:"boundaryDigest"`
	EndpointAdmission  string `json:"endpointAdmissionDigest"`
	LegacyRevocation   string `json:"legacyRevocationDigest"`
	LegacyRequestDrain string `json:"legacyRequestDrainDigest"`
	WriterIdentity     string `json:"writerIdentityDigest"`
	ObservedAt         int64  `json:"observedAtUnix"`
	ExpiresAt          int64  `json:"expiresAtUnix"`
	Signature          []byte `json:"signature"`
}

// OfflineHoldAuthorityBoundary binds the resource, signed topology and
// proposal, dedicated writer, and both observed admission object revisions.
// Computing it provides no evidence that admission is enforced.
func OfflineHoldAuthorityBoundary(topology productioncontrol.VerifiedParticipationTopology, s productioncontrol.Submission, writer string, guard FreezeObservation) (string, error) {
	p, err := decodeHold(s)
	if err != nil {
		return "", err
	}
	member, enrolled := enrolledMember(topology.Policy, p.To.Node)
	policy, binding, err := RenderOfflineHoldGuard(s, writer)
	if err != nil || enrolled != nil || topology.Policy.Schema != productioncontrol.BoundParticipationPolicySchema ||
		topology.Policy.Kubernetes == nil || topology.Policy.Kubernetes.Validate() != nil ||
		topology.Version != p.Topology || topology.ApprovalDigest != p.TopologyApproval ||
		topology.Policy.Generation != topology.Version.Generation || !canonicalDigest(topology.IntentDigest) ||
		member.NodeUID != p.To.UID || member.Group != p.To.Group ||
		guard.Node != p.To.Node || !guard.CurrentTypeChecked ||
		guard.Policy.Name != policy.Name || guard.Binding.Name != binding.Name ||
		guard.Policy.UID == "" || guard.Policy.ResourceVersion == "" ||
		guard.Binding.UID == "" || guard.Binding.ResourceVersion == "" {
		return "", errors.New("offline hold authority requires the exact approved topology, writer, and installed guard revisions")
	}
	intent, err := s.Intent.Digest()
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(struct {
		Intent         string                                           `json:"intentDigest"`
		TopologyIntent string                                           `json:"topologyIntentDigest"`
		Namespace      string                                           `json:"namespace"`
		Kubernetes     productioncontrol.ParticipationKubernetesBinding `json:"kubernetes"`
		Writer         string                                           `json:"writer"`
		Guard          FreezeObservation                                `json:"guard"`
	}{intent, topology.IntentDigest, topology.Policy.Namespace, *topology.Policy.Kubernetes, writer, guard})
	if err != nil {
		return "", err
	}
	return digest(append([]byte("fugue.participation.offline-boundary.v1\x00"), raw...)), nil
}

func (a OfflineHoldAuthority) SigningInput() ([]byte, error) {
	seen := make(map[string]bool, 5)
	for _, d := range []string{a.BoundaryDigest, a.EndpointAdmission, a.LegacyRevocation, a.LegacyRequestDrain, a.WriterIdentity} {
		if !canonicalDigest(d) || seen[d] {
			return nil, errors.New("offline hold authority needs distinct admission, revocation, drainage, and identity evidence")
		}
		seen[d] = true
	}
	if a.ObservedAt <= 0 || a.ExpiresAt <= a.ObservedAt ||
		a.ExpiresAt-a.ObservedAt > int64(maxOfflineAuthorityAge/time.Second) {
		return nil, errors.New("offline hold authority has an invalid lifetime")
	}
	a.Signature = nil
	raw, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	return append([]byte("fugue.participation.offline-authority.v1\x00"), raw...), nil
}

// verifyOfflineHoldAuthority is a pre-dispatch verifier, not a trust
// provisioner or a collector. Its key must come from independently
// provisioned verifier configuration, not the submitter or the writer.
func verifyOfflineHoldAuthority(raw []byte, observer ed25519.PublicKey, boundary string, now time.Time) error {
	var a OfflineHoldAuthority
	if len(raw) == 0 || len(raw) > maxOfflineAuthorityBytes || len(observer) != ed25519.PublicKeySize ||
		decodeStrict(raw, &a) != nil || !canonicalDigest(boundary) {
		return errors.New("missing or malformed offline hold authority statement")
	}
	message, err := a.SigningInput()
	canonical, encodeErr := json.Marshal(a)
	if err != nil || encodeErr != nil || !bytes.Equal(raw, canonical) || len(a.Signature) != ed25519.SignatureSize ||
		!ed25519.Verify(observer, message, a.Signature) {
		return errors.New("offline hold authority lacks a canonical trusted provider signature")
	}
	if a.BoundaryDigest != boundary || now.Unix() < a.ObservedAt || now.Unix() >= a.ExpiresAt {
		return errors.New("offline hold authority is stale or does not cover the exact dispatch")
	}
	return nil
}
