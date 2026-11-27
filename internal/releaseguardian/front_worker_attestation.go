package releaseguardian

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"slices"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	authentication "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// FrontWorkerCutoverAttestation binds the technical observer's evidence to
// the exact approved handoff and the admission objects seen at the provider.
type FrontWorkerCutoverAttestation struct {
	Scope              productioncontrol.Scope `json:"scope"`
	IntentDigest       string                  `json:"intentDigest"`
	ApprovalDigest     string                  `json:"approvalDigest"`
	JournalDigest      string                  `json:"journalDigest"`
	PolicyDigest       string                  `json:"policyDigest"`
	EndpointAdmission  string                  `json:"endpointAdmissionDigest"`
	LegacyRevocation   string                  `json:"legacyRevocationDigest"`
	LegacyRequestDrain string                  `json:"legacyRequestDrainDigest"`
	ShadowEvidence     string                  `json:"shadowEvidenceDigest"`
	NativeRuntime      string                  `json:"nativeRuntimeDigest"`
	ObservedAt         int64                   `json:"observedAtUnix"`
	ExpiresAt          int64                   `json:"expiresAtUnix"`
	Signature          []byte                  `json:"signature"`
}

func (a FrontWorkerCutoverAttestation) SigningInput() ([]byte, error) {
	if !nativeWorkerClusterPattern.MatchString(a.Scope.ClusterID) || a.Scope.ResourceID == "" {
		return nil, errors.New("Front/Worker cutover attestation lacks a production scope")
	}
	seen := map[string]bool{}
	for _, value := range []string{a.IntentDigest, a.ApprovalDigest, a.JournalDigest, a.PolicyDigest,
		a.EndpointAdmission, a.LegacyRevocation, a.LegacyRequestDrain, a.ShadowEvidence, a.NativeRuntime} {
		if !digestPattern.MatchString(value) || seen[value] {
			return nil, errors.New("Front/Worker cutover attestation requires distinct and complete evidence digests")
		}
		seen[value] = true
	}
	if a.ObservedAt <= 0 || a.ExpiresAt <= a.ObservedAt || a.ExpiresAt-a.ObservedAt > int64(maxCutoverAttestationAge/time.Second) {
		return nil, errors.New("invalid Front/Worker cutover evidence lifetime")
	}
	a.Signature = nil
	raw, err := declarativerelease.CanonicalJSON(a)
	if err != nil {
		return nil, err
	}
	return append([]byte("fugue.front-worker.cutover.attestation.v1\x00"), raw...), nil
}

// FrontWorkerPolicyDigest identifies the desired native scope and the live,
// type-checked policy and binding identities. Recreating an identical object
// or changing its resourceVersion invalidates the signed observation.
func FrontWorkerPolicyDigest(config FrontWorkerCutover, observed []FrontWorkerPolicyObservation) (string, error) {
	expected, err := FrontWorkerCutoverPolicies(config)
	if err != nil || config.Phase != KubeCutoverNative || len(expected) != len(observed) {
		return "", errors.New("invalid native Front/Worker admission observation")
	}
	for i, pair := range expected {
		policy := observed[i]
		if policy.PolicyName != pair.Policy.Name || policy.PolicyUID == "" || policy.PolicyRevision == "" ||
			policy.BindingUID == "" || policy.BindingRevision == "" {
			return "", errors.New("incomplete Front/Worker policy revision evidence")
		}
	}
	raw, err := declarativerelease.CanonicalJSON(struct {
		Config FrontWorkerCutover
		Live   []FrontWorkerPolicyObservation
	}{config, observed})
	if err != nil {
		return "", err
	}
	return digest(append([]byte("fugue.front-worker.cutover.policy.v1\x00"), raw...)), nil
}

type FrontWorkerProviderGate struct {
	client kubernetes.Interface
	config FrontWorkerCutover
	signed FrontWorkerCutoverAttestation
	now    func() time.Time
}

func NewFrontWorkerProviderGate(client kubernetes.Interface, config FrontWorkerCutover, journal *productioncontrol.Journal,
	raw []byte, publicKey ed25519.PublicKey) (*FrontWorkerProviderGate, error) {
	if client == nil || config.Phase != KubeCutoverNative || config.validate() != nil || journal == nil {
		return nil, errors.New("Front/Worker cutover requires a native scope, authenticated Kubernetes client and execution journal")
	}
	if err := journal.VerifyIndependentObserverKey(publicKey); err != nil {
		return nil, err
	}
	var a FrontWorkerCutoverAttestation
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || len(raw) > MaxCutoverAttestationBytes || decodeStrict(raw, &a) != nil {
		return nil, errors.New("invalid Front/Worker cutover attestation")
	}
	canonical, err := declarativerelease.CanonicalJSON(a)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, errors.New("Front/Worker cutover attestation is not canonical")
	}
	message, err := a.SigningInput()
	if err != nil || !ed25519.Verify(publicKey, message, a.Signature) {
		return nil, errors.New("Front/Worker cutover evidence lacks the trusted observer signature")
	}
	return &FrontWorkerProviderGate{client, config, a, time.Now}, nil
}

func (g *FrontWorkerProviderGate) Verify(ctx context.Context, scope productioncontrol.Scope, intentDigest, approvalDigest string, plan productioncontrol.FrontWorkerShadow) error {
	return g.verify(ctx, scope, intentDigest, approvalDigest, plan, true)
}

// VerifyRecovery checks the same identity, evidence signature and provider
// revisions. A recovery can restore the retained Worker after approval
// expiry, but cannot originate a handoff or arm an existing one.
func (g *FrontWorkerProviderGate) VerifyRecovery(ctx context.Context, claim productioncontrol.Claim, plan productioncontrol.FrontWorkerShadow) error {
	return g.verify(ctx, claim.Scope, claim.IntentDigest, claim.ApprovalDigest, plan, false)
}

func (g *FrontWorkerProviderGate) verify(ctx context.Context, scope productioncontrol.Scope, intentDigest, approvalDigest string, plan productioncontrol.FrontWorkerShadow, fresh bool) error {
	a := g.signed
	if scope != a.Scope || intentDigest != a.IntentDigest || approvalDigest != a.ApprovalDigest || plan.Journal.Digest != a.JournalDigest ||
		scope.ResourceID != productioncontrol.FrontWorkerResource(g.config.Namespace, g.config.Group) ||
		plan.Namespace != g.config.Namespace || plan.Journal.Recipient.Group != g.config.Group ||
		plan.GroupFence != FrontRepairFenceName(g.config.Group) || !slices.Contains(g.config.ComponentLeases, plan.LeaseName) ||
		(plan.Retirement != nil && !slices.Contains(g.config.WorkerDaemonSets, plan.Retirement.DaemonSet)) {
		return errors.New("Front/Worker approval and cutover evidence refer to different resources or handoffs")
	}
	review, err := g.client.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authentication.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("verify Front/Worker Guardian Kubernetes identity: %w", err)
	}
	if review.Status.UserInfo.Username != g.config.Guardian {
		return errors.New("Front/Worker provider resolved a different Guardian identity")
	}
	live, err := ObserveFrontWorkerCutover(ctx, g.client, g.config)
	if err != nil {
		return err
	}
	liveDigest, err := FrontWorkerPolicyDigest(g.config, live)
	if err != nil || liveDigest != a.PolicyDigest {
		return errors.New("Front/Worker live admission revisions differ from the signed provider evidence")
	}
	at := g.now().Unix()
	if at < a.ObservedAt || (fresh && at >= a.ExpiresAt) {
		return errors.New("Front/Worker cutover evidence is outside its validity window")
	}
	return nil
}
