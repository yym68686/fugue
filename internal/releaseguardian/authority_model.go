package releaseguardian

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/edgeauthority"
)

const (
	RouteBundleRecordKind        = edgeauthority.RouteBundleRecordKind
	CandidateAuthorityKind       = "CandidateAuthority"
	CurrentAuthorityKind         = "CurrentAuthority"
	CandidateCanaryResultKind    = "CandidateCanaryResult"
	GroupQualificationRecordKind = "GroupQualificationRecord"
)

type AuthoritySlot string

const (
	AuthoritySlotA AuthoritySlot = "a"
	AuthoritySlotB AuthoritySlot = "b"
)

func (slot AuthoritySlot) Validate() error {
	if slot != AuthoritySlotA && slot != AuthoritySlotB {
		return errors.New("authority worker slot is invalid")
	}
	return nil
}

// RouteBundleRecord has exactly one producer/consumer wire identity. Keeping a
// local look-alike caused Guardian to reject byte-valid Edge Control records.
type RouteBundleRecord = edgeauthority.RouteBundleRecord

type CandidateAuthorityState string

const (
	CandidateAuthorityLoaded   CandidateAuthorityState = "loaded"
	CandidateAuthorityVerified CandidateAuthorityState = "verified"
	CandidateAuthorityRejected CandidateAuthorityState = "rejected"
)

// CandidateAuthority is the group-local mutable candidate pointer. Kubernetes
// UID/ResourceVersion is its external CAS token; Generation orders state
// transitions without granting user traffic.
type CandidateAuthority struct {
	APIVersion          string                  `json:"apiVersion"`
	Kind                string                  `json:"kind"`
	GroupID             string                  `json:"groupId"`
	RecordDigest        string                  `json:"recordDigest"`
	BundleGeneration    string                  `json:"bundleGeneration"`
	ServingGeneration   string                  `json:"servingGeneration,omitempty"`
	AuthoritySequence   uint64                  `json:"authoritySequence,omitempty"`
	CandidateSequence   uint64                  `json:"candidateSequence,omitempty"`
	WorkerSlot          AuthoritySlot           `json:"workerSlot"`
	ReleaseRecordDigest string                  `json:"releaseRecordDigest"`
	State               CandidateAuthorityState `json:"state"`
	Generation          int64                   `json:"generation"`
	CanaryResultDigest  string                  `json:"canaryResultDigest,omitempty"`
}

func (candidate CandidateAuthority) Validate() error {
	if candidate.APIVersion != APIVersion || candidate.Kind != CandidateAuthorityKind ||
		!groupPattern.MatchString(candidate.GroupID) || !digestPattern.MatchString(candidate.RecordDigest) ||
		candidate.WorkerSlot.Validate() != nil || !digestPattern.MatchString(candidate.ReleaseRecordDigest) ||
		(candidate.State != CandidateAuthorityLoaded && candidate.State != CandidateAuthorityVerified && candidate.State != CandidateAuthorityRejected) ||
		candidate.Generation < 1 {
		return errors.New("candidate authority is invalid")
	}
	if candidate.State == CandidateAuthorityLoaded {
		if candidate.CanaryResultDigest != "" {
			return errors.New("loaded candidate cannot carry a canary result")
		}
	} else if !candidate.HasPromotionWitness() || !authorityGenerationPattern.MatchString(candidate.BundleGeneration) ||
		!authorityGenerationPattern.MatchString(candidate.ServingGeneration) || !digestPattern.MatchString(candidate.CanaryResultDigest) {
		return errors.New("terminal candidate must bind a canary result")
	}
	if candidate.BundleGeneration != "" && !authorityGenerationPattern.MatchString(candidate.BundleGeneration) {
		return errors.New("candidate authority bundle generation is invalid")
	}
	if candidate.ServingGeneration != "" && !authorityGenerationPattern.MatchString(candidate.ServingGeneration) {
		return errors.New("candidate authority serving generation is invalid")
	}
	if (candidate.AuthoritySequence == 0) != (candidate.CandidateSequence == 0) {
		return errors.New("candidate promotion witness is incomplete")
	}
	return nil
}

func (candidate CandidateAuthority) HasPromotionWitness() bool {
	return candidate.AuthoritySequence > 0 && candidate.CandidateSequence > 0
}

// CurrentAuthority is the only pointer that grants ordinary user traffic for
// a group. The previous fields are the immediately reversible LKG authority.
type CurrentAuthority struct {
	APIVersion            string        `json:"apiVersion"`
	Kind                  string        `json:"kind"`
	GroupID               string        `json:"groupId"`
	CurrentRecordDigest   string        `json:"currentRecordDigest"`
	CurrentWorkerSlot     AuthoritySlot `json:"currentWorkerSlot"`
	PreviousRecordDigest  string        `json:"previousRecordDigest,omitempty"`
	PreviousWorkerSlot    AuthoritySlot `json:"previousWorkerSlot,omitempty"`
	AuthorityEpoch        int64         `json:"authorityEpoch"`
	BaselineReceiptDigest string        `json:"baselineReceiptDigest,omitempty"`
}

func (authority CurrentAuthority) Validate() error {
	if authority.APIVersion != APIVersion || authority.Kind != CurrentAuthorityKind ||
		!groupPattern.MatchString(authority.GroupID) || !digestPattern.MatchString(authority.CurrentRecordDigest) ||
		authority.CurrentWorkerSlot.Validate() != nil || authority.AuthorityEpoch < 1 {
		return errors.New("current authority is invalid")
	}
	if authority.BaselineReceiptDigest != "" && !digestPattern.MatchString(authority.BaselineReceiptDigest) {
		return errors.New("current authority baseline receipt is invalid")
	}
	if authority.PreviousRecordDigest == "" && authority.PreviousWorkerSlot == "" {
		return nil
	}
	if !digestPattern.MatchString(authority.PreviousRecordDigest) || authority.PreviousWorkerSlot.Validate() != nil ||
		authority.PreviousRecordDigest == authority.CurrentRecordDigest || authority.PreviousWorkerSlot == authority.CurrentWorkerSlot {
		return errors.New("current authority LKG is invalid")
	}
	return nil
}

// CandidateCanaryResult is immutable and candidate-bound. A canary result for
// one record, slot, release, or group cannot authorize another candidate.
type CandidateCanaryResult struct {
	APIVersion            string        `json:"apiVersion"`
	Kind                  string        `json:"kind"`
	GroupID               string        `json:"groupId"`
	CandidateRecordDigest string        `json:"candidateRecordDigest"`
	AuthoritySequence     uint64        `json:"authoritySequence"`
	CandidateSequence     uint64        `json:"candidateSequence"`
	BundleGeneration      string        `json:"bundleGeneration"`
	ServingGeneration     string        `json:"servingGeneration"`
	WorkerSlot            AuthoritySlot `json:"workerSlot"`
	WorkerSourceSHA       string        `json:"workerSourceSha"`
	WorkerImageDigest     string        `json:"workerImageDigest"`
	WorkerCohortDigest    string        `json:"workerCohortDigest"`
	ReleaseRecordDigest   string        `json:"releaseRecordDigest"`
	RouteState            HealthState   `json:"routeState"`
	DependencyState       HealthState   `json:"dependencyState"`
	EvidenceDigest        string        `json:"evidenceDigest"`
	ObservedAt            string        `json:"observedAt"`
	ExpiresAt             string        `json:"expiresAt"`
	KeyID                 string        `json:"keyId"`
	Signature             string        `json:"signature"`
	ResultDigest          string        `json:"resultDigest"`
}

func (result CandidateCanaryResult) Seal() (CandidateCanaryResult, error) {
	result.APIVersion = APIVersion
	result.Kind = CandidateCanaryResultKind
	result.ResultDigest = ""
	if err := result.validateUnsigned(time.Time{}); err != nil {
		return CandidateCanaryResult{}, err
	}
	raw, err := declarativerelease.CanonicalJSON(result)
	if err != nil {
		return CandidateCanaryResult{}, err
	}
	result.ResultDigest = digest(raw)
	return result, nil
}

func (result CandidateCanaryResult) Validate(now time.Time) error {
	if !digestPattern.MatchString(result.ResultDigest) || result.validateUnsigned(now) != nil {
		return errors.New("candidate canary result is invalid")
	}
	copy := result
	copy.ResultDigest = ""
	raw, err := declarativerelease.CanonicalJSON(copy)
	if err != nil || digest(raw) != result.ResultDigest {
		return errors.New("candidate canary result digest is invalid")
	}
	return nil
}

// CandidateCanaryPublicKey derives the public half of the candidate-only
// signing identity. The private key never needs to be projected into the
// authority writer: it receives only this public key.
func CandidateCanaryPublicKey(signingMaterial []byte) (ed25519.PublicKey, error) {
	if len(signingMaterial) < 32 || len(signingMaterial) > 4096 {
		return nil, errors.New("candidate canary signing material is invalid")
	}
	seedInput := append([]byte("fugue-candidate-canary-ed25519-v1\x00"), signingMaterial...)
	seed := sha256.Sum256(seedInput)
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	publicKey := make(ed25519.PublicKey, ed25519.PublicKeySize)
	copy(publicKey, privateKey[ed25519.SeedSize:])
	return publicKey, nil
}

// SignCandidateCanaryResult seals a candidate-bound canary result with a
// domain-separated Ed25519 identity. The canary signer is deliberately
// distinct from the authority writer: only the prober receives signing
// material, while the writer receives the derived public key.
func SignCandidateCanaryResult(result CandidateCanaryResult, signingMaterial []byte) (CandidateCanaryResult, error) {
	if len(signingMaterial) < 32 || len(signingMaterial) > 4096 {
		return CandidateCanaryResult{}, errors.New("candidate canary signing key is invalid")
	}
	result.APIVersion = APIVersion
	result.Kind = CandidateCanaryResultKind
	result.Signature = testableSignaturePlaceholder
	result.ResultDigest = ""
	if err := result.validateUnsigned(time.Time{}); err != nil {
		return CandidateCanaryResult{}, err
	}
	result.Signature = ""
	raw, err := candidateCanarySigningBytes(result)
	if err != nil {
		return CandidateCanaryResult{}, err
	}
	seedInput := append([]byte("fugue-candidate-canary-ed25519-v1\x00"), signingMaterial...)
	seed := sha256.Sum256(seedInput)
	result.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(ed25519.NewKeyFromSeed(seed[:]), raw))
	return result.Seal()
}

// VerifySignature verifies only the independent canary attestation. Callers
// must still Validate freshness and bind CandidateAuthority UID/RV before
// using the result in an authority decision.
func (result CandidateCanaryResult) VerifySignature(publicKey []byte) error {
	if len(publicKey) != ed25519.PublicKeySize || result.Validate(time.Time{}) != nil {
		return errors.New("candidate canary signature input is invalid")
	}
	want := result.Signature
	copy := result
	copy.Signature = ""
	copy.ResultDigest = ""
	raw, err := candidateCanarySigningBytes(copy)
	if err != nil {
		return err
	}
	signature, err := base64.RawURLEncoding.DecodeString(want)
	if err != nil || !ed25519.Verify(ed25519.PublicKey(publicKey), raw, signature) {
		return errors.New("candidate canary signature is invalid")
	}
	return nil
}

func candidateCanarySigningBytes(result CandidateCanaryResult) ([]byte, error) {
	return declarativerelease.CanonicalJSON(struct {
		Domain string                `json:"domain"`
		Result CandidateCanaryResult `json:"result"`
	}{Domain: "fugue-candidate-canary-result/v1", Result: result})
}

func (result CandidateCanaryResult) validateUnsigned(now time.Time) error {
	if result.APIVersion != APIVersion || result.Kind != CandidateCanaryResultKind ||
		!groupPattern.MatchString(result.GroupID) || !digestPattern.MatchString(result.CandidateRecordDigest) ||
		result.AuthoritySequence == 0 || result.CandidateSequence == 0 ||
		!authorityGenerationPattern.MatchString(result.BundleGeneration) || !authorityGenerationPattern.MatchString(result.ServingGeneration) ||
		!shaPattern.MatchString(result.WorkerSourceSHA) ||
		!digestPattern.MatchString(result.WorkerImageDigest) || !digestPattern.MatchString(result.WorkerCohortDigest) ||
		result.WorkerSlot.Validate() != nil || !digestPattern.MatchString(result.ReleaseRecordDigest) ||
		(result.RouteState != HealthHealthy && result.RouteState != HealthDegraded) ||
		(result.DependencyState != HealthHealthy && result.DependencyState != HealthDegraded) ||
		!digestPattern.MatchString(result.EvidenceDigest) || !componentPattern.MatchString(result.KeyID) ||
		!candidateCanarySignaturePattern.MatchString(result.Signature) {
		return errors.New("candidate canary result identity is invalid")
	}
	observedAt, observedErr := time.Parse(time.RFC3339Nano, result.ObservedAt)
	expiresAt, expiresErr := time.Parse(time.RFC3339Nano, result.ExpiresAt)
	if observedErr != nil || expiresErr != nil || !observedAt.Equal(observedAt.UTC()) ||
		!expiresAt.Equal(expiresAt.UTC()) || !expiresAt.After(observedAt) || expiresAt.Sub(observedAt) > time.Minute {
		return errors.New("candidate canary result freshness is invalid")
	}
	if !now.IsZero() && now.UTC().After(expiresAt) {
		return errors.New("candidate canary result is expired")
	}
	return nil
}

var candidateCanarySignaturePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{86}$`)

var authorityGenerationPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{2,255}$`)

const AuthorityBaselineReceiptKind = "AuthorityBaselineReceipt"

type AuthorityBaselineNodeWitness struct {
	NodeName              string `json:"nodeName"`
	FrontPodUID           string `json:"frontPodUid"`
	FrontResourceVersion  string `json:"frontResourceVersion"`
	WorkerPodUID          string `json:"workerPodUid"`
	WorkerResourceVersion string `json:"workerResourceVersion"`
	ActivationGeneration  uint64 `json:"activationGeneration"`
	BundleGeneration      string `json:"bundleGeneration"`
	ServingGeneration     string `json:"servingGeneration"`
	WorkerSourceSHA       string `json:"workerSourceSha"`
	WorkerImageDigest     string `json:"workerImageDigest"`
}

type AuthorityBaselineReceipt struct {
	APIVersion           string                         `json:"apiVersion"`
	Kind                 string                         `json:"kind"`
	GroupID              string                         `json:"groupId"`
	BeforeRecordDigest   string                         `json:"beforeRecordDigest"`
	BeforeWorkerSlot     AuthoritySlot                  `json:"beforeWorkerSlot"`
	BeforeAuthorityEpoch int64                          `json:"beforeAuthorityEpoch"`
	RecordDigest         string                         `json:"recordDigest"`
	WorkerSlot           AuthoritySlot                  `json:"workerSlot"`
	AuthorityEpoch       int64                          `json:"authorityEpoch"`
	Nodes                []AuthorityBaselineNodeWitness `json:"nodes"`
	ObservedAt           string                         `json:"observedAt"`
	ReceiptDigest        string                         `json:"receiptDigest"`
}

func (receipt AuthorityBaselineReceipt) Seal() (AuthorityBaselineReceipt, error) {
	receipt.APIVersion, receipt.Kind, receipt.ReceiptDigest = APIVersion, AuthorityBaselineReceiptKind, ""
	sort.Slice(receipt.Nodes, func(i, j int) bool { return receipt.Nodes[i].NodeName < receipt.Nodes[j].NodeName })
	if err := receipt.validateUnsigned(); err != nil {
		return AuthorityBaselineReceipt{}, err
	}
	raw, err := declarativerelease.CanonicalJSON(receipt)
	if err != nil {
		return AuthorityBaselineReceipt{}, err
	}
	receipt.ReceiptDigest = digest(raw)
	return receipt, nil
}

func (receipt AuthorityBaselineReceipt) Validate() error {
	if !digestPattern.MatchString(receipt.ReceiptDigest) || receipt.validateUnsigned() != nil {
		return errors.New("authority baseline receipt is invalid")
	}
	copy := receipt
	copy.ReceiptDigest = ""
	raw, err := declarativerelease.CanonicalJSON(copy)
	if err != nil || digest(raw) != receipt.ReceiptDigest {
		return errors.New("authority baseline receipt digest is invalid")
	}
	return nil
}

func (receipt AuthorityBaselineReceipt) validateUnsigned() error {
	if receipt.APIVersion != APIVersion || receipt.Kind != AuthorityBaselineReceiptKind || !groupPattern.MatchString(receipt.GroupID) ||
		!digestPattern.MatchString(receipt.BeforeRecordDigest) || receipt.BeforeWorkerSlot.Validate() != nil || receipt.BeforeAuthorityEpoch < 1 ||
		!digestPattern.MatchString(receipt.RecordDigest) || receipt.WorkerSlot.Validate() != nil || receipt.AuthorityEpoch < 1 ||
		receipt.RecordDigest == receipt.BeforeRecordDigest || receipt.WorkerSlot == receipt.BeforeWorkerSlot || len(receipt.Nodes) < 1 || len(receipt.Nodes) > 100 {
		return errors.New("authority baseline receipt identity is invalid")
	}
	observedAt, err := time.Parse(time.RFC3339Nano, receipt.ObservedAt)
	if err != nil || !observedAt.Equal(observedAt.UTC()) {
		return errors.New("authority baseline receipt time is invalid")
	}
	seen := map[string]bool{}
	for _, node := range receipt.Nodes {
		if !componentPattern.MatchString(node.NodeName) || seen[node.NodeName] || len(node.FrontPodUID) < 8 || len(node.WorkerPodUID) < 8 ||
			strings.TrimSpace(node.FrontResourceVersion) == "" || strings.TrimSpace(node.WorkerResourceVersion) == "" || node.ActivationGeneration < 1 ||
			!authorityGenerationPattern.MatchString(node.BundleGeneration) || !authorityGenerationPattern.MatchString(node.ServingGeneration) ||
			!shaPattern.MatchString(node.WorkerSourceSHA) || !digestPattern.MatchString(node.WorkerImageDigest) {
			return errors.New("authority baseline node witness is invalid")
		}
		seen[node.NodeName] = true
	}
	return nil
}

const testableSignaturePlaceholder = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

type AuthorityTransitionAction string

const (
	AuthorityCandidateRejected AuthorityTransitionAction = "candidate_rejected"
	AuthorityCurrentSwitched   AuthorityTransitionAction = "current_switched"
	AuthorityCurrentReverted   AuthorityTransitionAction = "current_reverted"
)

type AuthorityTransitionReceipt struct {
	APIVersion         string                    `json:"apiVersion"`
	Kind               string                    `json:"kind"`
	GroupID            string                    `json:"groupId"`
	Action             AuthorityTransitionAction `json:"action"`
	CandidateDigest    string                    `json:"candidateDigest,omitempty"`
	CanaryResultDigest string                    `json:"canaryResultDigest,omitempty"`
	Before             CurrentAuthority          `json:"before"`
	After              CurrentAuthority          `json:"after"`
	ObservedAt         string                    `json:"observedAt"`
	ReceiptDigest      string                    `json:"receiptDigest"`
}

func (receipt AuthorityTransitionReceipt) Seal() (AuthorityTransitionReceipt, error) {
	receipt.APIVersion = APIVersion
	receipt.Kind = "AuthorityTransitionReceipt"
	receipt.ReceiptDigest = ""
	if err := receipt.validateUnsigned(); err != nil {
		return AuthorityTransitionReceipt{}, err
	}
	raw, err := declarativerelease.CanonicalJSON(receipt)
	if err != nil {
		return AuthorityTransitionReceipt{}, err
	}
	receipt.ReceiptDigest = digest(raw)
	return receipt, nil
}

func (receipt AuthorityTransitionReceipt) Validate() error {
	if !digestPattern.MatchString(receipt.ReceiptDigest) || receipt.validateUnsigned() != nil {
		return errors.New("authority transition receipt is invalid")
	}
	copy := receipt
	copy.ReceiptDigest = ""
	raw, err := declarativerelease.CanonicalJSON(copy)
	if err != nil || digest(raw) != receipt.ReceiptDigest {
		return errors.New("authority transition receipt digest is invalid")
	}
	return nil
}

func (receipt AuthorityTransitionReceipt) validateUnsigned() error {
	if receipt.APIVersion != APIVersion || receipt.Kind != "AuthorityTransitionReceipt" ||
		!groupPattern.MatchString(receipt.GroupID) || receipt.Before.Validate() != nil || receipt.After.Validate() != nil ||
		receipt.Before.GroupID != receipt.GroupID || receipt.After.GroupID != receipt.GroupID {
		return errors.New("authority transition receipt identity is invalid")
	}
	switch receipt.Action {
	case AuthorityCandidateRejected:
		if receipt.After != receipt.Before {
			return errors.New("candidate rejection changed current authority")
		}
	case AuthorityCurrentSwitched:
		if receipt.After.AuthorityEpoch != receipt.Before.AuthorityEpoch+1 ||
			receipt.After.CurrentRecordDigest != receipt.CandidateDigest ||
			receipt.After.PreviousRecordDigest != receipt.Before.CurrentRecordDigest ||
			receipt.After.PreviousWorkerSlot != receipt.Before.CurrentWorkerSlot ||
			receipt.After.CurrentRecordDigest == receipt.Before.CurrentRecordDigest || receipt.After.CurrentWorkerSlot == receipt.Before.CurrentWorkerSlot {
			return errors.New("authority switch receipt is invalid")
		}
	case AuthorityCurrentReverted:
		if receipt.After.AuthorityEpoch != receipt.Before.AuthorityEpoch+1 || receipt.Before.CurrentRecordDigest != receipt.CandidateDigest ||
			receipt.After.CurrentRecordDigest != receipt.Before.PreviousRecordDigest || receipt.After.CurrentWorkerSlot != receipt.Before.PreviousWorkerSlot ||
			receipt.After.PreviousRecordDigest != receipt.Before.CurrentRecordDigest || receipt.After.PreviousWorkerSlot != receipt.Before.CurrentWorkerSlot {
			return errors.New("authority revert receipt is invalid")
		}
	default:
		return errors.New("authority transition receipt action is invalid")
	}
	if !digestPattern.MatchString(receipt.CandidateDigest) || !digestPattern.MatchString(receipt.CanaryResultDigest) {
		return errors.New("authority transition receipt candidate binding is invalid")
	}
	observedAt, err := time.Parse(time.RFC3339Nano, receipt.ObservedAt)
	if err != nil || !observedAt.Equal(observedAt.UTC()) {
		return errors.New("authority transition receipt time is invalid")
	}
	return nil
}
