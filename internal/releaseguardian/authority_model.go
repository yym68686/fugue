package releaseguardian

import (
	"context"
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
	RouteBundleRecordKind             = edgeauthority.RouteBundleRecordKind
	CandidateAuthorityKind            = "CandidateAuthority"
	CurrentAuthorityKind              = "CurrentAuthority"
	CandidateCanaryResultKind         = "CandidateCanaryResult"
	GroupQualificationRecordKind      = "GroupQualificationRecord"
	AuthorityNormalizationReceiptKind = "AuthorityNormalizationReceipt"
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
	APIVersion                 string                  `json:"apiVersion"`
	Kind                       string                  `json:"kind"`
	GroupID                    string                  `json:"groupId"`
	RecordDigest               string                  `json:"recordDigest"`
	BundleGeneration           string                  `json:"bundleGeneration"`
	ServingGeneration          string                  `json:"servingGeneration,omitempty"`
	AuthoritySequence          uint64                  `json:"authoritySequence,omitempty"`
	CandidateSequence          uint64                  `json:"candidateSequence,omitempty"`
	CurrentPublicationSequence uint64                  `json:"currentPublicationSequence,omitempty"`
	CurrentRecoveryEpoch       uint64                  `json:"currentRecoveryEpoch,omitempty"`
	CurrentBundleDigest        string                  `json:"currentBundleDigest,omitempty"`
	CurrentServingGeneration   string                  `json:"currentServingGeneration,omitempty"`
	CandidateEpoch             uint64                  `json:"candidateEpoch,omitempty"`
	WorkerSlot                 AuthoritySlot           `json:"workerSlot"`
	ReleaseRecordDigest        string                  `json:"releaseRecordDigest"`
	WorkerSourceSHA            string                  `json:"workerSourceSha,omitempty"`
	WorkerImageDigest          string                  `json:"workerImageDigest,omitempty"`
	State                      CandidateAuthorityState `json:"state"`
	Generation                 int64                   `json:"generation"`
	CanaryResultDigest         string                  `json:"canaryResultDigest,omitempty"`
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
	legacyLoadedWitness := candidate.State == CandidateAuthorityLoaded && candidate.AuthoritySequence > 0 && candidate.CandidateSequence > 0 &&
		((candidate.CurrentPublicationSequence == 0 && candidate.CurrentRecoveryEpoch == 0 && candidate.CurrentBundleDigest == "" && candidate.CurrentServingGeneration == "" && candidate.CandidateEpoch == 0) ||
			(candidate.CurrentPublicationSequence > 0 && candidate.CurrentPublicationSequence <= candidate.AuthoritySequence &&
				digestPattern.MatchString(candidate.CurrentBundleDigest) && candidate.CurrentServingGeneration == "" && candidate.CandidateEpoch > candidate.CurrentPublicationSequence))
	if candidate.AuthoritySequence != 0 && !legacyLoadedWitness && (candidate.CurrentPublicationSequence == 0 || candidate.CurrentPublicationSequence > candidate.AuthoritySequence ||
		!digestPattern.MatchString(candidate.CurrentBundleDigest) || !authorityGenerationPattern.MatchString(candidate.CurrentServingGeneration) ||
		candidate.CandidateEpoch == 0 || candidate.CandidateEpoch <= candidate.CurrentPublicationSequence) {
		return errors.New("candidate promotion CAS witness is invalid")
	}
	if candidate.AuthoritySequence == 0 && (candidate.CurrentPublicationSequence != 0 || candidate.CurrentRecoveryEpoch != 0 || candidate.CurrentBundleDigest != "" || candidate.CurrentServingGeneration != "" || candidate.CandidateEpoch != 0) {
		return errors.New("candidate promotion CAS witness is incomplete")
	}
	if (candidate.WorkerSourceSHA == "") != (candidate.WorkerImageDigest == "") ||
		(candidate.WorkerSourceSHA != "" && (!shaPattern.MatchString(candidate.WorkerSourceSHA) || !digestPattern.MatchString(candidate.WorkerImageDigest))) {
		return errors.New("candidate Worker release identity is invalid")
	}
	return nil
}

// HasWorkerReleaseIdentity distinguishes an explicitly staged Worker release
// from the historical Edge Control self-candidates. Only the former is
// eligible to acquire ordinary traffic authority.
func (candidate CandidateAuthority) HasWorkerReleaseIdentity() bool {
	return shaPattern.MatchString(candidate.WorkerSourceSHA) && digestPattern.MatchString(candidate.WorkerImageDigest)
}

func (candidate CandidateAuthority) HasPromotionWitness() bool {
	return candidate.AuthoritySequence > 0 && candidate.CandidateSequence > 0 && candidate.CurrentPublicationSequence > 0 &&
		digestPattern.MatchString(candidate.CurrentBundleDigest) && authorityGenerationPattern.MatchString(candidate.CurrentServingGeneration) &&
		candidate.CandidateEpoch > candidate.CurrentPublicationSequence
}

// CurrentAuthority is the only pointer that grants ordinary user traffic for
// a group. The previous fields are the immediately reversible LKG authority.
type CurrentAuthority struct {
	APIVersion                string        `json:"apiVersion"`
	Kind                      string        `json:"kind"`
	GroupID                   string        `json:"groupId"`
	CurrentRecordDigest       string        `json:"currentRecordDigest"`
	CurrentWorkerSlot         AuthoritySlot `json:"currentWorkerSlot"`
	CurrentFrontGeneration    uint64        `json:"currentFrontGeneration,omitempty"`
	CurrentBundleGeneration   string        `json:"currentBundleGeneration,omitempty"`
	CurrentWorkerSourceSHA    string        `json:"currentWorkerSourceSha,omitempty"`
	CurrentWorkerImageDigest  string        `json:"currentWorkerImageDigest,omitempty"`
	PreviousRecordDigest      string        `json:"previousRecordDigest,omitempty"`
	PreviousWorkerSlot        AuthoritySlot `json:"previousWorkerSlot,omitempty"`
	PreviousFrontGeneration   uint64        `json:"previousFrontGeneration,omitempty"`
	PreviousBundleGeneration  string        `json:"previousBundleGeneration,omitempty"`
	PreviousWorkerSourceSHA   string        `json:"previousWorkerSourceSha,omitempty"`
	PreviousWorkerImageDigest string        `json:"previousWorkerImageDigest,omitempty"`
	AuthorityEpoch            int64         `json:"authorityEpoch"`
	BaselineReceiptDigest     string        `json:"baselineReceiptDigest,omitempty"`
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
	currentFrontEmpty := authority.CurrentFrontGeneration == 0 && authority.CurrentBundleGeneration == "" && authority.CurrentWorkerSourceSHA == "" && authority.CurrentWorkerImageDigest == ""
	currentFrontValid := authority.CurrentFrontGeneration > 0 && authorityGenerationPattern.MatchString(authority.CurrentBundleGeneration) &&
		shaPattern.MatchString(authority.CurrentWorkerSourceSHA) && digestPattern.MatchString(authority.CurrentWorkerImageDigest)
	if !currentFrontEmpty && !currentFrontValid {
		return errors.New("current authority Front identity is invalid")
	}
	if authority.PreviousRecordDigest == "" && authority.PreviousWorkerSlot == "" {
		if authority.PreviousFrontGeneration != 0 || authority.PreviousBundleGeneration != "" || authority.PreviousWorkerSourceSHA != "" || authority.PreviousWorkerImageDigest != "" {
			return errors.New("current authority has an unbound Front LKG")
		}
		return nil
	}
	previousFrontEmpty := authority.PreviousFrontGeneration == 0 && authority.PreviousBundleGeneration == "" && authority.PreviousWorkerSourceSHA == "" && authority.PreviousWorkerImageDigest == ""
	if currentFrontEmpty && previousFrontEmpty && digestPattern.MatchString(authority.PreviousRecordDigest) && authority.PreviousWorkerSlot.Validate() == nil &&
		authority.PreviousRecordDigest != authority.CurrentRecordDigest && authority.PreviousWorkerSlot != authority.CurrentWorkerSlot {
		return nil
	}
	if !digestPattern.MatchString(authority.PreviousRecordDigest) || authority.PreviousWorkerSlot.Validate() != nil ||
		authority.PreviousRecordDigest == authority.CurrentRecordDigest || authority.PreviousWorkerSlot == authority.CurrentWorkerSlot ||
		!currentFrontValid || authority.PreviousFrontGeneration == 0 || !authorityGenerationPattern.MatchString(authority.PreviousBundleGeneration) ||
		!shaPattern.MatchString(authority.PreviousWorkerSourceSHA) || !digestPattern.MatchString(authority.PreviousWorkerImageDigest) {
		return errors.New("current authority LKG is invalid")
	}
	return nil
}

// CandidateCanaryResult is immutable and candidate-bound. A canary result for
// one record, slot, release, or group cannot authorize another candidate.
type CandidateCanaryResult struct {
	APIVersion                 string        `json:"apiVersion"`
	Kind                       string        `json:"kind"`
	GroupID                    string        `json:"groupId"`
	CandidateRecordDigest      string        `json:"candidateRecordDigest"`
	AuthoritySequence          uint64        `json:"authoritySequence"`
	CandidateSequence          uint64        `json:"candidateSequence"`
	CurrentPublicationSequence uint64        `json:"currentPublicationSequence"`
	CurrentRecoveryEpoch       uint64        `json:"currentRecoveryEpoch"`
	CurrentBundleDigest        string        `json:"currentBundleDigest"`
	CurrentServingGeneration   string        `json:"currentServingGeneration"`
	CandidateEpoch             uint64        `json:"candidateEpoch"`
	BundleGeneration           string        `json:"bundleGeneration"`
	ServingGeneration          string        `json:"servingGeneration"`
	WorkerSlot                 AuthoritySlot `json:"workerSlot"`
	WorkerSourceSHA            string        `json:"workerSourceSha"`
	WorkerImageDigest          string        `json:"workerImageDigest"`
	WorkerCohortDigest         string        `json:"workerCohortDigest"`
	ReleaseRecordDigest        string        `json:"releaseRecordDigest"`
	RouteState                 HealthState   `json:"routeState"`
	DependencyState            HealthState   `json:"dependencyState"`
	EvidenceDigest             string        `json:"evidenceDigest"`
	ObservedAt                 string        `json:"observedAt"`
	ExpiresAt                  string        `json:"expiresAt"`
	KeyID                      string        `json:"keyId"`
	Signature                  string        `json:"signature"`
	ResultDigest               string        `json:"resultDigest"`
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
		result.AuthoritySequence == 0 || result.CandidateSequence == 0 || result.CurrentPublicationSequence == 0 ||
		result.CurrentPublicationSequence > result.AuthoritySequence || !digestPattern.MatchString(result.CurrentBundleDigest) ||
		!authorityGenerationPattern.MatchString(result.CurrentServingGeneration) ||
		result.CandidateEpoch <= result.CurrentPublicationSequence ||
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

// AuthorityNormalizationReceipt is the one-time witness used when a legacy
// writer switched the real Front before CurrentAuthority became the sole
// traffic pointer. It deliberately records no executable previous authority:
// the inactive slot must pass a fresh canary before it can become the next LKG.
type AuthorityNormalizationReceipt struct {
	APIVersion            string                         `json:"apiVersion"`
	Kind                  string                         `json:"kind"`
	GroupID               string                         `json:"groupId"`
	BaselineReceiptDigest string                         `json:"baselineReceiptDigest"`
	Before                CurrentAuthority               `json:"before"`
	After                 CurrentAuthority               `json:"after"`
	Nodes                 []AuthorityBaselineNodeWitness `json:"nodes"`
	ObservedAt            string                         `json:"observedAt"`
	ReceiptDigest         string                         `json:"receiptDigest"`
}

func (receipt AuthorityNormalizationReceipt) Seal() (AuthorityNormalizationReceipt, error) {
	receipt.APIVersion, receipt.Kind, receipt.ReceiptDigest = APIVersion, AuthorityNormalizationReceiptKind, ""
	sort.Slice(receipt.Nodes, func(i, j int) bool { return receipt.Nodes[i].NodeName < receipt.Nodes[j].NodeName })
	if err := receipt.validateUnsigned(); err != nil {
		return AuthorityNormalizationReceipt{}, err
	}
	raw, err := declarativerelease.CanonicalJSON(receipt)
	if err != nil {
		return AuthorityNormalizationReceipt{}, err
	}
	receipt.ReceiptDigest = digest(raw)
	return receipt, nil
}

func (receipt AuthorityNormalizationReceipt) Validate() error {
	if !digestPattern.MatchString(receipt.ReceiptDigest) || receipt.validateUnsigned() != nil {
		return errors.New("authority normalization receipt is invalid")
	}
	copy := receipt
	copy.ReceiptDigest = ""
	raw, err := declarativerelease.CanonicalJSON(copy)
	if err != nil || digest(raw) != receipt.ReceiptDigest {
		return errors.New("authority normalization receipt digest is invalid")
	}
	return nil
}

func (receipt AuthorityNormalizationReceipt) validateUnsigned() error {
	if receipt.APIVersion != APIVersion || receipt.Kind != AuthorityNormalizationReceiptKind ||
		!groupPattern.MatchString(receipt.GroupID) || !digestPattern.MatchString(receipt.BaselineReceiptDigest) ||
		receipt.Before.Validate() != nil || receipt.After.Validate() != nil || receipt.Before.GroupID != receipt.GroupID || receipt.After.GroupID != receipt.GroupID ||
		receipt.Before.BaselineReceiptDigest != receipt.BaselineReceiptDigest || receipt.After.BaselineReceiptDigest != receipt.BaselineReceiptDigest ||
		receipt.After.AuthorityEpoch <= receipt.Before.AuthorityEpoch || receipt.After.CurrentRecordDigest == receipt.Before.CurrentRecordDigest ||
		receipt.After.CurrentWorkerSlot == receipt.Before.CurrentWorkerSlot || receipt.After.PreviousRecordDigest != "" || receipt.After.PreviousWorkerSlot != "" ||
		len(receipt.Nodes) < 1 || len(receipt.Nodes) > 100 {
		return errors.New("authority normalization receipt identity is invalid")
	}
	observedAt, err := time.Parse(time.RFC3339Nano, receipt.ObservedAt)
	if err != nil || !observedAt.Equal(observedAt.UTC()) {
		return errors.New("authority normalization receipt time is invalid")
	}
	seen := map[string]bool{}
	for _, node := range receipt.Nodes {
		if !componentPattern.MatchString(node.NodeName) || seen[node.NodeName] || len(node.FrontPodUID) < 8 || len(node.WorkerPodUID) < 8 ||
			strings.TrimSpace(node.FrontResourceVersion) == "" || strings.TrimSpace(node.WorkerResourceVersion) == "" ||
			node.ActivationGeneration != receipt.After.CurrentFrontGeneration || node.BundleGeneration != receipt.After.CurrentBundleGeneration ||
			node.WorkerSourceSHA != receipt.After.CurrentWorkerSourceSHA || node.WorkerImageDigest != receipt.After.CurrentWorkerImageDigest ||
			!authorityGenerationPattern.MatchString(node.ServingGeneration) {
			return errors.New("authority normalization node witness is invalid")
		}
		seen[node.NodeName] = true
	}
	return nil
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

const AuthorityTransitionJournalKind = "AuthorityTransitionJournal"

type AuthorityTransitionPhase string

const (
	AuthorityTransitionPrepared  AuthorityTransitionPhase = "prepared"
	AuthorityTransitionActivated AuthorityTransitionPhase = "activated"
)

// AuthorityTransitionJournal is the durable group-local witness written before
// the first external authority mutation. It lets a restarted Guardian replay
// an idempotent Edge Control promotion and resume/compensate a partial Front CAS
// without accepting a new candidate or a new LKG.
type AuthorityTransitionJournal struct {
	APIVersion         string                         `json:"apiVersion"`
	Kind               string                         `json:"kind"`
	GroupID            string                         `json:"groupId"`
	Phase              AuthorityTransitionPhase       `json:"phase"`
	CurrentUID         string                         `json:"currentUid"`
	CurrentRV          string                         `json:"currentResourceVersion"`
	Before             CurrentAuthority               `json:"before"`
	Candidate          CandidateAuthority             `json:"candidate"`
	CanaryResultDigest string                         `json:"canaryResultDigest"`
	PreviousNodes      []AuthorityBaselineNodeWitness `json:"previousNodes"`
	Activation         *FrontAuthorityReceipt         `json:"activation,omitempty"`
	CreatedAt          string                         `json:"createdAt"`
	JournalDigest      string                         `json:"journalDigest"`
}

func (journal AuthorityTransitionJournal) Seal() (AuthorityTransitionJournal, error) {
	journal.APIVersion, journal.Kind, journal.JournalDigest = APIVersion, AuthorityTransitionJournalKind, ""
	if err := journal.validateUnsigned(); err != nil {
		return AuthorityTransitionJournal{}, err
	}
	raw, err := declarativerelease.CanonicalJSON(journal)
	if err != nil {
		return AuthorityTransitionJournal{}, err
	}
	journal.JournalDigest = digest(raw)
	return journal, nil
}

func (journal AuthorityTransitionJournal) Validate() error {
	if !digestPattern.MatchString(journal.JournalDigest) || journal.validateUnsigned() != nil {
		return errors.New("authority transition journal is invalid")
	}
	copy := journal
	copy.JournalDigest = ""
	raw, err := declarativerelease.CanonicalJSON(copy)
	if err != nil || digest(raw) != journal.JournalDigest {
		return errors.New("authority transition journal digest is invalid")
	}
	return nil
}

func (journal AuthorityTransitionJournal) validateUnsigned() error {
	if journal.APIVersion != APIVersion || journal.Kind != AuthorityTransitionJournalKind || !groupPattern.MatchString(journal.GroupID) ||
		journal.Before.Validate() != nil || journal.Candidate.Validate() != nil || journal.Before.GroupID != journal.GroupID ||
		journal.Candidate.GroupID != journal.GroupID || journal.Candidate.State != CandidateAuthorityVerified ||
		journal.Candidate.CanaryResultDigest != journal.CanaryResultDigest || !digestPattern.MatchString(journal.CanaryResultDigest) ||
		strings.TrimSpace(journal.CurrentUID) == "" || strings.TrimSpace(journal.CurrentRV) == "" || len(journal.PreviousNodes) < 1 || len(journal.PreviousNodes) > 100 {
		return errors.New("authority transition journal identity is invalid")
	}
	seenNodes := map[string]bool{}
	for _, node := range journal.PreviousNodes {
		if !componentPattern.MatchString(node.NodeName) || seenNodes[node.NodeName] || len(node.FrontPodUID) < 8 || len(node.WorkerPodUID) < 8 ||
			strings.TrimSpace(node.FrontResourceVersion) == "" || strings.TrimSpace(node.WorkerResourceVersion) == "" ||
			node.ActivationGeneration < 1 || !authorityGenerationPattern.MatchString(node.BundleGeneration) ||
			!authorityGenerationPattern.MatchString(node.ServingGeneration) || !shaPattern.MatchString(node.WorkerSourceSHA) ||
			!digestPattern.MatchString(node.WorkerImageDigest) {
			return errors.New("authority transition journal previous witness is invalid")
		}
		seenNodes[node.NodeName] = true
	}
	createdAt, err := time.Parse(time.RFC3339Nano, journal.CreatedAt)
	if err != nil || !createdAt.Equal(createdAt.UTC()) {
		return errors.New("authority transition journal time is invalid")
	}
	if journal.Phase == AuthorityTransitionPrepared {
		if journal.Activation != nil {
			return errors.New("prepared authority journal carries activation")
		}
	} else if journal.Phase != AuthorityTransitionActivated || journal.Activation == nil || journal.Activation.GroupID != journal.GroupID {
		return errors.New("authority transition journal phase is invalid")
	}
	return nil
}

// FrontAuthorityTarget carries only the exact candidate identity authorized by
// a signed canary. The runtime adapter owns all resource names and commands.
type FrontAuthorityTarget struct {
	GroupID                   string
	TargetSlot                AuthoritySlot
	CandidateBundleGeneration string
	ServingGeneration         string
	FrontBundleGeneration     string
	AuthoritySequence         uint64
	PublicationSequence       uint64
	RecoveryEpoch             uint64
	PublishedBundleDigest     string
	PreviousServingGeneration string
	PreviousSlot              AuthoritySlot
	PreviousFrontGeneration   uint64
	PreviousBundleGeneration  string
	PreviousWorkerSourceSHA   string
	PreviousWorkerImageDigest string
	CandidateEpoch            uint64
	WorkerSourceSHA           string
	WorkerImageDigest         string
	WorkerCohortDigest        string
	CandidateRecordDigest     string
	CanaryResultDigest        string
	PreviousNodes             []AuthorityBaselineNodeWitness
}

type FrontAuthorityReceipt struct {
	GroupID                   string
	PreviousSlot              AuthoritySlot
	PreviousGeneration        uint64
	PreviousBundleGeneration  string
	PreviousWorkerSourceSHA   string
	PreviousWorkerImageDigest string
	TargetSlot                AuthoritySlot
	TargetGeneration          uint64
	TargetBundleGeneration    string
	TargetWorkerSourceSHA     string
	TargetWorkerImageDigest   string
}

// FrontAuthorityActivator is a group-local transaction. Implementations keep
// the group Lease until CurrentAuthority CAS is either committed or rolled
// back, so another group remains independent throughout the switch.
type FrontAuthorityActivator interface {
	BeginPromote(context.Context, FrontAuthorityTarget) (FrontAuthorityTransaction, error)
	BeginRestore(context.Context, CurrentAuthority) (FrontAuthorityTransaction, error)
}

// AuthorityHealthObserver attributes a post-switch route failure by comparing
// ordinary user traffic with the exact inactive LKG slot. Both paths are
// outside the Guardian process; a shared failure is dependency degradation,
// not permission to roll back a healthy component.
type AuthorityHealthObserver interface {
	ObserveCurrentAndLKG(context.Context, CurrentAuthority) (currentHealthy, lkgHealthy bool, evidenceDigest string, err error)
}

type FrontAuthorityTransaction interface {
	Receipt() FrontAuthorityReceipt
	Commit(context.Context) error
	Rollback(context.Context) error
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
