package productioncontrol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const registryRehomeAdmissionDomain = "fugue.production.registry-rehome-admission.v1"

// RegistryRehomeAdmission is independent technical evidence, not the owner's
// authorization. The observer retains the underlying immutable records and
// must bind them to the exact provider request and the next journal fence.
type RegistryRehomeAdmission struct {
	ClusterID          string `json:"clusterId"`
	ResourceID         string `json:"resourceId"`
	IntentDigest       string `json:"intentDigest"`
	ApprovalDigest     string `json:"approvalDigest"`
	Fence              int64  `json:"fence"`
	PolicyScope        string `json:"policyScopeDigest"`
	ProviderWrite      string `json:"providerWriteDigest"`
	EndpointAdmission  string `json:"endpointAdmissionDigest"`
	LegacyRevocation   string `json:"legacyRevocationDigest"`
	LegacyRequestDrain string `json:"legacyRequestDrainDigest"`
	ForwardNodePull    string `json:"forwardNodePullDigest"`
	RollbackNodePull   string `json:"rollbackNodePullDigest"`
	RegistryRetention  string `json:"registryRetentionDigest"`
	ObservedAt         int64  `json:"observedAtUnix"`
	ExpiresAt          int64  `json:"expiresAtUnix"`
	Signature          []byte `json:"signature"`
}

func (a RegistryRehomeAdmission) scope() Scope { return Scope{a.ClusterID, a.ResourceID} }

func (a RegistryRehomeAdmission) SigningInput() ([]byte, error) {
	if !a.scope().valid() || !deploymentScope.MatchString(a.ResourceID) || a.Fence < 1 ||
		a.ObservedAt <= 0 || a.ExpiresAt <= a.ObservedAt || a.ExpiresAt-a.ObservedAt > int64(MaxWitnessAge/time.Second) {
		return nil, errors.New("invalid registry rehome admission scope, fence or lifetime")
	}
	digests := []string{a.IntentDigest, a.ApprovalDigest, a.PolicyScope, a.ProviderWrite, a.EndpointAdmission,
		a.LegacyRevocation, a.LegacyRequestDrain, a.ForwardNodePull, a.RollbackNodePull, a.RegistryRetention}
	seen := make(map[string]bool, len(digests))
	for _, value := range digests {
		if !digestPattern.MatchString(value) || seen[value] {
			return nil, errors.New("registry rehome requires distinct and complete technical evidence")
		}
		seen[value] = true
	}
	a.Signature = nil
	return signedBytes(registryRehomeAdmissionDomain, a)
}

func (a RegistryRehomeAdmission) fresh(now time.Time) bool {
	return now.Unix() >= a.ObservedAt && now.Unix() < a.ExpiresAt
}

func EncodeRegistryRehomeAdmission(a RegistryRehomeAdmission) ([]byte, string, error) {
	if _, err := a.SigningInput(); err != nil {
		return nil, "", err
	}
	if len(a.Signature) != ed25519.SignatureSize {
		return nil, "", errors.New("invalid registry rehome admission signature size")
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return nil, "", err
	}
	return raw, digest(raw), nil
}

type verifiedRegistryRehomeAdmission struct {
	admission RegistryRehomeAdmission
	raw       []byte
	digest    string
}

func readRegistryRehomeAdmission(raw []byte, key ed25519.PublicKey) (verifiedRegistryRehomeAdmission, error) {
	var a RegistryRehomeAdmission
	if len(raw) == 0 || len(raw) > MaxWitnessBytes || json.Unmarshal(raw, &a) != nil {
		return verifiedRegistryRehomeAdmission{}, errors.New("invalid registry rehome admission")
	}
	canonical, d, err := EncodeRegistryRehomeAdmission(a)
	if err != nil || !bytes.Equal(canonical, raw) {
		return verifiedRegistryRehomeAdmission{}, errors.New("registry rehome admission is not canonical")
	}
	message, err := a.SigningInput()
	if err != nil || !ed25519.Verify(key, message, a.Signature) {
		return verifiedRegistryRehomeAdmission{}, errors.New("registry rehome admission is not signed by the enrolled provider observer")
	}
	return verifiedRegistryRehomeAdmission{a, canonical, d}, nil
}

// RegistryRehomeBoundaryCheck must be configured in the dedicated provider
// runtime. It confirms that the exact attested policy and request are still
// active, not just that the observer signed a document.
type RegistryRehomeBoundaryCheck func(context.Context, RegistryRehomeAdmission, Submission) error

// RegistryRehomeJournal offers only the specialized single-use claim. A
// reserved claim is not a reusable provider permit and cannot be launched or
// completed as an ordinary component release.
type RegistryRehomeJournal struct {
	journal  *Journal
	scope    Scope
	observer ed25519.PublicKey
	boundary RegistryRehomeBoundaryCheck
}

// RegistryRehomeAdmissionReader is the read-only side of provider admission.
// Provision it with a SELECT-only database principal.
type RegistryRehomeAdmissionReader struct {
	journal  *Journal
	scope    Scope
	observer ed25519.PublicKey
}

func NewRegistryRehomeAdmissionReader(j *Journal, scope Scope, observer ed25519.PublicKey) (*RegistryRehomeAdmissionReader, error) {
	if j == nil || !scope.valid() || !deploymentScope.MatchString(scope.ResourceID) {
		return nil, errors.New("registry rehome admission needs an enrolled Deployment")
	}
	if err := j.VerifyIndependentObserverKey(observer); err != nil {
		return nil, err
	}
	return &RegistryRehomeAdmissionReader{j, scope, bytes.Clone(observer)}, nil
}

func NewRegistryRehomeJournal(j *Journal, scope Scope, observer ed25519.PublicKey, check RegistryRehomeBoundaryCheck) (*RegistryRehomeJournal, error) {
	if check == nil {
		return nil, errors.New("registry rehome requires independent live provider admission")
	}
	reader, err := NewRegistryRehomeAdmissionReader(j, scope, observer)
	if err != nil {
		return nil, err
	}
	return &RegistryRehomeJournal{reader.journal, reader.scope, reader.observer, check}, nil
}

// Reserve verifies the signed owner approval and separate short-lived
// technical evidence. The claim and the exact technical envelope commit
// atomically on the ordinary Deployment fence. Only created=true can lead
// to a dispatch, and the caller must then verify the provider boundary again.
func (e *RegistryRehomeJournal) Reserve(ctx context.Context, actor, intentDigest, approvalDigest string, technicalEvidence []byte) (Claim, bool, error) {
	if e == nil || e.journal == nil || !tokenPattern.MatchString(actor) ||
		!digestPattern.MatchString(intentDigest) || !digestPattern.MatchString(approvalDigest) {
		return Claim{}, false, errors.New("invalid registry rehome reservation")
	}
	v, err := readRegistryRehomeAdmission(technicalEvidence, e.observer)
	if err != nil {
		return Claim{}, false, err
	}
	a := v.admission
	if a.scope() != e.scope || a.IntentDigest != intentDigest || a.ApprovalDigest != approvalDigest || !a.fresh(e.journal.now()) {
		return Claim{}, false, errors.New("registry rehome evidence does not bind the enrolled request or is stale")
	}
	intent, payload, err := e.journal.ReadIntent(ctx, intentDigest)
	if err != nil {
		return Claim{}, false, err
	}
	if intent.scope() != e.scope || intent.Operation != ComponentRegistryMove {
		return Claim{}, false, errors.New("registry rehome evidence cannot authorize this intent")
	}
	if err := verifyRecordedApproval(ctx, e.journal.db, e.journal, intent, intentDigest, approvalDigest); err != nil {
		return Claim{}, false, err
	}
	if err := e.boundary(ctx, a, Submission{intent, payload}); err != nil {
		return Claim{}, false, fmt.Errorf("registry rehome live provider boundary: %w", err)
	}
	return e.journal.reserveExecution(ctx, actor, intentDigest, approvalDigest, &v)
}

// ReadAdmission reads the exact technical envelope recorded for an existing
// claim. This is recovery/audit evidence; expired evidence never reissues a
// claim. Verification still uses this enrolled observer key.
func (e *RegistryRehomeJournal) ReadAdmission(ctx context.Context, claim Claim) (RegistryRehomeAdmission, string, error) {
	if e == nil || e.journal == nil || claim.Scope != e.scope || claim.Fence < 1 {
		return RegistryRehomeAdmission{}, "", ErrStaleFence
	}
	return readRegistryRehomeClaimAdmission(ctx, e.journal.db, claim, e.observer)
}

// VerifyPendingWrite is usable by an independent provider admission server
// with read-only journal access. A claim, attestation and owner approval
// must all still be live. This grants no dispatch and never extends expiry.
func (e *RegistryRehomeAdmissionReader) VerifyPendingWrite(ctx context.Context, claim Claim, policy, write string) error {
	if e == nil || e.journal == nil || claim.Scope != e.scope || claim.Fence < 1 {
		return ErrStaleFence
	}
	a, _, err := readRegistryRehomeClaimAdmission(ctx, e.journal.db, claim, e.observer)
	if err != nil {
		return err
	}
	if a.PolicyScope != policy || a.ProviderWrite != write || !a.fresh(e.journal.now()) {
		return ErrStaleFence
	}
	if err := e.journal.VerifyOpenClaim(ctx, claim); err != nil {
		return err
	}
	if !a.fresh(e.journal.now()) {
		return ErrStaleFence
	}
	return nil
}

func readRegistryRehomeClaimAdmission(ctx context.Context, db queryRow, claim Claim, observer ed25519.PublicKey) (RegistryRehomeAdmission, string, error) {
	var raw []byte
	var d string
	err := db.QueryRowContext(ctx, `SELECT evidence_digest, envelope FROM fugue_control.registry_rehome_admissions
		WHERE intent_digest=$1 AND approval_digest=$2 AND fence=$3`, claim.IntentDigest, claim.ApprovalDigest, claim.Fence).Scan(&d, &raw)
	if err != nil {
		return RegistryRehomeAdmission{}, "", err
	}
	v, err := readRegistryRehomeAdmission(raw, observer)
	if err != nil || v.digest != d || v.admission.scope() != claim.Scope || v.admission.Fence != claim.Fence ||
		v.admission.IntentDigest != claim.IntentDigest || v.admission.ApprovalDigest != claim.ApprovalDigest {
		return RegistryRehomeAdmission{}, "", errors.New("persisted registry rehome admission does not match the claim")
	}
	return v.admission, v.digest, nil
}

func appendRegistryRehomeAdmission(ctx context.Context, tx *sql.Tx, v *verifiedRegistryRehomeAdmission) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO fugue_control.registry_rehome_admissions
		(intent_digest, approval_digest, fence, evidence_digest, envelope) VALUES ($1,$2,$3,$4,$5)`,
		v.admission.IntentDigest, v.admission.ApprovalDigest, v.admission.Fence, v.digest, v.raw)
	return err
}
