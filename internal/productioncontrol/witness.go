package productioncontrol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	witnessDomain   = "fugue.production.evidence.v1"
	MaxWitnessAge   = 5 * time.Minute
	MaxWitnessBytes = 8 << 10
)

type WitnessKind string

const (
	BaselineWitness WitnessKind = "baseline"
	TerminalWitness WitnessKind = "terminal"
)

// ExecutionWitness is an attestation by a configured provider observer, not
// an operator approval or an executor's self-report. ObservationDigest binds
// the positive provider readback. AuthorityDigest binds the evidence that
// legacy writers were revoked and drained (baseline), or that no further
// write under the claim can commit (terminal). Both underlying records must
// be retained by the observer before it attests. An observation alone is not
// evidence of drainage or dispatch closure.
type ExecutionWitness struct {
	Kind              WitnessKind `json:"kind"`
	KeyID             string      `json:"keyId"`
	ClusterID         string      `json:"clusterId"`
	ResourceID        string      `json:"resourceId"`
	Generation        int64       `json:"generation"`
	VerifiedDigest    string      `json:"verifiedDigest"`
	ObservationDigest string      `json:"observationDigest"`
	AuthorityDigest   string      `json:"authorityDigest"`
	ObservedAt        int64       `json:"observedAtUnix"`
	ExpiresAt         int64       `json:"expiresAtUnix"`
	IntentDigest      string      `json:"intentDigest,omitempty"`
	ApprovalDigest    string      `json:"approvalDigest,omitempty"`
	Fence             int64       `json:"fence,omitempty"`
	Outcome           Outcome     `json:"outcome,omitempty"`
	Signature         []byte      `json:"signature"`
}

func (w ExecutionWitness) scope() Scope     { return Scope{w.ClusterID, w.ResourceID} }
func (w ExecutionWitness) version() Version { return Version{w.Generation, w.VerifiedDigest} }

func (w ExecutionWitness) SigningInput() ([]byte, error) {
	if !w.scope().valid() || !w.version().valid() || !tokenPattern.MatchString(w.KeyID) ||
		!digestPattern.MatchString(w.ObservationDigest) || !digestPattern.MatchString(w.AuthorityDigest) ||
		w.ObservationDigest == w.AuthorityDigest || w.ObservedAt <= 0 || w.ExpiresAt <= w.ObservedAt ||
		w.ExpiresAt-w.ObservedAt > int64(MaxWitnessAge/time.Second) {
		return nil, errors.New("invalid production execution witness binding")
	}
	switch w.Kind {
	case BaselineWitness:
		if w.IntentDigest != "" || w.ApprovalDigest != "" || w.Fence != 0 || w.Outcome != "" {
			return nil, errors.New("baseline witness contains terminal fields")
		}
	case TerminalWitness:
		if !digestPattern.MatchString(w.IntentDigest) || !digestPattern.MatchString(w.ApprovalDigest) || w.Fence < 1 ||
			(w.Outcome != Applied && w.Outcome != Restored && w.Outcome != NotApplied) {
			return nil, errors.New("terminal witness lacks the exact claim and outcome")
		}
	default:
		return nil, errors.New("unsupported production execution witness")
	}
	w.Signature = nil
	return signedBytes(witnessDomain, w)
}

// EncodeExecutionWitness returns the canonical envelope and evidence digest
// to persist. The digest identifies the exact signed envelope, not the live
// state described inside it.
func EncodeExecutionWitness(w ExecutionWitness) ([]byte, string, error) {
	if _, err := w.SigningInput(); err != nil {
		return nil, "", err
	}
	if len(w.Signature) != ed25519.SignatureSize {
		return nil, "", errors.New("invalid production execution witness signature")
	}
	raw, err := json.Marshal(w)
	if err != nil {
		return nil, "", err
	}
	return raw, digest(raw), nil
}

// WitnessReader should fetch an immutable, content-addressed record from an
// independently provisioned evidence store. Reads must remain possible
// without GitHub. Authentication is checked separately from the storage path.
type WitnessReader interface {
	ReadExecutionWitness(context.Context, string) ([]byte, error)
}

type TrustedWitness struct {
	Scope     Scope
	PublicKey ed25519.PublicKey
}

type signedExecutionEvidence struct {
	source WitnessReader
	trust  map[string]TrustedWitness
	now    func() time.Time
}

// NewWitnessedExecutionJournal enables baseline and terminal recording only
// against provider attestations scoped to this resource. The verifier cannot
// collect provider evidence or construct a witness to unblock a claim.
func NewWitnessedExecutionJournal(journal *Journal, source WitnessReader, trusted map[string]TrustedWitness) (*ExecutionJournal, error) {
	if journal == nil || source == nil || len(trusted) == 0 {
		return nil, errors.New("production execution requires an independent witness source and trust")
	}
	keys := make(map[string]TrustedWitness, len(trusted))
	for id, key := range trusted {
		if !tokenPattern.MatchString(id) || !key.Scope.valid() || len(key.PublicKey) != ed25519.PublicKeySize {
			return nil, errors.New("invalid production execution witness trust")
		}
		if err := journal.VerifyIndependentObserverKey(key.PublicKey); err != nil {
			return nil, err
		}
		key.PublicKey = bytes.Clone(key.PublicKey)
		keys[id] = key
	}
	return NewExecutionJournal(journal, &signedExecutionEvidence{source, keys, journal.now})
}

func (v *signedExecutionEvidence) VerifyBaseline(ctx context.Context, baseline Baseline) error {
	w, err := v.read(ctx, baseline.EvidenceDigest, baseline.Scope)
	if err != nil {
		return err
	}
	if w.Kind != BaselineWitness || w.version() != baseline.Verified {
		return errors.New("production witness does not bind this baseline")
	}
	return nil
}

func (v *signedExecutionEvidence) VerifyTerminal(ctx context.Context, claim Claim, intent Intent, result ExecutionResult) error {
	intentDigest, err := intent.Digest()
	if err != nil || claim.IntentDigest != intentDigest || claim.Scope != intent.scope() {
		return errors.New("production witness request has no matching execution intent")
	}
	w, err := v.read(ctx, result.EvidenceDigest, claim.Scope)
	if err != nil {
		return err
	}
	if w.Kind != TerminalWitness || w.version() != result.Verified || w.IntentDigest != claim.IntentDigest ||
		w.ApprovalDigest != claim.ApprovalDigest || w.Fence != claim.Fence || w.Outcome != result.Outcome {
		return errors.New("production witness does not bind this terminal execution")
	}
	return nil
}

func (v *signedExecutionEvidence) read(ctx context.Context, evidenceDigest string, scope Scope) (ExecutionWitness, error) {
	var w ExecutionWitness
	if !digestPattern.MatchString(evidenceDigest) {
		return w, errors.New("invalid production execution witness digest")
	}
	raw, err := v.source.ReadExecutionWitness(ctx, evidenceDigest)
	if err != nil {
		return w, fmt.Errorf("read independent production execution witness: %w", err)
	}
	if len(raw) == 0 || len(raw) > MaxWitnessBytes || digest(raw) != evidenceDigest || json.Unmarshal(raw, &w) != nil {
		return w, errors.New("production execution witness does not match its digest")
	}
	canonical, _, err := EncodeExecutionWitness(w)
	if err != nil || !bytes.Equal(canonical, raw) || w.scope() != scope {
		return w, errors.New("invalid canonical production execution witness")
	}
	at := v.now().Unix()
	if at < w.ObservedAt || at >= w.ExpiresAt {
		return w, errors.New("production execution witness is outside its validity window")
	}
	key, ok := v.trust[w.KeyID]
	message, err := w.SigningInput()
	if !ok || key.Scope != scope || err != nil || !ed25519.Verify(key.PublicKey, message, w.Signature) {
		return w, errors.New("production execution witness lacks a trusted provider signature for this resource")
	}
	return w, nil
}
