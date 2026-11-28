package productioncontrol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"
)

const registryRehomeAppliedDomain = "fugue.production.registry-rehome-applied.v1"

// RegistryRehomeAppliedEvidence is not the owner approval or a provider
// dispatch permit. Its pins refer to independently retained observations,
// backup bytes and post-expiry proof that the temporary writer is closed.
type RegistryRehomeAppliedEvidence struct {
	ClusterID        string `json:"clusterId"`
	ResourceID       string `json:"resourceId"`
	IntentDigest     string `json:"intentDigest"`
	ApprovalDigest   string `json:"approvalDigest"`
	Fence            int64  `json:"fence"`
	AdmissionDigest  string `json:"admissionDigest"`
	BaselineDigest   string `json:"baselineDigest"`
	BundleDigest     string `json:"bundleDigest"`
	ReadbackDigest   string `json:"readbackDigest"`
	ServingEvent     string `json:"servingEventDigest"`
	ServingState     string `json:"servingStateDigest"`
	EndpointFreeze   string `json:"endpointFreezeDigest"`
	WriterRevocation string `json:"writerRevocationDigest"`
	RequestDrain     string `json:"requestDrainDigest"`
	BackupReadback   string `json:"backupReadbackDigest"`
	ObservedAt       int64  `json:"observedAtUnix"`
	ExpiresAt        int64  `json:"expiresAtUnix"`
	Signature        []byte `json:"signature"`
}

func (e RegistryRehomeAppliedEvidence) claim() Claim {
	return Claim{Scope{e.ClusterID, e.ResourceID}, e.Fence, e.IntentDigest, e.ApprovalDigest}
}

func (e RegistryRehomeAppliedEvidence) SigningInput() ([]byte, error) {
	if !e.claim().Scope.valid() || !deploymentScope.MatchString(e.ResourceID) || e.Fence < 1 || e.ObservedAt <= 0 ||
		e.ExpiresAt <= e.ObservedAt || e.ExpiresAt-e.ObservedAt > int64(MaxWitnessAge/time.Second) {
		return nil, errors.New("invalid registry rehome applied scope, fence or lifetime")
	}
	seen := make(map[string]bool)
	for _, d := range []string{e.IntentDigest, e.ApprovalDigest, e.AdmissionDigest, e.BaselineDigest, e.BundleDigest,
		e.ReadbackDigest, e.ServingEvent, e.ServingState, e.EndpointFreeze, e.WriterRevocation, e.RequestDrain, e.BackupReadback} {
		if !digestPattern.MatchString(d) || seen[d] {
			return nil, errors.New("registry rehome applied settlement requires distinct and complete evidence")
		}
		seen[d] = true
	}
	e.Signature = nil
	return signedBytes(registryRehomeAppliedDomain, e)
}

func (e RegistryRehomeAppliedEvidence) fresh(now time.Time) bool {
	return now.Unix() >= e.ObservedAt && now.Unix() < e.ExpiresAt
}

func EncodeRegistryRehomeAppliedEvidence(e RegistryRehomeAppliedEvidence) ([]byte, string, error) {
	if _, err := e.SigningInput(); err != nil {
		return nil, "", err
	}
	if len(e.Signature) != ed25519.SignatureSize {
		return nil, "", errors.New("invalid registry rehome applied signature size")
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return nil, "", err
	}
	return raw, digest(raw), nil
}

func readRegistryRehomeAppliedEvidence(raw []byte, key ed25519.PublicKey) (RegistryRehomeAppliedEvidence, string, error) {
	var e RegistryRehomeAppliedEvidence
	if len(raw) == 0 || len(raw) > MaxWitnessBytes || json.Unmarshal(raw, &e) != nil {
		return RegistryRehomeAppliedEvidence{}, "", errors.New("invalid registry rehome applied evidence")
	}
	canonical, pin, err := EncodeRegistryRehomeAppliedEvidence(e)
	if err != nil || !bytes.Equal(canonical, raw) {
		return RegistryRehomeAppliedEvidence{}, "", errors.New("registry rehome applied evidence is not canonical")
	}
	message, err := e.SigningInput()
	if err != nil || !ed25519.Verify(key, message, e.Signature) {
		return RegistryRehomeAppliedEvidence{}, "", errors.New("registry rehome applied evidence lacks the enrolled observer's signature")
	}
	return e, pin, nil
}

func bindRegistryRehomeApplied(intent Intent, claim Claim, a RegistryRehomeAdmission, admissionPin string, e RegistryRehomeAppliedEvidence) (ExecutionResult, error) {
	if intent.Operation != ComponentRegistryMove || intent.scope() != claim.Scope || claim != e.claim() ||
		intent.Generation == math.MaxInt64 || e.AdmissionDigest != admissionPin || e.ObservedAt < a.ExpiresAt ||
		e.BaselineDigest == intent.CurrentDigest || e.BaselineDigest == intent.TargetDigest ||
		e.EndpointFreeze == a.EndpointAdmission || e.WriterRevocation == a.LegacyRevocation || e.RequestDrain == a.LegacyRequestDrain ||
		e.BackupReadback == a.RegistryRetention || e.BackupReadback == a.RollbackNodePull {
		return ExecutionResult{}, errors.New("registry rehome applied result lacks distinct post-expiry evidence for its exact admission and new baseline")
	}
	return ExecutionResult{Outcome: Applied, Verified: Version{intent.Generation + 1, e.BaselineDigest}}, nil
}

// RegistryRehomeAppliedCheck must re-read the provider and compare the
// retained serving event, both exact artifacts and the recomputed baseline
// from independently pinned recovery bytes. It never grants a writer.
type RegistryRehomeAppliedCheck func(context.Context, RegistryRehomeAppliedEvidence) error

// CloseApplied requires a dedicated append-only principal, not the writer
// or the ordinary settlement role. Its check runs before taking the SQL lock.
func (r *RegistryRehomeAdmissionReader) CloseApplied(ctx context.Context, actor, intentDigest string, raw []byte, check RegistryRehomeAppliedCheck) (Recorded, error) {
	if r == nil || r.journal == nil || check == nil || !tokenPattern.MatchString(actor) || !digestPattern.MatchString(intentDigest) {
		return Recorded{}, errors.New("registry rehome applied settlement requires its enrolled observer, actor and live verification")
	}
	e, pin, err := readRegistryRehomeAppliedEvidence(raw, r.observer)
	if err != nil {
		return Recorded{}, err
	}
	j := r.journal
	intent, _, err := j.ReadIntent(ctx, intentDigest)
	if err != nil {
		return Recorded{}, err
	}
	claim, existing, err := j.ReadExecution(ctx, r.scope, intentDigest)
	if err != nil || claim != e.claim() {
		return Recorded{}, errors.Join(ErrStaleFence, err)
	}
	admission, admissionPin, err := r.ReadAdmission(ctx, claim)
	if err != nil {
		return Recorded{}, err
	}
	result, err := bindRegistryRehomeApplied(intent, claim, admission, admissionPin, e)
	if err != nil {
		return Recorded{}, err
	}
	result.EvidenceDigest = pin
	replay := func(db queryRow, existing ExecutionResult) (bool, error) {
		if _, err := replayResult(existing, result); err != nil {
			return false, err
		}
		stored, err := readRegistryRehomeAppliedRow(ctx, db, claim.IntentDigest, result)
		if err != nil || !bytes.Equal(stored, raw) {
			return false, errors.Join(ErrResultConflict, err)
		}
		return false, nil
	}
	if existing != nil {
		_, err := replay(j.db, *existing)
		if err != nil {
			return Recorded{}, err
		}
		return Recorded{Digest: pin}, nil
	}
	if !e.fresh(j.now()) {
		return Recorded{}, ErrStaleFence
	}
	if err := check(ctx, e); err != nil {
		return Recorded{}, err
	}
	return j.record(ctx, pin, func(tx *sql.Tx) (bool, error) {
		if err := lockResource(ctx, tx, claim.Scope); err != nil {
			return false, err
		}
		if stored, err := readResult(ctx, tx, intentDigest); err == nil {
			return replay(tx, stored)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		state, err := readResource(ctx, tx, claim.Scope)
		if err != nil || state.LastFence != claim.Fence || state.Pending != intentDigest ||
			state.LastVerified != (Version{intent.Generation, intent.CurrentDigest}) || !e.fresh(j.now()) {
			return false, errors.Join(ErrStaleFence, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_control.registry_rehome_applied
			(intent_digest,admission_digest,evidence_digest,baseline_digest,envelope) VALUES ($1,$2,$3,$4,$5)`,
			intentDigest, admissionPin, pin, e.BaselineDigest, raw); err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_control.execution_results
			(intent_digest,outcome,generation,verified_digest,evidence_digest,actor) VALUES ($1,$2,$3,$4,$5,$6)`,
			intentDigest, result.Outcome, result.Verified.Generation, result.Verified.Digest, pin, actor)
		return err == nil, err
	})
}

// ReadApplied verifies the terminal receipt for an exact claim. The result
// is historical: callers must separately bind the current resource head.
func (r *RegistryRehomeAdmissionReader) ReadApplied(ctx context.Context, intentDigest string) (RegistryRehomeAppliedEvidence, ExecutionResult, error) {
	var none RegistryRehomeAppliedEvidence
	if r == nil || r.journal == nil {
		return none, ExecutionResult{}, ErrStaleFence
	}
	j := r.journal
	claim, result, err := j.ReadExecution(ctx, r.scope, intentDigest)
	if err != nil || result == nil || result.Outcome != Applied {
		return none, ExecutionResult{}, errors.Join(errors.New("registry rehome has no applied settlement"), err)
	}
	raw, err := readRegistryRehomeAppliedRow(ctx, j.db, intentDigest, *result)
	if err != nil {
		return none, ExecutionResult{}, err
	}
	e, pin, err := readRegistryRehomeAppliedEvidence(raw, r.observer)
	if err != nil || pin != result.EvidenceDigest {
		return none, ExecutionResult{}, errors.Join(ErrResultConflict, err)
	}
	intent, _, err := j.ReadIntent(ctx, intentDigest)
	if err != nil {
		return none, ExecutionResult{}, err
	}
	a, admissionPin, err := r.ReadAdmission(ctx, claim)
	if err != nil {
		return none, ExecutionResult{}, err
	}
	want, err := bindRegistryRehomeApplied(intent, claim, a, admissionPin, e)
	want.EvidenceDigest = pin
	if err != nil || want != *result {
		return none, ExecutionResult{}, errors.Join(ErrResultConflict, err)
	}
	return e, *result, nil
}

func readRegistryRehomeAppliedRow(ctx context.Context, db queryRow, intentDigest string, result ExecutionResult) ([]byte, error) {
	var raw []byte
	err := db.QueryRowContext(ctx, `SELECT r.envelope FROM fugue_control.registry_rehome_applied r
		JOIN fugue_control.registry_rehome_admissions a USING (intent_digest)
		WHERE r.intent_digest=$1 AND r.evidence_digest=$2 AND r.baseline_digest=$3 AND r.admission_digest=a.evidence_digest`,
		intentDigest, result.EvidenceDigest, result.Verified.Digest).Scan(&raw)
	return raw, err
}
