package productioncontrol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

const registryRehomeNoWriteDomain = "fugue.production.registry-rehome-no-write.v1"

// RegistryRehomeNoWriteEvidence is a separate observer's assertion that the
// temporary writer was revoked, each endpoint enforces the restored freeze,
// all already admitted writes drained, and the original version is healthy.
// The observer retains the underlying immutable evidence outside GitHub.
// There is deliberately no field that can select a new verified version.
type RegistryRehomeNoWriteEvidence struct {
	ClusterID        string `json:"clusterId"`
	ResourceID       string `json:"resourceId"`
	IntentDigest     string `json:"intentDigest"`
	ApprovalDigest   string `json:"approvalDigest"`
	Fence            int64  `json:"fence"`
	AdmissionDigest  string `json:"admissionDigest"`
	ArchiveDigest    string `json:"archiveDigest"`
	ReadbackDigest   string `json:"readbackDigest"`
	EndpointFreeze   string `json:"endpointFreezeDigest"`
	WriterRevocation string `json:"writerRevocationDigest"`
	RequestDrain     string `json:"requestDrainDigest"`
	ServingHealth    string `json:"servingHealthDigest"`
	ObservedAt       int64  `json:"observedAtUnix"`
	ExpiresAt        int64  `json:"expiresAtUnix"`
	Signature        []byte `json:"signature"`
}

func (e RegistryRehomeNoWriteEvidence) claim() Claim {
	return Claim{Scope{e.ClusterID, e.ResourceID}, e.Fence, e.IntentDigest, e.ApprovalDigest}
}

func (e RegistryRehomeNoWriteEvidence) SigningInput() ([]byte, error) {
	if !e.claim().Scope.valid() || !deploymentScope.MatchString(e.ResourceID) || e.Fence < 1 ||
		e.ObservedAt <= 0 || e.ExpiresAt <= e.ObservedAt || e.ExpiresAt-e.ObservedAt > int64(MaxWitnessAge/time.Second) {
		return nil, errors.New("invalid registry rehome no-write scope, fence or lifetime")
	}
	seen := make(map[string]bool)
	for _, d := range []string{e.IntentDigest, e.ApprovalDigest, e.AdmissionDigest, e.ArchiveDigest, e.ReadbackDigest,
		e.EndpointFreeze, e.WriterRevocation, e.RequestDrain, e.ServingHealth} {
		if !digestPattern.MatchString(d) || seen[d] {
			return nil, errors.New("registry rehome no-write settlement requires distinct and complete evidence")
		}
		seen[d] = true
	}
	e.Signature = nil
	return signedBytes(registryRehomeNoWriteDomain, e)
}

func (e RegistryRehomeNoWriteEvidence) fresh(now time.Time) bool {
	return now.Unix() >= e.ObservedAt && now.Unix() < e.ExpiresAt
}

func EncodeRegistryRehomeNoWriteEvidence(e RegistryRehomeNoWriteEvidence) ([]byte, string, error) {
	if _, err := e.SigningInput(); err != nil {
		return nil, "", err
	}
	if len(e.Signature) != ed25519.SignatureSize {
		return nil, "", errors.New("invalid registry rehome no-write signature size")
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return nil, "", err
	}
	return raw, digest(raw), nil
}

func readRegistryRehomeNoWriteEvidence(raw []byte, key ed25519.PublicKey) (RegistryRehomeNoWriteEvidence, string, error) {
	var evidence RegistryRehomeNoWriteEvidence
	if len(raw) == 0 || len(raw) > MaxWitnessBytes || json.Unmarshal(raw, &evidence) != nil {
		return RegistryRehomeNoWriteEvidence{}, "", errors.New("invalid registry rehome no-write evidence")
	}
	canonical, evidenceDigest, err := EncodeRegistryRehomeNoWriteEvidence(evidence)
	if err != nil || !bytes.Equal(canonical, raw) {
		return RegistryRehomeNoWriteEvidence{}, "", errors.New("registry rehome no-write evidence is not canonical")
	}
	message, err := evidence.SigningInput()
	if err != nil || !ed25519.Verify(key, message, evidence.Signature) {
		return RegistryRehomeNoWriteEvidence{}, "", errors.New("registry rehome no-write evidence lacks the enrolled observer's signature")
	}
	return evidence, evidenceDigest, nil
}

// RegistryRehomeNoWriteCheck independently repeats frozen provider readback.
// It must match the exact readback digest and prove original placement, not
// choose the terminal result or infer drainage from an unchanged Deployment.
type RegistryRehomeNoWriteCheck func(context.Context, RegistryRehomeNoWriteEvidence) error

type RegistryRehomeNoWriteJournal struct {
	reader *RegistryRehomeAdmissionReader
	check  RegistryRehomeNoWriteCheck
}

// NewRegistryRehomeNoWriteJournal is intentionally separate from dispatch
// and admission. Its principal can append only no-write evidence and results.
func NewRegistryRehomeNoWriteJournal(j *Journal, scope Scope, observer ed25519.PublicKey, check RegistryRehomeNoWriteCheck) (*RegistryRehomeNoWriteJournal, error) {
	if check == nil {
		return nil, errors.New("registry rehome closure requires fresh provider readback")
	}
	r, err := NewRegistryRehomeAdmissionReader(j, scope, observer)
	if err != nil {
		return nil, err
	}
	return &RegistryRehomeNoWriteJournal{r, check}, nil
}

// CloseWithoutWrite records exactly one no-write result with its evidence.
// It never obtains a new claim, reissues the patch, or advances the LKG.
func (c *RegistryRehomeNoWriteJournal) CloseWithoutWrite(ctx context.Context, actor, intentDigest string, raw []byte) (Recorded, error) {
	if c == nil || c.reader == nil || c.check == nil || !tokenPattern.MatchString(actor) || !digestPattern.MatchString(intentDigest) {
		return Recorded{}, errors.New("registry rehome closure needs its enrolled journal, actor, and fresh provider readback")
	}
	r := c.reader
	e, d, err := readRegistryRehomeNoWriteEvidence(raw, r.observer)
	if err != nil {
		return Recorded{}, err
	}
	want := e.claim()
	if want.Scope != r.scope || want.IntentDigest != intentDigest {
		return Recorded{}, ErrStaleFence
	}
	j := r.journal
	intent, _, err := j.ReadIntent(ctx, intentDigest)
	if err != nil || intent.Operation != ComponentRegistryMove || intent.scope() != want.Scope {
		return Recorded{}, errors.Join(ErrStaleFence, err)
	}
	claim, terminal, err := j.ReadExecution(ctx, r.scope, intentDigest)
	if err != nil || claim != want {
		return Recorded{}, errors.Join(ErrStaleFence, err)
	}
	a, pin, err := r.ReadAdmission(ctx, claim)
	if err != nil || pin != e.AdmissionDigest || e.ObservedAt < a.ExpiresAt ||
		e.EndpointFreeze == a.EndpointAdmission || e.WriterRevocation == a.LegacyRevocation || e.RequestDrain == a.LegacyRequestDrain {
		return Recorded{}, errors.Join(errors.New("registry rehome closure needs distinct post-expiry boundary evidence for its exact admission"), err)
	}
	lkg := Version{intent.Generation, intent.CurrentDigest}
	result := ExecutionResult{NotApplied, lkg, d}
	readReplay := func(db queryRow, existing ExecutionResult) (bool, error) {
		if _, err := replayResult(existing, result); err != nil {
			return false, err
		}
		var stored []byte
		var admission string
		err := db.QueryRowContext(ctx, `SELECT envelope, admission_digest FROM fugue_control.registry_rehome_no_write
			WHERE intent_digest=$1 AND evidence_digest=$2`, intentDigest, d).Scan(&stored, &admission)
		if err != nil || admission != pin || !bytes.Equal(stored, raw) {
			return false, errors.Join(ErrResultConflict, err)
		}
		return false, nil
	}
	if terminal != nil {
		_, err := readReplay(j.db, *terminal)
		if err != nil {
			return Recorded{}, err
		}
		return Recorded{Digest: d}, nil
	}
	if !e.fresh(j.now()) {
		return Recorded{}, ErrStaleFence
	}
	if err := c.check(ctx, e); err != nil {
		return Recorded{}, err
	}
	return j.record(ctx, d, func(tx *sql.Tx) (bool, error) {
		if err := lockResource(ctx, tx, claim.Scope); err != nil {
			return false, err
		}
		if stored, err := readResult(ctx, tx, intentDigest); err == nil {
			return readReplay(tx, stored)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		state, err := readResource(ctx, tx, claim.Scope)
		if err != nil || state.LastFence != claim.Fence || state.Pending != intentDigest || state.LastVerified != lkg || !e.fresh(j.now()) {
			return false, errors.Join(ErrStaleFence, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_control.registry_rehome_no_write
			(intent_digest,admission_digest,evidence_digest,envelope) VALUES ($1,$2,$3,$4)`, intentDigest, pin, d, raw); err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_control.execution_results
			(intent_digest,outcome,generation,verified_digest,evidence_digest,actor) VALUES ($1,$2,$3,$4,$5,$6)`,
			intentDigest, result.Outcome, lkg.Generation, lkg.Digest, d, actor)
		return err == nil, err
	})
}
