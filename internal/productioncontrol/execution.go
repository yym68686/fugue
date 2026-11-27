package productioncontrol

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"
)

var (
	ErrBaselineConflict = errors.New("production resource already has a different baseline")
	ErrResourceBusy     = errors.New("production resource has an unresolved execution")
	ErrResourceDrift    = errors.New("production intent does not match the last verified resource version")
	ErrStaleFence       = errors.New("production execution fence no longer matches the claim")
	ErrResultConflict   = errors.New("production execution already has a different terminal result")
)

type Scope struct {
	ClusterID  string
	ResourceID string
}

func (s Scope) valid() bool {
	return tokenPattern.MatchString(s.ClusterID) && resourcePattern.MatchString(s.ResourceID)
}

func (i Intent) scope() Scope { return Scope{i.ClusterID, i.ResourceID} }

type Version struct {
	Generation int64
	Digest     string
}

func (v Version) valid() bool { return v.Generation > 0 && digestPattern.MatchString(v.Digest) }

type Baseline struct {
	Scope
	Verified       Version
	EvidenceDigest string
}

type Claim struct {
	Scope
	Fence          int64
	IntentDigest   string
	ApprovalDigest string
}

type Outcome string

const (
	Applied    Outcome = "applied"
	Restored   Outcome = "restored"
	NotApplied Outcome = "not_applied"
)

type ExecutionResult struct {
	Outcome        Outcome
	Verified       Version
	EvidenceDigest string
}

type ResourceState struct {
	Baseline
	LastVerified Version
	LastFence    int64
	LastIntent   string
	Pending      string
}

// ExecutionEvidence is configured by the control plane, never supplied by an
// intent submitter. VerifyBaseline must prove active provider-side authority
// and the imported LKG. VerifyTerminal must prove the observed terminal
// version and that this claim cannot still issue a delayed write. An
// unchanged observation alone is not proof of NotApplied.
type ExecutionEvidence interface {
	VerifyBaseline(context.Context, Baseline) error
	VerifyTerminal(context.Context, Claim, Intent, ExecutionResult) error
}

type ExecutionJournal struct {
	journal  *Journal
	evidence ExecutionEvidence
}

func NewExecutionJournal(journal *Journal, evidence ExecutionEvidence) (*ExecutionJournal, error) {
	if journal == nil || evidence == nil {
		return nil, errors.New("production execution requires a journal and an independent evidence verifier")
	}
	return &ExecutionJournal{journal: journal, evidence: evidence}, nil
}

func (e *ExecutionJournal) AnchorResource(ctx context.Context, actor string, baseline Baseline) (bool, error) {
	if !tokenPattern.MatchString(actor) || !baseline.Scope.valid() || !baseline.Verified.valid() || !digestPattern.MatchString(baseline.EvidenceDigest) {
		return false, errors.New("invalid production resource baseline")
	}
	if err := e.evidence.VerifyBaseline(ctx, baseline); err != nil {
		return false, err
	}
	record, err := e.journal.record(ctx, baseline.EvidenceDigest, func(tx *sql.Tx) (bool, error) {
		result, err := tx.ExecContext(ctx, `INSERT INTO fugue_control.resource_baselines
			(cluster_id,resource_id,generation,verified_digest,evidence_digest,actor)
			VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (cluster_id,resource_id) DO NOTHING`,
			baseline.ClusterID, baseline.ResourceID, baseline.Verified.Generation, baseline.Verified.Digest, baseline.EvidenceDigest, actor)
		if err != nil {
			return false, err
		}
		n, err := result.RowsAffected()
		if err != nil || n == 1 {
			return n == 1, err
		}
		stored, err := readResource(ctx, tx, baseline.Scope)
		if err != nil {
			return false, err
		}
		if stored.Baseline != baseline {
			return false, ErrBaselineConflict
		}
		return false, nil
	})
	return record.Created, err
}

func (e *ExecutionJournal) Resource(ctx context.Context, scope Scope) (ResourceState, error) {
	return e.journal.ReadResource(ctx, scope)
}

// ReadResource does not require mutation or settlement privileges. A serving
// observer must use the durable head, not infer it from provider health.
func (j *Journal) ReadResource(ctx context.Context, scope Scope) (ResourceState, error) {
	if !scope.valid() {
		return ResourceState{}, errors.New("invalid production resource identity")
	}
	return readResource(ctx, j.db, scope)
}

func (e *ExecutionJournal) ReadIntent(ctx context.Context, intentDigest string) (Intent, []byte, error) {
	return e.journal.ReadIntent(ctx, intentDigest)
}

// VerifyOpenClaim rechecks approval expiry, authority key and the pending
// journal fence immediately before provider writes. It is not a reservation:
// a rehydrated claim must never use it to initiate a second dispatch.
func (e *ExecutionJournal) VerifyOpenClaim(ctx context.Context, claim Claim) error {
	return e.journal.VerifyOpenClaim(ctx, claim)
}

func (j *Journal) VerifyOpenClaim(ctx context.Context, claim Claim) error {
	stored, result, err := j.ReadExecution(ctx, claim.Scope, claim.IntentDigest)
	if err != nil {
		return err
	}
	if stored != claim || result != nil {
		return ErrStaleFence
	}
	intent, _, err := j.ReadIntent(ctx, claim.IntentDigest)
	if err != nil {
		return err
	}
	state, err := j.ReadResource(ctx, claim.Scope)
	if err != nil {
		return err
	}
	if state.LastFence != claim.Fence || state.Pending != claim.IntentDigest {
		return ErrStaleFence
	}
	return verifyRecordedApproval(ctx, j.db, j, intent, claim.IntentDigest, claim.ApprovalDigest)
}

// Execution reads the persistent handle for recovery, whether or not a result
// is already known. It does not issue or renew dispatch authority.
func (e *ExecutionJournal) Execution(ctx context.Context, scope Scope, intentDigest string) (Claim, *ExecutionResult, error) {
	return e.journal.ReadExecution(ctx, scope, intentDigest)
}

// ReadExecution exposes an existing handle without equipping the caller
// with a verifier that can anchor or settle it.
func (j *Journal) ReadExecution(ctx context.Context, scope Scope, intentDigest string) (Claim, *ExecutionResult, error) {
	if !scope.valid() || !digestPattern.MatchString(intentDigest) {
		return Claim{}, nil, errors.New("invalid production execution identity")
	}
	claim, err := readClaim(ctx, j.db, intentDigest)
	if err != nil || claim.Scope != scope {
		if err == nil {
			err = sql.ErrNoRows
		}
		return Claim{}, nil, err
	}
	result, err := readResult(ctx, j.db, intentDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return claim, nil, nil
	}
	if err != nil {
		return Claim{}, nil, err
	}
	return claim, &result, nil
}

// ReserveExecution records a durable one-time claim. Only created=true
// permits attempting provider dispatch, after rechecking authority and CAS
// at the provider boundary. A commit error, repeated call, or restart never
// grants another dispatch. Pending claims have no time-based expiry.
func (e *ExecutionJournal) ReserveExecution(ctx context.Context, actor, intentDigest, approvalDigest string) (Claim, bool, error) {
	return e.journal.ReserveExecution(ctx, actor, intentDigest, approvalDigest)
}

func (j *Journal) ReserveExecution(ctx context.Context, actor, intentDigest, approvalDigest string) (claim Claim, created bool, err error) {
	if !tokenPattern.MatchString(actor) || !digestPattern.MatchString(intentDigest) || !digestPattern.MatchString(approvalDigest) {
		return Claim{}, false, errors.New("invalid production execution request")
	}
	record, err := j.record(ctx, intentDigest, func(tx *sql.Tx) (bool, error) {
		intent, payload, err := readIntent(ctx, tx, intentDigest)
		if err != nil {
			return false, err
		}
		if intent.Operation == ParticipationTopology {
			return false, errors.New("topology revisions do not authorize provider execution")
		}
		nodeScope := strings.HasPrefix(intent.ResourceID, ParticipationNodeResourcePrefix)
		if nodeScope != (intent.Operation == NodeParticipation) {
			return false, errors.New("node participation execution must use its canonical resource boundary")
		}
		if nodeScope {
			// Lock order is topology then resource. Activation uses the
			// same topology lock before checking for outstanding claims.
			if err := lockResource(ctx, tx, Scope{intent.ClusterID, ParticipationTopologyResource}); err != nil {
				return false, err
			}
		}
		if err := lockResource(ctx, tx, intent.scope()); err != nil {
			return false, err
		}
		existing, err := readClaim(ctx, tx, intentDigest)
		if err == nil {
			if existing.Scope != intent.scope() || existing.ApprovalDigest != approvalDigest {
				return false, ErrStaleFence
			}
			claim = existing
			return false, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		if nodeScope {
			if err := verifyParticipationExecutionTopology(ctx, tx, intent, payload); err != nil {
				return false, err
			}
		}
		state, err := readResource(ctx, tx, intent.scope())
		if err != nil {
			return false, err
		}
		if state.Pending != "" {
			return false, ErrResourceBusy
		}
		predecessor := Version{intent.Generation, intent.CurrentDigest}
		if state.LastVerified != predecessor || intent.RollbackDigest != predecessor.Digest {
			return false, ErrResourceDrift
		}
		if err := verifyRecordedApproval(ctx, tx, j, intent, intentDigest, approvalDigest); err != nil {
			return false, err
		}
		if state.LastFence == math.MaxInt64 {
			return false, errors.New("production resource fence is exhausted")
		}
		claim = Claim{intent.scope(), state.LastFence + 1, intentDigest, approvalDigest}
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_control.execution_claims
			(cluster_id,resource_id,fence,intent_digest,approval_digest,actor) VALUES ($1,$2,$3,$4,$5,$6)`,
			claim.ClusterID, claim.ResourceID, claim.Fence, claim.IntentDigest, claim.ApprovalDigest, actor)
		return err == nil, err
	})
	if err != nil {
		return Claim{}, false, err
	}
	return claim, record.Created, nil
}

// CompleteExecution adds a terminal observation; until then the head retains
// the last verified version and prevents any replacement claim. Recovery
// examines the existing fence rather than acquiring a fresh lease.
func (e *ExecutionJournal) CompleteExecution(ctx context.Context, actor string, claim Claim, result ExecutionResult) (bool, error) {
	if !tokenPattern.MatchString(actor) || !claim.Scope.valid() || claim.Fence < 1 ||
		!digestPattern.MatchString(claim.IntentDigest) || !digestPattern.MatchString(claim.ApprovalDigest) ||
		!result.Verified.valid() || !digestPattern.MatchString(result.EvidenceDigest) {
		return false, errors.New("invalid production execution result")
	}
	intent, _, err := e.journal.ReadIntent(ctx, claim.IntentDigest)
	if err != nil {
		return false, err
	}
	stored, err := readClaim(ctx, e.journal.db, claim.IntentDigest)
	if err != nil {
		return false, err
	}
	if stored != claim || claim.Scope != intent.scope() {
		return false, ErrStaleFence
	}
	if err := result.matches(intent); err != nil {
		return false, err
	}
	// Only immutable claims/results are read here. Verify provider evidence
	// outside the SQL transaction, then recheck the open fence inside it.
	if existing, err := readResult(ctx, e.journal.db, claim.IntentDigest); err == nil {
		return replayResult(existing, result)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if err := e.evidence.VerifyTerminal(ctx, claim, intent, result); err != nil {
		return false, err
	}
	record, err := e.journal.record(ctx, result.EvidenceDigest, func(tx *sql.Tx) (bool, error) {
		if err := lockResource(ctx, tx, claim.Scope); err != nil {
			return false, err
		}
		if existing, err := readResult(ctx, tx, claim.IntentDigest); err == nil {
			return replayResult(existing, result)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		state, err := readResource(ctx, tx, claim.Scope)
		if err != nil {
			return false, err
		}
		if state.LastFence != claim.Fence || state.Pending != claim.IntentDigest {
			return false, ErrStaleFence
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_control.execution_results
			(intent_digest,outcome,generation,verified_digest,evidence_digest,actor) VALUES ($1,$2,$3,$4,$5,$6)`,
			claim.IntentDigest, result.Outcome, result.Verified.Generation, result.Verified.Digest, result.EvidenceDigest, actor)
		return err == nil, err
	})
	return record.Created, err
}

func (result ExecutionResult) matches(intent Intent) error {
	switch result.Outcome {
	case Applied:
		if result.Verified.Digest == intent.TargetDigest && result.Verified.Generation >= intent.Generation &&
			(result.Verified.Generation > intent.Generation || intent.TargetDigest == intent.CurrentDigest) {
			return nil
		}
	case Restored:
		if result.Verified.Digest == intent.RollbackDigest && result.Verified.Generation > intent.Generation {
			return nil
		}
	case NotApplied:
		if result.Verified == (Version{intent.Generation, intent.CurrentDigest}) {
			return nil
		}
	}
	return errors.New("terminal production version does not match the approved intent")
}

func replayResult(existing, proposed ExecutionResult) (bool, error) {
	if existing != proposed {
		return false, ErrResultConflict
	}
	return false, nil
}

func lockResource(ctx context.Context, tx *sql.Tx, s Scope) error {
	// Collisions only serialize unrelated resources; row identity is checked
	// by the unique constraints and fully scoped queries, not by the hash.
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "fugue.production.execution\n"+s.ClusterID+"\n"+s.ResourceID)
	return err
}

func verifyRecordedApproval(ctx context.Context, db queryRow, journal *Journal, intent Intent, intentDigest, approvalDigest string) error {
	return verifyRecordedApprovalAt(ctx, db, journal, intent, intentDigest, approvalDigest, journal.now())
}

func verifyRecordedApprovalAt(ctx context.Context, db queryRow, journal *Journal, intent Intent, intentDigest, approvalDigest string, at time.Time) error {
	var envelope []byte
	var approval Approval
	if err := db.QueryRowContext(ctx, `SELECT envelope FROM fugue_control.approvals WHERE digest=$1 AND intent_digest=$2`, approvalDigest, intentDigest).Scan(&envelope); err != nil {
		return err
	}
	if recordedApprovalDigest(envelope) != approvalDigest || json.Unmarshal(envelope, &approval) != nil {
		return errors.New("stored production approval does not match its binding")
	}
	return approval.Verify(intent, journal.trusted, at)
}

func readClaim(ctx context.Context, db queryRow, intentDigest string) (Claim, error) {
	row := Claim{IntentDigest: intentDigest}
	err := db.QueryRowContext(ctx, `SELECT cluster_id,resource_id,fence,approval_digest FROM fugue_control.execution_claims WHERE intent_digest=$1`,
		intentDigest).Scan(&row.ClusterID, &row.ResourceID, &row.Fence, &row.ApprovalDigest)
	return row, err
}

func readResult(ctx context.Context, db queryRow, intentDigest string) (ExecutionResult, error) {
	var row ExecutionResult
	err := db.QueryRowContext(ctx, `SELECT outcome,generation,verified_digest,evidence_digest FROM fugue_control.execution_results WHERE intent_digest=$1`,
		intentDigest).Scan(&row.Outcome, &row.Verified.Generation, &row.Verified.Digest, &row.EvidenceDigest)
	return row, err
}

func readResource(ctx context.Context, db queryRow, s Scope) (ResourceState, error) {
	state := ResourceState{Baseline: Baseline{Scope: s}}
	var missingPrevious bool
	err := db.QueryRowContext(ctx, `SELECT b.generation,b.verified_digest,b.evidence_digest,COALESCE(c.fence,0),COALESCE(c.intent_digest,''),
		CASE WHEN r.intent_digest IS NULL THEN COALESCE(c.intent_digest,'') ELSE '' END,
		COALESCE(r.generation,pr.generation,b.generation),COALESCE(r.verified_digest,pr.verified_digest,b.verified_digest),
		COALESCE(c.fence>1 AND r.intent_digest IS NULL AND pr.intent_digest IS NULL,false)
		FROM fugue_control.resource_baselines b
		LEFT JOIN LATERAL (SELECT intent_digest,fence FROM fugue_control.execution_claims
			WHERE cluster_id=b.cluster_id AND resource_id=b.resource_id ORDER BY fence DESC LIMIT 1) c ON true
		LEFT JOIN fugue_control.execution_results r ON r.intent_digest=c.intent_digest
		LEFT JOIN fugue_control.execution_claims p ON p.cluster_id=b.cluster_id AND p.resource_id=b.resource_id AND p.fence=c.fence-1
		LEFT JOIN fugue_control.execution_results pr ON pr.intent_digest=p.intent_digest
		WHERE b.cluster_id=$1 AND b.resource_id=$2`, s.ClusterID, s.ResourceID).
		Scan(&state.Verified.Generation, &state.Verified.Digest, &state.EvidenceDigest, &state.LastFence, &state.LastIntent, &state.Pending,
			&state.LastVerified.Generation, &state.LastVerified.Digest, &missingPrevious)
	if err != nil {
		return ResourceState{}, err
	}
	if missingPrevious || !state.Verified.valid() || !state.LastVerified.valid() || !digestPattern.MatchString(state.EvidenceDigest) ||
		(state.LastIntent != "" && !digestPattern.MatchString(state.LastIntent)) ||
		(state.Pending != "" && state.Pending != state.LastIntent) {
		return ResourceState{}, errors.New("production resource contains an invalid execution chain")
	}
	return state, nil
}
