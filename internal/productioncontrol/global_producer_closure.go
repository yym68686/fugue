package productioncontrol

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
)

// GlobalProducerRollbackRef puts the original execution and its independent
// serving proof inside the new owner's signed intent.
type GlobalProducerRollbackRef struct {
	IntentDigest   string `json:"intentDigest"`
	ApprovalDigest string `json:"approvalDigest"`
	EvidenceDigest string `json:"evidenceDigest"`
}

func (r GlobalProducerRollbackRef) Valid() bool {
	return digestPattern.MatchString(r.IntentDigest) && digestPattern.MatchString(r.ApprovalDigest) &&
		digestPattern.MatchString(r.EvidenceDigest)
}

func (j *Journal) CheckAppliedGlobalProducerReceipt(ctx context.Context, c Claim, result ExecutionResult, throughFence int64) error {
	if j == nil {
		return ErrStaleFence
	}
	return checkAppliedGlobalProducerReceipt(ctx, j.db, c, result, throughFence)
}

func checkAppliedGlobalProducerReceipt(ctx context.Context, db queryRow, c Claim, result ExecutionResult, throughFence int64) error {
	if !c.Scope.valid() || c.ResourceID != GlobalProducerPolicyResource || c.Fence < 1 ||
		!(GlobalProducerRollbackRef{c.IntentDigest, c.ApprovalDigest, result.EvidenceDigest}).Valid() ||
		throughFence < c.Fence || result.Outcome != Applied || !result.Verified.valid() || result.Verified.Generation < 2 {
		return ErrStaleFence
	}
	var exact bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS(
  SELECT 1 FROM fugue_native_global_producer_receipts r
  JOIN fugue_native_global_producer_authority a ON a.cluster_id=r.cluster_id AND a.writer_role=r.writer_role
  JOIN fugue_control.execution_claims c ON c.intent_digest=r.intent_digest AND c.cluster_id=r.cluster_id
    AND c.resource_id='producer/global-policy' AND c.approval_digest=r.approval_digest AND c.fence=r.claim_fence
  JOIN fugue_control.execution_results e ON e.intent_digest=c.intent_digest
  WHERE r.intent_digest=$1 AND r.approval_digest=$2 AND r.cluster_id=$3 AND r.claim_fence=$4
    AND r.target_digest=$5 AND r.previous_fence=$6-1 AND e.outcome='applied'
    AND e.generation=$6 AND e.verified_digest=$5 AND e.evidence_digest=$7
    AND NOT EXISTS(SELECT 1 FROM fugue_control.execution_claims lc
      LEFT JOIN fugue_control.execution_results le ON le.intent_digest=lc.intent_digest
      WHERE lc.cluster_id=c.cluster_id AND lc.resource_id=c.resource_id AND lc.fence>c.fence AND lc.fence<=$8
        AND (le.outcome IS DISTINCT FROM 'not_applied'
          OR EXISTS(SELECT 1 FROM fugue_native_global_producer_receipts later WHERE later.intent_digest=lc.intent_digest))))`,
		c.IntentDigest, c.ApprovalDigest, c.ClusterID, c.Fence, result.Verified.Digest, result.Verified.Generation, result.EvidenceDigest, throughFence).Scan(&exact)
	if err != nil {
		return err
	}
	if !exact {
		return errors.New("DNS rollback lacks the original committed receipt and independently recorded result")
	}
	return nil
}

// checkGlobalProducerRollback is deliberately called both before granting a
// claim and before consuming it, while holding the canonical resource lock.
// Domain-specific inverse validation is also enforced in the receipt trigger.
func checkGlobalProducerRollback(ctx context.Context, tx *sql.Tx, intent Intent, payload []byte, state ResourceState, predecessorFence int64) error {
	var link struct {
		Reverses *GlobalProducerRollbackRef `json:"reverses"`
	}
	if err := json.Unmarshal(payload, &link); err != nil {
		return err
	}
	if link.Reverses == nil {
		return nil
	}
	ref := *link.Reverses
	if !ref.Valid() || predecessorFence < 1 || intent.Generation >= math.MaxInt64 ||
		state.LastVerified != (Version{intent.Generation, intent.CurrentDigest}) {
		return ErrResourceDrift
	}
	original, err := readClaim(ctx, tx, ref.IntentDigest)
	if err != nil || original.Scope != intent.scope() || original.ApprovalDigest != ref.ApprovalDigest {
		return ErrResourceDrift
	}
	return checkAppliedGlobalProducerReceipt(ctx, tx, original,
		ExecutionResult{Outcome: Applied, Verified: state.LastVerified, EvidenceDigest: ref.EvidenceDigest}, predecessorFence)
}

// CheckGlobalProducerFreeze verifies the closed, pre-activation provider
// boundary from the same database that will store the enrolled baseline.
// It does not prove that external credentials have been revoked or drained.
func (j *Journal) CheckGlobalProducerFreeze(ctx context.Context, release string, version Version, authorityDigest string) error {
	if j == nil || release == "" || !version.valid() || !digestPattern.MatchString(authorityDigest) {
		return errors.New("global producer baseline lacks exact freeze evidence")
	}
	var frozen bool
	if err := j.db.QueryRowContext(ctx, `SELECT EXISTS(
  SELECT 1 FROM fugue_native_global_producer_gate g WHERE g.singleton AND g.frozen AND g.baseline_release_id=$1
    AND g.baseline_fence=$2 AND g.baseline_digest=$3 AND g.evidence_digest=$4
    AND NOT EXISTS(SELECT 1 FROM fugue_native_global_producer_authority))`, release, version.Generation, version.Digest, authorityDigest).Scan(&frozen); err != nil {
		return err
	}
	if !frozen {
		return errors.New("global producer has not frozen its exact baseline")
	}
	return nil
}

// Called with the same resource advisory lock that the writer and its SQL
// receipt trigger must take. An in-flight write must finish before absence is
// meaningful; once the terminal row commits the trigger refuses a late write.
// The receipt is append-only and commits atomically with its exact selection.
func verifyGlobalProducerClosure(ctx context.Context, tx *sql.Tx, claim Claim, result ExecutionResult) error {
	var authority, present, exact, selected bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
  SELECT 1 FROM fugue_native_global_producer_gate g JOIN fugue_native_global_producer_authority a USING(singleton)
  WHERE g.singleton AND g.frozen AND a.cluster_id=$2),
  EXISTS(SELECT 1 FROM fugue_native_global_producer_receipts WHERE intent_digest=$1),
  EXISTS(SELECT 1 FROM fugue_native_global_producer_receipts r JOIN fugue_native_global_producer_authority a ON a.cluster_id=r.cluster_id
    WHERE r.intent_digest=$1 AND r.cluster_id=$2 AND r.approval_digest=$3 AND r.claim_fence=$4
      AND r.writer_role=a.writer_role AND r.target_digest=$5 AND r.previous_fence+1=$6),
  EXISTS(SELECT 1 FROM fugue_platform_release_lanes l
    JOIN fugue_platform_artifact_releases r ON r.id=l.active_release_id
    JOIN fugue_platform_artifacts a ON a.id=r.artifact_id
    WHERE l.lane_key='policy_snapshot|platform-config-producer|shadow' AND NOT l.frozen
      AND l.artifact_kind='policy_snapshot' AND l.scope_key='platform-config-producer' AND l.release_channel='shadow'
      AND r.lane_key=l.lane_key AND r.artifact_kind=l.artifact_kind AND r.scope_key=l.scope_key AND r.release_channel=l.release_channel
      AND r.status='active' AND r.fencing_token=$6 AND l.fencing_token=$6 AND r.generation=a.generation
      AND a.artifact_kind=l.artifact_kind AND a.scope_key=l.scope_key AND a.status='validated' AND a.content_hash=$5)`,
		claim.IntentDigest, claim.ClusterID, claim.ApprovalDigest, claim.Fence, result.Verified.Digest, result.Verified.Generation).
		Scan(&authority, &present, &exact, &selected)
	if err != nil {
		return err
	}
	if !authority || !selected || !(result.Outcome == Applied && exact || result.Outcome == NotApplied && !present) {
		return errors.New("global producer write did not conclusively close at the witnessed selection")
	}
	return nil
}
