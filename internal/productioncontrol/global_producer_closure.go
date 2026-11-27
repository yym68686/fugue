package productioncontrol

import (
	"context"
	"database/sql"
	"errors"
)

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
