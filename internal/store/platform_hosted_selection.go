package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

// The desired host selection and the receipts for its actual application
// have separate lifecycles. A partial forward must still be withdrawable.
type hostedSelectionCommand struct {
	Claim         HostedSuccessorClaim
	Program       string
	Authority     string
	BaselinePaths string
	Previous      string
	Fence         int64             // exact predecessor fence, zero for initial prepare
	Parts         map[string]string // digest of authenticated enrollment + zone -> content digest
	ValidUntil    time.Time
}

type hostedSelectionHead struct {
	Fence    int64
	Decision string
	Phase    string
}

type hostedSelectionStatus struct {
	hostedSelectionHead
	Required, Passing int
	EvidenceUntil     time.Time
}

// A current view, not a sticky verified bit. A negative, expired, missing,
// superseded or future observation never satisfies the selection. Query by
// exact scope and project as well as hostname; an old owner's cursor cannot
// be used to read the new owner's choice.
func pgHostedSelectionStatus(ctx context.Context, tx *sql.Tx, c HostedSuccessorClaim) (hostedSelectionStatus, error) {
	var out hostedSelectionStatus
	var until sql.NullTime
	err := tx.QueryRowContext(ctx, `SELECT h.fence,h.decision,h.phase,s.required,s.passing,
 CASE WHEN s.required=s.passing THEN least(h.valid_until,s.minimum) END
 FROM fugue_hosted_selections h CROSS JOIN LATERAL (
 SELECT count(*) AS required,count(r.part_key) AS passing,min(r.valid_until) AS minimum FROM jsonb_each_text(h.members) m
 LEFT JOIN fugue_hosted_selection_receipts r ON r.hostname=h.hostname AND r.fence=h.fence AND r.decision=h.decision
 AND r.part_key=m.key AND r.content_digest=m.value AND r.passing AND r.observed_at>=h.selected_at
 AND r.observed_at<=statement_timestamp() AND least(h.valid_until,r.valid_until)>statement_timestamp()
 ) s WHERE h.hostname=$1 AND h.scope_key=$2 AND h.project_id=$3`, c.Host, c.Scope, c.Project).Scan(&out.Fence, &out.Decision, &out.Phase, &out.Required, &out.Passing, &until)
	if until.Valid {
		out.EvidenceUntil = until.Time
	}
	return out, err
}

// Callers perform admission under the same transaction as this CAS. The
// clock, transition order and predecessor are checked again by PostgreSQL.
// Crucially, withdraw reads the *selected* image. An expired or superseded
// offer cannot erase it. Current rollback authority supplies its own expiry.
const selectHostedPhaseSQL = `WITH input AS (
 SELECT o.scope_key,o.project_id,o.sequence,o.draft_id,o.draft_digest,i.program_digest,i.body
 FROM fugue_hosted_path_prepare_offers o JOIN fugue_hosted_program_images i USING(scope_key,sequence,draft_id,draft_digest)
 WHERE $9<>'withdraw' AND o.scope_key=$2 AND o.project_id=$3 AND o.sequence=$4 AND o.draft_id=$5 AND o.draft_digest=$6
 AND o.prepared_digest=$14 AND o.forward_digest=$15 AND o.owner_digest=$16 AND o.path_digest=$17
 AND i.program_digest=$7 AND o.expires_at>= $13 AND o.expires_at>clock_timestamp()
 AND (o.draft_id,o.draft_digest)=(SELECT id,content_hash FROM fugue_platform_artifacts WHERE artifact_kind='hosted_path_proposal'
 AND scope_key=o.scope_key AND status='draft' ORDER BY updated_at DESC,id LIMIT 1)
 UNION ALL
 SELECT scope_key,project_id,offer_sequence,draft_id,draft_digest,program_digest,program FROM fugue_hosted_selections
 WHERE $9='withdraw' AND hostname=$1 AND scope_key=$2 AND project_id=$3 AND offer_sequence=$4 AND draft_id=$5 AND draft_digest=$6
 AND program_digest=$7 AND fence=$8 AND decision=$11 AND phase='forward'
 AND owner_digest=$16 AND path_digest=$17 AND prepared_digest=$14 AND forward_digest=$15 AND baseline_path_digest=$20
), applied AS (
 INSERT INTO fugue_hosted_selections AS old
 (hostname,scope_key,project_id,offer_sequence,draft_id,draft_digest,program_digest,program,fence,decision,phase,members,selected_at,valid_until,authority_digest,business_digest,previous_decision,previous_members,skipped_forward_from,owner_digest,path_digest,prepared_digest,forward_digest,baseline_path_digest)
 SELECT $1,input.scope_key,input.project_id,input.sequence,input.draft_id,input.draft_digest,input.program_digest,input.body,$8::bigint+1,$10,$9,$12::jsonb,clock_timestamp(),$13,$18,$19,$11,
 COALESCE(h.members,'{}'::jsonb),CASE WHEN $9='withdraw' THEN h.previous_decision ELSE '' END,$16,$17,$14,$15,$20
 FROM input LEFT JOIN fugue_hosted_selections h ON h.hostname=$1 AND h.fence=$8 AND h.decision=$11
 WHERE $13::timestamptz>clock_timestamp() AND
 (($8=0 AND $11='' AND $9='prepare') OR ($8>0 AND h.hostname IS NOT NULL
 AND NOT EXISTS(SELECT 1 FROM jsonb_object_keys($12::jsonb) k WHERE NOT h.members ? k)
 AND NOT EXISTS(SELECT 1 FROM jsonb_object_keys(h.members) k WHERE NOT $12::jsonb ? k)
 AND ($9<>'withdraw' OR $12::jsonb=h.previous_members)))
 ON CONFLICT(hostname) DO UPDATE SET offer_sequence=excluded.offer_sequence,draft_id=excluded.draft_id,
 draft_digest=excluded.draft_digest,program_digest=excluded.program_digest,
 program=CASE WHEN old.program_digest=excluded.program_digest THEN old.program ELSE excluded.program END,
 fence=excluded.fence,decision=excluded.decision,phase=excluded.phase,members=excluded.members,selected_at=excluded.selected_at,valid_until=excluded.valid_until,
 authority_digest=excluded.authority_digest,business_digest=excluded.business_digest,previous_decision=old.decision,previous_members=old.members,skipped_forward_from=excluded.skipped_forward_from,
 owner_digest=excluded.owner_digest,path_digest=excluded.path_digest,prepared_digest=excluded.prepared_digest,forward_digest=excluded.forward_digest,baseline_path_digest=excluded.baseline_path_digest
 WHERE old.scope_key=excluded.scope_key AND old.project_id=excluded.project_id AND old.fence=$8 AND old.decision=$11 AND (
 (excluded.phase='prepare' AND old.phase='withdraw' AND excluded.offer_sequence>old.offer_sequence) OR
 (old.program_digest=excluded.program_digest AND old.offer_sequence=excluded.offer_sequence AND
 ((excluded.phase='forward' AND old.phase IN ('prepare','withdraw')) OR (excluded.phase='withdraw' AND old.phase='forward'))))
 AND (excluded.phase='withdraw' OR (old.valid_until>clock_timestamp()
 AND NOT EXISTS(SELECT 1 FROM jsonb_each_text(old.members) AS m
 WHERE NOT EXISTS(SELECT 1 FROM fugue_hosted_selection_receipts r WHERE r.hostname=old.hostname AND r.fence=old.fence AND r.decision=old.decision
 AND r.part_key=m.key AND r.content_digest=m.value AND r.passing AND r.observed_at>=old.selected_at AND r.valid_until>clock_timestamp()))))
 RETURNING fence,decision,phase
) SELECT * FROM applied
 UNION ALL SELECT fence,decision,phase FROM fugue_hosted_selections
 WHERE hostname=$1 AND scope_key=$2 AND project_id=$3 AND fence=$8::bigint+1 AND decision=$10 AND phase=$9
 AND valid_until=$13 AND valid_until>clock_timestamp() AND NOT EXISTS(SELECT 1 FROM applied)`

func pgSelectHostedPhase(ctx context.Context, tx *sql.Tx, cmd hostedSelectionCommand) (hostedSelectionHead, error) {
	var head hostedSelectionHead
	c := cmd.Claim
	if c.Host == "" || c.Scope == "" || c.Project == "" || c.Draft == "" || c.Sequence < 1 || cmd.Fence < 0 || (cmd.Fence == 0) != (cmd.Previous == "") ||
		(cmd.Previous != "" && !platformproducer.ValidDigest(cmd.Previous)) || (c.Phase != "prepare" && c.Phase != "forward" && c.Phase != "withdraw") ||
		!cmd.ValidUntil.After(time.Now()) || len(cmd.Parts) == 0 || len(cmd.Parts) > 8192 {
		return head, ErrInvalidInput
	}
	for _, pin := range []string{c.DraftDigest, c.Owner, c.Paths, c.Prepared, c.Forward, c.Business, cmd.Program, cmd.Authority, cmd.BaselinePaths} {
		if !platformproducer.ValidDigest(pin) {
			return head, ErrInvalidInput
		}
	}
	for part, content := range cmd.Parts {
		if !platformproducer.ValidDigest(part) || !platformproducer.ValidDigest(content) {
			return head, ErrInvalidInput
		}
	}
	cmd.ValidUntil = cmd.ValidUntil.UTC().Truncate(time.Microsecond)
	decision, err := pc.Digest([]any{"fugue.hosted-selection/v1", cmd})
	if err != nil {
		return head, err
	}
	parts, err := json.Marshal(cmd.Parts)
	if err != nil {
		return head, err
	}
	// Serialize member reports against the same host transition, including
	// the initially absent row. Independent domains never contend here.
	if err = pgLockPromotionScope(ctx, tx, "hosted-selection:"+c.Host, true); err != nil {
		return head, err
	}
	err = tx.QueryRowContext(ctx, selectHostedPhaseSQL, c.Host, c.Scope, c.Project, c.Sequence, c.Draft, c.DraftDigest, cmd.Program, cmd.Fence, c.Phase, decision, cmd.Previous, string(parts), cmd.ValidUntil, c.Prepared, c.Forward, c.Owner, c.Paths, cmd.Authority, c.Business, cmd.BaselinePaths).Scan(&head.Fence, &head.Decision, &head.Phase)
	if errors.Is(err, sql.ErrNoRows) {
		return head, ErrConflict
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `DELETE FROM fugue_hosted_selection_receipts WHERE hostname=$1 AND fence<$2`, c.Host, head.Fence)
	}
	return head, err
}

// The caller has authenticated the reporter, verified its wire evidence
// and mapped it to this exact key. Store original evidence times, so that
// delayed retries cannot renew a passing observation.
type hostedPartReceipt struct {
	Host, Decision, Part, Content, Evidence string
	Fence                                   int64
	Passing                                 bool
	ObservedAt, ValidUntil                  time.Time
}

func pgRecordHostedPart(ctx context.Context, tx *sql.Tx, r hostedPartReceipt) error {
	if r.Host == "" || r.Fence < 1 {
		return ErrInvalidInput
	}
	for _, d := range []string{r.Decision, r.Part, r.Content, r.Evidence} {
		if !platformproducer.ValidDigest(d) {
			return ErrInvalidInput
		}
	}
	if err := pgLockPromotionScope(ctx, tx, "hosted-selection:"+r.Host, true); err != nil {
		return err
	}
	// Reject stale/changed commands even when a process is still capable of
	// reporting a healthy global heartbeat.
	var ok bool
	err := tx.QueryRowContext(ctx, `WITH eligible AS (
 SELECT hostname FROM fugue_hosted_selections h WHERE hostname=$1 AND fence=$2 AND decision=$3
 AND members->>$4=$5 AND $7::timestamptz>=selected_at AND $7<=clock_timestamp()
 AND $8::timestamptz>$7 AND $8>clock_timestamp() AND $8<=valid_until
 ), saved AS (INSERT INTO fugue_hosted_selection_receipts AS old (hostname,part_key,fence,decision,content_digest,evidence_digest,passing,observed_at,valid_until)
 SELECT hostname,$4,$2,$3,$5,$6,$9,$7,$8 FROM eligible ON CONFLICT(hostname,part_key) DO UPDATE SET
 fence=excluded.fence,decision=excluded.decision,content_digest=excluded.content_digest,evidence_digest=excluded.evidence_digest,
 passing=excluded.passing,observed_at=excluded.observed_at,valid_until=excluded.valid_until
 WHERE old.fence<excluded.fence OR (old.fence=excluded.fence AND old.decision=excluded.decision AND old.observed_at<excluded.observed_at AND old.evidence_digest<>excluded.evidence_digest)
 RETURNING true)
 SELECT EXISTS(SELECT 1 FROM saved) OR EXISTS(SELECT 1 FROM fugue_hosted_selection_receipts r JOIN eligible e USING(hostname)
 WHERE r.part_key=$4 AND r.fence=$2 AND r.decision=$3 AND r.content_digest=$5 AND r.evidence_digest=$6 AND r.observed_at=$7 AND r.valid_until=$8 AND r.passing=$9)`,
		r.Host, r.Fence, r.Decision, r.Part, r.Content, r.Evidence, r.ObservedAt, r.ValidUntil, r.Passing).Scan(&ok)
	if err == nil && !ok {
		return ErrConflict
	}
	return err
}
