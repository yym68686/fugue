package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"time"

	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

// The builder must resolve each immutable image, both memberships and the
// fresh current full before calling this storage primitive. Keys are not
// interchangeable even when both parents have identical DNS or route bytes.
type hostedRebindSlot struct {
	OldKey, Key, Content, Prepared, PreviousPacket string
}

type hostedRebindWrite struct {
	Next                               hostedSelectionCommand
	OldDraft, OldProgram, OldAuthority string
	Slots                              []hostedRebindSlot
	OldDirectory, Directory            []hostedMemberDirectoryEntry
}

// The original directory is checked under the same host lock. CAS on just
// a hostname and fence without checking the old member/zone coverage could
// grant a replacement process a predecessor it has never applied.
func pgRebindHostedPhase(ctx context.Context, tx *sql.Tx, w hostedRebindWrite) (hostedSelectionHead, error) {
	var h hostedSelectionHead
	cmd := w.Next
	c := cmd.Claim
	if c.Host == "" || c.Project == "" || c.Scope == "" || c.Draft == "" || w.OldDraft == "" || w.OldDraft == c.Draft || c.Sequence < 2 || cmd.Fence < 1 ||
		(c.Phase != "prepare" && c.Phase != "forward") || !cmd.ValidUntil.After(time.Now()) || len(w.Slots) == 0 || len(w.Slots) > 8192 ||
		len(w.OldDirectory) == 0 || len(w.Directory) != len(w.OldDirectory) || len(w.Directory) > len(w.Slots) {
		return h, ErrInvalidInput
	}
	for _, p := range []string{c.DraftDigest, c.Owner, c.Paths, c.Prepared, c.Forward, c.Business, w.OldProgram, cmd.Program, w.OldAuthority, cmd.Authority, cmd.BaselinePaths, cmd.Previous} {
		if !platformproducer.ValidDigest(p) {
			return h, ErrInvalidInput
		}
	}
	if w.OldProgram == cmd.Program || w.OldAuthority == cmd.Authority {
		return h, ErrConflict
	}
	old, selected, prepared, packets := map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}
	for _, s := range w.Slots {
		for _, p := range []string{s.OldKey, s.Key, s.Content, s.Prepared, s.PreviousPacket} {
			if !platformproducer.ValidDigest(p) {
				return h, ErrInvalidInput
			}
		}
		if s.OldKey == s.Key || old[s.OldKey] != "" || selected[s.Key] != "" || cmd.Parts[s.Key] != s.Content || c.Phase == "prepare" && s.Content != s.Prepared {
			return h, ErrConflict
		}
		old[s.OldKey], selected[s.Key], prepared[s.Key], packets[s.Key] = s.Content, s.Content, s.Prepared, s.PreviousPacket
	}
	if len(cmd.Parts) != len(selected) {
		return h, ErrConflict
	}
	dir := func(ds []hostedMemberDirectoryEntry) (map[[2]string]bool, error) {
		m := map[[2]string]bool{}
		for _, d := range ds {
			k := [2]string{d.Member, d.Kind}
			if d.Member == "" || d.Kind == "" || d.Set == "" || d.Child == "" || m[k] {
				return nil, ErrConflict
			}
			m[k] = true
		}
		return m, nil
	}
	before, err := dir(w.OldDirectory)
	if err != nil {
		return h, err
	}
	after, err := dir(w.Directory)
	if err != nil {
		return h, err
	}
	for k := range before {
		if !after[k] {
			return h, ErrConflict
		}
	}
	// Canonicalize all unordered command inputs before hashing the decision.
	cmd.ValidUntil = cmd.ValidUntil.UTC().Truncate(time.Microsecond)
	pin, err := pc.Digest([]any{"fugue.host-rebind/v1", cmd, w.OldDraft, w.OldProgram, w.OldAuthority, old, prepared, packets})
	if err != nil {
		return h, err
	}
	raw := make([]string, 0, 5)
	for _, value := range []any{old, selected, prepared, packets, w.Directory} {
		b, e := json.Marshal(value)
		if e != nil {
			return h, e
		}
		raw = append(raw, string(b))
	}
	if err = pgLockPromotionScope(ctx, tx, "hosted-selection:"+c.Host, true); err != nil {
		return h, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT member_id,kind,set_id,child_id FROM fugue_hosted_member_directory WHERE hostname=$1 FOR UPDATE`, c.Host)
	if err != nil {
		return h, err
	}
	current := map[hostedMemberDirectoryEntry]bool{}
	for rows.Next() {
		var d hostedMemberDirectoryEntry
		if err = rows.Scan(&d.Member, &d.Kind, &d.Set, &d.Child); err != nil {
			rows.Close()
			return h, err
		}
		current[d] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return h, err
	}
	if len(current) != len(before) || slices.ContainsFunc(w.OldDirectory, func(d hostedMemberDirectoryEntry) bool { return !current[d] }) {
		return h, ErrConflict
	}
	err = tx.QueryRowContext(ctx, `UPDATE fugue_hosted_selections h SET offer_sequence=o.sequence,draft_id=o.draft_id,draft_digest=o.draft_digest,
 program_digest=i.program_digest,program=i.body,fence=h.fence+1,decision=$17,authority_digest=$15,previous_decision=h.decision,
 members=$18::jsonb,previous_members=$19::jsonb,prepared_digest=o.prepared_digest,forward_digest=o.forward_digest,skipped_forward_from='',
 selected_at=clock_timestamp(),valid_until=$16 FROM fugue_hosted_path_prepare_offers o JOIN fugue_hosted_program_images i USING(scope_key,sequence,draft_id,draft_digest)
 WHERE h.hostname=$1 AND h.scope_key=$2 AND h.project_id=$3 AND h.phase=$4 AND h.fence=$5 AND h.decision=$6
 AND h.draft_id=$7 AND h.program_digest=$8 AND h.authority_digest=$9 AND h.members=$10::jsonb AND h.business_digest=$11
 AND h.baseline_path_digest=$20 AND h.owner_digest=o.owner_digest AND h.path_digest=o.path_digest AND o.owner_digest=$21 AND o.path_digest=$22
 AND h.offer_sequence<o.sequence AND h.scope_key=o.scope_key AND h.project_id=o.project_id AND o.sequence=$12 AND o.draft_id=$13 AND o.draft_digest=$14
 AND o.expires_at=$16 AND o.expires_at>clock_timestamp() AND i.program_digest=$23 AND o.prepared_digest=$24 AND o.forward_digest=$25
 AND (o.draft_id,o.draft_digest)=(SELECT id,content_hash FROM fugue_platform_artifacts WHERE artifact_kind='hosted_path_proposal' AND scope_key=o.scope_key AND status='draft' ORDER BY updated_at DESC,id LIMIT 1)
 AND NOT EXISTS (SELECT 1 FROM jsonb_each_text(h.members) m WHERE NOT EXISTS
 (SELECT 1 FROM fugue_hosted_selection_receipts r WHERE r.hostname=h.hostname AND r.fence=h.fence AND r.decision=h.decision
 AND r.part_key=m.key AND r.content_digest=m.value AND r.passing AND r.observed_at>=h.selected_at AND r.observed_at<=clock_timestamp()))
 RETURNING h.fence,h.decision,h.phase`, c.Host, c.Scope, c.Project, c.Phase, cmd.Fence, cmd.Previous, w.OldDraft, w.OldProgram, w.OldAuthority, raw[0], c.Business, c.Sequence, c.Draft, c.DraftDigest, cmd.Authority, cmd.ValidUntil, pin, raw[1], raw[2], cmd.BaselinePaths, c.Owner, c.Paths, cmd.Program, c.Prepared, c.Forward).Scan(&h.Fence, &h.Decision, &h.Phase)
	if errors.Is(err, sql.ErrNoRows) {
		return h, ErrConflict
	}
	if err != nil {
		return h, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO fugue_hosted_membership_rebinds(hostname,fence,decision,phase,predecessor_packets)
 VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT(hostname) DO UPDATE SET (fence,decision,phase,predecessor_packets)=(excluded.fence,excluded.decision,excluded.phase,excluded.predecessor_packets)`, c.Host, h.Fence, h.Decision, h.Phase, raw[3])
	if err != nil {
		return h, err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_hosted_member_directory WHERE hostname=$1`, c.Host); err != nil {
		return h, err
	}
	if _, err = tx.ExecContext(ctx, hostedDirectoryInsertSQL, c.Host, raw[4]); err != nil {
		return h, err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM fugue_hosted_selection_receipts WHERE hostname=$1 AND fence<$2`, c.Host, h.Fence)
	return h, err
}
