package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// Hints for the producer, not rollback authority. Look only at domains
// currently selected forward. Match the captured path digest to the *exact*
// original baseline. Unrelated edits cannot occupy a slot in this batch.
func (s *Store) HostedWithdrawalHints(ctx context.Context, intent pc.PlatformIntent, after string) ([]string, string, error) {
	if s == nil || s.db == nil || intent.Scope != pc.GlobalScopeKey {
		return nil, "", nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT hostname,baseline_path_digest FROM fugue_hosted_selections WHERE phase='forward' AND hostname>$1 AND valid_until>statement_timestamp() ORDER BY hostname LIMIT 33`, after)
	if err != nil {
		return nil, "", err
	}
	wanted := map[string]string{}
	last, next := "", ""
	for rows.Next() {
		var h, pin string
		if err = rows.Scan(&h, &pin); err != nil {
			rows.Close()
			return nil, "", err
		}
		if len(wanted) == 32 {
			next = last
			break
		}
		wanted[h] = pin
		last = h
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(wanted) == 0 {
		return nil, "", err
	}
	paths := map[string]map[string]string{}
	invalid := map[string]bool{}
	for _, r := range intent.Routes {
		if wanted[r.Hostname] == "" {
			continue
		}
		if paths[r.Hostname] == nil {
			paths[r.Hostname] = map[string]string{}
		}
		path := model.NormalizeAppRoutePathPrefix(r.PathPrefix)
		if _, ok := paths[r.Hostname][path]; ok || r.AppID == "" {
			invalid[r.Hostname] = true
		}
		paths[r.Hostname][path] = r.AppID
	}
	var out []string
	for host, routes := range paths {
		if invalid[host] || routes["/"] == "" {
			continue
		}
		digest, e := pc.Digest(routes)
		if e != nil {
			return nil, "", e
		}
		if wanted[host] == digest {
			out = append(out, host)
		}
	}
	slices.Sort(out)
	return out, next, nil
}

// A normal business edit restored the original routes. Resolve the retained
// image, not the latest offer; rebind the saved intent under the same locks
// as preparation. Do not require positive *forward* reports to withdraw a
// partially executed choice. A failed member can fetch the withdrawal later.
func (s *Store) WithdrawHostedSelection(ctx context.Context, source pc.HostedAddressSource, host string, bind func(ProjectHostRouteInputs, pc.HostedScopeProposal) (owner, paths, dir string, err error)) (int64, error) {
	if s == nil || s.db == nil || bind == nil || host == "" || source.Publication.Validate() != nil || !source.Authority.ExpiresAt.After(time.Now()) {
		return 0, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SET LOCAL lock_timeout='400ms'"); err != nil {
		return 0, err
	}
	for _, k := range []string{platformproducer.Scope, pc.GlobalScopeKey} {
		if err = pgLockPromotionScope(ctx, tx, k, false); err != nil {
			return 0, err
		}
	}
	c := HostedSuccessorClaim{Host: host, Phase: "withdraw"}
	cmd := hostedSelectionCommand{}
	var parts []byte
	err = tx.QueryRowContext(ctx, `SELECT scope_key,project_id,offer_sequence,draft_id,draft_digest,prepared_digest,forward_digest,owner_digest,path_digest,program_digest,fence,decision,authority_digest,baseline_path_digest,previous_members,valid_until FROM fugue_hosted_selections WHERE hostname=$1 AND phase='forward' AND valid_until>clock_timestamp()`, host).Scan(&c.Scope, &c.Project, &c.Sequence, &c.Draft, &c.DraftDigest, &c.Prepared, &c.Forward, &c.Owner, &c.Paths, &cmd.Program, &cmd.Fence, &cmd.Previous, &cmd.Authority, &cmd.BaselinePaths, &parts, &cmd.ValidUntil)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	read := pgDNSRouteSourceReader(ctx, tx)
	draft, err := read.artifact(c.Draft)
	if err != nil {
		return 0, err
	}
	raw, err := json.Marshal(draft.Content)
	var p pc.HostedScopeProposal
	if err != nil || len(raw) > 1<<20 || json.Unmarshal(raw, &p) != nil || p.Host != host || p.Base != source.Publication || !p.Authority.Same(source.Authority) || !hostedHoldsCurrent(p, source.EffectiveHolds) ||
		draft.ID != c.Draft || draft.ContentHash != c.DraftDigest || draft.ArtifactKind != model.PlatformArtifactKindHostedPathProposal || draft.Status != model.PlatformArtifactStatusDraft || draft.CreatedByID != platformproducer.Actor || draft.CreatedByType != model.ActorTypeBootstrap || !platformsafety.EvaluateArtifactIntegrity(draft, s.platformArtifactSigningKeyring()).Pass {
		return 0, ErrConflict
	}
	scope, key := NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "hosted-path", Hostname: host, TenantID: p.Before.Owner.TenantID, AppID: p.Before.Owner.AppID})
	pin, err := hostedAuthorityDigest(p)
	if err != nil || pin != cmd.Authority || c.Scope != key || draft.Scope != scope || draft.ScopeKey != key || !p.SupportedSchema() || !cmd.ValidUntil.Equal(p.Authority.ExpiresAt.UTC().Truncate(time.Microsecond)) {
		return 0, ErrConflict
	}
	if _, err = s.hostedSelectedBase(read, p); err != nil {
		return 0, err
	}
	in, err := pgProjectHostInputs(ctx, tx, c.Project, host, true)
	if err != nil {
		return 0, err
	}
	owner, paths, dir, err := bind(in, p)
	if err != nil || in.Project.ID != c.Project || in.Domain.Hostname != host || in.Domain.AppID != scope.AppID || in.Domain.TenantID != scope.TenantID || owner != c.Owner || paths != cmd.BaselinePaths || dir != "baseline" {
		return 0, ErrConflict
	}
	c.Business, err = hostedBusinessRevision(in)
	if err != nil {
		return 0, err
	}
	cmd.Claim = c
	if len(parts) > 2<<20 || json.Unmarshal(parts, &cmd.Parts) != nil {
		return 0, ErrConflict
	}
	h, err := pgSelectHostedPhase(ctx, tx, cmd)
	if err != nil {
		return 0, err
	}
	return h.Fence, tx.Commit()
}
