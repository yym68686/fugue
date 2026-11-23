package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/routeartifact"
)

// Offer only a fully composed and reversible host program. This row is the
// producer end of a handoff, not a consumer ACK or selection for live traffic.
// There is intentionally no generic API for writing it, changing its expiry,
// or promoting a proposal. An offer is supersedable; a claimant must compare
// its sequence and repeat live source checks before touching serving state.
func (s *Store) OfferCompiledHostedPreparation(parent context.Context, source pc.HostedAddressSource, id string, bind func(ProjectHostRouteInputs, pc.HostedScopeProposal) (owner, paths string, err error)) (int64, error) {
	if s == nil || s.db == nil || id == "" || bind == nil {
		return 0, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	draft, err := s.GetPlatformArtifactContext(ctx, id)
	if err != nil {
		return 0, err
	}
	var p pc.HostedScopeProposal
	raw, err := json.Marshal(draft.Content)
	if err != nil || len(raw) > 1<<20 || json.Unmarshal(raw, &p) != nil || draft.ID != id || draft.ArtifactKind != model.PlatformArtifactKindHostedPathProposal ||
		draft.Status != model.PlatformArtifactStatusDraft || draft.CreatedByType != model.ActorTypeBootstrap || draft.CreatedByID != platformproducer.Actor ||
		s.VerifyPlatformArtifactIntegrity(draft) != nil || p.Schema != pc.HostedProposalSchema || p.Host == "" || p.Before.Owner.Hostname != p.Host ||
		p.Base != source.Publication || !p.Authority.Same(source.Authority) || !p.Authority.ExpiresAt.After(time.Now()) {
		return 0, ErrConflict
	}
	scope, key := NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "hosted-path", Hostname: p.Host, TenantID: p.Before.Owner.TenantID, AppID: p.Before.Owner.AppID})
	if draft.Scope != scope || draft.ScopeKey != key {
		return 0, ErrConflict
	}
	app, err := s.GetAppMetadata(scope.AppID)
	if err != nil || app.ProjectID == "" || app.TenantID != scope.TenantID {
		return 0, ErrConflict
	}
	project := app.ProjectID
	program, err := routeartifact.FreezeHostedProgram(source, p, draft.ContentHash)
	if err != nil {
		return 0, fmt.Errorf("materialize reversible host program: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// Same lock order as the ordinary producer. Shared locks need not stop
	// serving readers, and the business rows stay locked only for this write.
	if _, err = tx.ExecContext(ctx, "SET LOCAL lock_timeout='400ms'"); err != nil {
		return 0, err
	}
	for _, k := range []string{platformproducer.Scope, pc.GlobalScopeKey} {
		if err = pgLockPromotionScope(ctx, tx, k, false); err != nil {
			return 0, err
		}
	}
	full, err := s.hostedSelectedBase(pgDNSRouteSourceReader(ctx, tx), p)
	if err != nil {
		return 0, err
	}
	in, err := pgProjectHostInputs(ctx, tx, project, p.Host, true)
	if err != nil {
		return 0, err
	}
	if in.Project.ID != project || in.Domain.AppID != scope.AppID || in.Domain.TenantID != scope.TenantID || in.Domain.Hostname != p.Host {
		return 0, ErrConflict
	}
	owner, paths, err := bind(in, p)
	if err != nil {
		return 0, err
	}
	if !platformproducer.ValidDigest(owner) || !platformproducer.ValidDigest(paths) {
		return 0, ErrInvalidInput
	}
	var epoch int64
	err = tx.QueryRowContext(ctx, hostedPrepareOfferCAS, key, id, draft.ContentHash, project, owner, paths, program.Prepared, program.Forward, p.Authority.ExpiresAt).Scan(&epoch)
	if err != nil {
		return 0, mapDBErr(err)
	}
	if epoch < 1 || !p.Authority.ExpiresAt.After(time.Now()) {
		return 0, ErrConflict
	}
	if err = putHostedProgramImage(ctx, tx, key, epoch, id, draft.ContentHash, program); err != nil {
		return 0, err
	}
	claim := HostedSuccessorClaim{Scope: key, Project: project, Host: p.Host, Draft: id, DraftDigest: draft.ContentHash, Owner: owner, Paths: paths, Prepared: program.Prepared, Forward: program.Forward, Phase: "prepare", Sequence: epoch}
	if err = pgAdmitHostedOffer(ctx, tx, full, p, in, claim, program, source.EffectiveHolds); err != nil {
		return 0, err
	}
	return epoch, tx.Commit()
}

// All offer writers take the project lock. Within this statement the most
// recent compiler-owned draft must still equal the authenticated one; a new
// observation never extends its source expiry. An exact retry is read-only.
const hostedPrepareOfferCAS = `WITH input AS MATERIALIZED (
 SELECT $1::text scope_key,$2::text draft_id,$3::text draft_digest,$4::text project_id,
 $5::text owner_digest,$6::text path_digest,$7::text prepared_digest,$8::text forward_digest,$9::timestamptz expires_at
 WHERE $9::timestamptz > clock_timestamp() AND ($2,$3) = (SELECT id,content_hash FROM fugue_platform_artifacts
 WHERE artifact_kind='hosted_path_proposal' AND scope_key=$1 AND status='draft' ORDER BY updated_at DESC,id ASC LIMIT 1)
), changed AS (
 INSERT INTO fugue_hosted_path_prepare_offers AS o
 (scope_key,draft_id,draft_digest,project_id,owner_digest,path_digest,prepared_digest,forward_digest,sequence,offered_at,expires_at)
 SELECT scope_key,draft_id,draft_digest,project_id,owner_digest,path_digest,prepared_digest,forward_digest,1,clock_timestamp(),expires_at FROM input
 ON CONFLICT (scope_key) DO UPDATE SET draft_id=excluded.draft_id,draft_digest=excluded.draft_digest,project_id=excluded.project_id,
 owner_digest=excluded.owner_digest,path_digest=excluded.path_digest,prepared_digest=excluded.prepared_digest,forward_digest=excluded.forward_digest,
 sequence=o.sequence+1,offered_at=excluded.offered_at,expires_at=excluded.expires_at
 WHERE o.draft_id<>excluded.draft_id AND o.project_id=excluded.project_id AND o.owner_digest=excluded.owner_digest RETURNING sequence
)
SELECT sequence FROM changed UNION ALL SELECT o.sequence FROM fugue_hosted_path_prepare_offers o JOIN input i
 USING(scope_key,draft_id,draft_digest,project_id,owner_digest,path_digest,prepared_digest,forward_digest,expires_at)
 WHERE NOT EXISTS (SELECT 1 FROM changed)`
