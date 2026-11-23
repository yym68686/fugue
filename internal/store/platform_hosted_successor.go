package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

// A pin on immutable proposal content plus the current host's saved business
// state. The verified full/LKG/fence remain independently required by the
// ordinary hosted address producer admission. This metadata is not an ACK.
const HostedSuccessorMetadata = "hosted_successor_claim_v1"

type HostedSuccessorClaim struct {
	Scope, Project, Host, Draft, DraftDigest, Owner, Paths, Prepared, Forward, Business, Phase string
	Sequence                                                                                   int64
}

func hostedBusinessRevision(in ProjectHostRouteInputs) (string, error) {
	d, z, r := in.Domain, in.Zone, in.DNS
	table, e := ProjectRoutesForHost(in.Table, d.Hostname)
	if e != nil {
		return "", e
	}
	table.ProjectID, table.TenantID = in.Project.ID, in.Project.TenantID
	bindings, e := CompileProjectRouteTableBindings(table, in.Apps)
	if e != nil {
		return "", e
	}
	implicit := map[string]string{}
	for _, a := range in.Apps {
		if a.Route != nil && strings.EqualFold(strings.TrimSpace(a.Route.Hostname), d.Hostname) {
			implicit[a.ID] = model.NormalizeAppRoutePathPrefix(a.Route.PathPrefix)
		}
	}
	return pc.Digest([]any{"host-successor-business/v1", in.Project.ID, in.Project.TenantID, table.Domains, table.Entrypoints, bindings, implicit,
		d.Hostname, d.AppID, d.TenantID, d.DNSMode, d.DNSStatus, d.Status, d.TLSStatus, d.RouteTarget, d.CreatedAt, d.DNSRecordKind, d.DNSRecordSource, d.DNSZoneID, d.DNSRecordID,
		z.ID, z.TenantID, z.ProjectID, z.ZoneName, z.Status, z.CreatedAt, r.ID, r.ZoneID, r.FQDN, r.TenantID, r.Type, r.TTL, r.Values, r.Source, r.SourceRefType, r.SourceRefID, r.Status, r.CreatedAt,
		r.FlattenMode, r.FlattenTarget, r.FlattenIPv4Policy, r.FlattenIPv6Policy, r.FlattenTTLPolicy, r.FlattenFallbackPolicy})
}

const hostedClaimRowSQL = `SELECT scope_key,project_id,draft_id,draft_digest,owner_digest,path_digest,prepared_digest,forward_digest,sequence,expires_at
 FROM fugue_hosted_path_prepare_offers o WHERE o.scope_key=$1`

const hostedClaimLatest = ` AND (o.draft_id,o.draft_digest) = (SELECT id,content_hash FROM fugue_platform_artifacts
 WHERE artifact_kind='hosted_path_proposal' AND scope_key=o.scope_key AND status='draft' ORDER BY updated_at DESC,id ASC LIMIT 1)`

func readHostedClaim(row *sql.Row) (c HostedSuccessorClaim, expires time.Time, err error) {
	err = row.Scan(&c.Scope, &c.Project, &c.Draft, &c.DraftDigest, &c.Owner, &c.Paths, &c.Prepared, &c.Forward, &c.Sequence, &expires)
	return
}

// Capture just one existing offer by indexed scope, never a global "latest".
// The caller compares the current project intent in the same snapshot as
// Business. The commit transaction recomputes Business under project locks.
func (s *Store) ReadHostedSuccessor(ctx context.Context, host, tenant, owner string, bind func(ProjectHostRouteInputs, pc.HostedScopeProposal) (string, string, string, error)) (c HostedSuccessorClaim, p pc.HostedScopeProposal, dir string, found bool, err error) {
	if s.db == nil || bind == nil {
		return c, p, "", false, nil
	}
	scope, key := NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "hosted-path", Hostname: host, TenantID: tenant, AppID: owner})
	c, _, err = readHostedClaim(s.db.QueryRowContext(ctx, hostedClaimRowSQL, key))
	if errors.Is(err, sql.ErrNoRows) {
		return c, p, "", false, nil
	}
	if err != nil {
		return c, p, "", false, err
	}
	a, err := s.GetPlatformArtifactContext(ctx, c.Draft)
	if err != nil {
		return c, p, "", true, err
	}
	raw, e := json.Marshal(a.Content)
	if e != nil || len(raw) > 1<<20 || json.Unmarshal(raw, &p) != nil || a.ID != c.Draft || a.ContentHash != c.DraftDigest ||
		a.Scope != scope || a.ScopeKey != key || a.Status != model.PlatformArtifactStatusDraft || a.ArtifactKind != model.PlatformArtifactKindHostedPathProposal ||
		a.CreatedByID != platformproducer.Actor || a.CreatedByType != model.ActorTypeBootstrap || s.VerifyPlatformArtifactIntegrity(a) != nil ||
		p.Host != host || p.Before.Owner.AppID != owner || p.Before.Owner.TenantID != tenant || p.Schema != pc.HostedProposalSchema {
		return c, p, "", true, ErrConflict
	}
	c.Host = host
	err = s.EditProjectHostRoutes(ctx, c.Project, host, false, func(in ProjectHostRouteInputs) (*model.ProjectRouteTable, error) {
		identity, paths, d, e := bind(in, p)
		if e != nil || identity != c.Owner || (d != "forward" && d != "baseline") || d == "forward" && paths != c.Paths {
			return nil, ErrConflict
		}
		dir = d
		c.Business, e = hostedBusinessRevision(in)
		return nil, e
	})
	return c, p, dir, true, err
}

func pgCheckHostedSuccessor(ctx context.Context, tx *sql.Tx, parent model.PlatformArtifact, g *platformProducerReleaseGuard) error {
	raw, present := parent.Metadata[HostedSuccessorMetadata]
	if !present {
		return nil
	}
	// Recovery remains the ordinary exact verified LKG path. A withdrawal
	// following a project edit is a new forward publication, not recovery.
	if g != nil && producerRecoveryPhase(g) {
		return nil
	}
	var want HostedSuccessorClaim
	if g == nil || parent.ArtifactKind != model.PlatformArtifactKindReleaseSet || parent.ScopeKey != pc.GlobalScopeKey || len(raw) > 8192 || json.Unmarshal([]byte(raw), &want) != nil ||
		want.Sequence < 1 || want.Project == "" || want.Host == "" || !platformproducer.ValidDigest(want.Business) ||
		want.Phase != "prepare" && want.Phase != "forward" && want.Phase != "withdraw" {
		return ErrConflict
	}
	ref, _, err := pc.HostedAddressAuthority(parent)
	if err != nil || ref == nil {
		return ErrConflict
	}
	in, err := pgProjectHostInputs(ctx, tx, want.Project, want.Host, true)
	if err != nil {
		return err
	}
	got, expires, err := readHostedClaim(tx.QueryRowContext(ctx, hostedClaimRowSQL+hostedClaimLatest+" FOR SHARE OF o", want.Scope))
	if err != nil {
		return err
	}
	got.Host, got.Phase = want.Host, want.Phase
	got.Business, err = hostedBusinessRevision(in)
	_, scope := NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "hosted-path", Hostname: in.Domain.Hostname, TenantID: in.Domain.TenantID, AppID: in.Domain.AppID})
	if err != nil || got != want || scope != want.Scope || want.Phase == "prepare" && !expires.After(time.Now()) {
		return ErrConflict
	}
	return nil
}
