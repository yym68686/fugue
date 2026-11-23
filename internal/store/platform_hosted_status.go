package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"maps"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/routeartifact"
)

// A retained independent head owns the answer even after expiry, withdrawal
// or a change of owner. Only an absent head permits legacy diagnostics.
func (s *Store) ReadHostedRouteStatus(ctx context.Context, project model.Project, plan model.ProjectHostRoutePlan, bind func(ProjectHostRouteInputs, pc.HostedScopeProposal) (owner, paths, dir string, err error)) (model.ProjectHostRoutePublicationStatus, bool, error) {
	empty := model.ProjectHostRoutePublicationStatus{}
	pending := model.ProjectHostRoutePublicationStatus{State: "pending", Reason: "independent hostname selection cannot verify these desired paths"}
	if s == nil || bind == nil {
		return pending, true, ErrInvalidInput
	}
	// The non-SQL backend implements neither independent selection nor receipts.
	if s.db == nil {
		return empty, false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return pending, true, err
	}
	defer tx.Rollback()
	host := plan.Edit.Hostname
	_, key := NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "hosted-path", Hostname: host, TenantID: project.TenantID, AppID: plan.HostnameOwnerAppID})
	var matches bool
	err = tx.QueryRowContext(ctx, `SELECT scope_key=$2 AND project_id=$3 FROM fugue_hosted_selections WHERE hostname=$1`, host, key, project.ID).Scan(&matches)
	if errors.Is(err, sql.ErrNoRows) {
		return empty, false, nil
	}
	if err != nil || !matches {
		return pending, true, err
	}
	out, err := s.pgHostedRouteStatus(ctx, tx, project, plan, key, bind)
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) || errors.Is(err, sql.ErrNoRows) {
		return pending, true, nil
	}
	if err != nil {
		return pending, true, err
	}
	return out, true, nil
}

func (s *Store) pgHostedRouteStatus(ctx context.Context, tx *sql.Tx, project model.Project, plan model.ProjectHostRoutePlan, key string, bind func(ProjectHostRouteInputs, pc.HostedScopeProposal) (owner, paths, dir string, err error)) (model.ProjectHostRoutePublicationStatus, error) {
	var empty model.ProjectHostRoutePublicationStatus
	host := plan.Edit.Hostname
	var id, hash, program, auth, business, owner, paths, phase string
	var body, manifest []byte
	var until time.Time
	err := tx.QueryRowContext(ctx, `SELECT draft_id,draft_digest,program_digest,program,authority_digest,business_digest,owner_digest,
 CASE WHEN phase='withdraw' THEN baseline_path_digest ELSE path_digest END,phase,members,valid_until FROM fugue_hosted_selections
 WHERE hostname=$1 AND scope_key=$2 AND project_id=$3 AND valid_until>clock_timestamp()`, host, key, project.ID).Scan(&id, &hash, &program, &body, &auth, &business, &owner, &paths, &phase, &manifest, &until)
	if err != nil {
		return empty, err
	}
	read := pgDNSRouteSourceReader(ctx, tx)
	p, err := s.hostedHeadProposal(read, host, key, id, hash, auth, until)
	if err != nil {
		return empty, err
	}
	in, err := pgProjectHostInputs(ctx, tx, project.ID, host, false)
	if err != nil {
		return empty, err
	}
	realOwner, realPaths, dir, err := bind(in, p)
	wanted, e := pc.Digest(plan.After)
	revision, re := hostedBusinessRevision(in)
	if err != nil || e != nil || re != nil || revision != business || realOwner != owner || realPaths != paths || paths != wanted || in.Project.ID != project.ID || in.Project.TenantID != project.TenantID || in.Domain.AppID != plan.HostnameOwnerAppID || in.Domain.Hostname != host || (dir != "forward" && dir != "baseline") || (phase == "withdraw") != (dir == "baseline") {
		return empty, ErrConflict
	}
	full, err := s.hostedSelectedBase(read, p)
	if err != nil {
		return empty, err
	}
	if err = s.checkHostedLiveHolds(ctx, tx, read, p, full, time.Now().UTC()); err != nil {
		return empty, err
	}
	unit, err := routeartifact.OpenHostedPhase(p, hash, program, body, phase)
	if err != nil {
		return empty, ErrConflict
	}
	return pgHostedPublishedParts(ctx, tx, full, p, unit, manifest, HostedSuccessorClaim{Host: host, Scope: key, Project: project.ID}, phase, until)
}

func pgHostedPublishedParts(ctx context.Context, tx *sql.Tx, full model.PlatformArtifact, p pc.HostedScopeProposal, unit *routeartifact.HostedPhaseResolver, manifest []byte, c HostedSuccessorClaim, phase string, until time.Time) (model.ProjectHostRoutePublicationStatus, error) {
	var empty model.ProjectHostRoutePublicationStatus
	current, directory, err := pgHostedCurrentManifest(ctx, tx, full, p, unit, nil)
	if err != nil {
		return empty, err
	}
	var selected map[string]string
	if len(current) == 0 || len(manifest) > 2<<20 || json.Unmarshal(manifest, &selected) != nil || !maps.Equal(current, selected) {
		return empty, ErrConflict
	}
	// The directory is only a discovery hint. Check each required entry
	// against its entire primary key rather than scanning every host.
	// An extra hint cannot obtain a packet or satisfy the exact manifest.
	raw, err := json.Marshal(directory)
	if err != nil {
		return empty, err
	}
	var complete bool
	err = tx.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM jsonb_to_recordset($2::jsonb)
 AS e(member_id text,kind text,set_id text,child_id text)
 WHERE NOT EXISTS (SELECT FROM fugue_hosted_member_directory d
 WHERE (d.member_id,d.kind,d.set_id,d.child_id,d.hostname)=(e.member_id,e.kind,e.set_id,e.child_id,$1)))`, c.Host, string(raw)).Scan(&complete)
	if err != nil {
		return empty, err
	}
	if !complete {
		return empty, ErrConflict
	}
	h, err := pgHostedSelectionStatus(ctx, tx, c)
	if err != nil {
		return empty, err
	}
	if h.Required != len(current) || h.Phase != phase || !until.After(time.Now()) {
		return empty, ErrConflict
	}
	return hostedRouteEvidence(h, time.Now().UTC()), nil
}

func hostedRouteEvidence(h hostedSelectionStatus, now time.Time) model.ProjectHostRoutePublicationStatus {
	ev := model.ProjectHostSelectionEvidence{Decision: h.Decision, Fence: h.Fence, Phase: h.Phase, Required: h.Required, Passing: h.Passing}
	out := model.ProjectHostRoutePublicationStatus{State: "pending", Reason: "waiting for fresh observations from all required hostname members", HostSelection: &ev}
	if h.Required > 0 && h.Passing == h.Required && h.EvidenceUntil.After(now) {
		ev.EvidenceUntil = &h.EvidenceUntil
	} else if !h.EvidenceUntil.IsZero() && !h.EvidenceUntil.After(now) {
		// At least one receipt expired since the snapshot. Do not invent
		// a count for the remaining members.
		out.HostSelection = nil
	}
	if h.Phase == "prepare" {
		out.Reason = "hostname preparation is not a serving selection"
	} else if (h.Phase == "forward" || h.Phase == "withdraw") && ev.EvidenceUntil != nil {
		out.State = "published"
		out.Reason = "independent hostname selection matches the desired paths with fresh physical member observations"
	}
	return out
}
