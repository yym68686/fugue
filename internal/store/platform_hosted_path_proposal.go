package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/routeartifact"
	"fugue/internal/schemamigrate"
)

// Only the compiler can supply these inputs. No HTTP body, canned route
// table, shadow diagnostic projection or release/rollback command reaches
// this method. Signing a draft creates no release, LKG or consumer assignment.
func (s *Store) EnsureCompiledHostedPathProposals(ctx context.Context, src pc.HostedAddressSource, stage pc.CompileResult, capture *pc.HostedTransitionCapture) ([]string, error) {
	if capture == nil {
		return nil, nil
	}
	if capture.Desired.InputSnapshot.CapturedAt == nil {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		var guarded bool
		if err := s.db.QueryRowContext(ctx, schemamigrate.HostedProposalGuardsReadySQL).Scan(&guarded); err != nil {
			return nil, err
		}
		if !guarded {
			return nil, fmt.Errorf("%w: proposal storage guards are not installed", ErrConflict)
		}
	}
	proposals, rejected := pc.BuildIndependentHostedScopeProposals(src, stage, capture)
	refusals := hostedExternalRefusals(rejected)
	if len(proposals) == 0 && len(refusals) == 0 {
		return nil, rejected
	}
	a, b := src.Authority, src.Publication
	full := model.PlatformPublicationPrecondition{ArtifactID: b.ReleaseSetID, ContentHash: b.ReleaseSetDigest, ReleaseID: b.ReleaseID, FencingToken: b.FencingToken}
	if err := s.ValidateDNSProducerSelection(ctx, a.Producer, full); err != nil {
		return nil, err
	}
	if err := s.ValidateHostedDispatchSelections(ctx, []HostedDispatchSelection{{Scope: pc.GlobalScopeKey, Full: full, LKGID: a.LKGID, EvidenceHash: a.EvidenceHash, ExpiresAt: a.ExpiresAt}}); err != nil {
		return nil, err
	}
	// Diagnostic persistence is not a write precondition for an independently
	// valid sibling. The status read rechecks current authority and desired paths.
	if err := s.rememberHostedExternalRefusals(ctx, src, *capture.Desired.InputSnapshot.CapturedAt, refusals); err != nil {
		rejected = errors.Join(rejected, fmt.Errorf("cache host compiler diagnostics: %w", err))
	}
	var ids []string
	for _, p := range proposals {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		schema, err := s.hostedDraftEncoding(ctx, src, p)
		if err != nil {
			rejected = errors.Join(rejected, fmt.Errorf("select draft encoding for %s: %w", p.Host, err))
			continue
		}
		p.Schema = schema
		draft, err := hostedProposalDraft(p, *capture.Desired.InputSnapshot.CapturedAt, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		stored, _, err := s.ensurePlatformArtifact(draft)
		if err != nil {
			return nil, err
		}
		if stored.Status != model.PlatformArtifactStatusDraft {
			return nil, ErrConflict
		}
		ids = append(ids, stored.ID)
	}
	return ids, rejected
}

func hostedProposalDraft(p pc.HostedScopeProposal, captured, now time.Time) (model.PlatformArtifact, error) {
	owner, a := p.Before.Owner, p.Authority
	if !p.SupportedSchema() || p.Host == "" || owner.Hostname != p.Host || owner.AppID == "" || owner.TenantID == "" ||
		owner != p.Prepared.Owner || owner != p.After.Owner || len(p.Before.Routes) == 0 || len(p.After.Routes) == 0 || len(p.Changes) == 0 ||
		a.Producer.ArtifactID == "" || a.Producer.ContentHash == "" || a.Producer.ReleaseID == "" || a.Producer.FencingToken <= 0 ||
		a.LKGID == "" || a.EvidenceHash == "" || !a.ExpiresAt.After(now) || captured.IsZero() || captured.After(now) || p.Base.Validate() != nil {
		return model.PlatformArtifact{}, ErrInvalidInput
	}
	// Generation covers only the original authority and host-owned semantics.
	// Capture time is the creation time of the *first* observation, never a
	// renewal. An assignment must separately check current authority, tenant
	// intent, runtime and point-in-time consumer observations.
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > 1<<20 {
		return model.PlatformArtifact{}, ErrInvalidInput
	}
	var content map[string]any
	if err := json.Unmarshal(raw, &content); err != nil {
		return model.PlatformArtifact{}, err
	}
	digest, err := pc.Digest(content)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	return model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindHostedPathProposal,
		Scope:      model.PlatformArtifactScope{ScopeType: "hosted-path", Hostname: p.Host, TenantID: owner.TenantID, AppID: owner.AppID},
		Generation: fmt.Sprintf("hostpath_%s", strings.TrimPrefix(digest, "sha256:")), Content: content,
		Status: model.PlatformArtifactStatusDraft, CreatedByType: model.ActorTypeBootstrap, CreatedByID: platformproducer.Actor,
		CreatedAt: captured.UTC()}, nil
}

// Choosing an encoding is not admission. In particular, deploying a new
// compiler must not rewrite a live v1 head to a different image. A later
// offer transaction independently rechecks membership and evidence.
func (s *Store) hostedDraftEncoding(ctx context.Context, src pc.HostedAddressSource, p pc.HostedScopeProposal) (string, error) {
	if s.db == nil {
		return pc.HostedProposalSchema, nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	read := pgDNSRouteSourceReader(ctx, tx)
	var key, id, hash, auth string
	var until time.Time
	err = tx.QueryRowContext(ctx, `SELECT scope_key,draft_id,draft_digest,authority_digest,valid_until FROM fugue_hosted_selections WHERE hostname=$1`, p.Host).Scan(&key, &id, &hash, &auth, &until)
	if err == nil {
		old, err := s.hostedHeadProposal(read, p.Host, key, id, hash, auth, until)
		if err != nil {
			return "", err
		}
		if old.Before.Owner != p.Before.Owner {
			return "", ErrConflict
		}
		return old.Schema, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	// Only pay for a second compiler projection if this release has at
	// least one authenticated v2 executor. An unrelated member cannot
	// enable the format: all recipients of this host are checked below.
	var offered bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_platform_consumer_instances WHERE scope_key=$1 AND release_set_id=$2 AND artifact_kind=$3 AND identity_verified AND compatibility_capabilities_json::jsonb ? $4)`, pc.GlobalScopeKey, p.Base.ReleaseSetID, model.PlatformArtifactKindDNSAnswerBundle, routeartifact.HostedContinuationCapability).Scan(&offered); err != nil {
		return "", err
	}
	if !offered {
		return pc.HostedProposalSchema, nil
	}
	full, err := s.hostedSelectedBase(read, p)
	if err != nil {
		return "", err
	}
	// Reuse the verified v1 physical projection to enumerate the closed
	// recipient set. Never sign or persist this provisional draft/image.
	stamp := time.Now().UTC()
	draft, err := hostedProposalDraft(p, stamp, stamp)
	if err != nil {
		return "", err
	}
	pin, err := pc.Digest(draft.Content)
	if err != nil {
		return "", err
	}
	program, err := routeartifact.FreezeHostedProgram(src, p, pin)
	if err != nil {
		return "", err
	}
	unit, err := routeartifact.OpenHostedPhase(p, pin, program.Digest, program.Wire, "prepare")
	if err != nil {
		return "", err
	}
	return pgHostedNewDraftEncoding(ctx, tx, full, p, unit, stamp)
}

func pgHostedNewDraftEncoding(ctx context.Context, tx *sql.Tx, full model.PlatformArtifact, p pc.HostedScopeProposal, unit *routeartifact.HostedPhaseResolver, now time.Time) (string, error) {
	p.Schema = pc.HostedMembershipProposalSchema
	parts, _, err := pgHostedCurrentManifest(ctx, tx, full, p, unit, func(m model.PlatformExpectedConsumer, a model.PlatformConsumerAssignment) (bool, error) {
		code, _, e := pgHostedMemberBlocker(ctx, tx, p, m, a, now, false)
		return code == "", e
	}, nil, nil)
	if err != nil {
		return "", err
	}
	if len(parts) == 0 {
		return pc.HostedProposalSchema, nil
	}
	return pc.HostedMembershipProposalSchema, nil
}
