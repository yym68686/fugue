package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
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
	if len(proposals) == 0 {
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
	var ids []string
	for _, p := range proposals {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
	if p.Schema != pc.HostedProposalSchema || p.Host == "" || owner.Hostname != p.Host || owner.AppID == "" || owner.TenantID == "" ||
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
