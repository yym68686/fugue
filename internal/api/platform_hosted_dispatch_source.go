package api

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
)

// Input to a host-scoped publisher; it is NOT an assignment. Referencing a
// signed Cell does not select its other tenants. Keep source lifetimes on the
// reference, not on the time at which the host projection is constructed.
type hostedDispatchSource struct {
	Scope, Parent, ParentDigest, Release, Route, RouteDigest, TLS, TLSDigest string
	Fence                                                                    int64
	LKG, LKGProof                                                            string
	ExpiresAt                                                                time.Time
	Host                                                                     hostedDispatchRows
}

type hostedDispatchRows = pc.HostedDispatchRows

func (s hostedDispatchSource) selection() store.HostedDispatchSelection {
	return store.HostedDispatchSelection{Scope: s.Scope, Full: model.PlatformPublicationPrecondition{ArtifactID: s.Parent, ContentHash: s.ParentDigest, ReleaseID: s.Release, FencingToken: s.Fence}, LKGID: s.LKG, EvidenceHash: s.LKGProof, ExpiresAt: s.ExpiresAt}
}

func selectHostedDispatchRows(host, tenant, owner string, paths map[string]string, projected model.EdgeRouteIntentSnapshot, tls []model.EdgeTLSAllowlistEntry) (hostedDispatchRows, error) {
	return pc.SelectHostedDispatchRows(host, tenant, owner, paths, projected, tls)
}

// Read only the selected verified full of ONE scope. Neither IDs nor fences
// are supplied by the admin diagnostic representation. The eventual writer
// must recheck these references together with its other source(s), unchanged
// project intent, and scoped DNS prerequisites in its publication transaction.
func (s *Server) readHostedDispatchSource(ctx context.Context, scope, host, tenant, owner string, paths map[string]string) (hostedDispatchSource, error) {
	bad := errors.New("no current verified full route and TLS source for this host")
	var empty hostedDispatchSource
	kind := model.PlatformArtifactKindReleaseSet
	parent, rel, ok, err := s.store.GetActivePlatformArtifact(kind, scope, model.PlatformArtifactReleaseChannelFull)
	if err != nil {
		return empty, err
	}
	if !ok || rel.ID == "" || rel.ArtifactID != parent.ID || rel.ArtifactKind != kind || rel.ScopeKey != scope || parent.ScopeKey != scope || rel.ReleaseChannel != model.PlatformArtifactReleaseChannelFull ||
		rel.Status != model.PlatformArtifactReleaseStatusActive || rel.FencingToken <= 0 || rel.VerificationState != model.PlatformArtifactVerificationStateVerified || rel.VerifiedLKGGeneration != parent.Generation ||
		!s.validateReleaseSetReferences(parent).Pass || s.store.VerifyPlatformArtifactIntegrity(parent) != nil {
		return empty, bad
	}
	role := parent.Metadata["publication_role"]
	if scope == pc.GlobalScopeKey {
		if role != "" || parent.Content["publication_role"] != nil && parent.Content["publication_role"] != "" {
			return empty, bad
		}
	} else if parent.Content["publication_role"] != pc.PublicationRoleCellRoutes {
		return empty, bad
	}
	lkg, err := s.store.GetPlatformLKG(kind, scope)
	if err != nil {
		return empty, err
	}
	if lkg == nil || lkg.ID == "" || lkg.VerifiedByReleaseID != rel.ID || lkg.VerificationEvidenceHash == "" || !platformsafety.EvaluatePlatformLKGSnapshot(*lkg, parent, s.bundleKeyring(), time.Now().UTC()).Pass {
		return empty, bad
	}
	route, err := s.consumerAssignmentChild(parent, model.PlatformArtifactKindEdgeRouteBundle)
	if err != nil {
		return empty, err
	}
	tls, err := s.consumerAssignmentChild(parent, model.PlatformArtifactKindCaddyRouteConfig)
	if err != nil {
		return empty, err
	}
	if route.ScopeKey != scope || tls.ScopeKey != scope || s.store.VerifyPlatformArtifactIntegrity(route) != nil || s.store.VerifyPlatformArtifactIntegrity(tls) != nil {
		return empty, bad
	}
	proj, err := pc.ProjectRouteArtifact(route)
	if err != nil {
		return empty, err
	}
	var entries []model.EdgeTLSAllowlistEntry
	raw, err := json.Marshal(tls.Content["tls_allowlist"])
	if err != nil || json.Unmarshal(raw, &entries) != nil {
		return empty, bad
	}
	rows, err := selectHostedDispatchRows(host, tenant, owner, paths, proj, entries)
	if err != nil {
		return empty, err
	}
	if err := s.store.CheckPlatformInputRead(ctx, []store.PlatformInputReadSelection{
		{ArtifactID: parent.ID, ContentHash: parent.ContentHash, ReleaseID: rel.ID, LKGKind: kind, LKGScope: scope, LKG: lkg},
		{ArtifactID: route.ID, ContentHash: route.ContentHash, ParentID: parent.ID, ParentHash: parent.ContentHash},
		{ArtifactID: tls.ID, ContentHash: tls.ContentHash, ParentID: parent.ID, ParentHash: parent.ContentHash},
	}); err != nil {
		return empty, err
	}
	last, again, ok, err := s.store.GetActivePlatformArtifact(kind, scope, model.PlatformArtifactReleaseChannelFull)
	if err != nil || !ok || again.ID != rel.ID || again.FencingToken != rel.FencingToken || again.Version != rel.Version || again.VerificationState != rel.VerificationState ||
		last.ID != parent.ID || last.ContentHash != parent.ContentHash || !lkg.ExpiresAt.After(time.Now()) || ctx.Err() != nil {
		return empty, errors.Join(bad, err)
	}
	source := hostedDispatchSource{Scope: scope, Parent: parent.ID, ParentDigest: parent.ContentHash, Release: rel.ID, Route: route.ID, RouteDigest: route.ContentHash, TLS: tls.ID, TLSDigest: tls.ContentHash,
		Fence: rel.FencingToken, LKG: lkg.ID, LKGProof: lkg.VerificationEvidenceHash, ExpiresAt: lkg.ExpiresAt, Host: rows}
	if err := s.store.ValidateHostedDispatchSelections(ctx, []store.HostedDispatchSelection{source.selection()}); err != nil {
		return empty, err
	}
	return source, nil
}
