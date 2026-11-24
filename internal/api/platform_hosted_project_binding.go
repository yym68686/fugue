package api

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/store"
)

// Identifies the business object, not its availability. A route rollback
// cannot roll the app version, TTL, domain ownership or a different project's
// routes back with it. Readers must compare this value at selection time.
type hostedProjectBinding struct {
	ProjectID, TenantID, OwnerID, Host, OwnerDigest, PathDigest string
	Direction                                                   string
}

func matchHostedProjectIntent(in store.ProjectHostRouteInputs, p pc.HostedScopeProposal) (hostedProjectBinding, error) {
	bad := errors.New("host proposal does not match the saved domain intent")
	d, r, z := in.Domain, in.DNS, in.Zone
	owner := p.Before.Owner
	if in.Project.ID == "" || d.CreatedAt.IsZero() ||
		p.Host != d.Hostname || owner.Hostname != d.Hostname || owner.AppID != d.AppID || owner.TenantID != d.TenantID || in.Project.TenantID != d.TenantID ||
		owner != p.After.Owner || owner != p.Prepared.Owner {
		return hostedProjectBinding{}, bad
	}
	if a := p.ExternalPath; a != nil {
		// The proposal does not supply authority over an external zone.
		// Bind the verified CNAME to its current project domain, and leave
		// address/TTL validation to the independently selected DNS child.
		target := a.Target
		if d.DNSMode != model.AppDomainDNSModeExternal || d.DNSRecordKind != model.AppDomainDNSRecordKindCNAME ||
			owner.Status != model.AppDomainStatusVerified || d.DNSZoneID != "" || d.DNSRecordID != "" || z.ID != "" || r.ID != "" ||
			target.Hostname != d.Hostname || target.AppID != d.AppID || target.TenantID != d.TenantID ||
			d.RouteTarget == "" || d.RouteTarget == d.Hostname || target.Target != d.RouteTarget || !slices.Contains(target.Members, d.Hostname) {
			return hostedProjectBinding{}, bad
		}
	} else if d.DNSMode != model.AppDomainDNSModeManaged || z.ID == "" || z.ID != d.DNSZoneID || r.ZoneID != z.ID || r.ID == "" || r.ID != d.DNSRecordID ||
		r.TenantID != d.TenantID || r.CreatedAt.IsZero() || r.TTL <= 0 || len(r.Values) != 1 {
		return hostedProjectBinding{}, bad
	}
	// Empty edit: read actual effective intent with the same interpreter as
	// host-plan/apply. It never writes or requests replicas. Deliberately do
	// not pass the intended target list as an edit (that would manufacture
	// an apparent match to an unapplied plan).
	scoped, err := store.ProjectRoutesForHost(in.Table, p.Host)
	if err != nil {
		return hostedProjectBinding{}, err
	}
	in.Table = scoped
	current, _, changed, err := buildProjectHostRoutePlan(in, model.ProjectHostRouteEdit{Hostname: p.Host})
	if err != nil || changed || !maps.Equal(current.Before, current.After) {
		return hostedProjectBinding{}, errors.Join(bad, err)
	}
	table := model.NormalizeProjectRouteTable(in.Table)
	table.ProjectID, table.TenantID = in.Project.ID, in.Project.TenantID
	bindings, err := store.CompileProjectRouteTableBindings(table, in.Apps)
	if err != nil {
		return hostedProjectBinding{}, err
	}
	for _, b := range bindings {
		if b.Hostname == p.Host && (b.StripPrefix || b.Rewrite != "") {
			return hostedProjectBinding{}, bad
		}
	}
	paths := func(rows pc.HostedDispatchRows) (map[string]string, error) {
		v := map[string]string{}
		for _, r := range rows.Routes {
			if r.Hostname != p.Host || r.TenantID != owner.TenantID || r.AppID == "" || r.PathPrefix == "" || v[r.PathPrefix] != "" {
				return nil, bad
			}
			v[r.PathPrefix] = r.AppID
		}
		if v["/"] == "" || !slices.Contains(slices.Collect(maps.Values(v)), d.AppID) {
			return nil, bad
		}
		return v, nil
	}
	before, err := paths(p.Before)
	if err != nil {
		return hostedProjectBinding{}, err
	}
	mid, err := paths(p.Prepared)
	if err != nil || !maps.Equal(mid, before) {
		return hostedProjectBinding{}, bad
	}
	after, err := paths(p.After)
	if err != nil || maps.Equal(before, after) {
		return hostedProjectBinding{}, bad
	}
	direction := ""
	switch {
	case maps.Equal(current.Before, after):
		direction = "forward"
	case maps.Equal(current.Before, before):
		direction = "baseline"
	default:
		return hostedProjectBinding{}, bad
	}
	// Keep DNS configuration, but not health checks/publication timestamps
	// or tenant workload liveness, in the ownership binding.
	identity, err := pc.Digest([]any{"host-project-owner/v1", in.Project.ID, in.Project.TenantID, d.Hostname, d.AppID, d.CreatedAt, d.RouteTarget, d.DNSRecordSource, d.DNSRecordKind,
		z.ID, z.ZoneName, z.ProjectID, z.CreatedAt, r.ID, r.CreatedAt, r.TTL, r.Values, r.FlattenMode, r.FlattenTarget, r.FlattenIPv4Policy, r.FlattenIPv6Policy, r.FlattenTTLPolicy, r.FlattenFallbackPolicy})
	if err != nil {
		return hostedProjectBinding{}, err
	}
	pathID, err := pc.Digest(current.Before)
	return hostedProjectBinding{in.Project.ID, d.TenantID, d.AppID, d.Hostname, identity, pathID, direction}, err
}

// Takes only a stored draft ID. No caller-provided base, owner, origin or
// route table. It is an optimistic read, NOT a release or a substitute for
// comparing the exact pointers again under the selection write transaction.
func (s *Server) readHostedProjectBinding(ctx context.Context, id string) (pc.HostedScopeProposal, hostedProjectBinding, error) {
	var p pc.HostedScopeProposal
	var b hostedProjectBinding
	bad := errors.New("compiler-owned host proposal is no longer current")
	a, err := s.store.GetPlatformArtifactContext(ctx, id)
	if err != nil {
		return p, b, err
	}
	if a.ID != id || a.ArtifactKind != model.PlatformArtifactKindHostedPathProposal || a.Status != model.PlatformArtifactStatusDraft ||
		a.CreatedByType != model.ActorTypeBootstrap || a.CreatedByID != platformproducer.Actor || s.store.VerifyPlatformArtifactIntegrity(a) != nil {
		return p, b, bad
	}
	raw, err := json.Marshal(a.Content)
	if err != nil || len(raw) > 1<<20 || json.Unmarshal(raw, &p) != nil || !p.SupportedSchema() || p.Base.Validate() != nil || !p.Authority.ExpiresAt.After(time.Now()) {
		return p, b, bad
	}
	scope, key := store.NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "hosted-path", Hostname: p.Host, AppID: p.Before.Owner.AppID, TenantID: p.Before.Owner.TenantID})
	if a.Scope != scope || a.ScopeKey != key {
		return p, b, bad
	}
	app, err := s.store.GetAppMetadata(scope.AppID)
	if err != nil || app.TenantID != scope.TenantID || app.ProjectID == "" {
		return p, b, bad
	}
	err = s.store.EditProjectHostRoutes(ctx, app.ProjectID, p.Host, false, func(in store.ProjectHostRouteInputs) (*model.ProjectRouteTable, error) {
		var e error
		b, e = matchHostedProjectIntent(in, p)
		return nil, e
	})
	if err != nil {
		return p, b, err
	}
	ref := p.Base
	full := model.PlatformPublicationPrecondition{ArtifactID: ref.ReleaseSetID, ContentHash: ref.ReleaseSetDigest, ReleaseID: ref.ReleaseID, FencingToken: ref.FencingToken}
	if err = s.store.ValidateDNSProducerSelection(ctx, p.Authority.Producer, full); err != nil {
		return p, b, err
	}
	err = s.store.ValidateHostedDispatchSelections(ctx, []store.HostedDispatchSelection{{Scope: pc.GlobalScopeKey, Full: full, LKGID: p.Authority.LKGID, EvidenceHash: p.Authority.EvidenceHash, ExpiresAt: p.Authority.ExpiresAt}})
	return p, b, err
}

// Only the latest draft of this exact tenant/owner/hostname may explain a
// pending preflight. Never seek an older matching proposal after a conflict.
// Neither this inspection nor a successful group render publishes anything.
func (s *Server) pendingHostedPathCandidate(ctx context.Context, project model.Project, plan model.ProjectHostRoutePlan) bool {
	_, key := store.NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "hosted-path", Hostname: plan.Edit.Hostname, TenantID: project.TenantID, AppID: plan.HostnameOwnerAppID})
	list, err := s.store.ListPlatformArtifactMetadata(model.PlatformArtifactFilter{ArtifactKind: model.PlatformArtifactKindHostedPathProposal, ScopeKey: key, Status: model.PlatformArtifactStatusDraft, Limit: 1})
	if err != nil || len(list) != 1 || list[0].ScopeKey != key {
		return false
	}
	p, b, err := s.readHostedProjectBinding(ctx, list[0].ID)
	if err != nil || b.ProjectID != project.ID || b.TenantID != project.TenantID || b.Direction != "forward" || b.Host != plan.Edit.Hostname || b.OwnerID != plan.HostnameOwnerAppID {
		return false
	}
	matches := func(rows pc.HostedDispatchRows, want map[string]string) bool {
		if len(rows.Routes) != len(want) {
			return false
		}
		for _, r := range rows.Routes {
			if want[r.PathPrefix] != r.AppID {
				return false
			}
		}
		return true
	}
	return matches(p.Before, plan.Before) && matches(p.After, plan.After) && ctx.Err() == nil
}
