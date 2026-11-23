package edge

import (
	"errors"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
	"fugue/internal/trafficbinding"
)

// The group envelope remains byte-for-byte the authority for membership,
// the certificate and the default routes. Never append hosted rows to it.
// Keep an independent, immutable per-host index for actual user requests.
type edgeHostedRoute struct {
	packet routeartifact.HostedSelectedPacket
	source string
}

var errHostedGroup = errors.New("host selection does not match the active group, route or TLS owner")

func hostedRouteSource(b model.EdgeRouteBundle, host string) (routeartifact.HostedRecipientInput, model.EdgeTLSAllowlistEntry, error) {
	var in routeartifact.HostedRecipientInput
	var owner model.EdgeTLSAllowlistEntry
	ids := map[string]bool{}
	for _, o := range b.TLSAllowlist {
		if o.Hostname == host {
			if owner.Hostname != "" || o.AppID == "" || o.TenantID == "" || o.Status != model.AppDomainStatusVerified {
				return in, owner, errHostedGroup
			}
			owner = o
		}
	}
	if owner.Hostname == "" {
		return in, owner, errHostedGroup
	}
	for _, r := range b.Routes {
		if r.Hostname == host {
			if r.EdgeGroupID != b.EdgeGroupID || r.TenantID != owner.TenantID {
				return in, owner, errHostedGroup
			}
			in.Routes = append(in.Routes, r)
			if r.CachePolicyID != "" {
				ids[r.CachePolicyID] = true
			}
		}
	}
	for _, c := range b.CachePolicies {
		if ids[c.ID] {
			in.Cache = append(in.Cache, c)
			delete(ids, c.ID)
		}
	}
	if len(in.Routes) == 0 || len(ids) != 0 {
		return in, owner, errHostedGroup
	}
	return in, owner, nil
}

func (h edgeHostedRoute) materialize(b model.EdgeRouteBundle, pub routePublicationMetadata, host string, now time.Time) (*edgeRouteIndex, error) {
	p := h.packet
	t := b.TrafficRelease
	if pub.Candidate || p.Delivery.Host != host || len(p.Delivery.Parts) != 1 || t == nil || t.ScopeKey != "global" || t.ReleaseChannel != "full" || t.ReleaseSetID != p.Parent || t.ReleaseID != p.Delivery.MembershipRelease || t.FencingToken != p.Delivery.MembershipFence || t.RouteArtifactID != p.Child || t.RouteArtifactDigest != p.ChildDigest || !p.ValidUntil.After(now) || !b.ValidUntil.After(now) {
		return nil, errHostedGroup
	}
	base, owner, e := hostedRouteSource(b, host)
	if e != nil {
		return nil, e
	}
	source, e := pc.Digest([]any{base, owner})
	if e != nil || source != h.source {
		return nil, errHostedGroup
	}
	part := p.Delivery.Parts[0]
	if part.Recipient.Kind != model.PlatformArtifactKindEdgeRouteBundle || part.Recipient.Group != b.EdgeGroupID {
		return nil, errHostedGroup
	}
	// Cache policies live in the group namespace. A scoped command may
	// refer to an existing exact policy; it cannot mutate shared policy.
	policies := map[string]model.CachePolicy{}
	for _, c := range b.CachePolicies {
		if _, dup := policies[c.ID]; dup {
			return nil, errHostedGroup
		}
		policies[c.ID] = c
	}
	ids := map[string]bool{}
	for _, c := range part.Content.Cache {
		if ids[c.ID] || !reflect.DeepEqual(c, policies[c.ID]) {
			return nil, errHostedGroup
		}
		ids[c.ID] = true
	}
	paths := map[string]bool{}
	for _, r := range part.Content.Routes {
		path := model.NormalizeAppRoutePathPrefix(r.PathPrefix)
		if r.Hostname != host || r.EdgeGroupID != b.EdgeGroupID || r.TenantID != owner.TenantID || r.AppID == "" || paths[path] || r.CachePolicyID != "" && !ids[r.CachePolicyID] {
			return nil, errHostedGroup
		}
		paths[path] = true
	}
	if !paths["/"] {
		return nil, errHostedGroup
	}
	until := p.ValidUntil
	if b.ValidUntil.Before(until) {
		until = b.ValidUntil
	}
	return buildEdgeRouteIndex(model.EdgeRouteBundle{Routes: part.Content.Routes, Version: p.Decision, ValidUntil: until}, b.EdgeGroupID, routePublicationMetadata{HostedDecision: p.Decision}), nil
}

// Called before publishing a new group index, under mu. Changing group,
// owner or base is not permission to silently restore an old A. Other
// hosts still switch to their legitimate new group configuration.
func (s *Service) attachHostedRoutes(idx *edgeRouteIndex, b model.EdgeRouteBundle) {
	if len(s.hostedRoutes) == 0 {
		return
	}
	idx.hosted = make(map[string]*edgeRouteIndex, len(s.hostedRoutes))
	now := time.Now().UTC()
	for host, h := range s.hostedRoutes {
		idx.hosted[host], _ = h.materialize(b, idx.publication, host, now)
	}
}

func (s *Service) reserveHostedRoutesOnRestart() error {
	if s.Config.CachePath == "" {
		return nil
	}
	root, e := filepath.Abs(s.Config.CachePath + ".hosted-routes")
	if e != nil {
		return e
	}
	hosts, e := platformconsumer.HostedCheckpointHosts(root)
	if e != nil || len(hosts) == 0 {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hostedRoutes == nil {
		s.hostedRoutes = map[string]edgeHostedRoute{}
	}
	for _, host := range hosts {
		if _, ok := s.hostedRoutes[host]; !ok {
			s.hostedRoutes[host] = edgeHostedRoute{}
		}
	}
	if s.bundle != nil {
		i := buildEdgeRouteIndex(*s.bundle, s.Config.EdgeGroupID, s.routePublication)
		s.attachHostedRoutes(i, *s.bundle)
		s.routeIndex.Store(i)
	}
	return nil
}

// projection is independently materialized from the checked global route
// assignment. No network I/O occurs under the snapshot lock. A restored
// checkpoint cannot become active without the current privately addressed
// command, even if its local signature remains good.
func (s *Service) applyHostedRoute(a model.PlatformArtifact, host string, who platformcontrol.PlatformComponentIdentityClaims, sel model.PlatformConsumerAssignment, projection model.EdgeRouteBundle) error {
	now := time.Now().UTC()
	keys := bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs)
	p, e := routeartifact.VerifyHostedPacket(a, host, who, sel, 0, "", keys, now)
	if e != nil {
		return e
	}
	r := p.Delivery.Parts[0].Recipient
	if s.Config.CachePath == "" || r.Node != s.Config.EdgeID || r.Group != s.Config.EdgeGroupID || r.Kind != model.PlatformArtifactKindEdgeRouteBundle || len(p.Delivery.Parts) != 1 || who.Component != model.PlatformConsumerComponentEdgeWorker {
		return errHostedGroup
	}
	activation, e := s.selectRouteBundleSource()
	if e != nil || activation.candidate || activation.preparation != nil || activation.activeSlot != "" && activation.activeSlot != s.Config.EdgeSlot {
		return errHostedGroup
	}
	root, e := filepath.Abs(s.Config.CachePath + ".hosted-routes")
	if e != nil {
		return e
	}
	cp := platformconsumer.HostedCheckpoint{Root: root, Host: host, Who: who, Serving: sel, Keys: keys}
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.routeIndex.Load()
	if s.bundle == nil || i == nil || i.publication.Candidate || s.bundle.Version != i.bundleVersion || !s.Config.CaddyEnabled || !s.snapshot.Healthy || s.snapshot.StaleCache || s.snapshot.MaxStaleExceeded || s.snapshot.CaddyAppliedVersion != s.bundle.Version || s.snapshot.CaddyLastError != "" || validateRouteBundleActivationBinding(activation, i.publication) != nil || !reflect.DeepEqual(projection.TrafficRelease, s.bundle.TrafficRelease) || trafficbinding.ValidateGroup(s.bundle.TrafficRelease, r.Group, true) != nil || s.verifyBundle(*s.bundle, now) != nil {
		return errHostedGroup
	}
	base, owner, e := hostedRouteSource(projection, host)
	if e != nil {
		return e
	}
	source, e := pc.Digest([]any{base, owner})
	if e != nil {
		return e
	}
	h := edgeHostedRoute{packet: p, source: source}
	target, e := h.materialize(*s.bundle, i.publication, host, now)
	if e != nil {
		return e
	}
	before := base
	if old, ok := s.hostedRoutes[host]; ok {
		if old.packet.Fence == 0 {
			// Recover a durable write that preceded an in-memory swap.
			saved, e := cp.Reserved()
			switch {
			case e == nil:
				old = edgeHostedRoute{packet: saved, source: source}
			case errors.Is(e, os.ErrNotExist) && p.Fence == 1 && p.Delivery.Phase == "prepare":
				// A crash after creating the host directory but before
				// committing the first packet. Commit still verifies the
				// live baseline and a genuinely absent predecessor.
			default:
				return e
			}
		}
		if old.packet.Fence != 0 {
			if old.source != source || old.packet.Delivery.Member != who.ConsumerID() || old.packet.Parent != p.Parent || len(old.packet.Delivery.Parts) != 1 {
				return errHostedGroup
			}
			before = old.packet.Delivery.Parts[0].Content
		}
	}
	one := func(in routeartifact.HostedRecipientInput) map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput {
		return map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput{r: in}
	}
	if _, e = cp.Commit(a, one(before), one(base), time.Now().UTC()); e != nil {
		// A rename may have reached disk before a failed directory sync.
		// In that case reserve only this hostname until a signed retry.
		if saved, readErr := cp.Reserved(); readErr != nil && !errors.Is(readErr, os.ErrNotExist) || readErr == nil && (saved.Fence != s.hostedRoutes[host].packet.Fence || saved.Decision != s.hostedRoutes[host].packet.Decision) {
			if s.hostedRoutes == nil {
				s.hostedRoutes = map[string]edgeHostedRoute{}
			}
			s.hostedRoutes[host] = edgeHostedRoute{}
			next := *i
			next.hosted = maps.Clone(i.hosted)
			if next.hosted == nil {
				next.hosted = map[string]*edgeRouteIndex{}
			}
			next.hosted[host] = nil
			s.routeIndex.Store(&next)
		}
		return e
	}
	if s.hostedRoutes == nil {
		s.hostedRoutes = map[string]edgeHostedRoute{}
	}
	s.hostedRoutes[host] = h
	next := *i
	next.hosted = maps.Clone(i.hosted)
	if next.hosted == nil {
		next.hosted = map[string]*edgeRouteIndex{}
	}
	next.hosted[host] = target
	s.routeIndex.Store(&next)
	return nil
}

func (s *Service) ensureHostedRoute(cmd platformconsumer.HostedCommand, host string, who platformcontrol.PlatformComponentIdentityClaims, sel model.PlatformConsumerAssignment, projection model.EdgeRouteBundle) error {
	p := cmd.Packet
	if _, b, e := s.hostedObservationState(p); e == nil && reflect.DeepEqual(b.TrafficRelease, projection.TrafficRelease) {
		root, e := filepath.Abs(s.Config.CachePath + ".hosted-routes")
		if e != nil {
			return e
		}
		keys := bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs)
		checkpoint := platformconsumer.HostedCheckpoint{Root: root, Host: host, Who: who, Serving: sel, Keys: keys}
		disk, e := checkpoint.Load(time.Now().UTC())
		if e != nil {
			return e
		}
		if disk.Fence != p.Fence || disk.Decision != p.Decision || !reflect.DeepEqual(disk, p) {
			return errHostedGroup
		}
		return nil
	}
	return s.applyHostedRoute(cmd.Artifact, host, who, sel, projection)
}
