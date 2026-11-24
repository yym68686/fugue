package edge

import (
	"context"
	"crypto/x509"
	"errors"
	"reflect"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/routeartifact"
	"fugue/internal/routeproof"
)

func (s *Service) hostedObservationState(p routeartifact.HostedSelectedPacket) (*edgeRouteIndex, model.EdgeRouteBundle, error) {
	var empty model.EdgeRouteBundle
	activation, e := s.selectRouteBundleSource()
	if e != nil || activation.reporterInactive || activation.candidate || activation.preparation != nil || activation.activeSlot != "" && activation.activeSlot != s.Config.EdgeSlot {
		return nil, empty, errHostedGroup
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := s.routeIndex.Load()
	if idx == nil || s.bundle == nil || !s.Config.CaddyEnabled || !s.snapshot.Healthy || s.snapshot.StaleCache || s.snapshot.MaxStaleExceeded || s.snapshot.CaddyAppliedVersion != s.bundle.Version || s.snapshot.CaddyLastError != "" || s.bundle.Version != idx.bundleVersion || idx.publication.Candidate || validateRouteBundleActivationBinding(activation, idx.publication) != nil {
		return nil, empty, errHostedGroup
	}
	h := s.hostedRoutes[p.Delivery.Host]
	child := idx.hosted[p.Delivery.Host]
	now := time.Now().UTC()
	if child == nil || !child.validUntil.After(now) || !s.bundle.ValidUntil.After(now) || h.packet.Fence != p.Fence || h.packet.Decision != p.Decision || h.packet.Parent != p.Parent || h.packet.ChildDigest != p.ChildDigest || h.packet.Delivery.Member != p.Delivery.Member || child.publication.HostedDecision != p.Decision || s.verifyBundle(*s.bundle, now) != nil {
		return nil, empty, errHostedGroup
	}
	return child, *s.bundle, nil
}

// The caller independently verifies the two children of the serving full
// and both privately addressed commands. TLS is observation only: no
// certificate issue, key transfer or Caddy config replacement occurs.
func (s *Service) witnessHostedWorker(ctx context.Context, route, p routeartifact.HostedSelectedPacket, payload platformTLSCandidatePayload, roots *x509.CertPool) ([]routeartifact.HostedSample, error) {
	bad := errors.New("hosted Worker observation is not the selected route and certificate owner")
	if len(route.Delivery.Parts) != 1 || len(p.Delivery.Parts) != 1 || !p.ValidUntil.After(time.Now()) || p.Fence != route.Fence || p.Decision != route.Decision || p.Delivery.Host != route.Delivery.Host || p.Delivery.Member != route.Delivery.Member || p.Parent != route.Parent || p.Authority != route.Authority || p.Business != route.Business || p.Delivery.ImageDigest != route.Delivery.ImageDigest || p.Delivery.Phase != route.Delivery.Phase {
		return nil, bad
	}
	start, b, e := s.hostedObservationState(route)
	if e != nil {
		return nil, e
	}
	r, part := route.Delivery.Parts[0], p.Delivery.Parts[0]
	host := p.Delivery.Host
	routes := r.Content.Routes
	kind := part.Recipient.Kind
	if r.Recipient.Kind != model.PlatformArtifactKindEdgeRouteBundle || part.Recipient.Node != s.Config.EdgeID || part.Recipient.Group != s.Config.EdgeGroupID || r.Recipient.Node != part.Recipient.Node || r.Recipient.Group != part.Recipient.Group {
		return nil, bad
	}
	mode, owner := "https", ""
	switch kind {
	case model.PlatformArtifactKindEdgeRouteBundle:
		if p.Child != route.Child || part.ContentDigest != r.ContentDigest {
			return nil, bad
		}
	case model.PlatformArtifactKindCaddyRouteConfig:
		mode = "tls"
		_, local, e := hostedRouteSource(b, host)
		if e != nil || len(part.Content.TLS) != 1 || local != part.Content.TLS[0] {
			return nil, bad
		}
		signed, e := routeartifact.CustomDomainCertificateOwner(payload.Certificates, b, host)
		if e != nil || signed.AppID != local.AppID || signed.TenantID != local.TenantID {
			return nil, bad
		}
		owner, e = pc.Digest(local)
		if e != nil {
			return nil, e
		}
		// A certificate served by a stale or other Worker can still verify
		// SNI. Bind the handshake to a nonce reply from this exact host head.
		var root []model.EdgeRouteBinding
		for _, row := range routes {
			if model.NormalizeAppRoutePathPrefix(row.PathPrefix) == "/" {
				root = append(root, row)
			}
		}
		if len(root) != 1 {
			return nil, bad
		}
		routes = root
	default:
		return nil, bad
	}
	policy := payload.Policy.TLSReadiness
	if policy == nil || pc.ValidateReadinessProbePolicy(policy) != nil || len(routes) == 0 || len(routes) > policy.MaxProbes {
		return nil, bad
	}
	pin, e := p.Delivery.PartKey(part)
	if e != nil {
		return nil, e
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(policy.ProbeIntervalSeconds)*time.Second)
	defer cancel()
	out := make([]routeartifact.HostedSample, 0, len(routes))
	for _, row := range routes {
		sample := routeartifact.HostedSample{Host: host, Name: host, Member: p.Delivery.Member, Decision: p.Decision, Fence: p.Fence, Part: pin, Content: part.ContentDigest, Transport: mode}
		proof, e := s.probeLocalRouteWithRoots(ctx, row, time.Duration(policy.ProbeTimeoutSeconds)*time.Second, roots, p.Decision, &sample)
		digest, de := routeproof.Digest(row)
		if e != nil || de != nil || proof.Kind != "hosted-route" || proof.Version != p.Decision || proof.Digest != digest || proof.EdgeID != s.Config.EdgeID || proof.GroupID != s.Config.EdgeGroupID || proof.State != s.platformServingRouteProofState(row) || proof.CheckedAt.Before(p.SelectedAt) {
			return nil, bad
		}
		if mode == "https" {
			sample.Path = model.NormalizeAppRoutePathPrefix(row.PathPrefix)
		} else {
			sample.RouteDigest, sample.State = "", ""
			sample.OwnerDigest = owner
		}
		for _, limit := range []time.Time{route.ValidUntil, p.ValidUntil, start.validUntil, sample.ObservedAt.Add(time.Duration(policy.FactFreshnessSeconds) * time.Second)} {
			if limit.Before(sample.ValidUntil) {
				sample.ValidUntil = limit
			}
		}
		out = append(out, sample)
	}
	after, current, e := s.hostedObservationState(route)
	if e != nil || after != start || b.Version != current.Version || !reflect.DeepEqual(b.TrafficRelease, current.TrafficRelease) {
		return nil, bad
	}
	return out, nil
}
