package api

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/routeartifact"
)

var errDiscoveryRoutesUnavailable = errors.New("verified discovery platform routes unavailable; retain current discovery")

// Public bootstrap metadata may summarize only the static platform entry names
// explicitly pinned to the verified traffic baseline. The full traffic graph
// also contains tenant routes and cannot be exposed as this summary.
func (s *Server) publishedDiscoveryPlatformRoutes() ([]model.PlatformRoute, error) {
	lkg, err := s.store.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil {
		return nil, errDiscoveryRoutesUnavailable
	}
	if lkg == nil {
		return []model.PlatformRoute{}, nil
	}
	read := newConsumerArtifactReader(s.store.GetPlatformArtifact)
	parent, err := read(lkg.ArtifactID)
	if err != nil || parent.ArtifactKind != model.PlatformArtifactKindReleaseSet || parent.ScopeKey != "global" || parent.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluatePlatformLKGSnapshot(*lkg, parent, s.bundleKeyring(), time.Now().UTC()).Pass || !validateReleaseSetReferences(parent, read).Pass {
		return nil, errDiscoveryRoutesUnavailable
	}
	var child model.PlatformArtifact
	for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig} {
		member, err := consumerAssignmentChild(parent, kind, read)
		if err != nil || member.ScopeKey != parent.ScopeKey || !platformsafety.EvaluateArtifactIntegrity(member, s.bundleKeyring()).Pass || !reflect.DeepEqual(platformconfig.LineageFromArtifact(parent), platformconfig.LineageFromArtifact(member)) {
			return nil, errDiscoveryRoutesUnavailable
		}
		if kind == model.PlatformArtifactKindEdgeRouteBundle {
			child = member
		}
	}
	staticID, digest := parent.Metadata[platformproducer.StaticIntentIDMetadata], parent.Metadata[platformproducer.StaticIntentDigestMetadata]
	if staticID == "" || !platformproducer.ValidDigest(digest) {
		return nil, errDiscoveryRoutesUnavailable
	}
	base, err := s.loadStaticPlatformIntent(staticID, digest)
	if err != nil {
		return nil, errDiscoveryRoutesUnavailable
	}
	out, err := discoveryPlatformRouteSummary(child, base.Routes)
	if err != nil {
		return nil, errDiscoveryRoutesUnavailable
	}
	current, err := s.store.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || current == nil || current.ArtifactID != lkg.ArtifactID || current.ContentHash != lkg.ContentHash || current.VerifiedByReleaseID != lkg.VerifiedByReleaseID {
		return nil, errDiscoveryRoutesUnavailable
	}
	return out, nil
}

func discoveryPlatformRouteSummary(child model.PlatformArtifact, declared []model.PlatformRoute) ([]model.PlatformRoute, error) {
	// Reuse the executor's typed route validation before narrowing the public
	// view. Decode compiled fields to retain their original group mode and TTL.
	if _, err := routeartifact.Project(child); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(child.Content["routes"])
	if err != nil {
		return nil, err
	}
	var routes []platformconfig.CompiledRoute
	if err = json.Unmarshal(raw, &routes); err != nil {
		return nil, err
	}
	allowed := map[string]bool{}
	for _, r := range declared {
		allowed[r.Hostname] = true
	}
	out := []model.PlatformRoute{}
	for _, r := range routes {
		if !allowed[r.Hostname] || model.NormalizeAppRoutePathPrefix(r.PathPrefix) != "/" {
			continue
		}
		// An explicit business binding can own a former platform entry. Such
		// ownership never grants the anonymous bootstrap endpoint its origin.
		if r.AppID != "" || r.TenantID != "" || r.RuntimeID != "" || r.OriginRef != "" {
			continue
		}
		if len(r.Upstreams) > 0 {
			return nil, errors.New("static platform entry cannot summarize weighted targets")
		}
		status := r.Status
		if !r.Enabled {
			status = model.EdgeRouteStatusDisabled
		} else if status == "" {
			status = model.EdgeRouteStatusActive
		}
		out = append(out, model.PlatformRoute{Hostname: r.Hostname, Kind: r.Kind, UpstreamKind: r.UpstreamKind, UpstreamScope: r.UpstreamScope, UpstreamURL: r.UpstreamURL, TLSPolicy: r.TLSPolicy, RoutePolicy: r.RoutePolicy, EdgeGroupMode: r.EdgeGroupMode, EdgeGroupID: r.EdgeGroupID, Status: status, StatusReason: r.StatusReason, TTL: r.TTL})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hostname < out[j].Hostname })
	return out, nil
}
