package api

import (
	"fmt"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
	"net/http"
	"net/netip"
	"slices"
	"strings"
)

func (s *Server) handleGetAppStaticEdgeRouting(w http.ResponseWriter, r *http.Request) {
	app, err := s.store.GetApp(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principalAllowsApp(mustPrincipal(r), app) {
		httpx.WriteError(w, http.StatusForbidden, "app is outside the credential boundary")
		return
	}
	writeAppStaticEdgeRouting(w, app)
}

func writeAppStaticEdgeRouting(w http.ResponseWriter, app model.App) {
	value := map[string]any{"app_id": app.ID, "hostname": "", "revision": model.AppRouteRevision(app.Route)}
	if app.Route != nil {
		value["hostname"] = app.Route.Hostname
		if app.Route.DNSRouting != nil {
			value["routing"] = app.Route.DNSRouting
		}
	}
	httpx.WriteJSON(w, http.StatusOK, value)
}

func (s *Server) handlePutAppStaticEdgeRouting(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required for platform DNS routing")
		return
	}
	var req struct {
		ExpectedRevision string                      `json:"expected_revision"`
		Routing          *model.AppStaticEdgeRouting `json:"routing,omitempty"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	app, err := s.store.GetApp(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if app.Route == nil || model.AppRouteRevision(app.Route) != req.ExpectedRevision {
		s.writeStoreError(w, store.ErrConflict)
		return
	}
	if req.Routing != nil {
		if err = model.ValidateAppStaticEdgeRouting(req.Routing); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if !staticEdgePlatformApp(app, s.appBaseDomain) {
			httpx.WriteError(w, http.StatusBadRequest, "static edge DNS requires an app root route in the platform application namespace")
			return
		}
		for _, c := range req.Routing.Candidates {
			registration, e := s.store.GetStaticEdgeRegistration(c.RegistrationID, app.TenantID, false)
			if e != nil || registration.Status != model.StaticEdgeStatusReady || registration.EdgeID != c.EdgeID {
				httpx.WriteError(w, http.StatusBadRequest, "candidate requires a ready static edge registration in the app tenant")
				return
			}
			for _, address := range c.Addresses {
				ip, _ := netip.ParseAddr(address)
				if !platformconfig.PublicDNSFlattenIP(ip) {
					httpx.WriteError(w, http.StatusBadRequest, "static edge address must be public")
					return
				}
			}
		}
		// Validate the exact desired authority and mixed order against a frozen
		// business projection before saving. Runtime readiness remains independent.
		business, e := s.store.CaptureRouteBusinessSnapshot(r.Context())
		if e != nil {
			s.writeStoreError(w, e)
			return
		}
		for _, other := range business.Apps {
			if other.ID != app.ID && other.Route != nil && other.Route.Hostname == app.Route.Hostname {
				httpx.WriteError(w, http.StatusConflict, "hostname has multiple app routes")
				return
			}
		}
		nodes, _, e := s.store.ListEdgeNodes("")
		if e != nil {
			s.writeStoreError(w, e)
			return
		}
		managed := map[string]bool{}
		for _, node := range nodes {
			managed[node.ID] = true
			for _, c := range req.Routing.Candidates {
				if c.EdgeID == node.ID || slices.Contains(c.Addresses, node.PublicIPv4) || node.PublicIPv6 != "" && slices.Contains(c.Addresses, node.PublicIPv6) {
					httpx.WriteError(w, http.StatusBadRequest, "static candidate overlaps a managed edge identity")
					return
				}
			}
		}
		for _, id := range req.Routing.Availability.OrderedEdgeIDs {
			if !managed[id] && !slices.ContainsFunc(req.Routing.Candidates, func(c model.StaticEdgeDNSCandidate) bool { return c.EdgeID == id }) {
				httpx.WriteError(w, http.StatusBadRequest, "order contains an unknown edge identity")
				return
			}
		}
	}
	app, err = s.store.SetAppStaticEdgeRouting(app.ID, req.ExpectedRevision, req.Routing)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.route.static_edge", "app", app.ID, app.TenantID, map[string]string{"hostname": app.Route.Hostname, "revision": model.AppRouteRevision(app.Route)})
	writeAppStaticEdgeRouting(w, app)
}

func staticEdgePlatformApp(app model.App, base string) bool {
	return app.Route != nil && base != "" && app.Route.Hostname != base && strings.HasSuffix(app.Route.Hostname, "."+base) && model.NormalizeAppRoutePathPrefix(app.Route.PathPrefix) == "/"
}

func projectAppStaticEdgeDNS(result *platformIntentProjectionResponse, business store.RouteBusinessSnapshot, base string) error {
	registrations := map[string]model.StaticEdgeRegistration{}
	for _, reg := range business.StaticEdgeRegistrations {
		registrations[reg.ID] = reg
	}
	for _, app := range business.Apps {
		if app.Route == nil || app.Route.DNSRouting == nil {
			continue
		}
		if !staticEdgePlatformApp(app, base) {
			return fmt.Errorf("static edge DNS app has lost platform root route ownership")
		}
		routing := model.CloneAppStaticEdgeRouting(app.Route.DNSRouting)
		// Revoked or pending metadata grants no new signed endpoint authority.
		revoked := map[string]bool{}
		routing.Candidates = slices.DeleteFunc(routing.Candidates, func(c model.StaticEdgeDNSCandidate) bool {
			reg := registrations[c.RegistrationID]
			valid := reg.TenantID == app.TenantID && reg.EdgeID == c.EdgeID && reg.Status == model.StaticEdgeStatusReady
			revoked[c.EdgeID] = !valid
			return !valid
		})
		if len(routing.Candidates) == 0 {
			continue
		}
		routing.Availability.OrderedEdgeIDs = slices.DeleteFunc(routing.Availability.OrderedEdgeIDs, func(id string) bool { return revoked[id] })
		if err := projectStaticDNSQueryInputs(result, platformconfig.StaticEdgeDNSRoute{AppID: app.ID, TenantID: app.TenantID, Hostname: app.Route.Hostname, Routing: *routing}); err != nil {
			return err
		}
	}
	if len(result.Policy.StaticEdgeRoutes) == 0 {
		return nil
	}
	result.Policy = platformconfig.NormalizePolicySnapshot(result.Policy)
	generation, err := platformconfig.PolicySnapshotGeneration(result.Policy)
	if err != nil {
		return err
	}
	result.Policy.Generation, result.RuntimeSnapshot.PolicyGeneration = generation, generation
	return nil
}

func projectStaticDNSQueryInputs(result *platformIntentProjectionResponse, route platformconfig.StaticEdgeDNSRoute) error {
	p := &result.Policy
	if p.PublicationRole != "" || p.DNSPlacementMode != platformconfig.DNSPlacementConsumerReadiness || p.DNSQueryPolicy == nil || p.DNSQueryPolicy.OrderedProjection == nil {
		return fmt.Errorf("static edge DNS requires a global consumer-readiness producer")
	}
	// Remove any other strategy for exactly this host; configuration for other
	// apps, DNS consumers and families is preserved.
	q := p.DNSQueryPolicy
	q.PhysicalRoutes = slices.DeleteFunc(q.PhysicalRoutes, func(r platformconfig.PhysicalQualityRoute) bool { return r.Hostname == route.Hostname })
	q.AvailabilityRoutes = slices.DeleteFunc(q.AvailabilityRoutes, func(r platformconfig.DNSAvailabilityRoute) bool { return r.Hostname == route.Hostname })
	q.AvailabilityRoutes = append(q.AvailabilityRoutes, platformconfig.DNSAvailabilityRoute{Hostname: route.Hostname, Policy: route.Routing.Availability})
	p.StaticEdgeRoutes = append(p.StaticEdgeRoutes, route)
	matched := 0
	for i := range p.DNSAnswerRules {
		rule := &p.DNSAnswerRules[i]
		if rule.Hostname != route.Hostname {
			continue
		}
		matched++
		key := rule.NodeID + "\x00" + rule.Hostname + "\x00" + rule.Type
		q.OrderedProjection.Overrides = slices.DeleteFunc(q.OrderedProjection.Overrides, func(o platformconfig.DNSOrderOverride) bool { return o.NodeID+"\x00"+o.Hostname+"\x00"+o.Type == key })
		order := model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: slices.Clone(route.Routing.Availability.OrderedEdgeIDs)}
		q.OrderedProjection.Overrides = append(q.OrderedProjection.Overrides, platformconfig.DNSOrderOverride{NodeID: rule.NodeID, Hostname: rule.Hostname, Type: rule.Type, Order: order})
		*rule = platformconfig.DNSAnswerRule{NodeID: rule.NodeID, Hostname: rule.Hostname, Type: rule.Type, TTLSeconds: rule.TTLSeconds, SelectionMode: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: &model.DNSPhysicalOrder{Version: order.Version}}
		for j := range result.RuntimeSnapshot.DNSSelections {
			fact := &result.RuntimeSnapshot.DNSSelections[j]
			if fact.NodeID+"\x00"+fact.Hostname+"\x00"+fact.Type != key {
				continue
			}
			candidates := slices.DeleteFunc(slices.Clone(fact.Candidates), func(c platformconfig.DNSSelectionCandidate) bool {
				return !slices.Contains(order.OrderedEdgeIDs, c.EdgeID)
			})
			for _, c := range route.Routing.Candidates {
				for _, address := range c.Addresses {
					if (fact.Type == "AAAA") == strings.Contains(address, ":") {
						candidates = append(candidates, platformconfig.DNSSelectionCandidate{IP: address, EdgeID: c.EdgeID, EdgeGroupID: platformconfig.StaticEdgeDNSGroup(c.EdgeID)})
					}
				}
			}
			for _, id := range order.OrderedEdgeIDs {
				if slices.ContainsFunc(candidates, func(c platformconfig.DNSSelectionCandidate) bool { return c.EdgeID == id }) {
					rule.PhysicalOrder.OrderedEdgeIDs = append(rule.PhysicalOrder.OrderedEdgeIDs, id)
				}
			}
			*fact = platformconfig.DNSSelectionObservation{NodeID: fact.NodeID, Hostname: fact.Hostname, Type: fact.Type, ObservedAt: fact.ObservedAt, Candidates: candidates, Reason: "explicit_independent_static_edge_order"}
			digest, err := platformconfig.Digest(fact)
			if err != nil {
				return err
			}
			fact.SourceDigest, fact.SourceGeneration = digest, "dns-observation_"+strings.TrimPrefix(digest, "sha256:")
		}
	}
	if matched == 0 {
		return fmt.Errorf("static edge hostname has no managed DNS query fallback")
	}
	return nil
}
