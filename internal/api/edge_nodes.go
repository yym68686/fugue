package api

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type createEdgeNodeTokenRequest struct {
	EdgeGroupID    string `json:"edge_group_id"`
	Region         string `json:"region,omitempty"`
	Country        string `json:"country,omitempty"`
	PublicHostname string `json:"public_hostname,omitempty"`
	PublicIPv4     string `json:"public_ipv4,omitempty"`
	PublicIPv6     string `json:"public_ipv6,omitempty"`
	MeshIP         string `json:"mesh_ip,omitempty"`
	Draining       bool   `json:"draining,omitempty"`
}

type edgeHeartbeatRequest struct {
	EdgeID                 string                        `json:"edge_id"`
	EdgeGroupID            string                        `json:"edge_group_id"`
	Region                 string                        `json:"region,omitempty"`
	Country                string                        `json:"country,omitempty"`
	PublicHostname         string                        `json:"public_hostname,omitempty"`
	PublicIPv4             string                        `json:"public_ipv4,omitempty"`
	PublicIPv6             string                        `json:"public_ipv6,omitempty"`
	MeshIP                 string                        `json:"mesh_ip,omitempty"`
	RouteBundleVersion     string                        `json:"route_bundle_version,omitempty"`
	DNSBundleVersion       string                        `json:"dns_bundle_version,omitempty"`
	ServingGeneration      string                        `json:"serving_generation,omitempty"`
	LKGGeneration          string                        `json:"lkg_generation,omitempty"`
	LastGoodGeneration     string                        `json:"last_good_generation,omitempty"`
	CacheCorruptGeneration string                        `json:"cache_corrupt_generation,omitempty"`
	CaddyRouteCount        int                           `json:"caddy_route_count,omitempty"`
	CaddyAppliedVersion    string                        `json:"caddy_applied_version,omitempty"`
	CaddyLastError         string                        `json:"caddy_last_error,omitempty"`
	CacheStatus            string                        `json:"cache_status,omitempty"`
	TLSStatus              string                        `json:"tls_status,omitempty"`
	TLSLastMessage         string                        `json:"tls_last_message,omitempty"`
	TLSReadyAt             *time.Time                    `json:"tls_ready_at,omitempty"`
	MaxStaleExceeded       bool                          `json:"max_stale_exceeded,omitempty"`
	Status                 string                        `json:"status"`
	Healthy                bool                          `json:"healthy"`
	Draining               bool                          `json:"draining"`
	LastError              string                        `json:"last_error,omitempty"`
	PerformanceSamples     []model.EdgePerformanceSample `json:"performance_samples,omitempty"`
}

func (s *Server) handleListEdgeNodes(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect edge nodes")
		return
	}
	nodes, groups, err := s.store.ListEdgeNodes(r.URL.Query().Get("edge_group_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if nodePolicies, err := s.loadClusterNodePolicyStatuses(r.Context(), principal); err == nil {
		nodes = activeEdgeNodesForPolicy(nodes, nodePolicies)
	} else if s.log != nil {
		s.log.Printf("edge node inventory continuing without node policy filter: %v", err)
	}
	nodes = freshEdgeNodes(nodes, time.Now().UTC())
	groups = activeEdgeGroupsForInventory(groups, nodes, nil)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"nodes":  nodes,
		"groups": groups,
	})
}

func (s *Server) handleGetEdgeNode(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect edge nodes")
		return
	}
	node, group, err := s.store.GetEdgeNode(r.PathValue("edge_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"node":  node,
		"group": group,
	})
}

func (s *Server) handleGetEdgeNodeQuality(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect edge node quality")
		return
	}
	edgeID := strings.TrimSpace(strings.ToLower(r.PathValue("edge_id")))
	if edgeID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "edge_id is required")
		return
	}
	now := time.Now().UTC()
	since, err := parseEdgeNodeQualitySince(r.URL.Query().Get("since"), now)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	node, group, err := s.store.GetEdgeNode(edgeID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	samples, err := s.store.ListEdgePerformanceSamples("", since)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	response := buildEdgeNodeQualityResponse(node, group, samples, since, now)
	httpx.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) handleCreateEdgeNodeToken(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can create edge node tokens")
		return
	}
	edgeID := strings.TrimSpace(r.PathValue("edge_id"))
	if edgeID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "edge_id is required")
		return
	}
	var req createEdgeNodeTokenRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	node, token, err := s.store.CreateEdgeNodeToken(model.EdgeNode{
		ID:             edgeID,
		EdgeGroupID:    req.EdgeGroupID,
		Region:         req.Region,
		Country:        req.Country,
		PublicHostname: req.PublicHostname,
		PublicIPv4:     req.PublicIPv4,
		PublicIPv6:     req.PublicIPv6,
		MeshIP:         req.MeshIP,
		Status:         model.EdgeHealthUnknown,
		Draining:       req.Draining,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "edge.node.token.create", "edge_node", node.ID, "", map[string]string{
		"edge_id":       node.ID,
		"edge_group_id": node.EdgeGroupID,
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{
		"node":  node,
		"token": token,
	})
}

func (s *Server) handleEdgeHeartbeat(w http.ResponseWriter, r *http.Request) {
	authContext, ok := s.authorizeEdgeRequest(w, r)
	if !ok {
		return
	}
	now := time.Now().UTC()
	var req edgeHeartbeatRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := authContext.constrain(&req.EdgeID, &req.EdgeGroupID); err != nil {
		httpx.WriteError(w, http.StatusForbidden, err.Error())
		return
	}
	status := model.NormalizeEdgeHealthStatus(req.Status)
	if status == "" {
		httpx.WriteError(w, http.StatusBadRequest, "status must be unknown, healthy, degraded, or unhealthy")
		return
	}
	req = s.enrichEdgeHeartbeatFromClusterNode(r.Context(), req)
	node, _, err := s.store.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:                  req.EdgeID,
		EdgeGroupID:         req.EdgeGroupID,
		Region:              req.Region,
		Country:             req.Country,
		PublicHostname:      req.PublicHostname,
		PublicIPv4:          req.PublicIPv4,
		PublicIPv6:          req.PublicIPv6,
		MeshIP:              req.MeshIP,
		RouteBundleVersion:  req.RouteBundleVersion,
		DNSBundleVersion:    req.DNSBundleVersion,
		ServingGeneration:   req.ServingGeneration,
		LKGGeneration:       req.LKGGeneration,
		CaddyRouteCount:     req.CaddyRouteCount,
		CaddyAppliedVersion: req.CaddyAppliedVersion,
		CaddyLastError:      req.CaddyLastError,
		CacheStatus:         req.CacheStatus,
		TLSStatus:           req.TLSStatus,
		TLSLastMessage:      req.TLSLastMessage,
		TLSReadyAt:          req.TLSReadyAt,
		Status:              status,
		Healthy:             req.Healthy,
		Draining:            req.Draining,
		LastError:           req.LastError,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if samples := sanitizeEdgeHeartbeatPerformanceSamples(req, now); len(samples) > 0 {
		if err := s.store.RecordEdgePerformanceSamples(samples, now.Add(-edgePerformanceSampleRetention)); err != nil && s.log != nil {
			s.log.Printf("edge performance sample ingest failed; edge_id=%s edge_group_id=%s error=%v", req.EdgeID, req.EdgeGroupID, err)
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"node":     node,
		"accepted": true,
	})
}

const edgePerformanceSampleRetention = 7 * 24 * time.Hour
const edgeNodeQualityDefaultWindow = 24 * time.Hour

func sanitizeEdgeHeartbeatPerformanceSamples(req edgeHeartbeatRequest, now time.Time) []model.EdgePerformanceSample {
	if len(req.PerformanceSamples) == 0 {
		return nil
	}
	out := make([]model.EdgePerformanceSample, 0, len(req.PerformanceSamples))
	for _, sample := range req.PerformanceSamples {
		sample.ID = strings.TrimSpace(sample.ID)
		sample.EdgeID = strings.TrimSpace(req.EdgeID)
		sample.EdgeGroupID = strings.TrimSpace(req.EdgeGroupID)
		sample.Hostname = normalizeExternalAppDomain(sample.Hostname)
		sample.PathPrefix = model.NormalizeAppRoutePathPrefix(sample.PathPrefix)
		sample.ClientCountry = strings.ToLower(strings.TrimSpace(sample.ClientCountry))
		sample.ClientRegion = strings.TrimSpace(sample.ClientRegion)
		sample.ClientASN = strings.TrimSpace(sample.ClientASN)
		sample.RuntimeRegion = strings.TrimSpace(sample.RuntimeRegion)
		sample.RouteGeneration = strings.TrimSpace(sample.RouteGeneration)
		sample.CacheStatus = strings.ToLower(strings.TrimSpace(sample.CacheStatus))
		sample.DNSPolicy = strings.ToLower(strings.TrimSpace(sample.DNSPolicy))
		if sample.ID == "" {
			sample.ID = model.NewID("edge_perf")
		}
		if sample.SampledAt.IsZero() {
			sample.SampledAt = now
		} else if sample.SampledAt.After(now.Add(5 * time.Minute)) {
			sample.SampledAt = now
		}
		out = append(out, sample)
	}
	return out
}

func parseEdgeNodeQualitySince(raw string, now time.Time) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return now.Add(-edgeNodeQualityDefaultWindow), nil
	}
	if duration, err := time.ParseDuration(raw); err == nil {
		if duration <= 0 {
			return time.Time{}, errors.New("since duration must be positive")
		}
		return now.Add(-duration), nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, errors.New("since must be a positive duration such as 24h or an RFC3339 timestamp")
	}
	return parsed.UTC(), nil
}

type edgeNodeQualityAccumulator struct {
	sampleRecordCount     int
	requestCount          int
	errorCount            int
	weightedSampleCount   int
	tlsHandshakeWeighted  int64
	ttfbWeighted          int64
	upstreamWeighted      int64
	totalWeighted         int64
	cacheHitCount         int
	cacheObservationCount int
	lastSampledAt         *time.Time
}

func buildEdgeNodeQualityResponse(node model.EdgeNode, group model.EdgeGroup, samples []model.EdgePerformanceSample, since, generatedAt time.Time) model.EdgeNodeQualityResponse {
	edgeID := strings.TrimSpace(strings.ToLower(node.ID))
	summary := edgeNodeQualityAccumulator{}
	routesByKey := map[string]*edgeNodeQualityRouteAccumulator{}
	for _, sample := range samples {
		if !strings.EqualFold(strings.TrimSpace(sample.EdgeID), edgeID) {
			continue
		}
		summary.add(sample)
		key := edgeNodeQualityRouteKey(sample)
		accumulator := routesByKey[key]
		if accumulator == nil {
			accumulator = &edgeNodeQualityRouteAccumulator{
				hostname:   strings.TrimSpace(sample.Hostname),
				pathPrefix: strings.TrimSpace(sample.PathPrefix),
			}
			routesByKey[key] = accumulator
		}
		accumulator.add(sample)
	}
	routes := make([]model.EdgeNodeQualityRoute, 0, len(routesByKey))
	for _, accumulator := range routesByKey {
		routes = append(routes, accumulator.route())
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Hostname != routes[j].Hostname {
			return routes[i].Hostname < routes[j].Hostname
		}
		return routes[i].PathPrefix < routes[j].PathPrefix
	})
	return model.EdgeNodeQualityResponse{
		Node:  node,
		Group: group,
		Summary: model.EdgeNodeQualitySummary{
			EdgeID:                edgeID,
			EdgeGroupID:           strings.TrimSpace(node.EdgeGroupID),
			Since:                 since.UTC(),
			SampleRecordCount:     summary.sampleRecordCount,
			RequestCount:          summary.requestCount,
			ErrorCount:            summary.errorCount,
			ErrorRate:             edgeNodeQualityRate(summary.errorCount, summary.requestCount),
			AvgTLSHandshakeMS:     summary.average(summary.tlsHandshakeWeighted),
			AvgTTFBMS:             summary.average(summary.ttfbWeighted),
			AvgUpstreamMS:         summary.average(summary.upstreamWeighted),
			AvgTotalMS:            summary.average(summary.totalWeighted),
			CacheHitCount:         summary.cacheHitCount,
			CacheObservationCount: summary.cacheObservationCount,
			CacheHitRate:          edgeNodeQualityRate(summary.cacheHitCount, summary.cacheObservationCount),
			TLSStatus:             strings.TrimSpace(node.TLSStatus),
			TLSLastMessage:        strings.TrimSpace(node.TLSLastMessage),
			TLSReadyAt:            node.TLSReadyAt,
			CacheStatus:           strings.TrimSpace(node.CacheStatus),
			CaddyRouteCount:       node.CaddyRouteCount,
			RouteBundleVersion:    strings.TrimSpace(node.RouteBundleVersion),
			DNSBundleVersion:      strings.TrimSpace(node.DNSBundleVersion),
			LastSampledAt:         summary.lastSampledAt,
		},
		Routes:      routes,
		GeneratedAt: generatedAt.UTC(),
	}
}

type edgeNodeQualityRouteAccumulator struct {
	edgeNodeQualityAccumulator
	hostname   string
	pathPrefix string
}

func (a *edgeNodeQualityAccumulator) add(sample model.EdgePerformanceSample) {
	weight := sample.SampleCount
	if weight <= 0 {
		weight = 1
	}
	a.sampleRecordCount++
	a.requestCount += weight
	a.errorCount += sample.ErrorCount
	a.weightedSampleCount += weight
	a.tlsHandshakeWeighted += sample.TLSHandshakeMS * int64(weight)
	a.ttfbWeighted += sample.TTFBMS * int64(weight)
	a.upstreamWeighted += sample.UpstreamMS * int64(weight)
	a.totalWeighted += sample.TotalMS * int64(weight)
	a.cacheHitCount += sample.CacheHitCount
	a.cacheObservationCount += sample.CacheObservationCount
	if a.lastSampledAt == nil || sample.SampledAt.After(*a.lastSampledAt) {
		sampledAt := sample.SampledAt.UTC()
		a.lastSampledAt = &sampledAt
	}
}

func (a edgeNodeQualityAccumulator) average(weighted int64) float64 {
	if a.weightedSampleCount <= 0 {
		return 0
	}
	return float64(weighted) / float64(a.weightedSampleCount)
}

func (a edgeNodeQualityRouteAccumulator) route() model.EdgeNodeQualityRoute {
	return model.EdgeNodeQualityRoute{
		Hostname:              a.hostname,
		PathPrefix:            a.pathPrefix,
		SampleRecordCount:     a.sampleRecordCount,
		RequestCount:          a.requestCount,
		ErrorCount:            a.errorCount,
		ErrorRate:             edgeNodeQualityRate(a.errorCount, a.requestCount),
		AvgTLSHandshakeMS:     a.average(a.tlsHandshakeWeighted),
		AvgTTFBMS:             a.average(a.ttfbWeighted),
		AvgUpstreamMS:         a.average(a.upstreamWeighted),
		AvgTotalMS:            a.average(a.totalWeighted),
		CacheHitCount:         a.cacheHitCount,
		CacheObservationCount: a.cacheObservationCount,
		CacheHitRate:          edgeNodeQualityRate(a.cacheHitCount, a.cacheObservationCount),
		LastSampledAt:         a.lastSampledAt,
	}
}

func edgeNodeQualityRate(numerator, denominator int) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func edgeNodeQualityRouteKey(sample model.EdgePerformanceSample) string {
	return strings.TrimSpace(sample.Hostname) + "\x00" + strings.TrimSpace(sample.PathPrefix)
}

func (s *Server) enrichEdgeHeartbeatFromClusterNode(ctx context.Context, req edgeHeartbeatRequest) edgeHeartbeatRequest {
	endpoint := s.discoverClusterNodeEndpoint(ctx, req.EdgeID)
	if strings.TrimSpace(req.Region) == "" {
		req.Region = endpoint.Region
	}
	if strings.TrimSpace(req.Country) == "" {
		req.Country = endpoint.Country
	}
	if strings.TrimSpace(req.PublicIPv4) == "" {
		req.PublicIPv4 = endpoint.PublicIPv4
	}
	if strings.TrimSpace(req.PublicIPv6) == "" {
		req.PublicIPv6 = endpoint.PublicIPv6
	}
	if strings.TrimSpace(req.MeshIP) == "" {
		req.MeshIP = endpoint.MeshIP
	}
	return req
}

type discoveredClusterNodeEndpoint struct {
	Region     string
	Country    string
	PublicIPv4 string
	PublicIPv6 string
	MeshIP     string
}

func (s *Server) discoverClusterNodeEndpoint(ctx context.Context, nodeName string) discoveredClusterNodeEndpoint {
	nodeName = strings.TrimSpace(nodeName)
	if s == nil || nodeName == "" {
		return discoveredClusterNodeEndpoint{}
	}
	snapshots, err := s.loadClusterNodeInventory(ctx)
	if err != nil {
		if s.log != nil {
			s.log.Printf("edge node endpoint discovery skipped; node=%s error=%v", nodeName, err)
		}
		return discoveredClusterNodeEndpoint{}
	}
	for _, snapshot := range snapshots {
		if !strings.EqualFold(strings.TrimSpace(snapshot.node.Name), nodeName) {
			continue
		}
		var out discoveredClusterNodeEndpoint
		out.Region = strings.TrimSpace(snapshot.node.Region)
		out.Country = strings.ToLower(strings.TrimSpace(snapshot.countryCode))
		out.MeshIP = strings.TrimSpace(snapshot.node.InternalIP)
		for _, value := range []string{snapshot.node.PublicIP, snapshot.node.ExternalIP} {
			ipValue := publicIPLiteral(value)
			if ipValue == "" {
				continue
			}
			ip := net.ParseIP(ipValue)
			if ip == nil {
				continue
			}
			if ip.To4() != nil {
				if out.PublicIPv4 == "" {
					out.PublicIPv4 = ipValue
				}
			} else if out.PublicIPv6 == "" {
				out.PublicIPv6 = ipValue
			}
		}
		return out
	}
	return discoveredClusterNodeEndpoint{}
}

type edgeAuthContext struct {
	EdgeID      string
	EdgeGroupID string
	Scoped      bool
}

func (ctx edgeAuthContext) constrain(edgeID *string, edgeGroupID *string) error {
	if !ctx.Scoped {
		return nil
	}
	if strings.TrimSpace(*edgeID) == "" {
		*edgeID = ctx.EdgeID
	}
	if strings.TrimSpace(*edgeGroupID) == "" {
		*edgeGroupID = ctx.EdgeGroupID
	}
	if !strings.EqualFold(strings.TrimSpace(*edgeGroupID), ctx.EdgeGroupID) {
		return errors.New("edge token cannot access another edge_group_id")
	}
	return nil
}

func (s *Server) authorizeEdgeRequest(w http.ResponseWriter, r *http.Request) (edgeAuthContext, bool) {
	token := edgeTokenFromRequest(r)
	if token == "" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return edgeAuthContext{}, false
	}
	if s.store != nil {
		node, err := s.store.AuthenticateEdgeNode(token)
		if err == nil {
			return edgeAuthContext{
				EdgeID:      node.ID,
				EdgeGroupID: node.EdgeGroupID,
				Scoped:      true,
			}, true
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrInvalidInput) {
			s.writeStoreError(w, err)
			return edgeAuthContext{}, false
		}
	}
	if s.allowLegacyEdgeToken && strings.TrimSpace(s.edgeTLSAskToken) != "" && subtleConstantCompare(token, s.edgeTLSAskToken) {
		return edgeAuthContext{}, true
	}
	http.Error(w, "forbidden", http.StatusForbidden)
	return edgeAuthContext{}, false
}

func edgeTokenFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	if token := strings.TrimSpace(r.URL.Query().Get("token")); token != "" {
		return token
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	parts := strings.SplitN(authz, " ", 2)
	if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
		return strings.TrimSpace(parts[1])
	}
	return ""
}
