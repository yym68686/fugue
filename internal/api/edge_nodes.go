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
		sample.Method = strings.ToUpper(strings.TrimSpace(sample.Method))
		sample.TrafficClass = normalizeEdgeTrafficClass(sample.TrafficClass)
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

func normalizeEdgeTrafficClass(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "large_body_api", "small_api", "dynamic_api", "static_cacheable", "streaming", "sse", "websocket", "html_dynamic":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ""
	}
}

type edgeNodeQualityAccumulator struct {
	sampleRecordCount         int
	requestCount              int
	errorCount                int
	weightedSampleCount       int
	tlsHandshakeWeighted      int64
	ttfbWeighted              int64
	upstreamWeighted          int64
	totalWeighted             int64
	uploadWeighted            int64
	uploadSampleCount         int
	minUploadBPS              int64
	bodyReadWeighted          int64
	bodyReadSampleCount       int
	maxReadGapWeighted        int64
	maxReadGapSampleCount     int
	bodyIncompleteCount       int
	bodyReadErrorCount        int
	responseEgressWeighted    int64
	responseEgressSamples     int
	responseWriteWeighted     int64
	responseWriteSamples      int
	originDNSWeighted         int64
	originDNSSamples          int
	originConnectWeighted     int64
	originConnectSamples      int
	originWriteWeighted       int64
	originWriteSamples        int
	originWaitWeighted        int64
	originWaitSamples         int
	originTTFBWeighted        int64
	originTTFBSamples         int
	originTotalWeighted       int64
	originTotalSamples        int
	activeRequestsWeighted    int64
	activeBodyWeighted        int64
	saturationSamples         int
	clientTCPRTTWeighted      float64
	clientTCPMinRTTWeighted   float64
	clientTCPRTTVarWeighted   float64
	clientTCPSampleCount      int
	clientTCPTotalRetrans     int64
	clientTCPBytesRetrans     int64
	clientTCPTotalRTO         int64
	clientTCPDeliveryWeighted int64
	clientTCPDeliverySamples  int
	cacheHitCount             int
	cacheObservationCount     int
	lastSampledAt             *time.Time
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
				hostname:     strings.TrimSpace(sample.Hostname),
				pathPrefix:   strings.TrimSpace(sample.PathPrefix),
				method:       strings.TrimSpace(sample.Method),
				trafficClass: strings.TrimSpace(sample.TrafficClass),
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
		if routes[i].PathPrefix != routes[j].PathPrefix {
			return routes[i].PathPrefix < routes[j].PathPrefix
		}
		if routes[i].Method != routes[j].Method {
			return routes[i].Method < routes[j].Method
		}
		return routes[i].TrafficClass < routes[j].TrafficClass
	})
	return model.EdgeNodeQualityResponse{
		Node:  node,
		Group: group,
		Summary: model.EdgeNodeQualitySummary{
			EdgeID:                    edgeID,
			EdgeGroupID:               strings.TrimSpace(node.EdgeGroupID),
			Since:                     since.UTC(),
			SampleRecordCount:         summary.sampleRecordCount,
			RequestCount:              summary.requestCount,
			ErrorCount:                summary.errorCount,
			ErrorRate:                 edgeNodeQualityRate(summary.errorCount, summary.requestCount),
			AvgTLSHandshakeMS:         summary.average(summary.tlsHandshakeWeighted),
			AvgTTFBMS:                 summary.average(summary.ttfbWeighted),
			AvgUpstreamMS:             summary.average(summary.upstreamWeighted),
			AvgTotalMS:                summary.average(summary.totalWeighted),
			AvgUploadBPS:              summary.averageByCount(summary.uploadWeighted, summary.uploadSampleCount),
			MinUploadBPS:              summary.minUploadBPS,
			AvgBodyReadMS:             summary.averageByCount(summary.bodyReadWeighted, summary.bodyReadSampleCount),
			AvgMaxReadGapMS:           summary.averageByCount(summary.maxReadGapWeighted, summary.maxReadGapSampleCount),
			BodyIncompleteCount:       summary.bodyIncompleteCount,
			BodyReadErrorCount:        summary.bodyReadErrorCount,
			AvgResponseEgressBPS:      summary.averageByCount(summary.responseEgressWeighted, summary.responseEgressSamples),
			AvgResponseWriteMS:        summary.averageByCount(summary.responseWriteWeighted, summary.responseWriteSamples),
			AvgOriginDNSMS:            summary.averageByCount(summary.originDNSWeighted, summary.originDNSSamples),
			AvgOriginConnectMS:        summary.averageByCount(summary.originConnectWeighted, summary.originConnectSamples),
			AvgOriginWriteMS:          summary.averageByCount(summary.originWriteWeighted, summary.originWriteSamples),
			AvgOriginWaitMS:           summary.averageByCount(summary.originWaitWeighted, summary.originWaitSamples),
			AvgOriginTTFBMS:           summary.averageByCount(summary.originTTFBWeighted, summary.originTTFBSamples),
			AvgOriginTotalMS:          summary.averageByCount(summary.originTotalWeighted, summary.originTotalSamples),
			AvgActiveRequests:         summary.averageByCount(summary.activeRequestsWeighted, summary.saturationSamples),
			AvgActiveBodyBuffers:      summary.averageByCount(summary.activeBodyWeighted, summary.saturationSamples),
			AvgClientTCPRTTMS:         summary.averageFloatByCount(summary.clientTCPRTTWeighted, summary.clientTCPSampleCount),
			AvgClientTCPMinRTTMS:      summary.averageFloatByCount(summary.clientTCPMinRTTWeighted, summary.clientTCPSampleCount),
			AvgClientTCPRTTVarMS:      summary.averageFloatByCount(summary.clientTCPRTTVarWeighted, summary.clientTCPSampleCount),
			ClientTCPRetransRate:      edgeNodeQualityRate64(summary.clientTCPTotalRetrans, summary.requestCount),
			ClientTCPBytesRetransRate: edgeNodeQualityRate64(summary.clientTCPBytesRetrans, summary.requestCount),
			ClientTCPRTORate:          edgeNodeQualityRate64(summary.clientTCPTotalRTO, summary.requestCount),
			AvgClientTCPDeliveryBPS:   summary.averageByCount(summary.clientTCPDeliveryWeighted, summary.clientTCPDeliverySamples),
			CacheHitCount:             summary.cacheHitCount,
			CacheObservationCount:     summary.cacheObservationCount,
			CacheHitRate:              edgeNodeQualityRate(summary.cacheHitCount, summary.cacheObservationCount),
			TLSStatus:                 strings.TrimSpace(node.TLSStatus),
			TLSLastMessage:            strings.TrimSpace(node.TLSLastMessage),
			TLSReadyAt:                node.TLSReadyAt,
			CacheStatus:               strings.TrimSpace(node.CacheStatus),
			CaddyRouteCount:           node.CaddyRouteCount,
			RouteBundleVersion:        strings.TrimSpace(node.RouteBundleVersion),
			DNSBundleVersion:          strings.TrimSpace(node.DNSBundleVersion),
			LastSampledAt:             summary.lastSampledAt,
		},
		Routes:      routes,
		GeneratedAt: generatedAt.UTC(),
	}
}

type edgeNodeQualityRouteAccumulator struct {
	edgeNodeQualityAccumulator
	hostname     string
	pathPrefix   string
	method       string
	trafficClass string
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
	if sample.UploadEffectiveBPS > 0 {
		a.uploadWeighted += sample.UploadEffectiveBPS * int64(weight)
		a.uploadSampleCount += weight
	}
	if uploadFloor := edgeNodeQualityUploadFloorBPS(sample); uploadFloor > 0 && (a.minUploadBPS == 0 || uploadFloor < a.minUploadBPS) {
		a.minUploadBPS = uploadFloor
	}
	if sample.BodyReadBlockMS > 0 {
		a.bodyReadWeighted += sample.BodyReadBlockMS * int64(weight)
		a.bodyReadSampleCount += weight
	}
	if sample.MaxReadGapMS > 0 {
		a.maxReadGapWeighted += sample.MaxReadGapMS * int64(weight)
		a.maxReadGapSampleCount += weight
	}
	a.bodyIncompleteCount += sample.BodyIncompleteCount
	a.bodyReadErrorCount += sample.BodyReadErrorCount
	if sample.ResponseEgressBPS > 0 {
		a.responseEgressWeighted += sample.ResponseEgressBPS * int64(weight)
		a.responseEgressSamples += weight
	}
	if sample.ResponseWriteMS > 0 {
		a.responseWriteWeighted += sample.ResponseWriteMS * int64(weight)
		a.responseWriteSamples += weight
	}
	if sample.OriginDNSMS > 0 {
		a.originDNSWeighted += sample.OriginDNSMS * int64(weight)
		a.originDNSSamples += weight
	}
	if sample.OriginConnectMS > 0 {
		a.originConnectWeighted += sample.OriginConnectMS * int64(weight)
		a.originConnectSamples += weight
	}
	if sample.OriginRequestWriteMS > 0 {
		a.originWriteWeighted += sample.OriginRequestWriteMS * int64(weight)
		a.originWriteSamples += weight
	}
	if sample.OriginResponseWaitMS > 0 {
		a.originWaitWeighted += sample.OriginResponseWaitMS * int64(weight)
		a.originWaitSamples += weight
	}
	if sample.OriginTTFBMS > 0 {
		a.originTTFBWeighted += sample.OriginTTFBMS * int64(weight)
		a.originTTFBSamples += weight
	}
	if sample.OriginTotalMS > 0 {
		a.originTotalWeighted += sample.OriginTotalMS * int64(weight)
		a.originTotalSamples += weight
	}
	if sample.ActiveRequests > 0 || sample.ActiveBodyBuffers > 0 {
		a.activeRequestsWeighted += int64(sample.ActiveRequests) * int64(weight)
		a.activeBodyWeighted += int64(sample.ActiveBodyBuffers) * int64(weight)
		a.saturationSamples += weight
	}
	if sample.ClientTCPRTTMS > 0 || sample.ClientTCPMinRTTMS > 0 || sample.ClientTCPRTTVarMS > 0 {
		a.clientTCPRTTWeighted += sample.ClientTCPRTTMS * float64(weight)
		a.clientTCPMinRTTWeighted += sample.ClientTCPMinRTTMS * float64(weight)
		a.clientTCPRTTVarWeighted += sample.ClientTCPRTTVarMS * float64(weight)
		a.clientTCPSampleCount += weight
	}
	a.clientTCPTotalRetrans += sample.ClientTCPTotalRetrans
	a.clientTCPBytesRetrans += sample.ClientTCPBytesRetrans
	a.clientTCPTotalRTO += sample.ClientTCPTotalRTO
	if sample.ClientTCPDeliveryBPS > 0 {
		a.clientTCPDeliveryWeighted += sample.ClientTCPDeliveryBPS * int64(weight)
		a.clientTCPDeliverySamples += weight
	}
	a.cacheHitCount += sample.CacheHitCount
	a.cacheObservationCount += sample.CacheObservationCount
	if a.lastSampledAt == nil || sample.SampledAt.After(*a.lastSampledAt) {
		sampledAt := sample.SampledAt.UTC()
		a.lastSampledAt = &sampledAt
	}
}

func edgeNodeQualityUploadFloorBPS(sample model.EdgePerformanceSample) int64 {
	uploadFloor := sample.UploadEffectiveBPS
	if sample.MinWindowBPS > 0 && (uploadFloor <= 0 || sample.MinWindowBPS < uploadFloor) {
		uploadFloor = sample.MinWindowBPS
	}
	return uploadFloor
}

func (a edgeNodeQualityAccumulator) average(weighted int64) float64 {
	if a.weightedSampleCount <= 0 {
		return 0
	}
	return float64(weighted) / float64(a.weightedSampleCount)
}

func (a edgeNodeQualityAccumulator) averageByCount(weighted int64, count int) float64 {
	if count <= 0 {
		return 0
	}
	return float64(weighted) / float64(count)
}

func (a edgeNodeQualityAccumulator) averageFloatByCount(weighted float64, count int) float64 {
	if count <= 0 {
		return 0
	}
	return weighted / float64(count)
}

func (a edgeNodeQualityRouteAccumulator) route() model.EdgeNodeQualityRoute {
	return model.EdgeNodeQualityRoute{
		Hostname:                  a.hostname,
		PathPrefix:                a.pathPrefix,
		Method:                    a.method,
		TrafficClass:              a.trafficClass,
		SampleRecordCount:         a.sampleRecordCount,
		RequestCount:              a.requestCount,
		ErrorCount:                a.errorCount,
		ErrorRate:                 edgeNodeQualityRate(a.errorCount, a.requestCount),
		AvgTLSHandshakeMS:         a.average(a.tlsHandshakeWeighted),
		AvgTTFBMS:                 a.average(a.ttfbWeighted),
		AvgUpstreamMS:             a.average(a.upstreamWeighted),
		AvgTotalMS:                a.average(a.totalWeighted),
		AvgUploadBPS:              a.averageByCount(a.uploadWeighted, a.uploadSampleCount),
		MinUploadBPS:              a.minUploadBPS,
		AvgBodyReadMS:             a.averageByCount(a.bodyReadWeighted, a.bodyReadSampleCount),
		AvgMaxReadGapMS:           a.averageByCount(a.maxReadGapWeighted, a.maxReadGapSampleCount),
		BodyIncompleteCount:       a.bodyIncompleteCount,
		BodyReadErrorCount:        a.bodyReadErrorCount,
		AvgResponseEgressBPS:      a.averageByCount(a.responseEgressWeighted, a.responseEgressSamples),
		AvgResponseWriteMS:        a.averageByCount(a.responseWriteWeighted, a.responseWriteSamples),
		AvgOriginDNSMS:            a.averageByCount(a.originDNSWeighted, a.originDNSSamples),
		AvgOriginConnectMS:        a.averageByCount(a.originConnectWeighted, a.originConnectSamples),
		AvgOriginWriteMS:          a.averageByCount(a.originWriteWeighted, a.originWriteSamples),
		AvgOriginWaitMS:           a.averageByCount(a.originWaitWeighted, a.originWaitSamples),
		AvgOriginTTFBMS:           a.averageByCount(a.originTTFBWeighted, a.originTTFBSamples),
		AvgOriginTotalMS:          a.averageByCount(a.originTotalWeighted, a.originTotalSamples),
		AvgActiveRequests:         a.averageByCount(a.activeRequestsWeighted, a.saturationSamples),
		AvgActiveBodyBuffers:      a.averageByCount(a.activeBodyWeighted, a.saturationSamples),
		AvgClientTCPRTTMS:         a.averageFloatByCount(a.clientTCPRTTWeighted, a.clientTCPSampleCount),
		AvgClientTCPMinRTTMS:      a.averageFloatByCount(a.clientTCPMinRTTWeighted, a.clientTCPSampleCount),
		AvgClientTCPRTTVarMS:      a.averageFloatByCount(a.clientTCPRTTVarWeighted, a.clientTCPSampleCount),
		ClientTCPRetransRate:      edgeNodeQualityRate64(a.clientTCPTotalRetrans, a.requestCount),
		ClientTCPBytesRetransRate: edgeNodeQualityRate64(a.clientTCPBytesRetrans, a.requestCount),
		ClientTCPRTORate:          edgeNodeQualityRate64(a.clientTCPTotalRTO, a.requestCount),
		AvgClientTCPDeliveryBPS:   a.averageByCount(a.clientTCPDeliveryWeighted, a.clientTCPDeliverySamples),
		CacheHitCount:             a.cacheHitCount,
		CacheObservationCount:     a.cacheObservationCount,
		CacheHitRate:              edgeNodeQualityRate(a.cacheHitCount, a.cacheObservationCount),
		LastSampledAt:             a.lastSampledAt,
	}
}

func edgeNodeQualityRate(numerator, denominator int) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func edgeNodeQualityRate64(numerator int64, denominator int) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func edgeNodeQualityRouteKey(sample model.EdgePerformanceSample) string {
	return strings.TrimSpace(sample.Hostname) + "\x00" + strings.TrimSpace(sample.PathPrefix) + "\x00" + strings.TrimSpace(sample.Method) + "\x00" + strings.TrimSpace(sample.TrafficClass)
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
