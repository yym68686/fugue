package api

import (
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleGetEdgeQualityRank(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect edge quality rank")
		return
	}
	hostname := normalizeExternalAppDomain(r.PathValue("hostname"))
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	now := time.Now().UTC()
	window, since, err := parseEdgeQualityRankWindow(r.URL.Query().Get("window"), r.URL.Query().Get("since"), now)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	scope, err := parseEdgeQualityRankScope(r.URL.Query().Get("scope"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	query := edgeQualityRankQuery{
		Hostname:         hostname,
		TrafficClass:     normalizeEdgeTrafficClass(r.URL.Query().Get("traffic_class")),
		Method:           strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("method"))),
		PathPrefixBucket: edgeQualityPathPrefixBucket(r.URL.Query().Get("path_prefix")),
		RequestedScope:   scope,
		Window:           window,
		Since:            since,
		GeneratedAt:      now,
	}
	nodes, _, err := s.store.ListEdgeNodes("")
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	var policy model.EdgeRoutePolicy
	if loaded, err := s.store.GetEdgeRoutePolicy(hostname); err == nil {
		policy = loaded
	} else if !errors.Is(err, store.ErrNotFound) {
		s.writeStoreError(w, err)
		return
	}
	var response model.EdgeQualityRankResponse
	if rollupWindow := normalizedEdgeQualityRankWindow(window); rollupWindow != "" {
		rollups, err := s.store.ListEdgeQualityRollups("", rollupWindow, since)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if len(rollups) > 0 {
			response = buildEdgeQualityRankResponseFromRollups(query, nodes, policy, rollups)
		}
	}
	if response.Hostname == "" {
		samples, err := s.store.ListEdgePerformanceSamples(hostname, since)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		response = buildEdgeQualityRankResponse(query, nodes, policy, samples)
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

type edgeQualityRankQuery struct {
	Hostname         string
	TrafficClass     string
	Method           string
	PathPrefixBucket string
	RequestedScope   edgeQualityRankScope
	Window           string
	Since            time.Time
	GeneratedAt      time.Time
}

type edgeQualityRankScope struct {
	Kind    string
	Value   string
	Country string
	Region  string
	ASN     string
}

type edgeQualityRankFilter struct {
	TrafficClass     string
	Method           string
	PathPrefixBucket string
}

func buildEdgeQualityRankResponse(query edgeQualityRankQuery, nodes []model.EdgeNode, policy model.EdgeRoutePolicy, samples []model.EdgePerformanceSample) model.EdgeQualityRankResponse {
	filters := edgeQualityRankFallbackFilters(query)
	scopes := edgeQualityRankFallbackScopes(query.RequestedScope)
	selectedScope := edgeQualityRankScope{Kind: "global", Value: "global"}
	selectedFilter := edgeQualityRankFilter{}
	selectedSamples := []model.EdgePerformanceSample{}
	fallbackLevel := 0
	fallbackReason := ""
	for filterIndex, filter := range filters {
		for scopeIndex, scope := range scopes {
			matched := edgeQualityFilterSamples(samples, filter, scope)
			if len(matched) == 0 {
				continue
			}
			selectedScope = scope
			selectedFilter = filter
			selectedSamples = matched
			fallbackLevel = filterIndex*len(scopes) + scopeIndex
			if fallbackLevel > 0 {
				fallbackReason = "insufficient samples for more specific filter or scope"
			}
			break
		}
		if len(selectedSamples) > 0 {
			break
		}
	}

	groups := make(map[string]*edgeDNSLatencyGroupAccumulator)
	for _, sample := range selectedSamples {
		edgeDNSLatencyAccumulate(groups, strings.TrimSpace(sample.EdgeGroupID), sample)
	}
	groupCandidates := make(map[string]edgeDNSLatencyCandidateProfile, len(groups))
	nodeCandidates := make(map[string]edgeDNSLatencyCandidateProfile)
	for edgeGroupID, accumulator := range groups {
		if accumulator == nil {
			continue
		}
		groupCandidate := edgeDNSLatencyCandidateFromAccumulator(accumulator)
		groupCandidates[edgeGroupID] = groupCandidate
		for edgeID, nodeAccumulator := range accumulator.NodeAccumulators {
			if nodeAccumulator == nil {
				continue
			}
			nodeCandidates[edgeID] = edgeDNSLatencyCandidateFromAccumulator(nodeAccumulator)
		}
	}

	candidates := make([]model.EdgeQualityRankCandidate, 0, len(nodes))
	hardGated := make([]model.EdgeQualityRankCandidate, 0)
	for _, node := range nodes {
		candidate := edgeQualityRankCandidateForNode(node, policy, query.GeneratedAt)
		if candidate.Excluded || !candidate.Healthy || candidate.Draining || !candidate.RouteReady || !candidate.TLSReady {
			candidate.Reason = firstNonEmpty(candidate.ExclusionReason, edgeQualityRankGateReason(candidate))
			hardGated = append(hardGated, candidate)
			continue
		}
		profile, ok := nodeCandidates[strings.TrimSpace(node.ID)]
		if !ok {
			profile, ok = groupCandidates[strings.TrimSpace(node.EdgeGroupID)]
		}
		if ok {
			edgeQualityApplyProfile(&candidate, profile)
		} else {
			candidate.Score = 999999
			candidate.ConfidencePenalty = 250
			candidate.ScoreBreakdown = map[string]float64{"confidence": 250}
			candidate.Reason = "no matching samples for selected scope"
		}
		candidates = append(candidates, candidate)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score < candidates[j].Score
		}
		if candidates[i].Confidence != candidates[j].Confidence {
			return candidates[i].Confidence > candidates[j].Confidence
		}
		if candidates[i].EdgeGroupID != candidates[j].EdgeGroupID {
			return candidates[i].EdgeGroupID < candidates[j].EdgeGroupID
		}
		return candidates[i].EdgeID < candidates[j].EdgeID
	})
	for index := range candidates {
		candidates[index].Rank = index + 1
	}

	return model.EdgeQualityRankResponse{
		Hostname:         query.Hostname,
		TrafficClass:     selectedFilter.TrafficClass,
		Method:           selectedFilter.Method,
		PathPrefixBucket: selectedFilter.PathPrefixBucket,
		RequestedScope:   query.RequestedScope.key(),
		SelectedScope:    selectedScope.key(),
		FallbackLevel:    fallbackLevel,
		FallbackReason:   fallbackReason,
		Window:           query.Window,
		Since:            query.Since,
		GeneratedAt:      query.GeneratedAt,
		Candidates:       candidates,
		HardGated:        hardGated,
	}
}

func buildEdgeQualityRankResponseFromRollups(query edgeQualityRankQuery, nodes []model.EdgeNode, policy model.EdgeRoutePolicy, rollups []model.EdgeQualityRollup) model.EdgeQualityRankResponse {
	filters := edgeQualityRankFallbackFilters(query)
	scopes := edgeQualityRankFallbackScopes(query.RequestedScope)
	hostnames := edgeQualityRankFallbackHostnames(query.Hostname)
	selectedScope := edgeQualityRankScope{Kind: "global", Value: "global"}
	selectedFilter := edgeQualityRankFilter{}
	selectedRollups := []model.EdgeQualityRollup{}
	fallbackLevel := 0
	fallbackReason := ""
	for hostnameIndex, hostname := range hostnames {
		for filterIndex, filter := range filters {
			for scopeIndex, scope := range scopes {
				matched := edgeQualityFilterRollups(rollups, hostname, filter, scope)
				if len(matched) == 0 {
					continue
				}
				selectedScope = scope
				selectedFilter = filter
				selectedRollups = latestEdgeQualityRollups(matched)
				fallbackLevel = hostnameIndex*len(filters)*len(scopes) + filterIndex*len(scopes) + scopeIndex
				switch {
				case hostname == edgeQualityPlatformRollupHostname:
					fallbackReason = "insufficient hostname rollups; using platform global rollup"
				case fallbackLevel > 0:
					fallbackReason = "insufficient rollups for more specific filter or scope"
				default:
					fallbackReason = ""
				}
				break
			}
			if len(selectedRollups) > 0 {
				break
			}
		}
		if len(selectedRollups) > 0 {
			break
		}
	}

	groupRollups := make(map[string]model.EdgeQualityRollup)
	nodeRollups := make(map[string]model.EdgeQualityRollup)
	for _, rollup := range selectedRollups {
		if strings.TrimSpace(rollup.EdgeID) != "" {
			nodeRollups[strings.TrimSpace(rollup.EdgeID)] = rollup
			continue
		}
		groupRollups[strings.TrimSpace(rollup.EdgeGroupID)] = rollup
	}

	candidates := make([]model.EdgeQualityRankCandidate, 0, len(nodes))
	hardGated := make([]model.EdgeQualityRankCandidate, 0)
	for _, node := range nodes {
		candidate := edgeQualityRankCandidateForNode(node, policy, query.GeneratedAt)
		if candidate.Excluded || !candidate.Healthy || candidate.Draining || !candidate.RouteReady || !candidate.TLSReady {
			candidate.Reason = firstNonEmpty(candidate.ExclusionReason, edgeQualityRankGateReason(candidate))
			hardGated = append(hardGated, candidate)
			continue
		}
		if rollup, ok := nodeRollups[strings.TrimSpace(node.ID)]; ok {
			edgeQualityApplyRollup(&candidate, rollup)
		} else if rollup, ok := groupRollups[strings.TrimSpace(node.EdgeGroupID)]; ok {
			edgeQualityApplyRollup(&candidate, rollup)
		} else {
			candidate.Score = 999999
			candidate.ConfidencePenalty = 250
			candidate.ScoreBreakdown = map[string]float64{"confidence": 250}
			candidate.Reason = "no matching rollup for selected scope"
		}
		candidates = append(candidates, candidate)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score < candidates[j].Score
		}
		if candidates[i].Confidence != candidates[j].Confidence {
			return candidates[i].Confidence > candidates[j].Confidence
		}
		if candidates[i].EdgeGroupID != candidates[j].EdgeGroupID {
			return candidates[i].EdgeGroupID < candidates[j].EdgeGroupID
		}
		return candidates[i].EdgeID < candidates[j].EdgeID
	})
	for index := range candidates {
		candidates[index].Rank = index + 1
	}
	return model.EdgeQualityRankResponse{
		Hostname:         query.Hostname,
		TrafficClass:     selectedFilter.TrafficClass,
		Method:           selectedFilter.Method,
		PathPrefixBucket: selectedFilter.PathPrefixBucket,
		RequestedScope:   query.RequestedScope.key(),
		SelectedScope:    selectedScope.key(),
		FallbackLevel:    fallbackLevel,
		FallbackReason:   fallbackReason,
		Window:           query.Window,
		Since:            query.Since,
		GeneratedAt:      query.GeneratedAt,
		Candidates:       candidates,
		HardGated:        hardGated,
	}
}

func edgeQualityRankCandidateForNode(node model.EdgeNode, policy model.EdgeRoutePolicy, now time.Time) model.EdgeQualityRankCandidate {
	tlsReady := strings.EqualFold(strings.TrimSpace(node.TLSStatus), model.EdgeTLSStatusReady) || node.TLSReadyAt != nil
	routeReady := strings.TrimSpace(node.RouteBundleVersion) != "" && node.CaddyRouteCount > 0
	candidate := model.EdgeQualityRankCandidate{
		EdgeID:      strings.TrimSpace(node.ID),
		EdgeGroupID: strings.TrimSpace(node.EdgeGroupID),
		Region:      strings.TrimSpace(node.Region),
		Country:     strings.TrimSpace(node.Country),
		Healthy:     node.Healthy,
		Draining:    node.Draining,
		RouteReady:  routeReady,
		TLSReady:    tlsReady,
	}
	excluded, reason := edgeQualityNodeExcludedByPolicy(node, policy, now)
	candidate.Excluded = excluded
	candidate.ExclusionReason = reason
	return candidate
}

func edgeQualityApplyProfile(candidate *model.EdgeQualityRankCandidate, profile edgeDNSLatencyCandidateProfile) {
	if candidate == nil {
		return
	}
	candidate.Score = profile.Score
	candidate.ScoreBreakdown = cloneFloat64Map(profile.ScoreBreakdown)
	candidate.Confidence = profile.Confidence
	candidate.ConfidencePenalty = profile.ConfidencePenalty
	candidate.SampleRecordCount = profile.SampleCount
	candidate.RequestCount = profile.SampleCount
	candidate.ErrorRate = profile.ErrorRate
	candidate.CacheHitRate = profile.HitRatio
	candidate.AvgTTFBMS = profile.TTFBMS
	candidate.AvgTotalMS = profile.TotalMS
	candidate.AvgUploadBPS = profile.UploadBPS
	candidate.AvgResponseEgressBPS = profile.ResponseEgressBPS
	candidate.ClientTCPRetransRate = profile.ClientTCPRetransRate
	candidate.ClientTCPBytesRetransRate = profile.ClientTCPBytesRetransRate
	candidate.ClientTCPRTORate = profile.ClientTCPRTORate
	candidate.Reason = edgeDNSLatencyReason("scoped_quality", profile)
}

func edgeQualityApplyRollup(candidate *model.EdgeQualityRankCandidate, rollup model.EdgeQualityRollup) {
	if candidate == nil {
		return
	}
	candidate.Score = rollup.Score
	candidate.ScoreBreakdown = cloneFloat64Map(rollup.ScoreBreakdown)
	candidate.Confidence = rollup.Confidence
	candidate.ConfidencePenalty = edgeDNSLatencyConfidencePenalty(rollup.Confidence)
	candidate.SampleRecordCount = rollup.SampleCount
	candidate.RequestCount = rollup.RequestCount
	candidate.ErrorRate = rollup.ErrorRate
	candidate.CacheHitRate = rollup.CacheHitRate
	candidate.AvgTTFBMS = firstPositiveFloat(rollup.P95TTFBMS, rollup.P50TTFBMS)
	candidate.AvgTotalMS = rollup.AvgTotalMS
	candidate.AvgUploadBPS = rollup.AvgUploadEffectiveBPS
	candidate.MinUploadBPS = int64(firstPositiveFloat(rollup.P10UploadEffectiveBPS, rollup.P10MinWindowBPS))
	candidate.AvgResponseEgressBPS = rollup.AvgResponseEgressBPS
	candidate.ClientTCPRetransRate = rollup.ClientTCPRetransRate
	candidate.ClientTCPBytesRetransRate = rollup.ClientTCPBytesRetransRate
	candidate.ClientTCPRTORate = rollup.ClientTCPRTORate
	candidate.LastSampledAt = &rollup.WindowEndedAt
	candidate.Reason = formatEdgeQualityRollupReason(rollup)
}

func edgeQualityRankGateReason(candidate model.EdgeQualityRankCandidate) string {
	switch {
	case candidate.Excluded:
		return "excluded by service edge policy"
	case !candidate.Healthy:
		return "edge node unhealthy"
	case candidate.Draining:
		return "edge node draining"
	case !candidate.RouteReady:
		return "edge node route bundle not ready"
	case !candidate.TLSReady:
		return "edge node TLS not ready"
	default:
		return ""
	}
}

func edgeQualityNodeExcludedByPolicy(node model.EdgeNode, policy model.EdgeRoutePolicy, now time.Time) (bool, string) {
	if strings.TrimSpace(policy.Hostname) == "" || !policy.Enabled {
		return false, ""
	}
	expires := policy.ExclusionExpiresAt
	exclusionActive := expires == nil || expires.IsZero() || now.Before(expires.UTC())
	if pinned := strings.TrimSpace(policy.EdgeGroupID); pinned != "" && !strings.EqualFold(pinned, node.EdgeGroupID) {
		return true, "edge group is not selected by service policy"
	}
	if exclusionActive {
		for _, edgeID := range policy.ExcludedEdgeIDs {
			if strings.EqualFold(strings.TrimSpace(edgeID), strings.TrimSpace(node.ID)) {
				return true, firstNonEmpty(strings.TrimSpace(policy.ExclusionReason), "edge node excluded by service policy")
			}
		}
		for _, edgeGroupID := range policy.ExcludedEdgeGroupIDs {
			if strings.EqualFold(strings.TrimSpace(edgeGroupID), strings.TrimSpace(node.EdgeGroupID)) {
				return true, firstNonEmpty(strings.TrimSpace(policy.ExclusionReason), "edge group excluded by service policy")
			}
		}
	}
	return false, ""
}

func edgeQualityRankFallbackFilters(query edgeQualityRankQuery) []edgeQualityRankFilter {
	full := edgeQualityRankFilter{
		TrafficClass:     strings.TrimSpace(query.TrafficClass),
		Method:           strings.TrimSpace(query.Method),
		PathPrefixBucket: strings.TrimSpace(query.PathPrefixBucket),
	}
	out := []edgeQualityRankFilter{full}
	if full.PathPrefixBucket != "" {
		next := full
		next.PathPrefixBucket = ""
		out = append(out, next)
	}
	if full.Method != "" {
		next := full
		next.Method = ""
		next.PathPrefixBucket = ""
		out = append(out, next)
	}
	if full.TrafficClass != "" {
		out = append(out, edgeQualityRankFilter{})
	}
	return edgeQualityDeduplicateFilters(out)
}

func edgeQualityRankFallbackHostnames(hostname string) []string {
	hostname = normalizeExternalAppDomain(hostname)
	if hostname == "" {
		return []string{edgeQualityPlatformRollupHostname}
	}
	return []string{hostname, edgeQualityPlatformRollupHostname}
}

func edgeQualityDeduplicateFilters(filters []edgeQualityRankFilter) []edgeQualityRankFilter {
	out := make([]edgeQualityRankFilter, 0, len(filters))
	seen := map[string]bool{}
	for _, filter := range filters {
		key := strings.Join([]string{filter.TrafficClass, filter.Method, filter.PathPrefixBucket}, "\x00")
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, filter)
	}
	return out
}

func edgeQualityRankFallbackScopes(scope edgeQualityRankScope) []edgeQualityRankScope {
	global := edgeQualityRankScope{Kind: "global", Value: "global"}
	switch scope.Kind {
	case "asn":
		return []edgeQualityRankScope{scope, global}
	case "region":
		return []edgeQualityRankScope{scope, {Kind: "country", Value: scope.Country, Country: scope.Country}, global}
	case "country":
		return []edgeQualityRankScope{scope, global}
	default:
		return []edgeQualityRankScope{global}
	}
}

func edgeQualityFilterSamples(samples []model.EdgePerformanceSample, filter edgeQualityRankFilter, scope edgeQualityRankScope) []model.EdgePerformanceSample {
	out := make([]model.EdgePerformanceSample, 0, len(samples))
	for _, sample := range samples {
		if filter.TrafficClass != "" && !strings.EqualFold(strings.TrimSpace(sample.TrafficClass), filter.TrafficClass) {
			continue
		}
		if filter.Method != "" && !strings.EqualFold(strings.TrimSpace(sample.Method), filter.Method) {
			continue
		}
		if filter.PathPrefixBucket != "" && edgeQualityPathPrefixBucket(sample.PathPrefix) != filter.PathPrefixBucket {
			continue
		}
		if !edgeQualitySampleMatchesScope(sample, scope) {
			continue
		}
		out = append(out, sample)
	}
	return out
}

func edgeQualityFilterRollups(rollups []model.EdgeQualityRollup, hostname string, filter edgeQualityRankFilter, scope edgeQualityRankScope) []model.EdgeQualityRollup {
	out := make([]model.EdgeQualityRollup, 0, len(rollups))
	hostname = normalizeExternalAppDomain(hostname)
	for _, rollup := range rollups {
		if hostname != "" && !strings.EqualFold(strings.TrimSpace(rollup.Hostname), hostname) {
			continue
		}
		if filter.TrafficClass != "" && !strings.EqualFold(strings.TrimSpace(rollup.TrafficClass), filter.TrafficClass) {
			continue
		}
		if filter.Method != "" && !strings.EqualFold(strings.TrimSpace(rollup.Method), filter.Method) {
			continue
		}
		if filter.PathPrefixBucket != "" && strings.TrimSpace(rollup.PathPrefixBucket) != filter.PathPrefixBucket {
			continue
		}
		if !edgeQualityRollupMatchesScope(rollup, scope) {
			continue
		}
		out = append(out, rollup)
	}
	return out
}

func latestEdgeQualityRollups(rollups []model.EdgeQualityRollup) []model.EdgeQualityRollup {
	if len(rollups) == 0 {
		return nil
	}
	latest := rollups[0].WindowEndedAt
	for _, rollup := range rollups[1:] {
		if rollup.WindowEndedAt.After(latest) {
			latest = rollup.WindowEndedAt
		}
	}
	out := make([]model.EdgeQualityRollup, 0, len(rollups))
	for _, rollup := range rollups {
		if rollup.WindowEndedAt.Equal(latest) {
			out = append(out, rollup)
		}
	}
	return out
}

func edgeQualityRollupMatchesScope(rollup model.EdgeQualityRollup, scope edgeQualityRankScope) bool {
	kind := strings.ToLower(strings.TrimSpace(rollup.ClientScopeKind))
	value := strings.ToLower(strings.TrimSpace(rollup.ClientScopeValue))
	switch scope.Kind {
	case "asn":
		return kind == "asn" && value == strings.ToLower(strings.TrimSpace(scope.ASN))
	case "region":
		return kind == "region" && value == strings.ToLower(strings.TrimSpace(scope.Country))+":"+strings.ToLower(strings.TrimSpace(scope.Region))
	case "country":
		return kind == "country" && value == strings.ToLower(strings.TrimSpace(scope.Country))
	default:
		return kind == "global" || kind == ""
	}
}

func parseEdgeQualityRankWindow(rawWindow, rawSince string, now time.Time) (string, time.Time, error) {
	if strings.TrimSpace(rawSince) != "" {
		since, err := parseEdgeNodeQualitySince(rawSince, now)
		if err != nil {
			return "", time.Time{}, err
		}
		return strings.TrimSpace(rawSince), since, nil
	}
	rawWindow = strings.TrimSpace(rawWindow)
	if rawWindow == "" {
		rawWindow = "30m"
	}
	duration, err := time.ParseDuration(rawWindow)
	if err != nil || duration <= 0 {
		return "", time.Time{}, errors.New("window must be a positive duration such as 30m, 6h, or 24h")
	}
	return rawWindow, now.Add(-duration), nil
}

func normalizedEdgeQualityRankWindow(window string) string {
	window = strings.ToLower(strings.TrimSpace(window))
	switch window {
	case "5m", "30m", "6h", "24h":
		return window
	default:
		return ""
	}
}

func parseEdgeQualityRankScope(raw string) (edgeQualityRankScope, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" || raw == "global" {
		return edgeQualityRankScope{Kind: "global", Value: "global"}, nil
	}
	kind, value, ok := strings.Cut(raw, ":")
	if !ok {
		return edgeQualityRankScope{}, errors.New("scope must be global, country:<country>, region:<country>:<region>, or asn:<asn>")
	}
	value = strings.TrimSpace(value)
	switch kind {
	case "asn":
		if value == "" {
			return edgeQualityRankScope{}, errors.New("asn scope value is required")
		}
		return edgeQualityRankScope{Kind: "asn", Value: value, ASN: value}, nil
	case "country":
		if value == "" {
			return edgeQualityRankScope{}, errors.New("country scope value is required")
		}
		return edgeQualityRankScope{Kind: "country", Value: value, Country: value}, nil
	case "region":
		value = strings.ReplaceAll(value, "-", ":")
		parts := strings.Split(value, ":")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return edgeQualityRankScope{}, errors.New("region scope must be region:<country>:<region>")
		}
		return edgeQualityRankScope{Kind: "region", Value: parts[0] + ":" + parts[1], Country: parts[0], Region: parts[1]}, nil
	default:
		return edgeQualityRankScope{}, errors.New("scope must be global, country:<country>, region:<country>:<region>, or asn:<asn>")
	}
}

func (scope edgeQualityRankScope) key() string {
	switch scope.Kind {
	case "asn":
		return "asn:" + strings.TrimSpace(scope.ASN)
	case "region":
		return "region:" + strings.TrimSpace(scope.Country) + ":" + strings.TrimSpace(scope.Region)
	case "country":
		return "country:" + strings.TrimSpace(scope.Country)
	default:
		return "global"
	}
}

func edgeQualitySampleMatchesScope(sample model.EdgePerformanceSample, scope edgeQualityRankScope) bool {
	switch scope.Kind {
	case "asn":
		return strings.EqualFold(strings.TrimSpace(sample.ClientASN), scope.ASN)
	case "region":
		return strings.EqualFold(strings.TrimSpace(sample.ClientCountry), scope.Country) &&
			strings.EqualFold(strings.TrimSpace(sample.ClientRegion), scope.Region)
	case "country":
		return strings.EqualFold(strings.TrimSpace(sample.ClientCountry), scope.Country)
	default:
		return true
	}
}

func edgeQualityPathPrefixBucket(pathPrefix string) string {
	pathPrefix = model.NormalizeAppRoutePathPrefix(pathPrefix)
	switch {
	case pathPrefix == "" || pathPrefix == "/":
		return ""
	case strings.HasPrefix(pathPrefix, "/_next/static"):
		return "/_next/static/*"
	case strings.HasPrefix(pathPrefix, "/assets"):
		return "/assets/*"
	case strings.HasPrefix(pathPrefix, "/api"):
		return "/api/*"
	case strings.HasPrefix(pathPrefix, "/upload"):
		return "/upload/*"
	case strings.HasPrefix(pathPrefix, "/stream"):
		return "/stream/*"
	default:
		return pathPrefix
	}
}

func cloneFloat64Map(in map[string]float64) map[string]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]float64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
