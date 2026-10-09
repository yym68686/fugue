package api

import (
	"context"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

const edgeDNSQualityRankingVersion = "edge-quality-scoped-v1"

const (
	edgeDNSLatencyWindow              = 24 * time.Hour
	edgeDNSLatencyMinGroupSamples     = 3
	edgeDNSLatencyMinGroups           = 2
	edgeDNSLatencyMinScoreDelta       = 75.0
	edgeDNSLatencyMinScoreRatio       = 0.20
	edgeDNSLatencyMinSwitchConfidence = 0.10
	edgeDNSLatencyWeightMin           = 20
	edgeDNSLatencyWeightMax           = 200
	edgeDNSExplorationPercent         = 5
	edgeDNSDecisionCooldown           = 30 * time.Minute
)

type edgeDNSLatencyProfileCatalog struct {
	Global map[string]*edgeDNSLatencyProfile
	Scoped map[string][]edgeDNSLatencyProfile
}

type edgeDNSLatencyProfile struct {
	Hostname              string
	Scope                 edgeDNSLatencyScope
	Enabled               bool
	Reason                string
	Weight                int
	BestEdgeGroupID       string
	ShadowBestEdgeGroupID string
	ShadowReason          string
	Candidates            map[string]edgeDNSLatencyCandidateProfile
	NodeCandidates        map[string]edgeDNSLatencyCandidateProfile
	CooldownUntil         time.Time
}

type edgeDNSLatencyCandidateProfile struct {
	EdgeGroupID               string
	EdgeID                    string
	Weight                    int
	Reason                    string
	Score                     float64
	ScoreBreakdown            map[string]float64
	TrafficClass              string
	TTFBMS                    float64
	UpstreamMS                float64
	TotalMS                   float64
	HitRatio                  float64
	ErrorRate                 float64
	UploadBPS                 float64
	BodyReadMS                float64
	MaxReadGapMS              float64
	BodyIncompleteRate        float64
	BodyReadErrorRate         float64
	ResponseEgressBPS         float64
	ResponseWriteMS           float64
	OriginConnectMS           float64
	OriginWriteMS             float64
	OriginWaitMS              float64
	OriginTTFBMS              float64
	OriginTotalMS             float64
	ActiveRequests            float64
	ActiveBodyBuffers         float64
	ClientTCPRTTMS            float64
	ClientTCPMinRTTMS         float64
	ClientTCPRTTVarMS         float64
	ClientTCPRetransRate      float64
	ClientTCPBytesRetransRate float64
	ClientTCPRTORate          float64
	ClientTCPDeliveryBPS      float64
	Confidence                float64
	ConfidencePenalty         float64
	SampleCount               int
	BodySampleCount           int
	Country                   string
	Region                    string
	ASN                       string
}

type edgeDNSLatencyGroupAccumulator struct {
	EdgeGroupID                       string
	EdgeID                            string
	SampleCount                       int
	TTFBWeightedMS                    float64
	UpstreamWeightedMS                float64
	TotalWeightedMS                   float64
	CacheHitCount                     int
	CacheObservationCount             int
	ErrorCount                        int
	UploadWeightedBPS                 float64
	UploadSampleCount                 int
	BodyReadWeightedMS                float64
	BodyReadSampleCount               int
	MaxReadGapWeightedMS              float64
	MaxReadGapSampleCount             int
	BodyIncompleteCount               int
	BodyReadErrorCount                int
	ResponseEgressWeightedBPS         float64
	ResponseEgressSampleCount         int
	ResponseWriteWeightedMS           float64
	ResponseWriteSampleCount          int
	OriginConnectWeightedMS           float64
	OriginConnectSampleCount          int
	OriginWriteWeightedMS             float64
	OriginWriteSampleCount            int
	OriginWaitWeightedMS              float64
	OriginWaitSampleCount             int
	OriginTTFBWeightedMS              float64
	OriginTTFBSampleCount             int
	OriginTotalWeightedMS             float64
	OriginTotalSampleCount            int
	ActiveRequestsWeighted            float64
	ActiveBodyWeighted                float64
	SaturationSampleCount             int
	ClientTCPRTTWeighted              float64
	ClientTCPMinRTTWeighted           float64
	ClientTCPRTTVarWeighted           float64
	ClientTCPMetricSampleCount        int
	ClientTCPTotalRetrans             int64
	ClientTCPBytesRetrans             int64
	ClientTCPTotalRTO                 int64
	ClientTCPRetransRateWeighted      float64
	ClientTCPBytesRetransRateWeighted float64
	ClientTCPRTORateWeighted          float64
	ClientTCPRateSampleCount          int
	ClientTCPDeliveryWeighted         float64
	ClientTCPDeliverySampleCount      int
	TrafficClassCounts                map[string]int
	CountryCounts                     map[string]int
	RegionCounts                      map[string]int
	ASNCounts                         map[string]int
	NodeAccumulators                  map[string]*edgeDNSLatencyGroupAccumulator
}

type edgeDNSLatencyScope struct {
	Country string
	Region  string
	ASN     string
}

func (s *Server) edgeDNSLatencyProfiles(_ edgeDNSBundleOptions) (edgeDNSLatencyProfileCatalog, error) {
	return s.edgeDNSLatencyProfilesAt(time.Now().UTC())
}

func (s *Server) edgeDNSLatencyProfilesAt(now time.Time) (edgeDNSLatencyProfileCatalog, error) {
	return s.edgeDNSLatencyProfilesWithContext(context.Background(), now)
}

func (s *Server) edgeDNSLatencyProfilesWithContext(ctx context.Context, now time.Time) (edgeDNSLatencyProfileCatalog, error) {
	if s.store == nil || s.edgeQualityRankingDisabled() {
		return edgeDNSLatencyProfileCatalog{}, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	builder, severe, err := s.loadEdgeDNSLatencyProfileBuilder(ctx, now, true)
	if err != nil {
		return edgeDNSLatencyProfileCatalog{}, err
	}
	decisions, err := s.store.ListEdgeDNSRoutingDecisions("")
	if err != nil {
		return edgeDNSLatencyProfileCatalog{}, err
	}
	catalog, _ := builder.finish(decisions, now)
	edgeDNSApplySevereDegradeGroupsToCatalog(&catalog, severe)
	return catalog, nil
}

func (s *Server) reconcileEdgeDNSRoutingDecisions(now time.Time) (int, error) {
	return s.reconcileEdgeDNSRoutingDecisionsWithContext(context.Background(), now)
}

func (s *Server) reconcileEdgeDNSRoutingDecisionsWithContext(ctx context.Context, now time.Time) (int, error) {
	if s.store == nil || s.edgeQualityRankingDisabled() {
		return 0, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	builder, _, err := s.loadEdgeDNSLatencyProfileBuilder(ctx, now, false)
	if err != nil {
		return 0, err
	}
	decisions, err := s.store.ListEdgeDNSRoutingDecisions("")
	if err != nil {
		return 0, err
	}
	_, updates := builder.finish(decisions, now)
	if len(updates) > 0 {
		sortEdgeDNSRoutingDecisionUpdates(updates)
		if err := s.store.UpsertEdgeDNSRoutingDecisions(updates); err != nil {
			return 0, err
		}
	}
	return len(updates), nil
}

type edgeDNSLatencyProfileBuilder struct {
	byHostnameScope map[string]map[string]map[string]*edgeDNSLatencyGroupAccumulator
}

// Aggregate the ordered database cursor without retaining the 24-hour raw
// sample slice. Both statistics and exact five-minute percentile weights use
// the same sample snapshot; a failed read publishes no partial catalog.
func (s *Server) loadEdgeDNSLatencyProfileBuilder(ctx context.Context, now time.Time, withSeverity bool) (*edgeDNSLatencyProfileBuilder, map[string]float64, error) {
	builder := newEdgeDNSLatencyProfileBuilder()
	var severe *edgeQualityRollupBuildState
	var weights map[store.EdgeQualityPercentileValueKey]int
	if withSeverity {
		target := &edgeQualityRollupWindowTarget{ID: 0, Window: "5m", Duration: 5 * time.Minute, StartedAt: now.Add(-5 * time.Minute), EndedAt: now}
		severe = newEdgeQualityRollupBuildState([]edgeQualityRollupWindowPlan{{Duration: target.Duration, Targets: map[int64]*edgeQualityRollupWindowTarget{now.UnixNano(): target}}})
		weights = make(map[store.EdgeQualityPercentileValueKey]int)
	}
	err := s.store.WalkEdgePerformanceSamples(ctx, "", now.Add(-edgeDNSLatencyWindow), func(sample model.EdgePerformanceSample) error {
		builder.add(sample)
		if severe != nil {
			severe.addSamples([]model.EdgePerformanceSample{sample}, weights)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	var degraded map[string]float64
	if severe != nil {
		degraded = edgeDNSSevereDegradeGroupsFromRollups(severe.rollups(edgeQualityPercentilesFromWeights(weights), now))
	}
	return builder, degraded, nil
}

func newEdgeDNSLatencyProfileBuilder() *edgeDNSLatencyProfileBuilder {
	return &edgeDNSLatencyProfileBuilder{byHostnameScope: make(map[string]map[string]map[string]*edgeDNSLatencyGroupAccumulator)}
}

func (builder *edgeDNSLatencyProfileBuilder) add(sample model.EdgePerformanceSample) {
	hostname := normalizeExternalAppDomain(sample.Hostname)
	edgeGroupID := strings.TrimSpace(sample.EdgeGroupID)
	if hostname == "" || edgeGroupID == "" {
		return
	}
	for _, scope := range edgeDNSLatencyScopesForSample(sample) {
		if _, ok := builder.byHostnameScope[hostname]; !ok {
			builder.byHostnameScope[hostname] = make(map[string]map[string]*edgeDNSLatencyGroupAccumulator)
		}
		scopeKey := scope.key()
		if _, ok := builder.byHostnameScope[hostname][scopeKey]; !ok {
			builder.byHostnameScope[hostname][scopeKey] = make(map[string]*edgeDNSLatencyGroupAccumulator)
		}
		edgeDNSLatencyAccumulate(builder.byHostnameScope[hostname][scopeKey], edgeGroupID, sample)
	}
}

func edgeDNSLatencyProfilesByHostname(samples []model.EdgePerformanceSample, decisions []model.EdgeDNSRoutingDecision, now time.Time) (edgeDNSLatencyProfileCatalog, []model.EdgeDNSRoutingDecision) {
	builder := newEdgeDNSLatencyProfileBuilder()
	for _, sample := range samples {
		builder.add(sample)
	}
	return builder.finish(decisions, now)
}

func (builder *edgeDNSLatencyProfileBuilder) finish(decisions []model.EdgeDNSRoutingDecision, now time.Time) (edgeDNSLatencyProfileCatalog, []model.EdgeDNSRoutingDecision) {
	return builder.finishWithCooldown(decisions, now, edgeDNSDecisionCooldown, false)
}

func (builder *edgeDNSLatencyProfileBuilder) finishWithCooldown(decisions []model.EdgeDNSRoutingDecision, now time.Time, cooldown time.Duration, explicit bool) (edgeDNSLatencyProfileCatalog, []model.EdgeDNSRoutingDecision) {
	byHostnameScope := builder.byHostnameScope

	decisionByKey := make(map[string]model.EdgeDNSRoutingDecision, len(decisions))
	for _, decision := range decisions {
		key := edgeDNSRoutingDecisionKey(normalizeExternalAppDomain(decision.Hostname), strings.TrimSpace(decision.ScopeKey))
		if key != "" {
			decisionByKey[key] = decision
		}
	}

	catalog := edgeDNSLatencyProfileCatalog{
		Global: make(map[string]*edgeDNSLatencyProfile),
		Scoped: make(map[string][]edgeDNSLatencyProfile),
	}
	updates := []model.EdgeDNSRoutingDecision{}
	hostnames := make([]string, 0, len(byHostnameScope))
	for hostname := range byHostnameScope {
		hostnames = append(hostnames, hostname)
	}
	sort.Strings(hostnames)
	for _, hostname := range hostnames {
		scopes := byHostnameScope[hostname]
		scopeKeys := make([]string, 0, len(scopes))
		for scopeKey := range scopes {
			scopeKeys = append(scopeKeys, scopeKey)
		}
		sort.Strings(scopeKeys)
		for _, scopeKey := range scopeKeys {
			groups := scopes[scopeKey]
			scope := edgeDNSLatencyScopeFromKey(scopeKey)
			profile := buildEdgeDNSLatencyProfile(hostname, scope, groups)
			if profile == nil || !profile.Enabled {
				continue
			}
			decisionKey := edgeDNSRoutingDecisionKey(hostname, scopeKey)
			profile, decision := applyEdgeDNSRoutingDecisionWithCooldown(profile, decisionByKey[decisionKey], now, cooldown, explicit)
			updates = append(updates, decision)
			if profile.Scope.global() {
				catalog.Global[hostname] = profile
				continue
			}
			catalog.Scoped[hostname] = append(catalog.Scoped[hostname], *profile)
		}
	}
	sortEdgeDNSRoutingDecisionUpdates(updates)
	for hostname := range catalog.Scoped {
		sort.Slice(catalog.Scoped[hostname], func(i, j int) bool {
			return catalog.Scoped[hostname][i].Scope.key() < catalog.Scoped[hostname][j].Scope.key()
		})
	}
	return catalog, updates
}

func sortEdgeDNSRoutingDecisionUpdates(decisions []model.EdgeDNSRoutingDecision) {
	sort.Slice(decisions, func(i, j int) bool {
		leftHost := normalizeExternalAppDomain(decisions[i].Hostname)
		rightHost := normalizeExternalAppDomain(decisions[j].Hostname)
		if leftHost != rightHost {
			return leftHost < rightHost
		}
		return strings.TrimSpace(strings.ToLower(decisions[i].ScopeKey)) < strings.TrimSpace(strings.ToLower(decisions[j].ScopeKey))
	})
}

func edgeDNSRoutingDecisionKey(hostname, scopeKey string) string {
	hostname = normalizeExternalAppDomain(hostname)
	scopeKey = strings.TrimSpace(strings.ToLower(scopeKey))
	if hostname == "" || scopeKey == "" {
		return ""
	}
	return hostname + "\x00" + scopeKey
}

func edgeDNSLatencyAccumulate(groups map[string]*edgeDNSLatencyGroupAccumulator, edgeGroupID string, sample model.EdgePerformanceSample) {
	accumulator := groups[edgeGroupID]
	if accumulator == nil {
		accumulator = &edgeDNSLatencyGroupAccumulator{
			EdgeGroupID:        edgeGroupID,
			CountryCounts:      make(map[string]int),
			RegionCounts:       make(map[string]int),
			ASNCounts:          make(map[string]int),
			TrafficClassCounts: make(map[string]int),
			NodeAccumulators:   make(map[string]*edgeDNSLatencyGroupAccumulator),
		}
		groups[edgeGroupID] = accumulator
	}
	edgeDNSLatencyAccumulateInto(accumulator, sample)
	edgeID := strings.TrimSpace(sample.EdgeID)
	if edgeID != "" {
		nodeAccumulator := accumulator.NodeAccumulators[edgeID]
		if nodeAccumulator == nil {
			nodeAccumulator = &edgeDNSLatencyGroupAccumulator{
				EdgeGroupID:        edgeGroupID,
				EdgeID:             edgeID,
				CountryCounts:      make(map[string]int),
				RegionCounts:       make(map[string]int),
				ASNCounts:          make(map[string]int),
				TrafficClassCounts: make(map[string]int),
			}
			accumulator.NodeAccumulators[edgeID] = nodeAccumulator
		}
		edgeDNSLatencyAccumulateInto(nodeAccumulator, sample)
	}
}

func edgeDNSLatencyAccumulateInto(accumulator *edgeDNSLatencyGroupAccumulator, sample model.EdgePerformanceSample) {
	if accumulator == nil {
		return
	}
	sampleCount := sample.SampleCount
	if sampleCount <= 0 {
		sampleCount = 1
	}
	accumulator.SampleCount += sampleCount
	accumulator.TTFBWeightedMS += float64(sample.TTFBMS) * float64(sampleCount)
	accumulator.UpstreamWeightedMS += float64(sample.UpstreamMS) * float64(sampleCount)
	accumulator.TotalWeightedMS += float64(sample.TotalMS) * float64(sampleCount)
	accumulator.CacheHitCount += sample.CacheHitCount
	accumulator.CacheObservationCount += sample.CacheObservationCount
	accumulator.ErrorCount += sample.ErrorCount
	if uploadBPS := edgeDNSPerformanceUploadBPS(sample); uploadBPS > 0 {
		accumulator.UploadWeightedBPS += float64(uploadBPS) * float64(sampleCount)
		accumulator.UploadSampleCount += sampleCount
	}
	if sample.BodyReadBlockMS > 0 {
		accumulator.BodyReadWeightedMS += float64(sample.BodyReadBlockMS) * float64(sampleCount)
		accumulator.BodyReadSampleCount += sampleCount
	}
	if sample.MaxReadGapMS > 0 {
		accumulator.MaxReadGapWeightedMS += float64(sample.MaxReadGapMS) * float64(sampleCount)
		accumulator.MaxReadGapSampleCount += sampleCount
	}
	accumulator.BodyIncompleteCount += sample.BodyIncompleteCount
	accumulator.BodyReadErrorCount += sample.BodyReadErrorCount
	if sample.ResponseEgressBPS > 0 {
		accumulator.ResponseEgressWeightedBPS += float64(sample.ResponseEgressBPS) * float64(sampleCount)
		accumulator.ResponseEgressSampleCount += sampleCount
	}
	if sample.ResponseWriteMS > 0 {
		accumulator.ResponseWriteWeightedMS += float64(sample.ResponseWriteMS) * float64(sampleCount)
		accumulator.ResponseWriteSampleCount += sampleCount
	}
	if sample.OriginConnectMS > 0 {
		accumulator.OriginConnectWeightedMS += float64(sample.OriginConnectMS) * float64(sampleCount)
		accumulator.OriginConnectSampleCount += sampleCount
	}
	if sample.OriginRequestWriteMS > 0 {
		accumulator.OriginWriteWeightedMS += float64(sample.OriginRequestWriteMS) * float64(sampleCount)
		accumulator.OriginWriteSampleCount += sampleCount
	}
	if sample.OriginResponseWaitMS > 0 {
		accumulator.OriginWaitWeightedMS += float64(sample.OriginResponseWaitMS) * float64(sampleCount)
		accumulator.OriginWaitSampleCount += sampleCount
	}
	if sample.OriginTTFBMS > 0 {
		accumulator.OriginTTFBWeightedMS += float64(sample.OriginTTFBMS) * float64(sampleCount)
		accumulator.OriginTTFBSampleCount += sampleCount
	}
	if sample.OriginTotalMS > 0 {
		accumulator.OriginTotalWeightedMS += float64(sample.OriginTotalMS) * float64(sampleCount)
		accumulator.OriginTotalSampleCount += sampleCount
	}
	if sample.ActiveRequests > 0 || sample.ActiveBodyBuffers > 0 {
		accumulator.ActiveRequestsWeighted += float64(sample.ActiveRequests) * float64(sampleCount)
		accumulator.ActiveBodyWeighted += float64(sample.ActiveBodyBuffers) * float64(sampleCount)
		accumulator.SaturationSampleCount += sampleCount
	}
	if sample.ClientTCPRTTMS > 0 || sample.ClientTCPMinRTTMS > 0 || sample.ClientTCPRTTVarMS > 0 {
		accumulator.ClientTCPRTTWeighted += sample.ClientTCPRTTMS * float64(sampleCount)
		accumulator.ClientTCPMinRTTWeighted += sample.ClientTCPMinRTTMS * float64(sampleCount)
		accumulator.ClientTCPRTTVarWeighted += sample.ClientTCPRTTVarMS * float64(sampleCount)
		accumulator.ClientTCPMetricSampleCount += sampleCount
	}
	accumulator.ClientTCPTotalRetrans += sample.ClientTCPTotalRetrans
	accumulator.ClientTCPBytesRetrans += sample.ClientTCPBytesRetrans
	accumulator.ClientTCPTotalRTO += sample.ClientTCPTotalRTO
	if sample.ClientTCPRetransRate > 0 || sample.ClientTCPBytesRetransRate > 0 || sample.ClientTCPRTORate > 0 {
		accumulator.ClientTCPRetransRateWeighted += sample.ClientTCPRetransRate * float64(sampleCount)
		accumulator.ClientTCPBytesRetransRateWeighted += sample.ClientTCPBytesRetransRate * float64(sampleCount)
		accumulator.ClientTCPRTORateWeighted += sample.ClientTCPRTORate * float64(sampleCount)
		accumulator.ClientTCPRateSampleCount += sampleCount
	}
	if sample.ClientTCPDeliveryBPS > 0 {
		accumulator.ClientTCPDeliveryWeighted += float64(sample.ClientTCPDeliveryBPS) * float64(sampleCount)
		accumulator.ClientTCPDeliverySampleCount += sampleCount
	}
	if value := strings.TrimSpace(sample.TrafficClass); value != "" {
		accumulator.TrafficClassCounts[value] += sampleCount
	}
	if value := strings.ToLower(strings.TrimSpace(sample.ClientCountry)); value != "" {
		accumulator.CountryCounts[value] += sampleCount
	}
	if value := strings.TrimSpace(sample.ClientRegion); value != "" {
		accumulator.RegionCounts[value] += sampleCount
	}
	if value := strings.TrimSpace(sample.ClientASN); value != "" {
		accumulator.ASNCounts[value] += sampleCount
	}
}

func edgeDNSPerformanceUploadBPS(sample model.EdgePerformanceSample) int64 {
	uploadBPS := sample.UploadEffectiveBPS
	if sample.MinWindowBPS > 0 && (uploadBPS <= 0 || sample.MinWindowBPS < uploadBPS) {
		uploadBPS = sample.MinWindowBPS
	}
	return uploadBPS
}

func edgeDNSLatencyScopesForSample(sample model.EdgePerformanceSample) []edgeDNSLatencyScope {
	country := strings.ToLower(strings.TrimSpace(sample.ClientCountry))
	region := strings.TrimSpace(sample.ClientRegion)
	asn := strings.TrimSpace(sample.ClientASN)
	scopes := []edgeDNSLatencyScope{{}}
	if !edgeDNSPerformanceSampleHasClientScope(sample) {
		return scopes
	}
	if country != "" {
		scopes = append(scopes, edgeDNSLatencyScope{Country: country})
	}
	if country != "" && region != "" {
		scopes = append(scopes, edgeDNSLatencyScope{Country: country, Region: region})
	}
	if asn != "" {
		scopes = append(scopes, edgeDNSLatencyScope{ASN: asn})
	}
	return scopes
}

func edgeDNSPerformanceSampleHasClientScope(sample model.EdgePerformanceSample) bool {
	if strings.TrimSpace(sample.ClientCountry) != "" || strings.TrimSpace(sample.ClientRegion) != "" || strings.TrimSpace(sample.ClientASN) != "" {
		return true
	}
	return strings.Contains(strings.ToLower(strings.TrimSpace(sample.DNSPolicy)), "client_scope")
}

func (scope edgeDNSLatencyScope) key() string {
	country := strings.ToLower(strings.TrimSpace(scope.Country))
	region := strings.ToLower(strings.TrimSpace(scope.Region))
	asn := strings.ToLower(strings.TrimSpace(scope.ASN))
	switch {
	case asn != "":
		return "asn:" + asn
	case country != "" && region != "":
		return "region:" + country + ":" + region
	case country != "":
		return "country:" + country
	default:
		return "global"
	}
}

func (scope edgeDNSLatencyScope) global() bool {
	return scope.key() == "global"
}

func edgeDNSLatencyScopeFromKey(key string) edgeDNSLatencyScope {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" || key == "global" {
		return edgeDNSLatencyScope{}
	}
	if strings.HasPrefix(key, "asn:") {
		return edgeDNSLatencyScope{ASN: strings.TrimPrefix(key, "asn:")}
	}
	if strings.HasPrefix(key, "region:") {
		parts := strings.SplitN(strings.TrimPrefix(key, "region:"), ":", 2)
		if len(parts) == 2 {
			return edgeDNSLatencyScope{Country: parts[0], Region: parts[1]}
		}
	}
	if strings.HasPrefix(key, "country:") {
		return edgeDNSLatencyScope{Country: strings.TrimPrefix(key, "country:")}
	}
	return edgeDNSLatencyScope{}
}

func buildEdgeDNSLatencyProfile(hostname string, scope edgeDNSLatencyScope, groups map[string]*edgeDNSLatencyGroupAccumulator) *edgeDNSLatencyProfile {
	candidates := make([]edgeDNSLatencyCandidateProfile, 0, len(groups))
	for _, accumulator := range groups {
		if accumulator == nil || accumulator.SampleCount < edgeDNSLatencyMinGroupSamples {
			continue
		}
		candidate := edgeDNSLatencyCandidateFromAccumulator(accumulator)
		candidates = append(candidates, candidate)
	}
	if len(candidates) < edgeDNSLatencyMinGroups {
		return nil
	}
	edgeDNSApplyPeerRelativePenalties(candidates)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score < candidates[j].Score
		}
		return candidates[i].EdgeGroupID < candidates[j].EdgeGroupID
	})
	best := candidates[0]
	second := candidates[1]
	if best.Confidence < edgeDNSLatencyMinSwitchConfidence {
		return nil
	}
	scoreDelta := second.Score - best.Score
	minDelta := edgeDNSLatencyMinScoreDelta
	if ratioDelta := best.Score * edgeDNSLatencyMinScoreRatio; ratioDelta > minDelta {
		minDelta = ratioDelta
	}
	if scoreDelta < minDelta {
		return nil
	}
	worstScore := candidates[len(candidates)-1].Score
	scoreSpan := worstScore - best.Score
	if scoreSpan <= 0 {
		scoreSpan = minDelta
	}

	profile := &edgeDNSLatencyProfile{
		Hostname:        normalizeExternalAppDomain(hostname),
		Scope:           scope,
		Enabled:         true,
		Reason:          "latency_aware_stable_window_24h",
		BestEdgeGroupID: best.EdgeGroupID,
		Candidates:      make(map[string]edgeDNSLatencyCandidateProfile, len(candidates)),
		NodeCandidates:  make(map[string]edgeDNSLatencyCandidateProfile),
	}
	for _, candidate := range candidates {
		penalty := candidate.Score - best.Score
		weight := edgeDNSClampLatencyWeight(edgeDNSLatencyWeightMax - int((penalty/scoreSpan)*float64(edgeDNSLatencyWeightMax-edgeDNSLatencyWeightMin)))
		if candidate.EdgeGroupID == best.EdgeGroupID {
			weight = edgeDNSLatencyWeightMax
			candidate.Reason = edgeDNSLatencyReason("latency_fast", candidate)
		} else {
			candidate.Reason = edgeDNSLatencyReason("latency_penalized", candidate)
		}
		candidate.Weight = weight
		profile.Candidates[candidate.EdgeGroupID] = candidate
		if groupAccumulator := groups[candidate.EdgeGroupID]; groupAccumulator != nil {
			for edgeID, nodeAccumulator := range groupAccumulator.NodeAccumulators {
				if nodeAccumulator == nil || nodeAccumulator.SampleCount < edgeDNSLatencyMinGroupSamples {
					continue
				}
				nodeCandidate := edgeDNSLatencyCandidateFromAccumulator(nodeAccumulator)
				nodeCandidate.Weight = weight
				nodeCandidate.Reason = edgeDNSLatencyReason("node_quality", nodeCandidate)
				profile.NodeCandidates[edgeID] = nodeCandidate
			}
		}
	}
	if bestCandidate, ok := profile.Candidates[profile.BestEdgeGroupID]; ok {
		profile.Weight = bestCandidate.Weight
	}
	return profile
}

func applyEdgeDNSRoutingDecision(profile *edgeDNSLatencyProfile, existing model.EdgeDNSRoutingDecision, now time.Time) (*edgeDNSLatencyProfile, model.EdgeDNSRoutingDecision) {
	return applyEdgeDNSRoutingDecisionWithCooldown(profile, existing, now, edgeDNSDecisionCooldown, false)
}

func applyEdgeDNSRoutingDecisionWithCooldown(profile *edgeDNSLatencyProfile, existing model.EdgeDNSRoutingDecision, now time.Time, cooldown time.Duration, explicit bool) (*edgeDNSLatencyProfile, model.EdgeDNSRoutingDecision) {
	if profile == nil {
		return profile, model.EdgeDNSRoutingDecision{}
	}
	if explicit && !existing.SwitchedAt.IsZero() {
		existing.CooldownUntil = existing.SwitchedAt.Add(cooldown)
	}
	previous := strings.TrimSpace(existing.SelectedEdgeGroupID)
	selected := strings.TrimSpace(profile.BestEdgeGroupID)
	profile.ShadowBestEdgeGroupID = selected
	profile.ShadowReason = profile.Reason
	cooldownUntil := existing.CooldownUntil
	switchedAt := existing.SwitchedAt
	if previous == "" {
		cooldownUntil = now.Add(cooldown)
		switchedAt = now
	} else if previous != selected {
		if now.Before(existing.CooldownUntil) {
			if _, ok := profile.Candidates[previous]; ok {
				selected = previous
				profile.BestEdgeGroupID = previous
				profile.CooldownUntil = existing.CooldownUntil
				profile.Reason = "latency_aware_cooldown_hold"
				profile.promoteSelected(previous, "latency_cooldown_hold")
			}
		} else {
			cooldownUntil = now.Add(cooldown)
			switchedAt = now
		}
	}
	if selected == "" {
		selected = profile.BestEdgeGroupID
	}
	if cooldownUntil.IsZero() {
		cooldownUntil = now.Add(cooldown)
	}
	if switchedAt.IsZero() {
		switchedAt = now
	}
	profile.BestEdgeGroupID = selected
	profile.CooldownUntil = cooldownUntil
	if candidate, ok := profile.Candidates[selected]; ok {
		profile.Weight = candidate.Weight
	}
	return profile, model.EdgeDNSRoutingDecision{
		Hostname:            profile.Hostname,
		ScopeKey:            profile.Scope.key(),
		Country:             profile.Scope.Country,
		Region:              profile.Scope.Region,
		ASN:                 profile.Scope.ASN,
		SelectedEdgeGroupID: selected,
		PreviousEdgeGroupID: previous,
		Reason:              profile.Reason,
		Score:               profile.selectedScore(),
		SampleCount:         profile.selectedSampleCount(),
		SwitchedAt:          switchedAt,
		CooldownUntil:       cooldownUntil,
		CreatedAt:           firstNonZeroTime(existing.CreatedAt, now),
		UpdatedAt:           now,
	}
}

func edgeDNSApplySevereDegradeToCatalog(catalog *edgeDNSLatencyProfileCatalog, samples []model.EdgePerformanceSample, now time.Time) {
	if catalog == nil {
		return
	}
	edgeDNSApplySevereDegradeGroupsToCatalog(catalog, edgeDNSSevereDegradeGroups(samples, now))
}

func edgeDNSApplySevereDegradeGroupsToCatalog(catalog *edgeDNSLatencyProfileCatalog, degraded map[string]float64) {
	if catalog == nil {
		return
	}
	if len(degraded) == 0 {
		return
	}
	for hostname, profile := range catalog.Global {
		if edgeDNSApplySevereDegradeToProfile(profile, degraded) {
			catalog.Global[hostname] = profile
		}
	}
	for hostname, profiles := range catalog.Scoped {
		for index := range profiles {
			edgeDNSApplySevereDegradeToProfile(&profiles[index], degraded)
		}
		catalog.Scoped[hostname] = profiles
	}
}

func edgeDNSSevereDegradeGroups(samples []model.EdgePerformanceSample, now time.Time) map[string]float64 {
	startedAt := now.Add(-5 * time.Minute)
	rollups := buildEdgeQualityRollupsForWindow(samples, "5m", startedAt, now, now)
	return edgeDNSSevereDegradeGroupsFromRollups(rollups)
}

func edgeDNSSevereDegradeGroupsFromRollups(rollups []model.EdgeQualityRollup) map[string]float64 {
	out := map[string]float64{}
	for _, rollup := range rollups {
		if strings.TrimSpace(rollup.EdgeID) != "" || rollup.ClientScopeKind != "global" {
			continue
		}
		penalty, _ := edgeQualitySevereDegradePenalty(rollup)
		if penalty <= 0 {
			continue
		}
		groupID := strings.TrimSpace(rollup.EdgeGroupID)
		if penalty > out[groupID] {
			out[groupID] = penalty
		}
	}
	return out
}

func edgeDNSApplySevereDegradeToProfile(profile *edgeDNSLatencyProfile, degraded map[string]float64) bool {
	if profile == nil || len(profile.Candidates) == 0 {
		return false
	}
	changed := false
	for groupID, candidate := range profile.Candidates {
		penalty := degraded[strings.TrimSpace(groupID)]
		if penalty <= 0 {
			continue
		}
		if candidate.ScoreBreakdown == nil {
			candidate.ScoreBreakdown = map[string]float64{}
		}
		candidate.ScoreBreakdown["severe_degrade"] = penalty
		candidate.Score += penalty
		candidate.Weight = edgeDNSLatencyWeightMin
		candidate.Reason = edgeDNSLatencyReason("severe_degrade_5m", candidate)
		profile.Candidates[groupID] = candidate
		changed = true
	}
	if !changed {
		return false
	}
	bestGroupID := ""
	bestScore := math.MaxFloat64
	worstScore := 0.0
	for groupID, candidate := range profile.Candidates {
		if candidate.Score < bestScore {
			bestScore = candidate.Score
			bestGroupID = groupID
		}
		if candidate.Score > worstScore {
			worstScore = candidate.Score
		}
	}
	if bestGroupID == "" {
		return true
	}
	scoreSpan := worstScore - bestScore
	if scoreSpan <= 0 {
		scoreSpan = 1
	}
	for groupID, candidate := range profile.Candidates {
		if groupID == bestGroupID {
			candidate.Weight = edgeDNSLatencyWeightMax
			candidate.Reason = edgeDNSLatencyReason("latency_fast_after_5m_degrade", candidate)
		} else if degraded[groupID] <= 0 {
			penalty := candidate.Score - bestScore
			candidate.Weight = edgeDNSClampLatencyWeight(edgeDNSLatencyWeightMax - int((penalty/scoreSpan)*float64(edgeDNSLatencyWeightMax-edgeDNSLatencyWeightMin)))
		}
		profile.Candidates[groupID] = candidate
	}
	profile.BestEdgeGroupID = bestGroupID
	profile.Reason = "latency_aware_5m_severe_degrade"
	profile.Weight = profile.Candidates[bestGroupID].Weight
	return true
}

func (profile *edgeDNSLatencyProfile) promoteSelected(edgeGroupID, reasonPrefix string) {
	if profile == nil {
		return
	}
	for key, candidate := range profile.Candidates {
		if key == edgeGroupID {
			candidate.Weight = edgeDNSLatencyWeightMax
			candidate.Reason = edgeDNSLatencyReason(reasonPrefix, candidate)
		} else if candidate.Weight >= edgeDNSLatencyWeightMax {
			candidate.Weight = edgeDNSLatencyWeightMax - 1
		}
		profile.Candidates[key] = candidate
	}
}

func (profile *edgeDNSLatencyProfile) selectedScore() float64 {
	if profile == nil {
		return 0
	}
	if candidate, ok := profile.Candidates[profile.BestEdgeGroupID]; ok {
		return candidate.Score
	}
	return 0
}

func (profile *edgeDNSLatencyProfile) selectedSampleCount() int {
	if profile == nil {
		return 0
	}
	if candidate, ok := profile.Candidates[profile.BestEdgeGroupID]; ok {
		return candidate.SampleCount
	}
	return 0
}

func (catalog edgeDNSLatencyProfileCatalog) globalProfile(hostname string) *edgeDNSLatencyProfile {
	hostname = normalizeExternalAppDomain(hostname)
	if hostname == "" || catalog.Global == nil {
		return nil
	}
	return catalog.Global[hostname]
}

func (catalog edgeDNSLatencyProfileCatalog) scopedProfiles(hostname string, answerIPs []string, candidateByIP map[string]model.EdgeDNSAnswerCandidate, routeReady map[string]bool, preferredEdgeGroupID, fallbackEdgeGroupID string, applyLatency bool) []model.EdgeDNSScopedAnswerCandidates {
	if !applyLatency {
		return nil
	}
	hostname = normalizeExternalAppDomain(hostname)
	if hostname == "" || len(catalog.Scoped[hostname]) == 0 {
		return nil
	}
	out := make([]model.EdgeDNSScopedAnswerCandidates, 0, len(catalog.Scoped[hostname]))
	for _, profile := range catalog.Scoped[hostname] {
		candidates := edgeDNSCandidatesForAnswerIPs(answerIPs, candidateByIP, routeReady, preferredEdgeGroupID, fallbackEdgeGroupID, &profile, applyLatency)
		if len(candidates) == 0 {
			continue
		}
		selectedEdgeGroupID := strings.TrimSpace(profile.BestEdgeGroupID)
		if selectedEdgeGroupID != "" && !edgeDNSCandidatesContainGroup(candidates, selectedEdgeGroupID) {
			selectedEdgeGroupID = ""
		}
		out = append(out, model.EdgeDNSScopedAnswerCandidates{
			ScopeKey:            profile.Scope.key(),
			Country:             profile.Scope.Country,
			Region:              profile.Scope.Region,
			ASN:                 profile.Scope.ASN,
			PolicyKind:          model.DNSAnswerPolicyKindLatencyAware,
			Reason:              profile.Reason,
			SelectedEdgeGroupID: selectedEdgeGroupID,
			CooldownUntil:       profile.CooldownUntil,
			Candidates:          candidates,
		})
	}
	return out
}

func edgeDNSCandidatesContainGroup(candidates []model.EdgeDNSAnswerCandidate, edgeGroupID string) bool {
	edgeGroupID = strings.TrimSpace(edgeGroupID)
	if edgeGroupID == "" {
		return false
	}
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate.EdgeGroupID) == edgeGroupID {
			return true
		}
	}
	return false
}

func edgeDNSLatencyCandidateFromAccumulator(accumulator *edgeDNSLatencyGroupAccumulator) edgeDNSLatencyCandidateProfile {
	candidate := edgeDNSLatencyCandidateProfile{
		EdgeGroupID:          strings.TrimSpace(accumulator.EdgeGroupID),
		EdgeID:               strings.TrimSpace(accumulator.EdgeID),
		SampleCount:          accumulator.SampleCount,
		BodySampleCount:      accumulator.BodyReadSampleCount,
		TTFBMS:               edgeDNSLatencyAverage(accumulator.TTFBWeightedMS, accumulator.SampleCount),
		UpstreamMS:           edgeDNSLatencyAverage(accumulator.UpstreamWeightedMS, accumulator.SampleCount),
		TotalMS:              edgeDNSLatencyAverage(accumulator.TotalWeightedMS, accumulator.SampleCount),
		UploadBPS:            edgeDNSLatencyAverage(accumulator.UploadWeightedBPS, accumulator.UploadSampleCount),
		BodyReadMS:           edgeDNSLatencyAverage(accumulator.BodyReadWeightedMS, accumulator.BodyReadSampleCount),
		MaxReadGapMS:         edgeDNSLatencyAverage(accumulator.MaxReadGapWeightedMS, accumulator.MaxReadGapSampleCount),
		ResponseEgressBPS:    edgeDNSLatencyAverage(accumulator.ResponseEgressWeightedBPS, accumulator.ResponseEgressSampleCount),
		ResponseWriteMS:      edgeDNSLatencyAverage(accumulator.ResponseWriteWeightedMS, accumulator.ResponseWriteSampleCount),
		OriginConnectMS:      edgeDNSLatencyAverage(accumulator.OriginConnectWeightedMS, accumulator.OriginConnectSampleCount),
		OriginWriteMS:        edgeDNSLatencyAverage(accumulator.OriginWriteWeightedMS, accumulator.OriginWriteSampleCount),
		OriginWaitMS:         edgeDNSLatencyAverage(accumulator.OriginWaitWeightedMS, accumulator.OriginWaitSampleCount),
		OriginTTFBMS:         edgeDNSLatencyAverage(accumulator.OriginTTFBWeightedMS, accumulator.OriginTTFBSampleCount),
		OriginTotalMS:        edgeDNSLatencyAverage(accumulator.OriginTotalWeightedMS, accumulator.OriginTotalSampleCount),
		ActiveRequests:       edgeDNSLatencyAverage(accumulator.ActiveRequestsWeighted, accumulator.SaturationSampleCount),
		ActiveBodyBuffers:    edgeDNSLatencyAverage(accumulator.ActiveBodyWeighted, accumulator.SaturationSampleCount),
		ClientTCPRTTMS:       edgeDNSLatencyAverage(accumulator.ClientTCPRTTWeighted, accumulator.ClientTCPMetricSampleCount),
		ClientTCPMinRTTMS:    edgeDNSLatencyAverage(accumulator.ClientTCPMinRTTWeighted, accumulator.ClientTCPMetricSampleCount),
		ClientTCPRTTVarMS:    edgeDNSLatencyAverage(accumulator.ClientTCPRTTVarWeighted, accumulator.ClientTCPMetricSampleCount),
		ClientTCPDeliveryBPS: edgeDNSLatencyAverage(accumulator.ClientTCPDeliveryWeighted, accumulator.ClientTCPDeliverySampleCount),
		TrafficClass:         edgeDNSDominantStringCount(accumulator.TrafficClassCounts, "html_dynamic"),
		Country:              edgeDNSDominantStringCount(accumulator.CountryCounts, ""),
		Region:               edgeDNSDominantStringCount(accumulator.RegionCounts, ""),
		ASN:                  edgeDNSDominantStringCount(accumulator.ASNCounts, ""),
		ScoreBreakdown:       map[string]float64{},
	}
	if candidate.TotalMS <= 0 {
		candidate.TotalMS = candidate.TTFBMS
	}
	if candidate.TTFBMS <= 0 {
		candidate.TTFBMS = candidate.TotalMS
	}
	if accumulator.CacheObservationCount > 0 {
		candidate.HitRatio = float64(accumulator.CacheHitCount) / float64(accumulator.CacheObservationCount)
	}
	if accumulator.SampleCount > 0 {
		candidate.ErrorRate = float64(accumulator.ErrorCount) / float64(accumulator.SampleCount)
		candidate.BodyIncompleteRate = float64(accumulator.BodyIncompleteCount) / float64(accumulator.SampleCount)
		candidate.BodyReadErrorRate = float64(accumulator.BodyReadErrorCount) / float64(accumulator.SampleCount)
		candidate.ClientTCPRetransRate = float64(accumulator.ClientTCPTotalRetrans) / float64(accumulator.SampleCount)
		candidate.ClientTCPBytesRetransRate = float64(accumulator.ClientTCPBytesRetrans) / float64(accumulator.SampleCount)
		candidate.ClientTCPRTORate = float64(accumulator.ClientTCPTotalRTO) / float64(accumulator.SampleCount)
	}
	if accumulator.ClientTCPRateSampleCount > 0 {
		candidate.ClientTCPRetransRate = edgeDNSLatencyAverage(accumulator.ClientTCPRetransRateWeighted, accumulator.ClientTCPRateSampleCount)
		candidate.ClientTCPBytesRetransRate = edgeDNSLatencyAverage(accumulator.ClientTCPBytesRetransRateWeighted, accumulator.ClientTCPRateSampleCount)
		candidate.ClientTCPRTORate = edgeDNSLatencyAverage(accumulator.ClientTCPRTORateWeighted, accumulator.ClientTCPRateSampleCount)
	}
	candidate.Confidence = edgeDNSLatencyCandidateConfidence(candidate, accumulator)
	candidate.ConfidencePenalty = edgeDNSLatencyConfidencePenalty(candidate.Confidence)
	candidate.Score = edgeDNSLatencyScore(candidate)
	return candidate
}

func edgeDNSLatencyScore(candidate edgeDNSLatencyCandidateProfile) float64 {
	breakdown := candidate.ScoreBreakdown
	if breakdown == nil {
		breakdown = map[string]float64{}
	}
	profile := edgeDNSQualityTrafficProfile(candidate.TrafficClass)
	network := candidate.ClientTCPRetransRate*profile.networkRetransWeight +
		candidate.ClientTCPBytesRetransRate*profile.networkBytesRetransWeight +
		candidate.ClientTCPRTORate*profile.networkRTOWeight +
		candidate.ClientTCPRTTVarMS*profile.networkRTTVarWeight +
		candidate.ClientTCPRTTMS*profile.networkRTTWeight
	if candidate.ClientTCPDeliveryBPS > 0 && candidate.ClientTCPDeliveryBPS < profile.deliveryFloorBPS {
		network += ((profile.deliveryFloorBPS - candidate.ClientTCPDeliveryBPS) / 1024) * profile.deliveryDeficitWeight
	}
	if network > profile.networkCap {
		network = profile.networkCap
	}
	breakdown["network"] = network
	latency := candidate.TTFBMS*profile.ttfbWeight + candidate.UpstreamMS*profile.upstreamWeight + candidate.TotalMS*profile.totalWeight
	breakdown["latency"] = latency
	errorPenalty := candidate.ErrorRate * profile.errorWeight
	breakdown["availability"] = errorPenalty
	origin := candidate.OriginConnectMS*profile.originConnectWeight +
		candidate.OriginWriteMS*profile.originWriteWeight +
		candidate.OriginWaitMS*profile.originWaitWeight +
		candidate.OriginTTFBMS*profile.originTTFBWeight +
		candidate.OriginTotalMS*profile.originTotalWeight
	breakdown["origin"] = origin
	upload := 0.0
	if candidate.BodySampleCount > 0 || profile.uploadAlways {
		if candidate.UploadBPS <= 0 {
			upload += profile.missingUploadPenalty
		} else if candidate.UploadBPS < profile.uploadFloorBPS {
			upload += ((profile.uploadFloorBPS - candidate.UploadBPS) / 1024) * profile.uploadDeficitWeight
		}
		upload += candidate.BodyReadMS * profile.bodyReadWeight
		upload += candidate.MaxReadGapMS * profile.maxReadGapWeight
		upload += candidate.BodyIncompleteRate * profile.bodyIncompleteWeight
		upload += candidate.BodyReadErrorRate * profile.bodyReadErrorWeight
		if upload > profile.uploadCap {
			upload = profile.uploadCap
		}
	}
	breakdown["upload"] = upload
	download := 0.0
	if profile.downloadAlways || candidate.ResponseEgressBPS > 0 {
		if candidate.ResponseEgressBPS > 0 && candidate.ResponseEgressBPS < profile.downloadFloorBPS {
			download += ((profile.downloadFloorBPS - candidate.ResponseEgressBPS) / 1024) * profile.downloadDeficitWeight
		}
		download += candidate.ResponseWriteMS * profile.responseWriteWeight
		if download > profile.downloadCap {
			download = profile.downloadCap
		}
	}
	breakdown["download"] = download
	cache := 0.0
	if profile.cacheAware {
		if candidate.HitRatio > 0 && candidate.HitRatio < 1 {
			cache = (1 - candidate.HitRatio) * profile.cacheMissWeight
		} else if candidate.HitRatio == 0 {
			cache = profile.cacheMissWeight
		}
	}
	breakdown["cache"] = cache
	saturation := candidate.ActiveRequests*0.20 + candidate.ActiveBodyBuffers*5
	breakdown["saturation"] = saturation
	breakdown["confidence"] = candidate.ConfidencePenalty
	return network + latency + errorPenalty + origin + upload + download + cache + saturation + candidate.ConfidencePenalty
}

type edgeDNSQualityTrafficWeights struct {
	networkRetransWeight      float64
	networkBytesRetransWeight float64
	networkRTOWeight          float64
	networkRTTVarWeight       float64
	networkRTTWeight          float64
	networkCap                float64
	deliveryFloorBPS          float64
	deliveryDeficitWeight     float64
	ttfbWeight                float64
	upstreamWeight            float64
	totalWeight               float64
	errorWeight               float64
	originConnectWeight       float64
	originWriteWeight         float64
	originWaitWeight          float64
	originTTFBWeight          float64
	originTotalWeight         float64
	uploadAlways              bool
	uploadFloorBPS            float64
	uploadDeficitWeight       float64
	missingUploadPenalty      float64
	bodyReadWeight            float64
	maxReadGapWeight          float64
	bodyIncompleteWeight      float64
	bodyReadErrorWeight       float64
	uploadCap                 float64
	downloadAlways            bool
	downloadFloorBPS          float64
	downloadDeficitWeight     float64
	responseWriteWeight       float64
	downloadCap               float64
	cacheAware                bool
	cacheMissWeight           float64
}

func edgeDNSQualityTrafficProfile(trafficClass string) edgeDNSQualityTrafficWeights {
	base := edgeDNSQualityTrafficWeights{
		networkRetransWeight:      80,
		networkBytesRetransWeight: 120,
		networkRTOWeight:          120,
		networkRTTVarWeight:       0.20,
		networkRTTWeight:          0.04,
		networkCap:                450,
		deliveryFloorBPS:          256 * 1024,
		deliveryDeficitWeight:     0.12,
		ttfbWeight:                0.35,
		upstreamWeight:            0.20,
		totalWeight:               0.15,
		errorWeight:               700,
		originConnectWeight:       0.10,
		originWriteWeight:         0.20,
		originWaitWeight:          0.15,
		originTTFBWeight:          0.10,
		originTotalWeight:         0.05,
		uploadFloorBPS:            256 * 1024,
		uploadDeficitWeight:       1.00,
		missingUploadPenalty:      150,
		bodyReadWeight:            0.03,
		maxReadGapWeight:          0.08,
		bodyIncompleteWeight:      700,
		bodyReadErrorWeight:       700,
		uploadCap:                 700,
		downloadFloorBPS:          512 * 1024,
		downloadDeficitWeight:     0.50,
		responseWriteWeight:       0.05,
		downloadCap:               450,
	}
	switch strings.ToLower(strings.TrimSpace(trafficClass)) {
	case "large_body_api":
		base.networkRetransWeight = 140
		base.networkBytesRetransWeight = 180
		base.networkRTOWeight = 180
		base.networkCap = 650
		base.uploadAlways = true
		base.uploadFloorBPS = 512 * 1024
		base.uploadDeficitWeight = 1.25
		base.bodyReadWeight = 0.05
		base.maxReadGapWeight = 0.12
		base.bodyIncompleteWeight = 900
		base.bodyReadErrorWeight = 900
		base.uploadCap = 900
	case "static_cacheable":
		base.networkRetransWeight = 110
		base.networkBytesRetransWeight = 160
		base.networkRTOWeight = 150
		base.downloadAlways = true
		base.downloadFloorBPS = 1024 * 1024
		base.downloadDeficitWeight = 0.70
		base.cacheAware = true
		base.cacheMissWeight = 240
		base.originConnectWeight = 0.05
		base.originWriteWeight = 0.05
		base.originWaitWeight = 0.05
		base.originTTFBWeight = 0.05
		base.originTotalWeight = 0.03
	case "streaming", "sse", "websocket":
		base.networkRetransWeight = 150
		base.networkBytesRetransWeight = 190
		base.networkRTOWeight = 220
		base.networkCap = 750
		base.totalWeight = 0.02
		base.errorWeight = 850
		base.originTotalWeight = 0.01
	case "html_dynamic":
		base.ttfbWeight = 0.45
		base.originWaitWeight = 0.20
		base.originTTFBWeight = 0.12
	case "dynamic_api", "small_api":
		base.ttfbWeight = 0.42
		base.upstreamWeight = 0.22
		base.errorWeight = 800
	}
	return base
}

func edgeDNSLatencyCandidateConfidence(candidate edgeDNSLatencyCandidateProfile, accumulator *edgeDNSLatencyGroupAccumulator) float64 {
	if accumulator == nil || candidate.SampleCount <= 0 {
		return 0
	}
	sampleConfidence := float64(candidate.SampleCount) / 50.0
	if sampleConfidence > 1 {
		sampleConfidence = 1
	}
	metricCompleteness := 0.55
	if candidate.TTFBMS > 0 || candidate.TotalMS > 0 {
		metricCompleteness += 0.15
	}
	if candidate.UploadBPS > 0 || candidate.ResponseEgressBPS > 0 {
		metricCompleteness += 0.10
	}
	if candidate.ClientTCPRTTMS > 0 || candidate.ClientTCPRetransRate > 0 || candidate.ClientTCPRTORate > 0 {
		metricCompleteness += 0.15
	}
	if candidate.HitRatio > 0 || accumulator.CacheObservationCount > 0 {
		metricCompleteness += 0.05
	}
	if metricCompleteness > 1 {
		metricCompleteness = 1
	}
	confidence := sampleConfidence * metricCompleteness
	if confidence < 0 {
		return 0
	}
	if confidence > 1 {
		return 1
	}
	return confidence
}

func edgeDNSLatencyConfidencePenalty(confidence float64) float64 {
	if confidence >= 0.85 {
		return 0
	}
	if confidence <= 0 {
		return 250
	}
	return (0.85 - confidence) * 220
}

func edgeDNSApplyPeerRelativePenalties(candidates []edgeDNSLatencyCandidateProfile) {
	bestUpload := 0.0
	bestDownload := 0.0
	for _, candidate := range candidates {
		if candidate.UploadBPS > bestUpload {
			bestUpload = candidate.UploadBPS
		}
		if candidate.ResponseEgressBPS > bestDownload {
			bestDownload = candidate.ResponseEgressBPS
		}
	}
	for index := range candidates {
		breakdown := candidates[index].ScoreBreakdown
		if breakdown == nil {
			breakdown = map[string]float64{}
			candidates[index].ScoreBreakdown = breakdown
		}
		if bestUpload > 0 && candidates[index].UploadBPS > 0 && candidates[index].UploadBPS < bestUpload*0.5 {
			penalty := ((bestUpload / candidates[index].UploadBPS) - 1) * 80
			if penalty > 300 {
				penalty = 300
			}
			breakdown["upload_peer"] = penalty
			candidates[index].Score += penalty
		}
		if bestDownload > 0 && candidates[index].ResponseEgressBPS > 0 && candidates[index].ResponseEgressBPS < bestDownload*0.5 {
			penalty := ((bestDownload / candidates[index].ResponseEgressBPS) - 1) * 50
			if penalty > 200 {
				penalty = 200
			}
			breakdown["download_peer"] = penalty
			candidates[index].Score += penalty
		}
	}
}

func edgeDNSLatencyAverage(sum float64, count int) float64 {
	if count <= 0 || sum <= 0 {
		return 0
	}
	return sum / float64(count)
}

func edgeDNSDominantStringCount(counts map[string]int, fallback string) string {
	bestKey := strings.TrimSpace(fallback)
	bestCount := -1
	for key, count := range counts {
		if count > bestCount || (count == bestCount && key < bestKey) {
			bestKey = key
			bestCount = count
		}
	}
	if bestKey == "" {
		return fallback
	}
	return bestKey
}

func edgeDNSClampLatencyWeight(weight int) int {
	if weight < edgeDNSLatencyWeightMin {
		return edgeDNSLatencyWeightMin
	}
	if weight > edgeDNSLatencyWeightMax {
		return edgeDNSLatencyWeightMax
	}
	return weight
}

func edgeDNSLatencyReason(prefix string, candidate edgeDNSLatencyCandidateProfile) string {
	parts := []string{
		prefix,
		"ttfb_" + strconv.Itoa(int(candidate.TTFBMS+0.5)) + "ms",
		"upstream_" + strconv.Itoa(int(candidate.UpstreamMS+0.5)) + "ms",
		"class_" + strings.TrimSpace(candidate.TrafficClass),
		"upload_" + strconv.Itoa(int(candidate.UploadBPS+0.5)) + "bps",
		"hit_" + strconv.Itoa(int(candidate.HitRatio*100+0.5)) + "pct",
		"error_" + strconv.Itoa(int(candidate.ErrorRate*100+0.5)) + "pct",
		"tcp_retrans_" + strconv.Itoa(int(candidate.ClientTCPRetransRate*100+0.5)) + "pct",
		"confidence_" + strconv.Itoa(int(candidate.Confidence*100+0.5)) + "pct",
	}
	if candidate.Country != "" {
		parts = append(parts, "country_"+candidate.Country)
	}
	if candidate.Region != "" {
		parts = append(parts, "region_"+candidate.Region)
	}
	if candidate.ASN != "" {
		parts = append(parts, "asn_"+candidate.ASN)
	}
	return strings.Join(parts, "_")
}

func edgeDNSLatencyCandidateReason(profile *edgeDNSLatencyProfile, candidate edgeDNSLatencyCandidateProfile, groupID, preferredEdgeGroupID string) string {
	reason := strings.TrimSpace(candidate.Reason)
	if profile == nil || !profile.Enabled {
		return reason
	}
	if groupID == profile.BestEdgeGroupID {
		return reason
	}
	if preferredEdgeGroupID != "" && groupID == preferredEdgeGroupID {
		return strings.Replace(reason, "latency_penalized", "geo_close_but_slow", 1)
	}
	return reason
}

func edgeDNSCandidatesForAnswerIPs(answerIPs []string, candidateByIP map[string]model.EdgeDNSAnswerCandidate, routeReady map[string]bool, preferredEdgeGroupID, fallbackEdgeGroupID string, latencyProfile *edgeDNSLatencyProfile, applyLatency bool) []model.EdgeDNSAnswerCandidate {
	out := make([]model.EdgeDNSAnswerCandidate, 0, len(answerIPs))
	seen := map[string]bool{}
	for _, raw := range answerIPs {
		ip := normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeA, raw)
		if ip == "" {
			ip = normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeAAAA, raw)
		}
		if ip == "" || seen[ip] {
			continue
		}
		seen[ip] = true
		candidate, ok := candidateByIP[ip]
		if !ok {
			candidate = model.EdgeDNSAnswerCandidate{IP: ip, Healthy: true, RouteReady: true, TLSReady: true}
		}
		groupID := strings.TrimSpace(candidate.EdgeGroupID)
		if routeReady != nil {
			candidate.RouteReady = routeReady[groupID]
		}
		candidate.Priority = edgeDNSCandidatePriority(groupID, preferredEdgeGroupID, fallbackEdgeGroupID)
		candidate.Reason = edgeDNSCandidateReason(groupID, preferredEdgeGroupID, fallbackEdgeGroupID)
		if applyLatency && latencyProfile != nil && latencyProfile.Enabled {
			latencyCandidate, hasLatencyCandidate := latencyProfile.Candidates[groupID]
			if hasLatencyCandidate {
				candidate.Weight = latencyCandidate.Weight
				candidate.Reason = edgeDNSLatencyCandidateReason(latencyProfile, latencyCandidate, groupID, preferredEdgeGroupID)
				candidate.TrafficClass = latencyCandidate.TrafficClass
				candidate.Score = latencyCandidate.Score
				candidate.ScoreBreakdown = latencyCandidate.ScoreBreakdown
			}
			if nodeCandidate, ok := latencyProfile.NodeCandidates[strings.TrimSpace(candidate.EdgeID)]; ok {
				if nodeCandidate.Weight > 0 {
					candidate.Weight = nodeCandidate.Weight
				}
				if nodeCandidate.Reason != "" {
					candidate.Reason = edgeDNSLatencyCandidateReason(latencyProfile, nodeCandidate, groupID, preferredEdgeGroupID)
				}
				if nodeCandidate.TrafficClass != "" {
					candidate.TrafficClass = nodeCandidate.TrafficClass
				}
				candidate.Score = nodeCandidate.Score
				candidate.ScoreBreakdown = nodeCandidate.ScoreBreakdown
			}
		}
		if candidate.Weight <= 0 {
			candidate.Weight = 100
		}
		out = append(out, candidate)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority < out[j].Priority
		}
		if out[i].Weight != out[j].Weight {
			return out[i].Weight > out[j].Weight
		}
		if out[i].Score > 0 && out[j].Score > 0 && out[i].Score != out[j].Score {
			return out[i].Score < out[j].Score
		}
		if out[i].EdgeGroupID != out[j].EdgeGroupID {
			return out[i].EdgeGroupID < out[j].EdgeGroupID
		}
		return out[i].IP < out[j].IP
	})
	return out
}

func edgeDNSAnswerPolicy(options edgeDNSBundleOptions, preferredEdgeGroupID, fallbackEdgeGroupID string, answerIPs []string, candidateByIP map[string]model.EdgeDNSAnswerCandidate, latencyProfile *edgeDNSLatencyProfile, ttl int, applyLatency bool) model.DNSAnswerPolicy {
	allowed := make([]string, 0, len(answerIPs))
	for _, ip := range answerIPs {
		normalized := normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeA, ip)
		if normalized == "" {
			normalized = normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeAAAA, ip)
		}
		candidate, ok := candidateByIP[normalized]
		if !ok {
			continue
		}
		if groupID := strings.TrimSpace(candidate.EdgeGroupID); groupID != "" && !stringSliceContains(allowed, groupID) {
			allowed = append(allowed, groupID)
		}
	}
	sort.Strings(allowed)
	preferred := uniqueSortedNonEmptyStrings(preferredEdgeGroupID, strings.TrimSpace(options.EdgeGroupID))
	fallback := uniqueSortedNonEmptyStrings(fallbackEdgeGroupID)
	policyKind := model.DNSAnswerPolicyKindGeo
	reason := "geo_healthy_route_ready"
	weight := 0
	selectedEdgeGroupID := edgeDNSLegacySelectedEdgeGroup(preferred, allowed)
	shadowSelectedEdgeGroupID := ""
	shadowReason := ""
	rankingVersion := ""
	rankingScope := ""
	if latencyProfile != nil && latencyProfile.Enabled {
		rankingVersion = edgeDNSQualityRankingVersion
		rankingScope = latencyProfile.Scope.key()
		shadowSelectedEdgeGroupID = strings.TrimSpace(latencyProfile.BestEdgeGroupID)
		shadowReason = "shadow_" + strings.TrimSpace(latencyProfile.Reason)
	}
	if applyLatency && latencyProfile != nil && latencyProfile.Enabled {
		policyKind = model.DNSAnswerPolicyKindLatencyAware
		reason = latencyProfile.Reason
		weight = latencyProfile.Weight
		selectedEdgeGroupID = strings.TrimSpace(latencyProfile.BestEdgeGroupID)
		shadowSelectedEdgeGroupID = strings.TrimSpace(latencyProfile.ShadowBestEdgeGroupID)
		shadowReason = strings.TrimSpace(latencyProfile.ShadowReason)
		if preferredEdgeGroupID != "" && latencyProfile.BestEdgeGroupID != "" && preferredEdgeGroupID != latencyProfile.BestEdgeGroupID {
			reason = "latency_aware_geo_close_but_slow"
		}
	}
	if selectedEdgeGroupID != "" && !stringSliceContains(allowed, selectedEdgeGroupID) {
		shadowSelectedEdgeGroupID = selectedEdgeGroupID
		if shadowReason == "" {
			shadowReason = "latency selected edge group is not in allowed answers"
		}
		selectedEdgeGroupID = ""
	}
	return model.DNSAnswerPolicy{
		PolicyKind:                policyKind,
		AllowedEdgeGroups:         allowed,
		PreferredEdgeGroups:       preferred,
		FallbackEdgeGroups:        fallback,
		TTLSeconds:                edgeDNSPolicyTTL(ttl),
		ECSEnabled:                true,
		HealthRequired:            true,
		RouteReadyRequired:        true,
		ExplorationPercent:        edgeDNSExplorationPercent,
		SwitchCooldownSec:         int(edgeDNSDecisionCooldown.Seconds()),
		RankingVersion:            rankingVersion,
		RankingScope:              rankingScope,
		Weight:                    weight,
		Reason:                    reason,
		SelectedEdgeGroupID:       selectedEdgeGroupID,
		ShadowSelectedEdgeGroupID: shadowSelectedEdgeGroupID,
		ShadowReason:              shadowReason,
	}
}

func edgeDNSLegacySelectedEdgeGroup(preferred, allowed []string) string {
	for _, groupID := range preferred {
		groupID = strings.TrimSpace(groupID)
		if groupID != "" && stringSliceContains(allowed, groupID) {
			return groupID
		}
	}
	if len(allowed) == 1 {
		return strings.TrimSpace(allowed[0])
	}
	return ""
}

func edgeDNSCandidatePriority(groupID, preferredEdgeGroupID, fallbackEdgeGroupID string) int {
	groupID = strings.TrimSpace(groupID)
	preferredEdgeGroupID = strings.TrimSpace(preferredEdgeGroupID)
	fallbackEdgeGroupID = strings.TrimSpace(fallbackEdgeGroupID)
	switch {
	case groupID != "" && preferredEdgeGroupID != "" && groupID == preferredEdgeGroupID:
		return 0
	case groupID != "" && fallbackEdgeGroupID != "" && groupID == fallbackEdgeGroupID:
		return 100
	default:
		return 50
	}
}

func edgeDNSCandidateReason(groupID, preferredEdgeGroupID, fallbackEdgeGroupID string) string {
	groupID = strings.TrimSpace(groupID)
	preferredEdgeGroupID = strings.TrimSpace(preferredEdgeGroupID)
	fallbackEdgeGroupID = strings.TrimSpace(fallbackEdgeGroupID)
	switch {
	case groupID != "" && preferredEdgeGroupID != "" && groupID == preferredEdgeGroupID:
		return "same_region"
	case groupID != "" && fallbackEdgeGroupID != "" && groupID == fallbackEdgeGroupID:
		return "fallback_healthy"
	default:
		return "global_route_ready"
	}
}
