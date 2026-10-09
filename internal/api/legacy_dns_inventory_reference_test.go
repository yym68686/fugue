package api

import (
	"context"
	"errors"
	"fugue/internal/model"
	"fugue/internal/store"
	"strings"
	"time"
)

func (s *Server) edgeDNSAnswerIPsByGroup(ctx context.Context) (map[string][]string, error) {
	byGroup, _, err := s.edgeDNSAnswerInventory(ctx, "", time.Now().UTC())
	return byGroup, err
}

func (s *Server) edgeDNSAnswerCandidateByIP(ctx context.Context, options edgeDNSBundleOptions, now time.Time) (map[string]model.EdgeDNSAnswerCandidate, error) {
	_, byIP, err := s.edgeDNSAnswerInventory(ctx, options.EdgeGroupID, now)
	return byIP, err
}

func (s *Server) edgeDNSAnswerInventory(ctx context.Context, localEdgeGroupID string, now time.Time) (map[string][]string, map[string]model.EdgeDNSAnswerCandidate, error) {
	byGroup := map[string][]string{}
	byIP := map[string]model.EdgeDNSAnswerCandidate{}
	if s == nil || s.store == nil {
		return byGroup, byIP, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	nodes, _, err := s.store.ListActiveEdgeNodes("")
	if err != nil {
		if errors.Is(err, store.ErrEdgeInstanceFencingNotReady) {
			return byGroup, byIP, nil
		}
		return nil, nil, err
	}
	liveServingByNode := s.edgeLiveServingByNode(ctx, now)
	for _, node := range nodes {
		if !edgeNodeRouteServingCapableWithLive(node, now, liveServingByNode) || !edgeNodeDNSEligible(node) || !edgeNodeDNSCacheValid(node) {
			continue
		}
		groupID := strings.TrimSpace(node.EdgeGroupID)
		if groupID == "" {
			continue
		}
		for _, rawIP := range []string{node.PublicIPv4, node.PublicIPv6} {
			normalized := normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeA, rawIP)
			if normalized == "" {
				normalized = normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeAAAA, rawIP)
			}
			if normalized == "" {
				continue
			}
			byGroup[groupID] = appendEdgeDNSUniqueIP(byGroup[groupID], normalized)
			byIP[normalized] = edgeDNSAnswerCandidateForNode(normalized, node, localEdgeGroupID)
		}
	}
	return byGroup, byIP, nil
}

func edgeDNSAnswerCandidateForNode(ip string, node model.EdgeNode, localEdgeGroupID string) model.EdgeDNSAnswerCandidate {
	groupID := strings.TrimSpace(node.EdgeGroupID)
	reason := edgeDNSCandidateReason(groupID, strings.TrimSpace(localEdgeGroupID), "")
	if model.NormalizeEdgeWorkloadMode(node.WorkloadMode) == model.EdgeWorkloadModeDynamic {
		reason = strings.TrimSpace(reason + "; dynamic_" + edgeNodeEffectiveCanaryState(node))
	}
	return model.EdgeDNSAnswerCandidate{
		IP:                strings.TrimSpace(ip),
		EdgeID:            strings.TrimSpace(node.ID),
		EdgeGroupID:       groupID,
		Region:            strings.TrimSpace(node.Region),
		Country:           strings.ToLower(strings.TrimSpace(node.Country)),
		WorkloadMode:      edgeNodeEffectiveWorkloadMode(node),
		CanaryState:       edgeNodeEffectiveCanaryState(node),
		CanaryWeight:      edgeNodeEffectiveCanaryWeight(node),
		PublicProbeStatus: edgeNodeEffectivePublicProbeStatus(node),
		ServingGeneration: strings.TrimSpace(node.ServingGeneration),
		LKGGeneration:     strings.TrimSpace(node.LKGGeneration),
		CacheStatus:       strings.TrimSpace(node.CacheStatus),
		DNSEligible:       edgeNodeDNSEligible(node),
		Priority:          edgeDNSCandidatePriority(groupID, strings.TrimSpace(localEdgeGroupID), ""),
		Weight:            edgeNodeEffectiveCanaryWeight(node),
		Reason:            reason,
		Healthy:           node.Healthy && !node.Draining,
		RouteReady:        edgeNodeHasRouteState(node),
		TLSReady:          edgeNodeTLSReadyForDNS(node),
	}
}
