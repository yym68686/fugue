package api

import (
	"context"
	"fmt"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"time"
)

func (s *Server) captureDirectDNSQueries(ctx context.Context, result *platformIntentProjectionResponse, policy platformconfig.DNSQueryPolicy) error {
	return s.captureDirectDNSQueriesWithNodes(ctx, result, policy, nil)
}

func (s *Server) captureDirectDNSQueriesWithNodes(ctx context.Context, result *platformIntentProjectionResponse, policy platformconfig.DNSQueryPolicy, declaredNodes []model.EdgeNode) error {
	if err := platformconfig.ValidateDNSQueryPolicy(&policy); err != nil {
		return err
	}
	if policy.OrderedProjection == nil {
		return fmt.Errorf("legacy DNS selector retired; explicit physical ordered projection required")
	}
	now := time.Now().UTC()
	list := s.store.ListActiveEdgeNodes
	if result.Policy.DNSPlacementMode == platformconfig.DNSPlacementConsumerReadiness {
		list = s.store.ListEdgeNodes
	}
	nodes := declaredNodes
	if nodes == nil {
		var err error
		nodes, _, err = list("")
		if err != nil {
			return err
		}
	}
	eligible := make([]model.EdgeNode, 0, len(nodes))
	if result.Policy.DNSPlacementMode == platformconfig.DNSPlacementConsumerReadiness {
		// projectDirectDNSQueries restricts these to the frozen authoritative
		// node/group/address topology. No serving health is inferred here.
		eligible = nodes
	} else {
		live := s.edgeLiveServingByNode(ctx, now)
		for _, node := range nodes {
			if edgeNodeRouteServingCapableWithLive(node, now, live) && edgeNodeDNSEligible(node) && edgeNodeDNSCacheValid(node) {
				eligible = append(eligible, node)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := projectDirectDNSQueries(result, policy, eligible, now); err != nil {
		return err
	}
	return s.capturePhysicalDNSQueries(ctx, result, policy)
}

func projectDirectDNSQueries(result *platformIntentProjectionResponse, strategy platformconfig.DNSQueryPolicy, nodes []model.EdgeNode, observed time.Time) error {
	if err := platformconfig.ValidateDNSQueryPolicy(&strategy); err != nil {
		return err
	}
	if strategy.OrderedProjection == nil {
		return fmt.Errorf("legacy DNS selector retired; explicit physical ordered projection required")
	}
	return projectOrderedDNSQueries(result, strategy, nodes, observed)
}
