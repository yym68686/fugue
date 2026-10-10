package api

import (
	"context"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

func (s *Server) captureClientProbeCapacityWitnesses(ctx context.Context, input model.EdgeClientProbeRequest, nodes []model.EdgeNode, proofs []routeprobe.Proof) {
	if s.newClusterNodeClient == nil || len(nodes) == 0 || len(nodes) > 8 || len(nodes) != len(proofs) || ctx.Err() != nil {
		return
	}
	client, err := s.newClusterNodeClient()
	if err != nil {
		return
	}
	defer client.closeIdleConnections()
	targets := make([]qualityCapacityTarget, 0, len(nodes))
	for _, node := range nodes {
		targets = append(targets, qualityCapacityTarget{edgeID: node.ID, groupID: node.EdgeGroupID, address: node.PublicIPv4})
	}
	capacities := captureQualityNodeCapacity(ctx, targets, func(ctx context.Context, edgeID, address string) (*model.EdgeNetworkNodeCapacity, error) {
		return readNetworkNodeCapacity(ctx, client, edgeID, address, time.Now)
	})
	witnesses := clientProbeCapacityWitnesses(input, nodes, proofs, capacities, time.Now().UTC())
	if len(witnesses) == 0 {
		return
	}
	persistContext, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := s.store.RecordEdgeNetworkRouteWitnesses(persistContext, witnesses, time.Now().UTC().Add(-time.Hour)); err != nil && s.log != nil {
		s.log.Printf("client probe capacity witness persistence unavailable; hostname=%s error=%v", input.Hostname, err)
	}
}

func clientProbeCapacityWitnesses(input model.EdgeClientProbeRequest, nodes []model.EdgeNode, proofs []routeprobe.Proof, capacities []edgequality.NodeCapacitySample, now time.Time) []model.EdgeNetworkSample {
	if len(nodes) == 0 || len(nodes) > 8 || len(nodes) != len(proofs) || len(capacities) > 8 {
		return nil
	}
	witnesses := []model.EdgeNetworkSample{}
	for index, node := range nodes {
		proof := proofs[index]
		if proof.EdgeID != node.ID || proof.GroupID != node.EdgeGroupID || proof.State != "" || proof.CheckedAt.IsZero() || proof.CheckedAt.After(now) || now.Sub(proof.CheckedAt) > 12*time.Second || !proof.ValidUntil.After(now) {
			continue
		}
		for _, sample := range capacities {
			if sample.EdgeID != node.ID || sample.EdgeGroupID != node.EdgeGroupID || sample.Address != node.PublicIPv4 || edgequality.ValidateNodeCapacitySample(sample, now) != nil || !sample.Capacity.ValidUntil.After(now) {
				continue
			}
			capacity := sample.Capacity
			witness := model.EdgeNetworkSample{ID: model.NewID("probe_capacity"), EdgeID: node.ID, EdgeGroupID: node.EdgeGroupID, Hostname: input.Hostname, PathPrefix: input.Path,
				TrafficClass: input.TrafficClass, RouteDigest: proof.Digest, BundleVersion: proof.Version, Source: "route_tls_witness_v1", ObservedAt: proof.CheckedAt,
				RouteWitness: &model.EdgeNetworkRouteWitness{Address: node.PublicIPv4, ValidUntil: proof.ValidUntil, NodeCapacity: &capacity}}
			if model.ValidateEdgeNetworkSample(witness) == nil {
				witnesses = append(witnesses, witness)
			}
			break
		}
	}
	return witnesses
}
