package platformproducer

import (
	"fmt"
	"reflect"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
)

// ValidateMembershipExpansion compares immutable input contents after callers
// have authenticated their signatures and exact policy references. Only one
// already declared Edge can join the static execution membership. This grants
// neither process enrollment nor serving/public transport authority.
func ValidateMembershipExpansion(previous, next Policy, oldStatic, newStatic, oldProjection, newProjection model.PlatformArtifact) error {
	fail := func() error {
		return fmt.Errorf("membership expansion must add one declared Edge without changing other configuration")
	}
	before, err := DecodeStaticIntent(oldStatic)
	if err != nil {
		return fail()
	}
	after, err := DecodeStaticIntent(newStatic)
	if err != nil {
		return fail()
	}
	oldPolicy, err := DecodeProjectionPolicy(oldProjection, before.Consumers, previous.HostedZoneTemplates)
	if err != nil {
		return fail()
	}
	newPolicy, err := DecodeProjectionPolicy(newProjection, after.Consumers, next.HostedZoneTemplates)
	if err != nil || ValidatePinnedSources(previous, before, &oldPolicy) != nil || ValidatePinnedSources(next, after, &newPolicy) != nil || next.RoutePlacementTransition == nil {
		return fail()
	}
	without := func(content map[string]any, fields ...string) map[string]any {
		out := make(map[string]any, len(content))
		for k, v := range content {
			out[k] = v
		}
		for _, k := range fields {
			delete(out, k)
		}
		return out
	}
	if oldStatic.Generation == newStatic.Generation || oldProjection.Generation == newProjection.Generation ||
		!reflect.DeepEqual(without(oldStatic.Content, "generation", "edge_topology"), without(newStatic.Content, "generation", "edge_topology")) ||
		!reflect.DeepEqual(without(oldProjection.Content, "generation", "consumer_topology_digest"), without(newProjection.Content, "generation", "consumer_topology_digest")) ||
		before.EdgeTopology == nil || after.EdgeTopology == nil || len(after.EdgeTopology.Edges) != len(before.EdgeTopology.Edges)+1 {
		return fail()
	}
	oldTopology, newTopology := before.EdgeTopology.Clone(), after.EdgeTopology.Clone()
	oldTopology.Edges, newTopology.Edges = nil, nil
	if !reflect.DeepEqual(oldTopology, newTopology) {
		return fail()
	}
	newEdges := make(map[string]edgetopology.Edge, len(after.EdgeTopology.Edges))
	for _, edge := range after.EdgeTopology.Edges {
		newEdges[edge.ID] = edge
	}
	for _, edge := range before.EdgeTopology.Edges {
		if !reflect.DeepEqual(edge, newEdges[edge.ID]) {
			return fail()
		}
	}
	return nil
}
