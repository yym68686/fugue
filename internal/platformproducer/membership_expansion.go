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
	return validateMembershipChange(previous, next, oldStatic, newStatic, oldProjection, newProjection, false)
}

// ValidateMembershipShrink removes one explicitly selected execution member.
// Runtime health cannot alter membership, and no observation from the removed
// member is needed to validate the signed configuration change.
func ValidateMembershipShrink(previous, next Policy, oldStatic, newStatic, oldProjection, newProjection model.PlatformArtifact) error {
	return validateMembershipChange(previous, next, oldStatic, newStatic, oldProjection, newProjection, true)
}

func validateMembershipChange(previous, next Policy, oldStatic, newStatic, oldProjection, newProjection model.PlatformArtifact, shrink bool) error {
	fail := func() error {
		return fmt.Errorf("membership change must add or remove exactly one declared Edge without changing other configuration")
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
		before.EdgeTopology == nil || after.EdgeTopology == nil || len(after.EdgeTopology.Edges) != len(before.EdgeTopology.Edges)+membershipDelta(shrink) {
		return fail()
	}
	oldTopology, newTopology := before.EdgeTopology.Clone(), after.EdgeTopology.Clone()
	oldTopology.Edges, newTopology.Edges = nil, nil
	if !reflect.DeepEqual(oldTopology, newTopology) {
		return fail()
	}
	if shrink && (len(after.EdgeTopology.Edges) == 0 || newPolicy.MinimumHealthyEdges != nil && len(after.EdgeTopology.Edges) < *newPolicy.MinimumHealthyEdges) {
		return fail()
	}
	// Compare the smaller set with the larger: every retained identity must be
	// byte-for-byte unchanged; replacement is not a membership-only operation.
	smaller, larger := before.EdgeTopology.Edges, after.EdgeTopology.Edges
	if shrink {
		smaller, larger = larger, smaller
	}
	newEdges := make(map[string]edgetopology.Edge, len(larger))
	for _, edge := range larger {
		newEdges[edge.ID] = edge
	}
	for _, edge := range smaller {
		if !reflect.DeepEqual(edge, newEdges[edge.ID]) {
			return fail()
		}
	}
	return nil
}

func membershipDelta(shrink bool) int {
	if shrink {
		return -1
	}
	return 1
}
