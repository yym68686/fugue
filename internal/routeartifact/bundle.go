package routeartifact

import (
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
	"fugue/internal/routebinding"
)

// MaterializeForGroup builds detached executor input from an immutable artifact.
// It adds no observed health, validity lease, signature or serving authority.
// Those must be supplied by the release/apply protocol, never inferred here.
func MaterializeForGroup(artifact model.PlatformArtifact, groupID string) (model.EdgeRouteBundle, error) {
	if !platformRouteArtifactGroupID.MatchString(groupID) || strings.TrimSpace(artifact.Generation) == "" {
		return model.EdgeRouteBundle{}, fmt.Errorf("route materialization requires group and artifact generation")
	}
	snapshot, err := Project(artifact)
	if err != nil {
		return model.EdgeRouteBundle{}, err
	}
	if len(snapshot.Routes) == 0 {
		return model.EdgeRouteBundle{}, fmt.Errorf("candidate route index is empty")
	}
	bundle := model.EdgeRouteBundle{
		SchemaVersion: model.BundleSchemaVersionV1, Version: artifact.Generation,
		Generation: artifact.Generation, EdgeGroupID: groupID,
		Routes: make([]model.EdgeRouteBinding, 0, len(snapshot.Routes)),
	}
	used := map[string]bool{}
	for _, intent := range snapshot.Routes {
		switch intent.TargetGroupMode {
		case model.EdgeRouteIntentGroupModeAllGroups:
		case model.EdgeRouteIntentGroupModePinnedGroup:
			if intent.PinnedEdgeGroupID != groupID {
				continue
			}
		default:
			return model.EdgeRouteBundle{}, fmt.Errorf("invalid route target group mode")
		}
		binding := routebinding.FromIntent(intent, groupID)
		binding.RouteGeneration = intent.Generation
		bundle.Routes = append(bundle.Routes, binding)
		used[binding.CachePolicyID] = true
	}
	for _, policy := range snapshot.CachePolicies {
		if used[policy.ID] {
			bundle.CachePolicies = append(bundle.CachePolicies, policy)
		}
	}
	sort.Slice(bundle.CachePolicies, func(i, j int) bool { return bundle.CachePolicies[i].ID < bundle.CachePolicies[j].ID })
	return bundle, nil
}
