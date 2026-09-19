package routeartifact

import (
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
	"fugue/internal/routebinding"
	"fugue/internal/trafficbinding"
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
	return MaterializeSnapshotForGroup(snapshot, groupID)
}

func MaterializeSnapshotForGroup(snapshot model.EdgeRouteIntentSnapshot, groupID string) (model.EdgeRouteBundle, error) {
	if !platformRouteArtifactGroupID.MatchString(groupID) || snapshot.Generation == "" {
		return model.EdgeRouteBundle{}, fmt.Errorf("route materialization requires group and generation")
	}
	if err := trafficbinding.ValidateProjection(snapshot); err != nil {
		return model.EdgeRouteBundle{}, err
	}
	if err := trafficbinding.ValidateGroup(snapshot.TrafficRelease, groupID, false); err != nil {
		return model.EdgeRouteBundle{}, err
	}
	if len(snapshot.Routes) == 0 {
		return model.EdgeRouteBundle{}, fmt.Errorf("candidate route index is empty")
	}
	bundle := model.EdgeRouteBundle{
		SchemaVersion: model.BundleSchemaVersionV1, Version: snapshot.Generation,
		Generation: snapshot.Generation, TrafficRelease: trafficbinding.Clone(snapshot.TrafficRelease), EdgeGroupID: groupID,
		Routes: make([]model.EdgeRouteBinding, 0, len(snapshot.Routes)),
	}
	used := map[string]bool{}
	hosts := map[string]bool{}
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
		hosts[binding.Hostname] = true
		used[binding.CachePolicyID] = true
	}
	for _, policy := range snapshot.CachePolicies {
		if used[policy.ID] {
			bundle.CachePolicies = append(bundle.CachePolicies, policy)
		}
	}
	for _, entry := range snapshot.TLSAllowlist {
		if hosts[entry.Hostname] {
			bundle.TLSAllowlist = append(bundle.TLSAllowlist, entry)
		}
	}
	sort.Slice(bundle.CachePolicies, func(i, j int) bool { return bundle.CachePolicies[i].ID < bundle.CachePolicies[j].ID })
	return bundle, nil
}
