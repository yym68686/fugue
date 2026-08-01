package api

import (
	"testing"

	"fugue/internal/model"
)

type edgeRouteHeartbeatStore interface {
	UpdateEdgeHeartbeat(model.EdgeNode) (model.EdgeNode, model.EdgeGroup, error)
	PutEdgeActiveEpoch(model.EdgeActiveEpoch) (model.EdgeActiveEpoch, error)
	UpdateEdgeInstanceHeartbeat(model.EdgeNodeInstance) (model.EdgeNodeInstance, error)
}

func recordActiveEdgeHeartbeatForAPITest(t *testing.T, storeState edgeRouteHeartbeatStore, node model.EdgeNode) error {
	t.Helper()
	if _, _, err := storeState.UpdateEdgeHeartbeat(node); err != nil {
		return err
	}
	epoch := "test-" + node.EdgeGroupID
	if _, err := storeState.PutEdgeActiveEpoch(model.EdgeActiveEpoch{
		EdgeGroupID: node.EdgeGroupID, Slot: model.EdgeSlotDirect, ReleaseEpoch: epoch, FenceSequence: 1, MinHealthyInstances: 1,
	}); err != nil {
		return err
	}
	instance := model.EdgeNodeInstance{
		EdgeID: node.ID, EdgeGroupID: node.EdgeGroupID, Slot: model.EdgeSlotDirect,
		InstanceUID: "test-" + node.ID, ReleaseEpoch: epoch, Node: node,
	}
	for observation := 0; observation < 2; observation++ {
		if _, err := storeState.UpdateEdgeInstanceHeartbeat(instance); err != nil {
			return err
		}
	}
	return nil
}
