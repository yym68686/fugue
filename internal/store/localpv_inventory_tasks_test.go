package store

import (
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestGenericNodeTaskWriterCannotActForLocalPVInventoryScheduler(t *testing.T) {
	var state Store
	_, err := state.CreateNodeUpdateTask(model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   localPVInventorySchedulerActor,
	}, "updater", "node", "runtime", model.NodeUpdateTaskTypeReportLocalPV, nil)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("generic task writer accepted a scoped scheduler actor: %v", err)
	}
}
