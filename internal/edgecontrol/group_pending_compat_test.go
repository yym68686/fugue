package edgecontrol

import (
	"errors"
	"fugue/internal/inputarchive"
	"testing"
)

func TestRetiredPendingRetentionDoesNotEraseLegacyRequirements(t *testing.T) {
	for _, state := range []persistentGroupState{
		{PendingRootsRequired: true},
		{PendingRootRecords: []GroupPendingRootRecord{{Receipt: inputarchive.PendingRootReceipt{RegistryRevision: 1}}}},
	} {
		if !errors.Is(validateGroupPendingRoots(state), ErrGroupPendingRoot) {
			t.Fatal("invalid legacy retention state treated as empty")
		}
	}
	if err := validateGroupPendingRoots(persistentGroupState{}); err != nil {
		t.Fatal(err)
	}
}
