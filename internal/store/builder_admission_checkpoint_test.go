package store

import (
	"errors"
	"testing"
)

func TestBuilderAdmissionCheckpointNamespaceCannotBeBypassed(t *testing.T) {
	// A zero Store ensures unauthorized calls reject before they reach SQL.
	s := &Store{}
	ctx := t.Context()
	write := func([]byte) ([]byte, error) {
		t.Fatal("out-of-scope checkpoint callback ran")
		return nil, nil
	}
	for _, key := range []string{
		"builder-memory/v1/tenant/app",
		"builder-permanent-failures/v1/tenant/app",
		"builder-oom-history/v1/tenant/app/fingerprint",
	} {
		if ok, err := s.UpdateObservationCheckpoint(ctx, key, write); ok || !errors.Is(err, errBuilderAdmissionCheckpointScope) {
			t.Fatalf("generic writer accepted %q: %t %v", key, ok, err)
		}
	}
	for _, key := range []string{backupInventoryCheckpointPrefix + "tenant", backupInventoryOwnerKey} {
		if ok, err := s.BuilderAdmissionCheckpoints().Update(ctx, key, write); ok || !errors.Is(err, errBuilderAdmissionCheckpointScope) {
			t.Fatalf("admission writer accepted backup key %q: %t %v", key, ok, err)
		}
	}
}
