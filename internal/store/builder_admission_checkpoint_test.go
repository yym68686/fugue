package store

import (
	"errors"
	"testing"

	"fugue/internal/buildadmission/contract"
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

func TestPostgresBuilderAdmissionCheckpointPortPreservesLegacyKeys(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	s.db.SetMaxOpenConns(4)
	ctx := t.Context()
	port := s.BuilderAdmissionCheckpoints()
	entries := []struct {
		key string
	}{
		{contract.Memory("tenant", "app")},
		{contract.Failure("tenant", "app")},
		{contract.OOM("tenant", "app", "fingerprint")},
	}
	legacy := []byte(`{"version":1,"retained":"legacy"}`)
	for _, entry := range entries {
		t.Run(entry.key, func(t *testing.T) {
			storedKey := observationCheckpointKey(entry.key)
			if _, err := s.db.ExecContext(ctx, `INSERT INTO fugue_meta(key,value) VALUES($1,$2)`, storedKey, string(legacy)); err != nil {
				t.Fatal(err)
			}
			got, err := port.Read(ctx, entry.key)
			if err != nil || string(got) != string(legacy) {
				t.Fatalf("read existing checkpoint: %q, err=%v", got, err)
			}
			ok, err := port.Update(ctx, entry.key, func(raw []byte) ([]byte, error) {
				if string(raw) != string(legacy) {
					t.Fatalf("update received %q, want legacy value %q", raw, legacy)
				}
				var acquired bool
				if err := s.db.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, storedKey).Scan(&acquired); err != nil || acquired {
					t.Fatalf("checkpoint lock was not held through callback: acquired=%t err=%v", acquired, err)
				}
				return append(append([]byte(nil), raw...), []byte("-updated")...), nil
			})
			if !ok || err != nil {
				t.Fatalf("update legacy checkpoint: acquired=%t err=%v", ok, err)
			}
			if ok, err := s.UpdateObservationCheckpoint(ctx, entry.key, func([]byte) ([]byte, error) {
				t.Fatal("generic checkpoint writer bypassed the admission port")
				return nil, nil
			}); ok || !errors.Is(err, errBuilderAdmissionCheckpointScope) {
				t.Fatalf("generic checkpoint writer accepted namespace: acquired=%t err=%v", ok, err)
			}
			got, err = port.Read(ctx, entry.key)
			if err != nil || string(got) != string(append(append([]byte(nil), legacy...), []byte("-updated")...)) {
				t.Fatalf("read updated legacy checkpoint: %q, err=%v", got, err)
			}
		})
	}
}
