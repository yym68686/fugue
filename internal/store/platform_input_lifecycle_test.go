package store

import (
	"context"
	"reflect"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

func TestCompilerInputPairPreservesAuditAndImmutableRegistration(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	for _, migrate := range []func(context.Context, string) error{schemamigrate.MigrateCompilerInputCodec, schemamigrate.MigrateCompilerInputShared, schemamigrate.MigrateCompilerInputSeparated} {
		if err := migrate(t.Context(), address); err != nil {
			t.Fatal(err)
		}
	}
	s := &Store{db: db, databaseURL: address, dbReady: true}
	s.storagePolicy.value = StoragePolicy{Version: 1, CompilerInputChunks: true}
	s.storagePolicy.separated = compilerInputCodecPolicy{Version: 1, Enabled: true}
	original := separatedFixture(t)
	runtime := platformconfig.ConfigurationRuntimeInput(original)
	digest, _ := platformconfig.RuntimeSnapshotDigest(original)
	runtimeDigest, _ := platformconfig.RuntimeSnapshotDigest(runtime)
	for range 2 {
		if err := s.EnsurePlatformCompilerInputPairContext(t.Context(), original, digest, &runtime, runtimeDigest); err != nil {
			t.Fatal(err)
		}
	}
	assertSeparatedEquivalent(t, s, original, digest)
	assertSeparatedEquivalent(t, s, runtime, runtimeDigest)
	var first time.Time
	if err := db.QueryRow(`SELECT registered_at FROM fugue_platform_input_audit_bindings WHERE original_digest=$1 AND runtime_digest=$2`, digest, runtimeDigest).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsurePlatformCompilerInputPairContext(t.Context(), original, digest, &runtime, runtimeDigest); err != nil {
		t.Fatal(err)
	}
	var again time.Time
	if err := db.QueryRow(`SELECT registered_at FROM fugue_platform_input_audit_bindings WHERE original_digest=$1`, digest).Scan(&again); err != nil || !again.Equal(first) {
		t.Fatal("replay extended lifetime", err)
	}
	bad := runtime
	bad.Facts = map[string]any{"unexpected": true}
	if err := s.EnsurePlatformCompilerInputPairContext(t.Context(), original, digest, &bad, runtimeDigest); err == nil {
		t.Fatal("non-equivalent operational input registered")
	}
	for _, days := range []int{0, 1, 30} {
		plan, err := s.PlanCompilerInputAuditRetention(t.Context(), days)
		if err != nil {
			t.Fatal(err)
		}
		if plan.ExecutionAuthorized || len(plan.Entries) != 1 {
			t.Fatal("preview granted deletion", plan)
		}
		if days == 0 && (plan.Entries[0].ExpiresAt != nil || plan.Entries[0].State != "retain-indefinitely") {
			t.Fatal("zero expires immediately")
		}
	}
	got, err := s.GetPlatformCompilerInput(digest)
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatal("preview removed original", err)
	}
}
