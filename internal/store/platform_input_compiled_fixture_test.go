package store

import (
	"context"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
	"fugue/internal/testfixture/celldns"
)

func prepareCompilerInputTestSchema(t *testing.T, s *Store) {
	t.Helper()
	if s.db == nil {
		return
	}
	for _, migrate := range []func(context.Context, string) error{schemamigrate.MigrateBoundedContent, schemamigrate.MigrateCompilerInputArchive, schemamigrate.MigrateCompilerInputCodec, schemamigrate.MigrateCompilerInputShared, schemamigrate.MigrateCompilerInputSeparated} {
		if err := migrate(t.Context(), s.databaseURL); err != nil {
			t.Fatal("install production compiler input representation schema", err)
		}
	}
	// The disposable test administrator installs the same fixed guard as CI.
	if _, err := s.db.Exec(schemamigrate.CompilerInputZeroTempSQL); err != nil {
		t.Fatal("install compiler input zero-temp guard", err)
	}
	enforceCompilerInputHistoryTestPhase(t, s)
}

func prepareAuthorizedCellDNSPublication(t *testing.T, s *Store, transition bool) (platformconfig.CompileRequest, []model.PlatformArtifact, []model.PlatformArtifactRelease) {
	t.Helper()
	if s.db == nil {
		return celldns.SourceAuthorizedRequest(t, transition)
	}
	req, policies, releases, inputs, sources := celldns.CompletePublication(t, transition, true)
	for _, input := range inputs {
		prepareCompiledInputPin(t, s, input)
		if _, _, err := s.EnsurePlatformArtifact(input.PolicyArtifact); err != nil {
			t.Fatal(err)
		}
	}
	for _, source := range sources {
		if _, _, err := s.EnsurePlatformArtifact(source); err != nil {
			t.Fatal(err)
		}
	}
	return req, policies, releases
}

// Persist exactly the normalized compiler input, using the real separated
// writer and full-readback pin protocol. Never synthesize verified pin rows or
// remove lineage to make a release admissible. File-store compatibility tests
// keep their existing, separate persistence contract.
func prepareCompiledInputPin(t *testing.T, s *Store, compiled platformconfig.CompileResult) {
	t.Helper()
	if s.db == nil {
		return
	}
	prepareCompilerInputTestSchema(t, s)
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED", "true")
	s.storagePolicy.mu.Lock()
	s.storagePolicy.value.CompilerInputChunks = true
	s.storagePolicy.separated = compilerInputCodecPolicy{Version: 1, Enabled: true}
	s.storagePolicy.mu.Unlock()
	digest := compiled.Lineage.InputSnapshotDigest
	if err := s.EnsurePlatformCompilerInput(compiled.InputSnapshot, digest); err != nil {
		t.Fatal("persist normalized compiler input", err)
	}
	if pin, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{
		Key: "fixture/compiled/" + digest[7:], Digest: digest, MaxRawBytes: 8 << 20, Apply: true,
	}); err != nil || !pin.Verified {
		t.Fatal("complete independent compiler input readback", pin, err)
	}
}
