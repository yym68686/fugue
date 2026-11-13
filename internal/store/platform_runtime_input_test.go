package store

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

func TestConfigurationRuntimeRetentionStillProtectsOriginalProvenancePostgres(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "intent"}, Policy: platformconfig.PolicySnapshot{Generation: "policy", RuntimeInputMode: platformconfig.RuntimeInputConfigurationV1},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{Facts: map[string]any{"audit": "original"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Retention must resolve the original compiler sources as well as the
	// independently stored runtime input. A source absent from the ledger is
	// an incomplete inventory, even when both snapshots are present.
	for _, source := range []model.PlatformArtifact{compiled.IntentArtifact, compiled.PolicyArtifact} {
		source, err := normalizePlatformArtifactForStore(source)
		if err != nil {
			t.Fatal(err)
		}
		content, _ := json.Marshal(source.Content)
		metadata, _ := json.Marshal(source.Metadata)
		if _, err := db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,content_json,metadata_json,created_at,updated_at)
 VALUES($1,$2,$3,$4,'validated',$5,$6,$7,now(),now())`, source.ID, source.ArtifactKind, source.ScopeKey, source.Generation, source.ContentHash, content, metadata); err != nil {
			t.Fatal(err)
		}
	}
	a, err := normalizePlatformArtifactForStore(compiled.RouteArtifact)
	if err != nil {
		t.Fatal(err)
	}
	content, _ := json.Marshal(a.Content)
	metadata, _ := json.Marshal(a.Metadata)
	if _, err := db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,content_json,metadata_json,created_at,updated_at)
 VALUES($1,$2,$3,$4,'validated',$5,$6,$7,now(),now())`, a.ID, a.ArtifactKind, a.ScopeKey, a.Generation, a.ContentHash, content, metadata); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO fugue_platform_lkg_snapshots(id,artifact_id,artifact_kind,scope_key,generation,content_hash,expires_at,created_at,updated_at)
 VALUES('runtime-lkg',$1,$2,$3,$4,$5,now()+interval '1 day',now(),now())`, a.ID, a.ArtifactKind, a.ScopeKey, a.Generation, a.ContentHash); err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range []platformconfig.RuntimeSnapshot{compiled.InputSnapshot, *compiled.RuntimeInput} {
		digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(snapshot)
		if _, err := db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,storage_location,created_at)
 VALUES($1,$2,$3,262144,1,'hot',now())`, digest, inputFormatV1, len(raw)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO fugue_platform_input_hot_chunks(content_hash,ordinal,chunk_hash,data) VALUES($1,0,$1,$2)`, digest, raw); err != nil {
			t.Fatal(err)
		}
	}
	report, err := RunCompilerInputRetentionInventory(t.Context(), address)
	if err != nil || !report.UniverseComplete || report.Totals["H"].Inputs != 2 || report.Totals["D"].Inputs != 0 || report.ExecutionAuthorized {
		t.Fatal("operational independence released original provenance protection", report, err)
	}
	if _, err := db.Exec(`DELETE FROM fugue_platform_artifacts WHERE artifact_kind=$1 AND generation=$2`, model.PlatformArtifactKindPlatformIntent, compiled.IntentArtifact.Generation); err != nil {
		t.Fatal(err)
	}
	report, err = RunCompilerInputRetentionInventory(t.Context(), address)
	if err == nil || report.UniverseComplete || report.ClassificationComplete || len(report.Entries) != 0 || report.ExecutionAuthorized {
		t.Fatal("missing original compiler source was treated as disposable or a complete inventory", report, err)
	}
}

func TestConfigurationRuntimeColdFileReadIsIndependentAndFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	keys := bundleauth.NewKeyring("synthetic-runtime-input-key", "runtime", "", "", nil)
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	s.ConfigurePlatformArtifactSigning(keys)
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{
		Intent:          platformconfig.PlatformIntent{Generation: "intent"},
		Policy:          platformconfig.PolicySnapshot{Generation: "policy", RuntimeInputMode: platformconfig.RuntimeInputConfigurationV1},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{Facts: map[string]any{"audit": strings.Repeat("original", 10000)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsurePlatformCompilerInput(compiled.InputSnapshot, compiled.Lineage.InputSnapshotDigest); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsurePlatformCompilerInput(*compiled.RuntimeInput, compiled.Lineage.RuntimeInputDigest); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, a := range []model.PlatformArtifact{compiled.IntentArtifact, compiled.PolicyArtifact, compiled.RouteArtifact, compiled.DNSArtifact, compiled.TLSArtifact} {
		stored, _, err := s.EnsurePlatformArtifact(a)
		if err != nil {
			t.Fatal(err)
		}
		if a.ArtifactKind != model.PlatformArtifactKindPlatformIntent && a.ArtifactKind != model.PlatformArtifactKindPolicySnapshot {
			ids = append(ids, stored.ID)
		}
	}
	parent, _, err := s.EnsurePlatformArtifact(platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, ids, time.Now().UTC()))
	if err != nil {
		t.Fatal(err)
	}
	// Delete only the original synthetic audit input in this disposable fixture.
	// No retirement permission or production obligation is inferred by this test.
	if err := s.withLockedState(true, func(st *model.State) error {
		st.PlatformArtifactContents = slices.DeleteFunc(st.PlatformArtifactContents, func(c model.PlatformArtifactContent) bool {
			return c.ContentHash == compiled.Lineage.InputSnapshotDigest
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s = New(path) // empty process caches, persisted signed artifacts only
	s.ConfigurePlatformArtifactSigning(keys)
	parent, err = s.GetPlatformArtifact(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	reads := []PlatformInputReadSelection{{ArtifactID: parent.ID, ContentHash: parent.ContentHash}}
	if err := s.CheckPlatformInputRead(t.Context(), reads); err != nil {
		t.Fatal("operational closure still reads original history", err)
	}
	if err := s.CheckPlatformInputRead(t.Context(), []PlatformInputReadSelection{{ArtifactID: parent.ID, ContentHash: parent.ContentHash, OriginalInput: true}}); err == nil {
		t.Fatal("full-history disclosure silently dropped original input protection")
	}
	if _, err := s.GetPlatformCompilerInput(compiled.Lineage.InputSnapshotDigest); !errors.Is(err, ErrNotFound) {
		t.Fatal("original unexpectedly readable", err)
	}
	got, err := s.GetPlatformArtifactRuntimeInput(parent)
	if err != nil || got.Facts != nil || got.IntentGeneration != compiled.Lineage.IntentGeneration {
		t.Fatal("cold runtime read failed", err)
	}
	// A restored full original must not hide a corrupt operational input.
	if err := s.EnsurePlatformCompilerInput(compiled.InputSnapshot, compiled.Lineage.InputSnapshotDigest); err != nil {
		t.Fatal(err)
	}
	if err := s.withLockedState(true, func(st *model.State) error {
		for i := range st.PlatformArtifactContents {
			if st.PlatformArtifactContents[i].ContentHash == compiled.Lineage.RuntimeInputDigest {
				st.PlatformArtifactContents[i].Content["policy_generation"] = "corrupt"
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetPlatformArtifactRuntimeInput(parent); err == nil {
		t.Fatal("corrupt runtime input fell back to full history")
	}
	if err := s.CheckPlatformInputRead(t.Context(), reads); err == nil {
		t.Fatal("corrupt runtime input admitted")
	}
}

func TestConfigurationRuntimeUsesSeparatedPGWithoutOriginalChunks(t *testing.T) {
	s := separatedTestStore(t)
	keys := bundleauth.NewKeyring("synthetic-runtime-pg-key", "runtime", "", "", nil)
	s.ConfigurePlatformArtifactSigning(keys)
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "intent"}, Policy: platformconfig.PolicySnapshot{Generation: "policy", RuntimeInputMode: platformconfig.RuntimeInputConfigurationV1},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{Facts: map[string]any{"original": strings.Repeat("audit", 10000)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Never persist the old snapshot. This is local storage/read-path coverage,
	// not a representative confidential recovery or full business E2E proof.
	if err := s.EnsurePlatformCompilerInput(*compiled.RuntimeInput, compiled.Lineage.RuntimeInputDigest); err != nil {
		t.Fatal(err)
	}
	parent, err := normalizePlatformArtifactForStore(compiled.ReleaseArtifact)
	if err != nil {
		t.Fatal(err)
	}
	parent.GenerationSequence = 1
	parent, err = platformsafety.SignPlatformArtifact(parent, keys)
	if err != nil {
		t.Fatal(err)
	}
	fresh := &Store{db: s.db, databaseURL: s.databaseURL, dbReady: true}
	fresh.ConfigurePlatformArtifactSigning(keys)
	if _, err := fresh.GetPlatformCompilerInput(compiled.Lineage.InputSnapshotDigest); !errors.Is(err, ErrNotFound) {
		t.Fatal("original unexpectedly exists", err)
	}
	if got, err := fresh.GetPlatformArtifactRuntimeInput(parent); err != nil || got.Facts != nil {
		t.Fatal("separated PG cold read required original", err)
	}
}
