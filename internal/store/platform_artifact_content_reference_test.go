package store

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func TestArtifactContentReferencePostgresReadsSignaturesAndRollback(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	s := &Store{db: db, databaseURL: address, dbReady: true}
	configureTestPlatformArtifactSigning(s)
	for _, enabled := range []bool{false, true, false} {
		s.storagePolicy.artifactReferences = compilerInputCodecPolicy{Version: 1, Enabled: enabled}
		input := platformArtifactEnsureFixture(model.NewID("generation"), 100)
		input.Content["hostname"] = "reference.example.test"
		a, err := s.CreatePlatformArtifact(input)
		if err != nil {
			t.Fatal(err)
		}
		var inline bool
		var ref *string
		if err := db.QueryRow(`SELECT content_json IS NOT NULL,content_ref_hash FROM fugue_platform_artifacts WHERE id=$1`, a.ID).Scan(&inline, &ref); err != nil || inline == enabled || enabled && (ref == nil || *ref != a.ContentHash) {
			t.Fatal("incorrect physical representation", inline, ref, err)
		}
		expect, _ := json.Marshal(a)
		check := func(got model.PlatformArtifact, err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
			if err := s.VerifyPlatformArtifactIntegrity(got); err != nil {
				t.Fatal("signature changed", err)
			}
			actual, _ := json.Marshal(got)
			if string(actual) != string(expect) {
				t.Fatal("artifact API changed")
			}
		}
		check(s.GetPlatformArtifact(a.ID))
		check(s.GetPlatformArtifact(a.ID)) // conditional body-elision/cache path
		check(s.pgGetPlatformArtifactByIdentity(a.ArtifactKind, a.ScopeKey, a.Generation))
		check(pgGetPlatformArtifactForUpdate(t.Context(), db, a.ID, true))
		check(pgGetPlatformArtifactByGenerationForUpdate(t.Context(), db, a.ArtifactKind, a.ScopeKey, a.Generation))
		check(readCompilerSourceArtifact(t.Context(), db, "id=$1", a.ID))
		items, err := s.ListPlatformArtifacts(model.PlatformArtifactFilter{Generation: a.Generation})
		if err != nil || len(items) != 1 {
			t.Fatal(err)
		}
		check(items[0], nil)
		metadata, err := s.pgListPlatformArtifactsView(model.PlatformArtifactFilter{Generation: a.Generation}, false)
		if err != nil || len(metadata) != 1 || metadata[0].Content != nil {
			t.Fatal("metadata read hydrated body", err)
		}
		validated, err := s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "synthetic", Pass: true}})
		if err != nil || !reflect.DeepEqual(validated.Content, a.Content) {
			t.Fatal("validation lost body", err)
		}
		if enabled {
			if _, err := db.Exec(`DELETE FROM fugue_platform_artifact_contents WHERE content_hash=$1`, a.ContentHash); err == nil {
				t.Fatal("referenced body deletion accepted")
			}
			// Reader-only rollback does not depend on the writer policy.
			rollback := &Store{db: db, databaseURL: address, dbReady: true}
			got, err := rollback.GetPlatformArtifact(a.ID)
			if err != nil || !reflect.DeepEqual(got.Content, a.Content) {
				t.Fatal("reader-only rollback failed", err)
			}
		}
		if err := schemamigrate.MigratePlatformArtifactContent(t.Context(), address); err != nil {
			t.Fatal("idempotent migration", err)
		}
	}
}

func TestArtifactContentReferenceMissingAndCorruptBodiesFailClosed(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	s := &Store{db: db, databaseURL: address, dbReady: true}
	configureTestPlatformArtifactSigning(s)
	s.storagePolicy.artifactReferences = compilerInputCodecPolicy{Version: 1, Enabled: true}
	a, err := s.CreatePlatformArtifact(platformArtifactEnsureFixture("reference-corruption", 100))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE fugue_platform_artifact_contents SET content_json='{"corrupt":true}' WHERE content_hash=$1`, a.ContentHash); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetPlatformArtifact(a.ID)
	if err == nil && s.VerifyPlatformArtifactIntegrity(got) == nil {
		t.Fatal("corrupt CAS passed signature")
	}
	if _, _, err := s.EnsurePlatformArtifact(platformArtifactEnsureFixture("reference-corruption", 100)); !errors.Is(err, ErrConflict) {
		t.Fatal("corrupt reuse accepted", err)
	}
	if _, err := db.Exec(`UPDATE fugue_platform_artifact_contents SET content_kind='' WHERE content_hash=$1`, a.ContentHash); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetPlatformArtifact(a.ID); err == nil {
		t.Fatal("missing/wrong-kind body became empty artifact")
	}
}

func TestArtifactContentDeduplicationBoundedAndPreservesSignedHistory(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	s := &Store{db: db, databaseURL: address, dbReady: true}
	configureTestPlatformArtifactSigning(s)
	artifacts := []model.PlatformArtifact{}
	for range 5 {
		a, err := s.CreatePlatformArtifact(platformArtifactEnsureFixture(model.NewID("generation"), 100))
		if err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, a)
	}
	if count, err := s.deduplicatePlatformArtifactContent(t.Context()); err != nil || count != 0 {
		t.Fatal("inactive writer migrated history", count, err)
	}
	s.storagePolicy.artifactReferences = compilerInputCodecPolicy{Version: 1, Enabled: true}
	count, err := s.deduplicatePlatformArtifactContent(t.Context())
	if err != nil || count != 4 {
		t.Fatal("batch bound", count, err)
	}
	count, err = s.deduplicatePlatformArtifactContent(t.Context())
	if err != nil || count != 1 {
		t.Fatal("durable keyset resume", count, err)
	}
	for _, before := range artifacts {
		after, err := s.GetPlatformArtifact(before.ID)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := json.Marshal(before)
		got, _ := json.Marshal(after)
		if string(want) != string(got) || s.VerifyPlatformArtifactIntegrity(after) != nil {
			t.Fatal("migration changed signed history")
		}
		var reference bool
		if err := db.QueryRow(`SELECT content_json IS NULL AND content_ref_hash=content_hash FROM fugue_platform_artifacts WHERE id=$1`, before.ID).Scan(&reference); err != nil || !reference {
			t.Fatal("inline duplicate remained", err)
		}
	}
}
