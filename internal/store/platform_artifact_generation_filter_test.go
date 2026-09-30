package store

import (
	"errors"
	"fugue/internal/model"
	"testing"
)

func TestPlatformArtifactExactGenerationFiltersBeforeLimit(t *testing.T) {
	t.Run("file", func(t *testing.T) { verifyPlatformArtifactGenerationFilter(t, newPlatformArtifactEnsureTestStore(t)) })
	t.Run("postgres", func(t *testing.T) {
		s := billingBatchPGStore(t)
		configureTestPlatformArtifactSigning(s)
		verifyPlatformArtifactGenerationFilter(t, s)
	})
}

func verifyPlatformArtifactGenerationFilter(t *testing.T, s *Store) {
	t.Helper()
	scope := model.NewID("generation_filter")
	create := func(generation, area, kind string) model.PlatformArtifact {
		in := platformArtifactEnsureFixture(generation, 100)
		in.Scope = model.PlatformArtifactScope{ScopeType: "global", Key: area}
		in.ArtifactKind = kind
		a, err := s.CreatePlatformArtifact(in)
		if err != nil {
			t.Fatal(err)
		}
		if s.usingDatabase() {
			t.Cleanup(func() {
				if _, err := s.db.Exec(`DELETE FROM fugue_platform_artifacts WHERE id=$1`, a.ID); err != nil {
					t.Error(err)
				}
			})
		}
		return a
	}
	kind := model.PlatformArtifactKindEdgeRouteBundle
	one := create("TargetCase", scope, kind)
	create("unrelated-newer", scope, kind)
	create("TargetCase", scope+"-other", kind)
	create("TargetCase", scope, model.PlatformArtifactKindPolicySnapshot)
	for _, generation := range []string{" TargetCase ", "targetcase", "missing"} {
		filter := model.PlatformArtifactFilter{ArtifactKind: kind, ScopeKey: scope, Generation: generation, Limit: 1}
		full, err := s.ListPlatformArtifacts(filter)
		if err != nil {
			t.Fatal(err)
		}
		metadata, err := s.ListPlatformArtifactMetadata(filter)
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		if generation == " TargetCase " {
			want = 1
		}
		if len(full) != want || len(metadata) != want {
			t.Fatal("exact filter changed case/scope/kind or applied limit first", generation, len(full), len(metadata))
		}
		if want == 1 && (full[0].ID != one.ID || full[0].Content == nil || metadata[0].ID != one.ID || metadata[0].Content != nil) {
			t.Fatal("generation query lost content/metadata projection")
		}
	}
	wantDuplicates, wantRows := 2, 3
	if s.usingDatabase() {
		in := platformArtifactEnsureFixture("TargetCase", 100)
		in.Scope = model.PlatformArtifactScope{ScopeType: "global", Key: scope}
		if _, err := s.CreatePlatformArtifact(in); !errors.Is(err, ErrConflict) {
			t.Fatal("PostgreSQL generation uniqueness changed", err)
		}
		wantDuplicates, wantRows = 1, 2
	} else {
		create("TargetCase", scope, kind)
	}
	duplicates, err := s.ListPlatformArtifacts(model.PlatformArtifactFilter{ArtifactKind: kind, ScopeKey: scope, Generation: "TargetCase", Limit: 2})
	if err != nil || len(duplicates) != wantDuplicates {
		t.Fatal("exact lookup concealed ambiguous generations", len(duplicates), err)
	}
	rows, err := s.ListPlatformArtifacts(model.PlatformArtifactFilter{ArtifactKind: kind, ScopeKey: scope, Limit: 10})
	if err != nil || len(rows) != wantRows {
		t.Fatal("omitted generation changed original listing", len(rows), err)
	}
}
