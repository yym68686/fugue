package store

import (
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestPlatformArtifactMetadataPreservesFullListing(t *testing.T) {
	t.Run("file", func(t *testing.T) { verifyPlatformArtifactMetadata(t, newPlatformArtifactEnsureTestStore(t)) })
	t.Run("postgres", func(t *testing.T) {
		s := billingBatchPGStore(t)
		configureTestPlatformArtifactSigning(s)
		verifyPlatformArtifactMetadata(t, s)
	})
}

func verifyPlatformArtifactMetadata(t *testing.T, s *Store) {
	t.Helper()
	scope := model.NewID("metadata_scope")
	for _, generation := range []string{"generation_a", "generation_b"} {
		input := platformArtifactEnsureFixture(generation, 100)
		input.Scope = model.PlatformArtifactScope{ScopeType: "global", Key: scope}
		input.Content["diagnostic_payload"] = strings.Repeat("complete payload ", 10000)
		artifact, created, err := s.EnsurePlatformArtifact(input)
		if err != nil || !created {
			t.Fatalf("create artifact: created=%v error=%v", created, err)
		}
		if s.usingDatabase() {
			t.Cleanup(func() {
				if _, err := s.db.Exec(`DELETE FROM fugue_platform_artifacts WHERE id=$1`, artifact.ID); err != nil {
					t.Error(err)
				}
			})
		}
	}
	for _, limit := range []int{1, 10} {
		filter := model.PlatformArtifactFilter{ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ScopeKey: " " + strings.ToUpper(scope) + " ", Limit: limit}
		full, err := s.ListPlatformArtifacts(filter)
		if err != nil {
			t.Fatal(err)
		}
		metadata, err := s.ListPlatformArtifactMetadata(filter)
		if err != nil {
			t.Fatal(err)
		}
		if len(full) == 0 || len(metadata) != len(full) {
			t.Fatalf("full=%d metadata=%d", len(full), len(metadata))
		}
		for i, original := range full {
			if original.Content["diagnostic_payload"] == nil {
				t.Fatal("full listing lost content")
			}
			original.Content = nil
			if !reflect.DeepEqual(original, metadata[i]) {
				t.Fatalf("metadata changed fields or ordering at row %d", i)
			}
			detail, err := s.GetPlatformArtifact(original.ID)
			if err != nil || detail.Content["diagnostic_payload"] == nil {
				t.Fatalf("metadata read changed durable content: %v", err)
			}
		}
	}
}
