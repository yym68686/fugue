package store

import (
	"fugue/internal/model"
	"path/filepath"
	"strings"
	"testing"
)

func TestImageRetirementSurvivesMetadataRemovalAndRevivalWins(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	im, err := s.UpsertImage(model.Image{ImageRef: "registry.example/demo@" + digest, CanonicalDigest: digest, LifecycleState: model.ImageLifecycleDeleting})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate metadata retention without discarding independent retirement evidence.
	if err := s.withLockedState(true, func(state *model.State) error { state.Images = nil; return nil }); err != nil {
		t.Fatal(err)
	}
	all, err := s.ListImagesWithRetirementAuthority()
	if err != nil || len(all) != 1 || all[0].ID != im.ID {
		t.Fatalf("retirement lost: %+v %v", all, err)
	}
	im.LifecycleState = model.ImageLifecycleAvailable
	if _, err := s.UpsertImage(im); err != nil {
		t.Fatal(err)
	}
	all, err = s.ListImagesWithRetirementAuthority()
	if err != nil || len(all) != 1 || all[0].LifecycleState != model.ImageLifecycleAvailable {
		t.Fatalf("stale tombstone beat live generation: %+v %v", all, err)
	}
	archived, err := s.ListImageRetirements()
	if err != nil || len(archived) != 1 || archived[0].LifecycleState != model.ImageLifecycleDeleting {
		t.Fatalf("immutable archive overwritten: %+v %v", archived, err)
	}
}
