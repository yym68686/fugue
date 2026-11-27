package api

import (
	"testing"

	"fugue/internal/store"
)

func TestRetainedTrafficCompleteInputProof(t *testing.T) {
	s, _, _, _ := retainedTrafficAPIFixture(t)
	source, err := s.observeRetainedTrafficSource(t.Context(), "global")
	if err != nil {
		t.Fatal(err)
	}
	for _, read := range []store.PlatformInputReadSelection{
		{ArtifactID: source.parent.ID, ContentHash: source.parent.ContentHash},
		{ArtifactID: source.route.ID, ContentHash: source.route.ContentHash},
		{ArtifactID: source.inputs[0].ID, ContentHash: source.inputs[0].ContentHash},
		{ArtifactID: source.inputs[1].ID, ContentHash: source.inputs[1].ContentHash},
		{ArtifactID: source.parent.ID, ContentHash: source.parent.ContentHash, ReleaseID: source.release.ID},
		{LKGKind: source.lkg.ArtifactKind, LKGScope: source.lkg.ScopeKey, LKG: &source.lkg},
	} {
		if err := s.store.CheckPlatformInputRead(t.Context(), []store.PlatformInputReadSelection{read}); err != nil {
			t.Errorf("original selected dependency proof refused %+v: %v", read, err)
		}
	}
}
