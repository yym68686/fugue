package api

import (
	"encoding/json"
	"os"
	"testing"

	"fugue/internal/model"
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

func TestRetainedTrafficMissingOriginalOrSiblingRefusesWithoutRenewing(t *testing.T) {
	s, _, parent, path := retainedTrafficAPIFixture(t)
	original, err := s.readRetainedTrafficSource(t.Context(), "global")
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"missing input", "corrupt input", "input size", "missing sibling", "duplicate latest LKG"} {
		t.Run(scenario, func(t *testing.T) {
			var st model.State
			if err := json.Unmarshal(baseline, &st); err != nil {
				t.Fatal(err)
			}
			for i := range st.PlatformArtifactContents {
				if st.PlatformArtifactContents[i].ContentHash != parent.Metadata["input_snapshot_digest"] {
					continue
				}
				switch scenario {
				case "missing input":
					st.PlatformArtifactContents = append(st.PlatformArtifactContents[:i], st.PlatformArtifactContents[i+1:]...)
				case "corrupt input":
					st.PlatformArtifactContents[i].Content = map[string]any{"intent_generation": "wrong"}
				case "input size":
					st.PlatformArtifactContents[i].SizeBytes++
				}
				break
			}
			if scenario == "missing sibling" {
				for i, a := range st.PlatformArtifacts {
					if a.ArtifactKind == model.PlatformArtifactKindCaddyRouteConfig {
						st.PlatformArtifacts = append(st.PlatformArtifacts[:i], st.PlatformArtifacts[i+1:]...)
						break
					}
				}
			}
			if scenario == "duplicate latest LKG" {
				st.PlatformLKGSnapshots = append(st.PlatformLKGSnapshots, original.lkg)
			}
			body, _ := json.Marshal(st)
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := s.readRetainedTrafficSource(t.Context(), "global"); err == nil {
				t.Fatal("retained source bypassed full original closure", scenario)
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != string(body) {
				t.Fatal("retained source restored data or renewed its observation", err)
			}
		})
	}
}
