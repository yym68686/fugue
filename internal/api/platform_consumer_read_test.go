package api

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestConsumerArtifactReadsAreRequestScopedAndDoNotCacheFailures(t *testing.T) {
	calls := 0
	value := model.PlatformArtifact{ID: "child", Status: model.PlatformArtifactStatusValidated}
	load := func(id string) (model.PlatformArtifact, error) {
		calls++
		if id != "child" {
			return model.PlatformArtifact{}, store.ErrNotFound
		}
		return value, nil
	}
	read := newConsumerArtifactReader(load)
	first, err := read(" child ")
	if err != nil {
		t.Fatal(err)
	}
	second, err := read("child")
	if err != nil || !reflect.DeepEqual(first, second) || calls != 1 {
		t.Fatal("duplicate read not eliminated", calls, err)
	}
	for i := 0; i < 2; i++ {
		if _, err := read("absent"); !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
	}
	if calls != 3 {
		t.Fatal("failed lookup was cached")
	}
	value.Status = model.PlatformArtifactStatusDraft
	next, err := newConsumerArtifactReader(load)("child")
	if err != nil || next.Status != model.PlatformArtifactStatusDraft || calls != 4 {
		t.Fatal("request reused a previous validation observation")
	}
}

func TestConsumerArtifactReadReusePreservesReferenceValidation(t *testing.T) {
	parent := model.PlatformArtifact{Generation: "release-a", Content: map[string]any{"artifact_ids": []any{"child"}, "artifact_kinds": []any{model.PlatformArtifactKindEdgeRouteBundle}}}
	child := model.PlatformArtifact{ID: "child", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, Status: model.PlatformArtifactStatusValidated, Metadata: map[string]string{"release_set_generation": "release-a"}, Content: map[string]any{"policy": nil}}
	calls := 0
	load := func(string) (model.PlatformArtifact, error) { calls++; return child, nil }
	read := newConsumerArtifactReader(load)
	result := validateReleaseSetReferences(parent, read)
	if !result.Pass {
		t.Fatal(result)
	}
	selected, err := consumerAssignmentChild(parent, child.ArtifactKind, read)
	if err != nil || !reflect.DeepEqual(selected, child) {
		t.Fatal(selected, err)
	}
	if again := validateReleaseSetReferences(parent, read); !reflect.DeepEqual(result, again) || calls != 1 {
		t.Fatal("reference validation changed or reread child", again, calls)
	}
	child.Status = model.PlatformArtifactStatusDraft
	if result := validateReleaseSetReferences(parent, newConsumerArtifactReader(load)); result.Pass {
		t.Fatal("next request accepted invalid child")
	}
}

func TestServingAssignmentSelectionRereadsOnlyAnEmptyServingIntersection(t *testing.T) {
	old := consumerArtifactLookup{Release: model.PlatformArtifactRelease{ID: "previous", ReleaseChannel: "full"}}
	next := consumerArtifactLookup{Release: model.PlatformArtifactRelease{ID: "next", ReleaseChannel: "gray"}}
	for _, mode := range []string{"advanced", "removed", "persistent", "error", "shadow"} {
		t.Run(mode, func(t *testing.T) {
			reads, selections := 0, 0
			initial := old
			if mode == "shadow" {
				initial.Release.ReleaseChannel = "shadow"
			}
			got, err := selectServingConsumerAssignments([]consumerArtifactLookup{initial}, func() ([]consumerArtifactLookup, error) {
				reads++
				if mode == "removed" {
					return nil, nil
				}
				return []consumerArtifactLookup{next}, nil
			}, func(item consumerArtifactLookup) (bool, error) {
				selections++
				if mode == "error" {
					return false, store.ErrConflict
				}
				return mode == "advanced" && item.Release.ID == next.Release.ID, nil
			})
			switch mode {
			case "advanced":
				if err != nil || reads != 1 || selections != 2 || len(got) != 1 || got[0].Release.ID != next.Release.ID {
					t.Fatal("new assignment lost", got, err, reads, selections)
				}
			case "removed":
				if err != nil || reads != 1 || len(got) != 0 {
					t.Fatal("removed assignment resurrected", got, err, reads)
				}
			case "persistent":
				if err != nil || reads != 2 || selections != 3 || len(got) != 0 {
					t.Fatal("selection retry unbounded", got, err, reads, selections)
				}
			case "error":
				if !errors.Is(err, store.ErrConflict) || reads != 0 || len(got) != 0 {
					t.Fatal("stable error retried or accepted", got, err, reads)
				}
			case "shadow":
				if err != nil || reads != 0 || selections != 0 || len(got) != 0 {
					t.Fatal("shadow became serving", got, err, reads)
				}
			}
		})
	}
}

func BenchmarkConsumerArtifactRepeatedReads(b *testing.B) {
	// Each request still loads and validates its current artifacts. Only repeated
	// reads within that request are shared; JSON-rich child size is represented.
	for name, reuse := range map[string]bool{"uncached": false, "request_scoped": true} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			raw, err := json.Marshal(model.PlatformArtifact{ID: "child", Content: map[string]any{"payload": strings.Repeat("x", 1<<20)}})
			if err != nil {
				b.Fatal(err)
			}
			load := func(id string) (model.PlatformArtifact, error) {
				var artifact model.PlatformArtifact
				err := json.Unmarshal(raw, &artifact)
				return artifact, err
			}
			for b.Loop() {
				read := load
				if reuse {
					read = newConsumerArtifactReader(load)
				}
				for range 3 {
					if _, err := read("child"); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
