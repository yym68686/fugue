package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestPlatformExpectedConsumerSetPersistsAndCannotMutateRevision(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "store.json")
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	set := expectedConsumerSetFixture(t, 1)
	created, err := s.CreatePlatformExpectedConsumerSet(set)
	if err != nil {
		t.Fatalf("create expected consumer set: %v", err)
	}
	if created.ScopeKey != "global" || created.Scope.Key != "global" || len(created.Consumers) != 1 {
		t.Fatalf("unexpected normalized expected consumer set: %+v", created)
	}

	created.Consumers[0].AcceptedProtocolVersions[0] = "mutated"
	got, err := s.GetPlatformExpectedConsumerSet(set.ID)
	if err != nil {
		t.Fatalf("get expected consumer set: %v", err)
	}
	if got.Consumers[0].AcceptedProtocolVersions[0] != model.PlatformConsumerProtocolVersionV1 {
		t.Fatalf("stored expected consumer set was mutated through returned data: %+v", got.Consumers[0])
	}

	reopened := New(path)
	if err := reopened.Init(); err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	listed, err := reopened.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{
		ReleaseSetID: set.ReleaseSetID,
		ArtifactKind: set.ArtifactKind,
		ScopeKey:     "GLOBAL",
	})
	if err != nil {
		t.Fatalf("list expected consumer sets: %v", err)
	}
	if len(listed) != 1 || !reflect.DeepEqual(listed[0], got) {
		t.Fatalf("unexpected persisted expected consumer sets: %+v", listed)
	}

	if _, err := reopened.CreatePlatformExpectedConsumerSet(set); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected duplicate expected consumer set id to conflict, got %v", err)
	}
	sameRevision := set
	sameRevision.ID = "expectedconsumerset_conflicting_revision"
	if _, err := reopened.CreatePlatformExpectedConsumerSet(sameRevision); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected release revision mutation to conflict, got %v", err)
	}
}

func TestPlatformExpectedConsumerSetRejectsInvalidCardinality(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	set := expectedConsumerSetFixture(t, 1)
	set.RequiredCardinality++
	if _, err := s.CreatePlatformExpectedConsumerSet(set); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected invalid cardinality to be rejected, got %v", err)
	}
}

func expectedConsumerSetFixture(t *testing.T, revision int64) model.PlatformExpectedConsumerSet {
	t.Helper()
	preparedAt := time.Date(2026, 7, 10, 9, 0, 0, 0, time.UTC)
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID:      "release-set-test",
		ArtifactReleaseID: "artifact-release-test",
		ArtifactKind:      model.PlatformArtifactKindCaddyRouteConfig,
		Scope:             model.PlatformArtifactScope{ScopeType: "global"},
		ScopeKey:          "global",
		Generation:        "generation-test",
		Revision:          revision,
		PreparedAt:        preparedAt.Add(time.Duration(revision-1) * time.Minute),
		Topology: platformcontrol.ExpectedConsumerTopology{
			EdgeNodes: []model.EdgeNode{{
				ID:          "edge-test",
				EdgeGroupID: "edge-group-test",
				Country:     "US",
			}},
		},
	})
	if err != nil {
		t.Fatalf("build expected consumer set fixture: %v", err)
	}
	return set
}
