package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

// Use the file backend's real original RuntimeSnapshot and signed typed graph.
// It has no PG pin ledger and must neither require nor simulate one.
func fileTaskInputFixture(t *testing.T, withLKG bool) (*Store, string) {
	t.Helper()
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("neutral-file-task", "neutral", "", "", nil))
	snapshot, digest := compilerDNSFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	artifacts := []model.PlatformArtifact{}
	for _, a := range []model.PlatformArtifact{
		{ID: "task-child", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
			Content: map[string]any{"lineage": map[string]any{"input_snapshot_digest": digest}}},
		{ID: "task-parent", ArtifactKind: model.PlatformArtifactKindReleaseSet,
			Content: map[string]any{"artifact_ids": []string{"task-child"}}},
	} {
		a.Generation, a.Status = a.ID, model.PlatformArtifactStatusValidated
		a, err := normalizePlatformArtifactForStore(a)
		if err != nil {
			t.Fatal(err)
		}
		a.GenerationSequence = 1
		a, err = platformsafety.SignPlatformArtifact(a, s.platformArtifactSigningKeyring())
		if err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, a)
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		state.PlatformArtifacts = append(state.PlatformArtifacts, artifacts...)
		state.NodeUpdaters = append(state.NodeUpdaters, model.NodeUpdater{ID: "file-task-updater", Capabilities: []string{
			model.NodeUpdateTaskTypeRefreshDesiredState, model.NodeUpdateTaskTypeDiagnoseNode,
		}})
		if withLKG {
			a, now := artifacts[1], time.Now().UTC()
			lkg, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{
				ID: "file-task-lkg", ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, Scope: a.Scope, ScopeKey: a.ScopeKey,
				SchemaVersion: a.SchemaVersion, Generation: a.Generation, GenerationSequence: a.GenerationSequence, ContentHash: a.ContentHash,
				ArtifactProvenance: a.Provenance, VerifiedByReleaseID: "file-task-release", VerificationEvidenceHash: "sha256:" + strings.Repeat("a", 64),
				ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}, s.platformArtifactSigningKeyring())
			if err != nil {
				return err
			}
			state.PlatformLKGSnapshots = append(state.PlatformLKGSnapshots, lkg)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return s, s.path
}

func TestTaskFileCompleteOriginalClosureSurvivesStoreRestart(t *testing.T) {
	for _, implicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "nested-explicit", true: "implicit-current-lkg"}[implicit], func(t *testing.T) {
			s, path := fileTaskInputFixture(t, implicit)
			taskType, payload := model.NodeUpdateTaskTypeRefreshDesiredState, map[string]string{}
			if !implicit {
				taskType, payload = model.NodeUpdateTaskTypeDiagnoseNode, map[string]string{"arguments_json": `{"nested":{"artifact_id":"task-parent"}}`}
			}
			task, err := s.CreateNodeUpdateTask(taskCreateAdmin(), "file-task-updater", "", "", taskType, payload)
			if err != nil || task.Status != model.NodeUpdateTaskStatusPending {
				t.Fatal("complete file input could not create dependent task", task, err)
			}
			fresh := New(path)
			fresh.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("neutral-file-task", "neutral", "", "", nil))
			claimed, err := fresh.ClaimNodeUpdateTask(task.ID, task.NodeUpdaterID)
			if err != nil || claimed.Status != model.NodeUpdateTaskStatusRunning || claimed.ClaimedAt == nil {
				t.Fatal("fresh file store could not verify original before claim", claimed, err)
			}
			replay, err := fresh.ClaimNodeUpdateTask(task.ID, task.NodeUpdaterID)
			if err != nil || replay.ClaimedAt == nil || !replay.ClaimedAt.Equal(*claimed.ClaimedAt) {
				t.Fatal("valid running replay lost original claim time", replay, err)
			}
		})
	}
}

func TestTaskFileChangedOriginalOrSelectorRefusesWithoutMutation(t *testing.T) {
	for _, change := range []string{"missing-body", "corrupt-body", "wrong-size", "missing-child", "bad-child-signature", "bad-lkg-signature", "expired-lkg", "new-cold-lkg", "keyring"} {
		t.Run(change, func(t *testing.T) {
			s, path := fileTaskInputFixture(t, true)
			task, err := s.CreateNodeUpdateTask(taskCreateAdmin(), "file-task-updater", "", "", model.NodeUpdateTaskTypeRefreshDesiredState, map[string]string{})
			if err != nil {
				t.Fatal(err)
			}
			if change == "keyring" {
				s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("different-file-task", "different", "", "", nil))
			} else if err := s.withLockedState(true, func(st *model.State) error {
				switch change {
				case "missing-body":
					st.PlatformArtifactContents = nil
				case "corrupt-body":
					st.PlatformArtifactContents[0].Content = map[string]any{"wrong": "original"}
				case "wrong-size":
					st.PlatformArtifactContents[0].SizeBytes++
				case "missing-child":
					st.PlatformArtifacts = st.PlatformArtifacts[1:]
				case "bad-child-signature":
					st.PlatformArtifacts[0].Content["unverified"] = true
				case "bad-lkg-signature":
					st.PlatformLKGSnapshots[0].VerificationEvidenceHash = "sha256:" + strings.Repeat("b", 64)
				case "expired-lkg":
					st.PlatformLKGSnapshots[0].ExpiresAt = time.Now().Add(-time.Minute)
				case "new-cold-lkg":
					// A task claim reselects the current LKG; its create-time
					// success never grants authority to a later missing input.
					st.PlatformLKGSnapshots[0].ArtifactID = "missing-new-configuration"
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.ClaimNodeUpdateTask(task.ID, "other-updater"); !errors.Is(err, ErrNotFound) {
				t.Fatal("dependency check disclosed a task to a different owner", err)
			}
			if claimed, err := s.ClaimNodeUpdateTask(task.ID, task.NodeUpdaterID); !errors.Is(err, ErrConflict) || claimed.ID != "" {
				t.Fatal("changed file dependencies were claimed", claimed, err)
			}
			// Use a distinct payload so this exercises admission of a new
			// pending root rather than existing-task deduplication.
			if created, err := s.CreateNodeUpdateTask(taskCreateAdmin(), "file-task-updater", "", "", model.NodeUpdateTaskTypeRefreshDesiredState, map[string]string{"request": "new"}); !errors.Is(err, ErrConflict) || created.ID != "" {
				t.Fatal("changed file dependencies created a task", created, err)
			}
			after, err := os.ReadFile(path)
			if err != nil || string(before) != string(after) {
				t.Fatal("failed file admission changed state", err)
			}
		})
	}
}
