package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func insertTaskConfigurationLKG(t *testing.T, s *Store, artifact string) {
	t.Helper()
	a, err := s.GetPlatformArtifact(artifact)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	snapshot, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{
		ID: "task-configuration-lkg", ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, Scope: a.Scope, ScopeKey: a.ScopeKey,
		SchemaVersion: a.SchemaVersion, Generation: a.Generation, GenerationSequence: a.GenerationSequence, ContentHash: a.ContentHash,
		ArtifactProvenance: a.Provenance, VerifiedByReleaseID: "task-configuration-release", VerificationEvidenceHash: "sha256:" + strings.Repeat("a", 64),
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}, s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgUpsertPlatformLKGSnapshot(t.Context(), s.db, snapshot); err != nil {
		t.Fatal(err)
	}
}

func TestTaskClaimPostgresImplicitConfigurationRequiresCompleteInputPins(t *testing.T) {
	for _, taskType := range []string{model.NodeUpdateTaskTypeRefreshJoinConfig, model.NodeUpdateTaskTypeRefreshDesiredState,
		model.NodeUpdateTaskTypeReloadLKGBundle, model.NodeUpdateTaskTypeRestartStatelessNodeService} {
		t.Run(taskType, func(t *testing.T) {
			s, pins := inputTaskAdmissionFixture(t, 1)
			insertTaskConfigurationLKG(t, s, "activation-root")
			if _, err := s.db.Exec(`UPDATE fugue_node_update_tasks SET task_type=$1,payload_json='{"service":"fugue-edge.service"}'`, taskType); err != nil {
				t.Fatal(err)
			}
			proof, _, err := s.prepareNodeTaskInputAdmission(t.Context(), "admission-task")
			if err != nil || len(proof.inputs) != 1 || proof.inputs[0] != pins[0].Digest {
				t.Fatal("empty payload lost implicit LKG input", proof, err)
			}
			if _, err := s.db.Exec(`DELETE FROM fugue_platform_input_local_pins`); err != nil {
				t.Fatal(err)
			}
			if task, err := s.ClaimNodeUpdateTask("admission-task", "admission-updater"); err == nil || task.ID != "" {
				t.Fatal("implicit input bypassed durable full-readback admission", task, err)
			}
			var status string
			if err := s.db.QueryRow(`SELECT status FROM fugue_node_update_tasks WHERE id='admission-task'`).Scan(&status); err != nil || status != "pending" {
				t.Fatal(status, err)
			}
		})
	}
}

// Thirty-two distinct artifact roots fill the first resolver batch. The task
// is discovered in the next batch, after its implicit LKG artifact was already
// verified and discarded. Root ordering must not reject a valid configuration,
// and the late selector must still undergo its complete signature check.
func TestActivationPostgresLateTaskSelectorChecksPreviouslyResolvedArtifact(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			s, pins := inputTaskAdmissionFixture(t, 1)
			insertTaskConfigurationLKG(t, s, "activation-root")
			if _, err := s.db.Exec(`UPDATE fugue_node_update_tasks SET task_type='refresh-desired-state',payload_json='{}'`); err != nil {
				t.Fatal(err)
			}
			roots := []inputarchive.HotRoot{{Source: "artifacts", ID: "first", Reference: inputarchive.HotReference{Kind: "artifact", ID: "activation-root"}}}
			for i := 0; i < 31; i++ {
				id := fmt.Sprintf("neutral-mixed-%02d", i)
				activationInsertSigned(t, s, id, model.PlatformArtifactKindEdgeRouteBundle, map[string]any{"neutral": i}, nil)
				roots = append(roots, inputarchive.HotRoot{Source: "artifacts", ID: id, Reference: inputarchive.HotReference{Kind: "artifact", ID: id}})
			}
			roots = append(roots, inputarchive.HotRoot{Source: "tasks", ID: "admission-task", Reference: inputarchive.HotReference{Kind: "task", ID: "admission-task"}})
			if corrupt {
				if _, err := s.db.Exec(`UPDATE fugue_platform_lkg_snapshots SET snapshot_provenance_json=jsonb_set(snapshot_provenance_json,'{signature}','"invalid"')`); err != nil {
					t.Fatal(err)
				}
			}
			proof, err := s.prepareCompilerInputActivation(t.Context(), roots)
			if corrupt {
				if err == nil || proof != nil {
					t.Fatal("late corrupt selector was accepted", err)
				}
				return
			}
			if err != nil || len(proof.inputs) != 1 || proof.inputs[0] != pins[0].Digest {
				t.Fatal("root ordering lost verified implicit dependencies", proof, err)
			}
			if err := s.withPreparedCompilerInputActivation(t.Context(), proof, pins, func(context.Context, *sql.Tx) error { return nil }); err != nil {
				t.Fatal("late-selector proof could not be consumed", err)
			}
		})
	}
}

func TestActivationPostgresArtifactOnlyIgnoresUnrelatedInvalidSelector(t *testing.T) {
	s, roots, _ := inputActivationFixture(t, 1)
	insertTaskConfigurationLKG(t, s, "activation-root")
	if _, err := s.db.Exec(`UPDATE fugue_platform_lkg_snapshots SET snapshot_provenance_json='{}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.prepareCompilerInputActivation(t.Context(), roots); err != nil {
		t.Fatal("artifact-only proof acquired unrelated selector admission", err)
	}
}

func TestTaskClaimPostgresBindsConfigurationSelectorIncludingAbsentAndABA(t *testing.T) {
	for _, change := range []string{"insert", "update", "delete", "aba"} {
		t.Run(change, func(t *testing.T) {
			s, _ := inputTaskAdmissionFixture(t, 1)
			if _, err := s.db.Exec(`UPDATE fugue_node_update_tasks SET task_type='refresh-desired-state',payload_json='{}'`); err != nil {
				t.Fatal(err)
			}
			if change != "insert" {
				insertTaskConfigurationLKG(t, s, "activation-root")
			}
			proof, pins, err := s.prepareNodeTaskInputAdmission(t.Context(), "admission-task")
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "insert":
				insertTaskConfigurationLKG(t, s, "activation-root")
			case "update":
				if _, err := s.db.Exec(`UPDATE fugue_platform_lkg_snapshots SET updated_at=updated_at`); err != nil {
					t.Fatal(err)
				}
			case "delete", "aba":
				if _, err := s.db.Exec(`DELETE FROM fugue_platform_lkg_snapshots`); err != nil {
					t.Fatal(err)
				}
				if change == "aba" {
					insertTaskConfigurationLKG(t, s, "activation-root")
				}
			}
			if task, err := s.pgClaimPreparedNodeUpdateTask(t.Context(), "admission-task", "admission-updater", proof, pins); err == nil || task.ID != "" {
				t.Fatal("changed selector consumed old proof", change, task, err)
			}
		})
	}
}

func TestTaskClaimPostgresImplicitConfigurationSucceedsWithOldChunksAbsent(t *testing.T) {
	s, pins := inputTaskAdmissionFixture(t, 1)
	insertTaskConfigurationLKG(t, s, "activation-root")
	if _, err := s.db.Exec(`UPDATE fugue_node_update_tasks SET task_type='refresh-desired-state',payload_json='{}'; DROP TABLE fugue_platform_input_chunks`); err != nil {
		t.Fatal(err)
	}
	task, err := s.ClaimNodeUpdateTask("admission-task", "admission-updater")
	if err != nil || task.Status != "running" || task.ClaimedAt == nil {
		t.Fatal(task, err)
	}
	readback, err := RunCompilerInputLocalReadback(t.Context(), s.databaseURL, CompilerInputLocalReadbackOptions{Digest: pins[0].Digest, MaxRawBytes: 1024})
	if err != nil || !readback.Verified {
		t.Fatal("independent reader failed after implicit claim", readback, err)
	}
}

func TestTaskClaimPostgresSelectorLockPreventsPhantomWithinShortTransaction(t *testing.T) {
	s, _ := inputTaskAdmissionFixture(t, 0)
	if _, err := s.db.Exec(`UPDATE fugue_node_update_tasks SET task_type='refresh-desired-state',payload_json='{}'`); err != nil {
		t.Fatal(err)
	}
	proof, pins, err := s.prepareNodeTaskInputAdmission(t.Context(), "admission-task")
	if err != nil {
		t.Fatal(err)
	}
	err = s.withPreparedCompilerInputActivation(t.Context(), proof, pins, func(ctx context.Context, tx *sql.Tx) error {
		other, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer other.Rollback()
		if _, err := other.Exec(`SET LOCAL lock_timeout='50ms'`); err != nil {
			return err
		}
		_, err = other.Exec(`INSERT INTO fugue_platform_lkg_snapshots(id,artifact_kind,scope_key) VALUES('phantom','release_set','global')`)
		if !strings.Contains(stringSQLState(err), "55P03") {
			t.Fatalf("selector insertion was not fenced: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func stringSQLState(err error) string {
	if e, ok := err.(interface{ SQLState() string }); ok {
		return e.SQLState()
	}
	return ""
}

func TestTaskPreparationPostgresImplicitSelectorTicketIsBound(t *testing.T) {
	s, _ := inputTaskAdmissionFixture(t, 1)
	insertTaskConfigurationLKG(t, s, "activation-root")
	if _, err := s.db.Exec(`UPDATE fugue_node_update_tasks SET task_type='refresh-desired-state',payload_json='{}'`); err != nil {
		t.Fatal(err)
	}
	c := NodeTaskInputPreparationContract{Version: 1, TaskID: "admission-task", UpdaterID: "admission-updater", OperationKey: "implicit-ticket", MaxInputs: 1, MaxRawBytes: 1024, MaxStoredBytes: 1024, MaxGetBytes: 65536, MaxGetRequests: 1, MaxSeconds: 30}
	plan, err := s.planNodeTaskInputPreparation(t.Context(), c, "sha256:"+strings.Repeat("b", 64))
	if err != nil || plan.Ticket.Validate(plan.VerificationKey, time.Now()) != nil {
		t.Fatal(plan, err)
	}
	found := false
	for _, tuple := range plan.Ticket.Tuples {
		if tuple.Table == "fugue_platform_lkg_snapshots" {
			found = true
		}
	}
	if !found {
		t.Fatal("selector absent from signed ticket")
	}
	if _, err := s.db.Exec(`UPDATE fugue_platform_lkg_snapshots SET updated_at=updated_at`); err != nil {
		t.Fatal(err)
	}
	if err := checkTaskPreparationTicket(t.Context(), s.db, plan.Ticket, false, nil); err == nil {
		t.Fatal("worker accepted changed implicit selector")
	}
}

func TestCompilerInputHotTaskReferencesDecodeTypedJSONStringAndGeneration(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{"nested": map[string]any{"artifact_id": "neutral-artifact"}, "artifact_kind": "release_set", "scope_key": "neutral", "generation": "neutral-generation"})
	refs, err := compilerInputHotTaskReferences(map[string]string{"arguments_json": string(raw)})
	if err != nil || len(refs) != 2 {
		t.Fatal(refs, err)
	}
	want := map[inputarchive.HotReference]bool{{Kind: "artifact", ID: "neutral-artifact"}: true, {Kind: "generation", ID: "neutral-generation", ArtifactKind: "release_set", ScopeKey: "neutral"}: true}
	for _, ref := range refs {
		if !want[ref] {
			t.Fatal(ref)
		}
		delete(want, ref)
	}
	if len(want) != 0 {
		t.Fatal(want)
	}
	for _, raw := range []string{`{`, `{"generation":"g","artifact_kind":"release_set"}`, `{"artifact_id":3}`} {
		if _, err := compilerInputHotTaskReferences(map[string]string{"arguments_json": raw}); err == nil {
			t.Fatal("malformed structured argument accepted", raw)
		}
	}
}
