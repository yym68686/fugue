package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

func TestTaskPreparationClosureOnlyAllowsLocationChange(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	m := inputManifest{Digest: digest, Format: inputFormatCodecV2, Location: "legacy", Size: 10, ChunkSize: 10, Count: 1, Created: time.Unix(1, 0).UTC()}
	a := &compilerInputActivationProof{inputs: []string{digest}, inputManifests: map[string]inputManifest{digest: m}}
	for _, name := range []string{"same", "restore", "created", "chunk-size", "chunk-count", "format", "raw-bytes", "missing", "reverse-location"} {
		t.Run(name, func(t *testing.T) {
			x := m
			switch name {
			case "restore":
				x.Location = "hot"
			case "created":
				x.Created = x.Created.Add(time.Second)
			case "chunk-size":
				x.ChunkSize++
			case "chunk-count":
				x.Count++
			case "format":
				x.Format = inputFormatV1
			case "raw-bytes":
				x.Size++
			case "reverse-location":
				x.Location = "other"
			}
			b := &compilerInputActivationProof{inputs: []string{digest}, inputManifests: map[string]inputManifest{digest: x}}
			if name == "missing" {
				delete(b.inputManifests, digest)
			}
			if sameTaskPreparationClosure(a, b) != (name == "same" || name == "restore") {
				t.Fatal("manifest identity/geometry changed across preparation", name)
			}
		})
	}
}

func TestTaskPreparationReaderRoleCannotFallBackToWriter(t *testing.T) {
	writer := inputarchive.S3Config{Endpoint: "https://archive.invalid", Bucket: "neutral-archive", Prefix: "inputs", Region: "auto", AccessKeyID: "writer-role", SecretAccessKey: "synthetic-writer-secret"}
	for _, name := range []string{"separate", "missing-id", "missing-secret", "same-role", "session-token"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_ACCESS_KEY_ID", "reader-role")
			t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_SECRET_ACCESS_KEY", "synthetic-reader-secret")
			cfg := writer
			switch name {
			case "missing-id":
				t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_ACCESS_KEY_ID", "")
			case "missing-secret":
				t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_SECRET_ACCESS_KEY", "")
			case "same-role":
				t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_ACCESS_KEY_ID", writer.AccessKeyID)
			case "session-token":
				cfg.SessionToken = "synthetic-session-token"
			}
			reader, err := taskPreparationReaderConfig(cfg)
			if name != "separate" {
				if !errors.Is(err, ErrConflict) || reader.AccessKeyID != "" || reader.SecretAccessKey != "" {
					t.Fatal("unsafe reader configuration accepted")
				}
				return
			}
			if err != nil || reader.AccessKeyID != "reader-role" || reader.SecretAccessKey != "synthetic-reader-secret" || reader.SessionToken != "" {
				t.Fatal("dedicated reader role unavailable")
			}
			a, _ := writer.Fingerprint()
			b, _ := reader.Fingerprint()
			if a != b {
				t.Fatal("credential selection changed backend identity")
			}
		})
	}
}

func TestTaskPreparationPostgresSingleConnectionAndPreviewTupleBinding(t *testing.T) {
	s, objects, backend, c := coldTaskAdmissionFixture(t)
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)
	before := s.db.Stats()
	if _, err := s.db.Exec(`UPDATE fugue_node_update_tasks SET updated_at=updated_at WHERE id=$1`, c.TaskID); err != nil {
		t.Fatal(err)
	}
	report, err := s.prepareNodeTaskInputs(t.Context(), c, true, objects, backend)
	if !errors.Is(err, ErrConflict) || report.Prepared || report.Usage.GetRequests != 0 {
		t.Fatal("stale physical tuple plan accepted", report, err)
	}
	c.ExpectedPlanSHA256 = ""
	preview, err := s.prepareNodeTaskInputs(t.Context(), c, false, objects, backend)
	if err != nil || preview.Prepared || preview.Usage.GetRequests != 0 {
		t.Fatal(preview, err)
	}
	c.ExpectedPlanSHA256 = preview.PlanSHA256
	report, err = s.prepareNodeTaskInputs(t.Context(), c, true, objects, backend)
	after := s.db.Stats()
	if err != nil || !report.Prepared || after.MaxOpenConnections != 1 || after.OpenConnections > 1 || after.WaitCount != before.WaitCount {
		t.Fatal("preparation did not reuse its single connection", report, err, before, after)
	}
}

func TestTaskPreparationNoPutCannotReachStorage(t *testing.T) {
	if !errors.Is((taskPreparationNoPut{}).Put(context.Background(), "neutral", []byte("neutral")), ErrConflict) {
		t.Fatal("task preparation permitted a PUT")
	}
}

func inputTaskAdmissionFixture(t *testing.T, inputs int) (*Store, []compilerInputPinBinding) {
	t.Helper()
	s, _, pins := inputActivationFixture(t, inputs)
	if _, err := s.db.Exec(`INSERT INTO fugue_node_updaters(id,status,token_prefix,token_hash,created_at,updated_at)
 VALUES('admission-updater','ready','neutral','neutral-hash',now(),now())`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO fugue_node_update_tasks(id,node_updater_id,task_type,payload_json,status,created_at,updated_at)
 VALUES('admission-task','admission-updater','neutral',jsonb_build_object('artifact_id','activation-root'),'pending',now(),now())`); err != nil {
		t.Fatal(err)
	}
	return s, pins
}

func TestTaskClaimPostgresRejectsChangesBetweenPrepareAndClaim(t *testing.T) {
	for _, change := range []string{"payload", "owner", "terminal", "pin", "keyring"} {
		t.Run(change, func(t *testing.T) {
			s, _ := inputTaskAdmissionFixture(t, 1)
			proof, pins, err := s.prepareNodeTaskInputAdmission(t.Context(), "admission-task")
			if err != nil {
				t.Fatal(err)
			}
			query := map[string]string{
				"payload":  `UPDATE fugue_node_update_tasks SET payload_json='{}'`,
				"owner":    `INSERT INTO fugue_node_updaters(id,status,token_prefix,token_hash,created_at,updated_at) VALUES('other','ready','other','other',now(),now()); UPDATE fugue_node_update_tasks SET node_updater_id='other'`,
				"terminal": `UPDATE fugue_node_update_tasks SET status='completed'`,
				"pin":      `UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL`,
			}[change]
			if query != "" {
				if _, err := s.db.Exec(query); err != nil {
					t.Fatal(err)
				}
			} else {
				s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("changed", "changed", "", "", nil))
			}
			if task, err := s.pgClaimPreparedNodeUpdateTask(t.Context(), "admission-task", "admission-updater", proof, pins); err == nil || task.ID != "" {
				t.Fatal("stale preparation claimed task", task, err)
			}
			var status string
			var claimed bool
			wantStatus := "pending"
			if change == "terminal" {
				wantStatus = "completed"
			}
			if err := s.db.QueryRow(`SELECT status,claimed_at IS NOT NULL FROM fugue_node_update_tasks WHERE id='admission-task'`).Scan(&status, &claimed); err != nil || claimed || status != wantStatus {
				t.Fatal("rejected preparation mutated task", status, claimed, err)
			}
		})
	}
}

func TestTaskClaimFileStoreCannotBypassDurableAdmission(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.withLockedState(true, func(state *model.State) error {
		state.NodeUpdateTasks = append(state.NodeUpdateTasks, model.NodeUpdateTask{ID: "neutral-task", NodeUpdaterID: "neutral-updater", Status: "pending", Payload: map[string]string{"artifact_id": "neutral-artifact"}})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNodeUpdateTask("neutral-task", "other"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if task, err := s.ClaimNodeUpdateTask("neutral-task", "neutral-updater"); !errors.Is(err, ErrConflict) || task.ID != "" {
		t.Fatal("file store bypassed durable dependency check", task, err)
	}
	if err := s.withLockedState(false, func(state *model.State) error {
		if state.NodeUpdateTasks[0].Status != "pending" || state.NodeUpdateTasks[0].ClaimedAt != nil {
			t.Fatal("rejected task was mutated")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTaskClaimPostgresConsumesCompleteSignedClosureWithOldChunksAbsent(t *testing.T) {
	s, pins := inputTaskAdmissionFixture(t, 2)
	if _, err := s.db.Exec(`DROP TABLE fugue_platform_input_chunks`); err != nil {
		t.Fatal(err)
	}
	task, err := s.ClaimNodeUpdateTask("admission-task", "admission-updater")
	if err != nil || task.Status != model.NodeUpdateTaskStatusRunning || task.ClaimedAt == nil {
		t.Fatal(task, err)
	}
	claimed := *task.ClaimedAt
	// Existing running-task replay remains idempotent, but still verifies
	// current admission rather than trusting the old claim's process cache.
	task, err = s.ClaimNodeUpdateTask(task.ID, task.NodeUpdaterID)
	if err != nil || task.ClaimedAt == nil || !task.ClaimedAt.Equal(claimed) {
		t.Fatal(task, err)
	}
	for _, pin := range pins {
		if _, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{
			Key: pin.Key, Digest: pin.Digest, ExpectedToken: pin.Token, Release: true, Apply: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// The real pending/running task remains a retention root after its
	// temporary preparation pins are gone. This is not full retirement.
	proof, err := s.prepareCompilerInputActivation(t.Context(), []inputarchive.HotRoot{{
		Source: "test", ID: task.ID, Reference: inputarchive.HotReference{Kind: "task", ID: task.ID},
	}})
	if err != nil || len(proof.inputs) != 2 {
		t.Fatal(proof, err)
	}
}

func TestTaskClaimPostgresRefusesUnpreparedOrChangedDependenciesWithoutClaim(t *testing.T) {
	for name, change := range map[string]string{
		"missing-pin":       `DELETE FROM fugue_platform_input_local_pins`,
		"pending-pin":       `UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL`,
		"old-epoch":         `UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`,
		"legacy":            `UPDATE fugue_platform_input_manifests SET storage_location='legacy'`,
		"bad-signature":     `UPDATE fugue_platform_artifacts SET content_json='{"tampered":true}' WHERE id='activation-artifact-000'`,
		"missing-artifact":  `UPDATE fugue_node_update_tasks SET payload_json='{"artifact_id":"missing"}'`,
		"oversized-payload": `UPDATE fugue_node_update_tasks SET payload_json=jsonb_build_object('neutral',repeat('x',65537))`,
	} {
		t.Run(name, func(t *testing.T) {
			s, _ := inputTaskAdmissionFixture(t, 1)
			if _, err := s.db.Exec(change); err != nil {
				t.Fatal(err)
			}
			if task, err := s.ClaimNodeUpdateTask("admission-task", "admission-updater"); err == nil || task.ID != "" {
				t.Fatal("unprepared claim succeeded", task, err)
			}
			var status string
			var claimed bool
			if err := s.db.QueryRow(`SELECT status,claimed_at IS NOT NULL FROM fugue_node_update_tasks WHERE id='admission-task'`).Scan(&status, &claimed); err != nil || status != "pending" || claimed {
				t.Fatal("rejected dependency mutated task", status, claimed, err)
			}
		})
	}
}

func TestTaskClaimPostgresPreservesOwnershipAndUnreferencedTasks(t *testing.T) {
	s, _ := inputTaskAdmissionFixture(t, 0)
	if _, err := s.db.Exec(`UPDATE fugue_node_update_tasks SET payload_json='{}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNodeUpdateTask("admission-task", "other-updater"); !errors.Is(err, ErrNotFound) {
		t.Fatal("ownership bypassed", err)
	}
	if task, err := s.ClaimNodeUpdateTask("admission-task", "admission-updater"); err != nil || task.Status != "running" {
		t.Fatal(task, err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_node_update_tasks SET status='completed'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNodeUpdateTask("admission-task", "admission-updater"); !errors.Is(err, ErrConflict) {
		t.Fatal("terminal task reactivated", err)
	}
}

func TestTaskClaimPostgresUnrelatedMaintenanceDoesNotRequireArchiveAdmission(t *testing.T) {
	s, _ := inputTaskAdmissionFixture(t, 0)
	for _, q := range []string{
		`UPDATE fugue_node_update_tasks SET payload_json='{"diagnostic":"bounded-maintenance"}'`,
		`UPDATE fugue_platform_input_archive_control SET writes_fenced=true`,
		`DROP TABLE fugue_platform_input_local_pins`,
	} {
		if _, err := s.db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	s.ConfigurePlatformArtifactSigning(bundleauth.Keyring{})
	task, err := s.ClaimNodeUpdateTask("admission-task", "admission-updater")
	if err != nil || task.Status != "running" || task.ClaimedAt == nil {
		t.Fatal("unrelated maintenance coupled to archive admission", task, err)
	}
	claimed := *task.ClaimedAt
	if task, err := s.ClaimNodeUpdateTask("admission-task", "admission-updater"); err != nil || task.ClaimedAt == nil || !task.ClaimedAt.Equal(claimed) {
		t.Fatal("unrelated running-task replay changed", task, err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_node_update_tasks SET status='pending',claimed_at=NULL,payload_json='{"input_snapshot_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'`); err != nil {
		t.Fatal(err)
	}
	if task, err := s.ClaimNodeUpdateTask("admission-task", "admission-updater"); err == nil || task.ID != "" {
		t.Fatal("declared dependency bypassed archive admission", task, err)
	}
}

func coldTaskAdmissionFixture(t *testing.T) (*Store, *archiveTestObjects, string, NodeTaskInputPreparationContract) {
	t.Helper()
	s, pins := inputTaskAdmissionFixture(t, 1)
	for _, pin := range pins {
		if _, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: pin.Key, Digest: pin.Digest, ExpectedToken: pin.Token, Release: true, Apply: true}); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		`INSERT INTO fugue_platform_input_chunks(content_hash,ordinal,chunk_hash,data) SELECT content_hash,ordinal,chunk_hash,data FROM fugue_platform_input_hot_chunks`,
		`DELETE FROM fugue_platform_input_hot_chunks`,
		`UPDATE fugue_platform_input_manifests SET storage_location='legacy'`,
	} {
		if _, err := s.db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	objects := &archiveTestObjects{}
	backend := "sha256:" + strings.Repeat("b", 64)
	digest := pins[0].Digest
	if _, err := copyCompilerInputArchive(t.Context(), s.db, objects, backend, CompilerInputArchiveMigrationOptions{Digest: digest, Apply: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := routeCompilerInputArchive(t.Context(), s.db, objects, backend, digest); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DROP TABLE fugue_platform_input_chunks`); err != nil {
		t.Fatal(err)
	}
	c := NodeTaskInputPreparationContract{Version: 1, TaskID: "admission-task", UpdaterID: "admission-updater", OperationKey: "neutral-prepare", MaxInputs: 1, MaxRawBytes: 1024, MaxStoredBytes: 1024, MaxGetBytes: 65536, MaxGetRequests: 1, MaxSeconds: 30}
	preview, err := s.prepareNodeTaskInputs(t.Context(), c, false, nil, "")
	if err != nil || preview.Prepared || preview.InputCount != 1 || preview.PlanSHA256 == "" || preview.RawBytes < 1 {
		t.Fatal(preview, err)
	}
	c.ExpectedPlanSHA256 = preview.PlanSHA256
	return s, objects, backend, c
}

func TestTaskPreparationPostgresColdRestoreFullPinThenFreshClaimWithoutOldTable(t *testing.T) {
	s, objects, backend, c := coldTaskAdmissionFixture(t)
	if _, err := s.ClaimNodeUpdateTask(c.TaskID, c.UpdaterID); err == nil {
		t.Fatal("cold task claimed before preparation")
	}
	report, err := s.prepareNodeTaskInputs(t.Context(), c, true, objects, backend)
	if err != nil || !report.Prepared || report.TaskActivated || report.RetirementAuthorized || len(report.Pins) != 1 || !report.Pins[0].Verified || report.Usage.GetRequests != 1 || report.Usage.PutRequests != 0 {
		t.Fatal(report, err)
	}
	var status string
	if err := s.db.QueryRow(`SELECT status FROM fugue_node_update_tasks WHERE id=$1`, c.TaskID).Scan(&status); err != nil || status != "pending" {
		t.Fatal("preparation activated task", status, err)
	}
	// A new pool/Store has no prepared graph or body cache. Archive is now
	// unavailable; exact persistent pins and independent bytes suffice.
	db, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fresh := &Store{db: db, databaseURL: s.databaseURL}
	fresh.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
	objects.failGet = true
	task, err := fresh.ClaimNodeUpdateTask(c.TaskID, c.UpdaterID)
	if err != nil || task.Status != "running" {
		t.Fatal(task, err)
	}
	readback, err := RunCompilerInputLocalReadback(t.Context(), s.databaseURL, CompilerInputLocalReadbackOptions{Digest: report.Pins[0].Digest, MaxRawBytes: 1024})
	if err != nil || !readback.Verified {
		t.Fatal(readback, err)
	}
}

func TestTaskClaimPostgresSelectsCurrentPinInsteadOfStaleVerifiedPin(t *testing.T) {
	s, pins := inputTaskAdmissionFixture(t, 1)
	if _, err := s.db.Exec(`UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`); err != nil {
		t.Fatal(err)
	}
	pin, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: "new-epoch-pin", Digest: pins[0].Digest, MaxRawBytes: 1024, Apply: true})
	if err != nil || !pin.Verified {
		t.Fatal(pin, err)
	}
	if _, err := s.ClaimNodeUpdateTask("admission-task", "admission-updater"); err != nil {
		t.Fatal("stale verified pin shadowed the current pin", err)
	}
}

func TestTaskPreparationPostgresRefusesUnavailableCorruptChangedAndOverBudget(t *testing.T) {
	for _, failure := range []string{"unavailable", "corrupt", "plan", "raw-budget", "stored-budget", "graph-change", "owner-change"} {
		t.Run(failure, func(t *testing.T) {
			s, objects, backend, c := coldTaskAdmissionFixture(t)
			var reader inputarchive.Objects = objects
			switch failure {
			case "unavailable":
				objects.failGet = true
			case "corrupt":
				for k := range objects.objects {
					objects.objects[k] = []byte("corrupt")
				}
			case "plan":
				c.ExpectedPlanSHA256 = strings.Repeat("0", 64)
			case "raw-budget":
				c.MaxRawBytes = 1
			case "stored-budget":
				c.MaxStoredBytes = 1
			case "graph-change", "owner-change":
				reader = restoreTestObjects{archiveTestObjects: objects, beforeGet: func(string) error {
					q := `UPDATE fugue_node_update_tasks SET payload_json='{}'`
					if failure == "owner-change" {
						q = `INSERT INTO fugue_node_updaters(id,status,token_prefix,token_hash,created_at,updated_at) VALUES('other','ready','other','other',now(),now()); UPDATE fugue_node_update_tasks SET node_updater_id='other'`
					}
					_, err := s.db.Exec(q)
					return err
				}}
			}
			if report, err := s.prepareNodeTaskInputs(t.Context(), c, true, reader, backend); err == nil || errors.Is(err, ErrInvalidInput) || report.Prepared || report.TaskActivated || report.InputCount != 1 {
				t.Fatal("failed preparation accepted", report, err)
			}
			var status string
			var claimed bool
			if err := s.db.QueryRow(`SELECT status,claimed_at IS NOT NULL FROM fugue_node_update_tasks`).Scan(&status, &claimed); err != nil || status != "pending" || claimed {
				t.Fatal("failed preparation activated task", status, claimed, err)
			}
		})
	}
}
