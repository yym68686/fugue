package store

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

func taskCreateAdmin() model.Principal {
	return model.Principal{Scopes: map[string]struct{}{"platform.admin": {}}}
}

func taskCreateFixture(t *testing.T) *Store {
	t.Helper()
	s, _, _ := inputActivationFixture(t, 1)
	if _, err := s.db.Exec(`INSERT INTO fugue_node_updaters(id,status,token_prefix,token_hash,capabilities_json,created_at,updated_at)
 VALUES('create-updater','active','neutral','neutral','["refresh-desired-state","diagnose-node","restart-k3s-agent"]',now(),now())`); err != nil {
		t.Fatal(err)
	}
	return s
}

func taskCreateAssertEmpty(t *testing.T, s *Store) {
	t.Helper()
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_node_update_tasks`).Scan(&count); err != nil || count != 0 {
		t.Fatal("rejected create installed a retention root", count, err)
	}
}

func TestTaskCreatePostgresRequiresSignedClosureButQueuesColdInputs(t *testing.T) {
	for _, mode := range []string{"explicit", "structured", "implicit"} {
		for _, failure := range []string{"none", "pin", "artifact", "legacy"} {
			t.Run(mode+"/"+failure, func(t *testing.T) {
				s := taskCreateFixture(t)
				taskType := model.NodeUpdateTaskTypeDiagnoseNode
				payload := map[string]string{"artifact_id": "activation-root"}
				if mode == "structured" {
					payload = map[string]string{"arguments_json": `{"nested":{"artifact_id":"activation-root"}}`}
				} else if mode == "implicit" {
					insertTaskConfigurationLKG(t, s, "activation-root")
					taskType, payload = model.NodeUpdateTaskTypeRefreshDesiredState, map[string]string{}
				}
				query := map[string]string{
					"pin":      `UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL`,
					"artifact": `UPDATE fugue_platform_artifacts SET content_json='{}' WHERE id='activation-root'`,
					"legacy":   `UPDATE fugue_platform_input_manifests SET storage_location='legacy'`,
				}[failure]
				if query != "" {
					if _, err := s.db.Exec(query); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := s.db.Exec(`DROP TABLE fugue_platform_input_chunks`); err != nil {
					t.Fatal(err)
				}
				task, err := s.CreateNodeUpdateTask(taskCreateAdmin(), "create-updater", "", "", taskType, payload)
				if failure == "artifact" {
					if !errors.Is(err, ErrConflict) || task.ID != "" {
						t.Fatal("unprepared dependency accepted", task, err)
					}
					taskCreateAssertEmpty(t, s)
					return
				}
				if err != nil || task.Status != "pending" || task.ClaimedAt != nil {
					t.Fatal("prepared create failed or activated execution", task, err)
				}
				// The actual pending task now protects the same complete closure
				// after operation pins are released. A new claim still requires
				// fresh readback pins; retention and activation are distinct.
				var pinKey, digest string
				var token int64
				if err := s.db.QueryRow(`SELECT pin_key,content_hash,token FROM fugue_platform_input_local_pins LIMIT 1`).Scan(&pinKey, &digest, &token); err != nil {
					t.Fatal(err)
				}
				if _, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: pinKey, Digest: digest, ExpectedToken: token, Release: true, Apply: true}); err != nil {
					t.Fatal(err)
				}
				proof, err := s.prepareCompilerInputActivation(t.Context(), nodeTaskActivationRoots(task.ID))
				if err != nil || len(proof.inputs) != 1 {
					t.Fatal("new task did not retain its complete input closure", proof, err)
				}
				if _, err := s.ClaimNodeUpdateTask(task.ID, task.NodeUpdaterID); err == nil {
					t.Fatal("pending metadata proof bypassed execution pin requirement")
				}
				if mode == "implicit" {
					again, err := s.CreateNodeUpdateTask(taskCreateAdmin(), "create-updater", "", "", taskType, payload)
					if err != nil || again.ID != task.ID {
						t.Fatal("pending task deduplication changed", again, err)
					}
				}
			})
		}
	}
}

func TestTaskCreatePostgresRechecksSelectorsInputsAndTargetAtomically(t *testing.T) {
	for _, change := range []string{"insert", "update", "delete", "aba", "manifest", "epoch", "keyring", "owner", "capability", "target-delete", "payload-alias"} {
		t.Run(change, func(t *testing.T) {
			s := taskCreateFixture(t)
			if change != "insert" {
				insertTaskConfigurationLKG(t, s, "activation-root")
			}
			payload := map[string]string{"neutral": "initial"}
			now := time.Now().UTC()
			task := model.NodeUpdateTask{ID: "new-task", NodeUpdaterID: "create-updater", Type: model.NodeUpdateTaskTypeRefreshDesiredState,
				Payload: payload, Status: "pending", CreatedAt: now, UpdatedAt: now}
			proof, err := s.prepareNodeTaskCreateAdmission(t.Context(), task)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "insert":
				insertTaskConfigurationLKG(t, s, "activation-root")
			case "aba":
				if _, err := s.db.Exec(`DELETE FROM fugue_platform_lkg_snapshots`); err != nil {
					t.Fatal(err)
				}
				insertTaskConfigurationLKG(t, s, "activation-root")
			case "keyring":
				s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("revoked", "revoked", "", "", nil))
			case "payload-alias":
				payload["artifact_id"] = "unverified-alias"
			default:
				q := map[string]string{
					"update":        `UPDATE fugue_platform_lkg_snapshots SET updated_at=updated_at`,
					"delete":        `DELETE FROM fugue_platform_lkg_snapshots`,
					"manifest":      `UPDATE fugue_platform_input_manifests SET storage_location='legacy'`,
					"epoch":         `UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`,
					"owner":         `INSERT INTO fugue_tenants(id,name,slug,status,created_at,updated_at) VALUES('different-tenant','Neutral','neutral','active',now(),now()); UPDATE fugue_node_updaters SET tenant_id='different-tenant'`,
					"capability":    `UPDATE fugue_node_updaters SET capabilities_json='["diagnose-node"]'`,
					"target-delete": `DELETE FROM fugue_node_updaters`,
				}[change]
				if _, err := s.db.Exec(q); err != nil {
					t.Fatal(err)
				}
			}
			created, err := s.pgCreatePreparedNodeUpdateTask(t.Context(), taskCreateAdmin(), proof)
			if change == "payload-alias" {
				if err != nil || created.Payload["artifact_id"] != "" || created.Payload["neutral"] != "initial" {
					t.Fatal("caller mutated validated create intent", created, err)
				}
				return
			}
			if err == nil || created.ID != "" {
				t.Fatal("stale proof inserted a retention root", created, err)
			}
			taskCreateAssertEmpty(t, s)
		})
	}
}

func TestTaskCreatePostgresMaintenanceAndBootstrapCompatibility(t *testing.T) {
	s := taskCreateFixture(t)
	for _, query := range []string{`DROP TABLE fugue_platform_input_archive_control`, `DROP TABLE fugue_platform_input_local_pins`, `DROP TABLE fugue_platform_lkg_snapshots`} {
		if _, err := s.db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	s.ConfigurePlatformArtifactSigning(bundleauth.Keyring{})
	if task, err := s.CreateNodeUpdateTask(taskCreateAdmin(), "create-updater", "", "", model.NodeUpdateTaskTypeDiagnoseNode, map[string]string{"targets_json": `[]`}); err != nil || task.Status != "pending" {
		t.Fatal("unrelated maintenance requires archive infrastructure", task, err)
	}
	s = taskCreateFixture(t)
	if task, err := s.CreateNodeUpdateTask(taskCreateAdmin(), "create-updater", "", "", model.NodeUpdateTaskTypeRefreshDesiredState, nil); err != nil || task.Status != "pending" {
		t.Fatal("empty initial installation cannot bootstrap", task, err)
	}
}

func TestTaskCreateFileStoreCannotAddDependentRoot(t *testing.T) {
	for _, mode := range []string{"explicit", "implicit", "bootstrap", "maintenance"} {
		t.Run(mode, func(t *testing.T) {
			s := New(filepath.Join(t.TempDir(), "state.json"))
			if err := s.withLockedState(true, func(state *model.State) error {
				state.NodeUpdaters = append(state.NodeUpdaters, model.NodeUpdater{ID: "create-updater", Capabilities: []string{model.NodeUpdateTaskTypeRefreshDesiredState, model.NodeUpdateTaskTypeDiagnoseNode}})
				if mode == "implicit" {
					state.PlatformLKGSnapshots = append(state.PlatformLKGSnapshots, model.PlatformLKGSnapshot{ArtifactKind: "release_set", ScopeKey: "global"})
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			taskType, payload := model.NodeUpdateTaskTypeRefreshDesiredState, map[string]string{}
			if mode == "explicit" {
				payload["artifact_id"] = "neutral-artifact"
			} else if mode == "maintenance" {
				taskType, payload = model.NodeUpdateTaskTypeDiagnoseNode, map[string]string{"targets_json": `[]`}
			}
			task, err := s.CreateNodeUpdateTask(taskCreateAdmin(), "create-updater", "", "", taskType, payload)
			if mode == "explicit" || mode == "implicit" {
				if !errors.Is(err, ErrConflict) || task.ID != "" {
					t.Fatal("file create bypassed durable admission", task, err)
				}
				return
			}
			if err != nil || task.Status != "pending" {
				t.Fatal(task, err)
			}
		})
	}
}

func TestTaskCreatePostgresRejectsMalformedAndOversizedPayload(t *testing.T) {
	for _, payload := range []map[string]string{{"arguments_json": `{`}, {"artifact_id": "missing"}, {"comment": strings.Repeat("x", 65537), "artifact_id": "activation-root"}} {
		s := taskCreateFixture(t)
		if task, err := s.CreateNodeUpdateTask(taskCreateAdmin(), "create-updater", "", "", model.NodeUpdateTaskTypeDiagnoseNode, payload); !errors.Is(err, ErrConflict) || task.ID != "" {
			t.Fatal("invalid dependent payload inserted", task, err)
		}
		taskCreateAssertEmpty(t, s)
	}
}

func TestTaskCreatePostgresColdQueueThenPrepareThenClaim(t *testing.T) {
	s, objects, backend, c := coldTaskAdmissionFixture(t)
	if _, err := s.db.Exec(`UPDATE fugue_node_updaters SET capabilities_json='["diagnose-node"]'; DELETE FROM fugue_node_update_tasks`); err != nil {
		t.Fatal(err)
	}
	task, err := s.CreateNodeUpdateTask(taskCreateAdmin(), "admission-updater", "", "", model.NodeUpdateTaskTypeDiagnoseNode, map[string]string{"artifact_id": "activation-root"})
	if err != nil || task.Status != "pending" {
		t.Fatal("cold task cannot establish protected pending root", task, err)
	}
	if _, err := s.ClaimNodeUpdateTask(task.ID, task.NodeUpdaterID); err == nil {
		t.Fatal("cold task executed before restoration")
	}
	c.TaskID, c.ExpectedPlanSHA256, c.OperationKey = task.ID, "", "create-cold-prepare"
	preview, err := s.prepareNodeTaskInputs(t.Context(), c, false, objects, backend)
	if err != nil {
		t.Fatal(err)
	}
	c.ExpectedPlanSHA256 = preview.PlanSHA256
	report, err := s.prepareNodeTaskInputs(t.Context(), c, true, objects, backend)
	if err != nil || !report.Prepared || report.TaskActivated || report.Usage.GetRequests != 1 || report.Usage.PutRequests != 0 {
		t.Fatal("cold create did not integrate with bounded preparation", report, err)
	}
	if task, err := s.ClaimNodeUpdateTask(task.ID, task.NodeUpdaterID); err != nil || task.Status != "running" {
		t.Fatal("prepared cold task cannot execute", task, err)
	}
}

func TestTaskCreatePostgresProspectiveProofCannotActivate(t *testing.T) {
	s := taskCreateFixture(t)
	task := model.NodeUpdateTask{ID: "prospective", NodeUpdaterID: "create-updater", Type: model.NodeUpdateTaskTypeDiagnoseNode,
		Status: "pending", Payload: map[string]string{"artifact_id": "activation-root"}}
	proof, err := s.prepareNodeTaskCreateAdmission(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	_, pins, err := s.prepareCompilerInputActivationPins(t.Context(), proof)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err := s.withPreparedCompilerInputActivation(t.Context(), proof, pins, func(context.Context, *sql.Tx) error {
		called = true
		return nil
	}); !errors.Is(err, ErrInvalidInput) || called {
		t.Fatal("pending-only proof became execution authority", err, called)
	}
	proof, err = s.prepareCompilerInputActivation(t.Context(), []inputarchive.HotRoot{{Source: "test", ID: "neutral", Reference: inputarchive.HotReference{Kind: "artifact", ID: "activation-root"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.withPreparedCompilerInputTaskCreation(t.Context(), proof, func(context.Context, *sql.Tx) error {
		called = true
		return nil
	}); !errors.Is(err, ErrInvalidInput) || called {
		t.Fatal("artifact-only proof became task creation authority", err, called)
	}
	taskCreateAssertEmpty(t, s)
}

func TestTaskCreatePostgresConcurrentPreparedCreatesDeduplicate(t *testing.T) {
	s := taskCreateFixture(t)
	insertTaskConfigurationLKG(t, s, "activation-root")
	proofs := make([]*compilerInputActivationProof, 2)
	for i, id := range []string{"concurrent-first", "concurrent-second"} {
		now := time.Now().UTC()
		var err error
		proofs[i], err = s.prepareNodeTaskCreateAdmission(t.Context(), model.NodeUpdateTask{ID: id, NodeUpdaterID: "create-updater",
			Type: model.NodeUpdateTaskTypeRefreshDesiredState, Status: "pending", CreatedAt: now, UpdatedAt: now})
		if err != nil {
			t.Fatal(err)
		}
	}
	type outcome struct {
		task model.NodeUpdateTask
		err  error
	}
	start, results := make(chan struct{}), make(chan outcome, 2)
	for _, proof := range proofs {
		go func(proof *compilerInputActivationProof) {
			<-start
			task, err := s.pgCreatePreparedNodeUpdateTask(t.Context(), taskCreateAdmin(), proof)
			results <- outcome{task, err}
		}(proof)
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil || first.task.ID == "" || first.task.ID != second.task.ID {
		t.Fatal("dependent creates did not serialize/deduplicate", first, second)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_node_update_tasks`).Scan(&count); err != nil || count != 1 {
		t.Fatal("duplicate retention roots installed", count, err)
	}
}

func TestTaskCreatePostgresSelectorAndRevocationHoldThroughCommit(t *testing.T) {
	s := taskCreateFixture(t)
	task := model.NodeUpdateTask{ID: "locked-create", NodeUpdaterID: "create-updater", Type: model.NodeUpdateTaskTypeRefreshDesiredState,
		Status: "pending", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	proof, err := s.prepareNodeTaskCreateAdmission(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	err = s.withPreparedCompilerInputTaskCreation(t.Context(), proof, func(ctx context.Context, tx *sql.Tx) error {
		other, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer other.Rollback()
		if _, err := other.Exec(`SET LOCAL lock_timeout='50ms'`); err != nil {
			return err
		}
		_, err = other.Exec(`INSERT INTO fugue_platform_lkg_snapshots(id,artifact_kind,scope_key) VALUES('phantom','release_set','global')`)
		if stringSQLState(err) != "55P03" {
			t.Fatalf("pending creation failed to lock selector absence: %v", err)
		}
		if err := insertNodeUpdateTask(ctx, tx, task); err != nil {
			return err
		}
		s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("revoked", "revoked", "", "", nil))
		return nil
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("revocation committed pending root", err)
	}
	taskCreateAssertEmpty(t, s)
}

func TestTaskCreatePostgresOrdinaryRoleAndTenantAuthority(t *testing.T) {
	s := taskCreateFixture(t)
	insertTaskConfigurationLKG(t, s, "activation-root")
	if _, err := s.db.Exec(`INSERT INTO fugue_tenants(id,name,slug,status,created_at,updated_at) VALUES('create-tenant','Neutral','neutral','active',now(),now());
 UPDATE fugue_node_updaters SET tenant_id='create-tenant'; DELETE FROM fugue_platform_input_local_pins; DROP TABLE fugue_platform_input_chunks`); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := s.db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	role := quotePostgresIntegrationIdentifier(t, "create_role_"+schema)
	if _, err := s.db.Exec("CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, q := range []string{"DROP OWNED BY " + role, "DROP ROLE " + role} {
			if _, err := s.db.Exec(q); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := s.db.Exec("GRANT USAGE ON SCHEMA " + quotePostgresIntegrationIdentifier(t, schema) + " TO " + role +
		"; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA " + quotePostgresIntegrationIdentifier(t, schema) + " TO " + role); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.User("create_role_" + schema)
	db, err := openPostgresDatabase(u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ordinary := &Store{db: db, databaseURL: u.String()}
	ordinary.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
	principal := model.Principal{TenantID: "other-tenant"}
	if _, err := ordinary.CreateNodeUpdateTask(principal, "create-updater", "", "", model.NodeUpdateTaskTypeRefreshDesiredState, nil); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross-tenant create accepted", err)
	}
	taskCreateAssertEmpty(t, s)
	principal.TenantID = "create-tenant"
	task, err := ordinary.CreateNodeUpdateTask(principal, "create-updater", "", "", model.NodeUpdateTaskTypeRefreshDesiredState, nil)
	if err != nil || task.TenantID != principal.TenantID || task.Status != "pending" {
		t.Fatal("ordinary-role cold pending create failed", task, err)
	}
	if _, err := ordinary.ClaimNodeUpdateTask(task.ID, task.NodeUpdaterID); err == nil {
		t.Fatal("ordinary-role create bypassed pin requirement")
	}
}
