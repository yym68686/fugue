package store

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/schemamigrate"

	"github.com/jackc/pgx/v5/pgconn"
)

func taskTicketFixture(t *testing.T) (*Store, *archiveTestObjects, NodeTaskInputPreparationPlan) {
	t.Helper()
	s, objects, backend, contract := coldTaskAdmissionFixture(t)
	contract.ExpectedPlanSHA256 = ""
	plan, err := s.planNodeTaskInputPreparation(t.Context(), contract, backend)
	if err != nil {
		t.Fatal("trusted ticket plan", err)
	}
	return s, objects, plan
}

func TestTaskPreparationTicketPostgresIsolatedRestoreThenTrustedConfirmation(t *testing.T) {
	s, objects, plan := taskTicketFixture(t)
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)
	// This entry point has only a public verification key, DB and GET reader;
	// it does not receive the Store or any platform keyring.
	receipt, err := restoreNodeTaskInputs(t.Context(), s.db, plan.Ticket, plan.VerificationKey, objects)
	if err != nil || receipt.Prepared || receipt.TaskActivated || receipt.RetirementAuthorized || len(receipt.Pins) != 1 || !receipt.Pins[0].Verified || receipt.Usage.GetRequests != 1 || receipt.Usage.PutRequests != 0 {
		t.Fatal("isolated restore receipt", receipt, err)
	}
	var status string
	if err := s.db.QueryRow(`SELECT status FROM fugue_node_update_tasks WHERE id=$1`, plan.Ticket.Contract.TaskID).Scan(&status); err != nil || status != "pending" {
		t.Fatal("restore activated task", status, err)
	}
	objects.failGet = true
	keys := s.platformArtifactSigningKeyring()
	confirmed, err := RunNodeTaskInputPreparationConfirmation(t.Context(), s.databaseURL, keys, plan.Ticket)
	if err != nil || !confirmed.Prepared || confirmed.TaskActivated || confirmed.RetirementAuthorized || len(confirmed.Pins) != 1 {
		t.Fatal("fresh trusted confirmation", confirmed, err)
	}
	// Clearing caches/pools and disabling GET cannot substitute old chunks:
	// the fixture dropped that relation before planning.
	db, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fresh := &Store{db: db, databaseURL: s.databaseURL}
	fresh.ConfigurePlatformArtifactSigning(keys)
	if task, err := fresh.ClaimNodeUpdateTask(plan.Ticket.Contract.TaskID, plan.Ticket.Contract.UpdaterID); err != nil || task.Status != "running" {
		t.Fatal("fresh claim after isolated restore", task, err)
	}
}

func TestTaskPreparationTicketPostgresOrdinaryApplicationRole(t *testing.T) {
	s, objects, plan := taskTicketFixture(t)
	insertTaskConfigurationLKG(t, s, "activation-root")
	if _, err := s.db.Exec(`UPDATE fugue_node_update_tasks SET task_type='refresh-desired-state',payload_json='{}'`); err != nil {
		t.Fatal(err)
	}
	contract := plan.Ticket.Contract
	contract.ExpectedPlanSHA256 = ""
	var err error
	plan, err = s.planNodeTaskInputPreparation(t.Context(), contract, plan.Ticket.BackendHash)
	if err != nil {
		t.Fatal("ordinary implicit configuration plan", err)
	}
	var schema string
	if err := s.db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	role := "task_prepare_role_" + strings.ReplaceAll(schema, "-", "_")
	quotedRole := quotePostgresIntegrationIdentifier(t, role)
	quotedSchema := quotePostgresIntegrationIdentifier(t, schema)
	if _, err := s.db.Exec("CREATE ROLE " + quotedRole + " LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, q := range []string{"DROP OWNED BY " + quotedRole, "DROP ROLE " + quotedRole} {
			if _, err := s.db.Exec(q); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := s.db.Exec("GRANT USAGE ON SCHEMA " + quotedSchema + " TO " + quotedRole +
		"; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA " + quotedSchema + " TO " + quotedRole +
		"; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA " + quotedSchema + " TO " + quotedRole); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.User(role)
	db, err := openPostgresDatabase(u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	assertPermissionDenied := func(query string) {
		t.Helper()
		_, err := db.Exec(query)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatalf("ordinary role acquired configuration authority: %v", err)
		}
	}
	assertPermissionDenied(`SET temp_file_limit=-1`)
	assertPermissionDenied(`ALTER FUNCTION fugue_input_safety.zero_temp_v1() RENAME TO bypass_temp`)
	assertPermissionDenied(`CREATE FUNCTION fugue_input_safety.bypass_temp() RETURNS int LANGUAGE sql AS 'SELECT 1'`)
	var before, after string
	if err := db.QueryRow(`SHOW temp_file_limit`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.EnforceCompilerInputZeroTemp(t.Context(), tx); err != nil {
		t.Fatal("ordinary zero-temp bound", err)
	}
	if _, err := tx.Exec(`SET LOCAL work_mem='1MB'; SET LOCAL statement_timeout='5s'; SET LOCAL max_parallel_workers_per_gather=0`); err != nil {
		t.Fatal(err)
	}
	// A real disk sort must be stopped by PostgreSQL, not merely by a
	// claimed resource bound or a test that only inspects current_setting.
	var bytes int64
	err = tx.QueryRow(`SELECT sum(length(x)) FROM
 (SELECT md5(i::text) x FROM generate_series(1,262144) i ORDER BY x) sorted`).Scan(&bytes)
	var spill *pgconn.PgError
	if !errors.As(err, &spill) || spill.Code != "53400" {
		t.Fatalf("zero-temp did not refuse real disk spill: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SHOW temp_file_limit`).Scan(&after); err != nil || after != before {
		t.Fatal("transaction guard changed the session default", before, after, err)
	}
	ordinary := &Store{db: db, databaseURL: u.String()}
	ordinary.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
	if _, err := restoreNodeTaskInputs(t.Context(), db, plan.Ticket, plan.VerificationKey, objects); err != nil {
		t.Fatal("ordinary worker restore", err)
	}
	confirmed, err := ordinary.confirmNodeTaskInputPreparation(t.Context(), plan.Ticket)
	if err != nil || !confirmed.Prepared {
		t.Fatal("ordinary trusted confirmation", confirmed, err)
	}
	if task, err := ordinary.ClaimNodeUpdateTask(plan.Ticket.Contract.TaskID, plan.Ticket.Contract.UpdaterID); err != nil || task.Status != "running" {
		t.Fatal("ordinary task claim", task, err)
	}
}

func TestTaskPreparationTicketPostgresRejectsChangedStateBeforeGET(t *testing.T) {
	for _, name := range []string{"owner", "payload", "epoch", "geometry", "created-at", "terminal"} {
		t.Run(name, func(t *testing.T) {
			s, objects, plan := taskTicketFixture(t)
			query := map[string]string{
				"owner":      `INSERT INTO fugue_node_updaters(id,status,token_prefix,token_hash,created_at,updated_at) VALUES('other','ready','other','other',now(),now()); UPDATE fugue_node_update_tasks SET node_updater_id='other'`,
				"payload":    `UPDATE fugue_node_update_tasks SET payload_json='{}'`,
				"epoch":      `UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`,
				"geometry":   `UPDATE fugue_platform_input_manifests SET chunk_size=chunk_size-1`,
				"created-at": `UPDATE fugue_platform_input_manifests SET created_at=created_at+interval '1 second'`,
				"terminal":   `UPDATE fugue_node_update_tasks SET status='completed'`,
			}[name]
			if _, err := s.db.Exec(query); err != nil {
				t.Fatal(err)
			}
			r, err := restoreNodeTaskInputs(t.Context(), s.db, plan.Ticket, plan.VerificationKey, objects)
			if err == nil || r.Prepared || r.TaskActivated || r.Usage.GetRequests != 0 {
				t.Fatal("changed state accepted", r, err)
			}
		})
	}
}

func TestTaskPreparationTicketPostgresConfirmationRejectsRevokedOrChangedState(t *testing.T) {
	for _, name := range []string{"keyring", "pin", "owner", "epoch", "geometry", "artifact"} {
		t.Run(name, func(t *testing.T) {
			s, objects, plan := taskTicketFixture(t)
			if _, err := restoreNodeTaskInputs(t.Context(), s.db, plan.Ticket, plan.VerificationKey, objects); err != nil {
				t.Fatal(err)
			}
			query := map[string]string{
				"pin":      `UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE state='verified'`,
				"owner":    `INSERT INTO fugue_node_updaters(id,status,token_prefix,token_hash,created_at,updated_at) VALUES('other','ready','other','other',now(),now()); UPDATE fugue_node_update_tasks SET node_updater_id='other'`,
				"epoch":    `UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`,
				"geometry": `UPDATE fugue_platform_input_manifests SET chunk_size=chunk_size-1`,
				"artifact": `UPDATE fugue_platform_artifacts SET content_json='{}' WHERE id='activation-artifact-000'`,
			}[name]
			if query != "" {
				if _, err := s.db.Exec(query); err != nil {
					t.Fatal(err)
				}
			} else {
				s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("changed", "changed", "", "", nil))
			}
			if r, err := s.confirmNodeTaskInputPreparation(t.Context(), plan.Ticket); err == nil || r.Prepared || r.TaskActivated {
				t.Fatal("changed confirmation accepted", r, err)
			}
		})
	}
}

func TestTaskPreparationTicketPostgresTamperExpiryAndBudgets(t *testing.T) {
	s, objects, plan := taskTicketFixture(t)
	private, err := taskPreparationSigningKey(s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"tampered-task", "tampered-backend", "expired", "future", "inputs-limit", "raw-limit", "wrong-key", "signature"} {
		t.Run(name, func(t *testing.T) {
			x := plan.Ticket
			key := plan.VerificationKey
			switch name {
			case "tampered-task":
				x.Contract.TaskID = "different-task"
			case "tampered-backend":
				x.BackendHash = "sha256:" + strings.Repeat("c", 64)
			case "expired":
				x.IssuedAt = time.Now().Add(-11 * time.Minute)
				x.ExpiresAt = x.IssuedAt.Add(10 * time.Minute)
			case "future":
				x.IssuedAt = time.Now().Add(time.Minute)
				x.ExpiresAt = x.IssuedAt.Add(10 * time.Minute)
			case "inputs-limit":
				x.Contract.MaxInputs = 0
			case "raw-limit":
				x.Contract.MaxRawBytes = 1
			case "wrong-key":
				key = base64.StdEncoding.EncodeToString(make([]byte, ed25519.PublicKeySize))
			case "signature":
				x.Signature = "invalid"
			}
			// Even a correctly signed expired/over-budget ticket is rejected.
			if name == "expired" || name == "future" || name == "inputs-limit" || name == "raw-limit" {
				raw, err := taskPreparationTicketPayload(x)
				if err != nil {
					t.Fatal(err)
				}
				x.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(private, raw))
			}
			if r, err := restoreNodeTaskInputs(t.Context(), s.db, x, key, objects); err == nil || r.Usage.GetRequests != 0 || r.Prepared {
				t.Fatal("invalid ticket reached restoration", r, err)
			}
		})
	}
	keys := s.platformArtifactSigningKeyring()
	keys.RevokedKeyIDs = map[string]struct{}{strings.ToLower(keys.PrimaryKeyID): {}}
	if _, err := taskPreparationSigningKey(keys); !errors.Is(err, ErrConflict) {
		t.Fatal("revoked primary could sign preparation")
	}
}

func TestTaskPreparationWorkerRejectsPlatformAndWriterSecrets(t *testing.T) {
	names := []string{"FUGUE_BUNDLE_SIGNING_KEY", "FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY", "FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "FUGUE_CREDENTIAL_ENCRYPTION_KEY", "FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY", "FUGUE_AGENT_EDGE_SIGNING_KEY_FILE", "FUGUE_COMPILER_INPUT_ARCHIVE_ACCESS_KEY_ID", "FUGUE_COMPILER_INPUT_ARCHIVE_SECRET_ACCESS_KEY", "FUGUE_COMPILER_INPUT_ARCHIVE_SESSION_TOKEN", "FUGUE_COMPILER_INPUT_ARCHIVE_WRITER_ACCESS_KEY_ID", "FUGUE_COMPILER_INPUT_ARCHIVE_WRITER_SECRET_ACCESS_KEY"}
	for _, name := range names {
		t.Setenv(name, "")
	}
	if err := taskPreparationWorkerIsolation(); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "synthetic-forbidden-secret")
			if !errors.Is(taskPreparationWorkerIsolation(), ErrConflict) {
				t.Fatal("platform or writer key admitted to worker")
			}
		})
	}
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_ENDPOINT", "https://archive.invalid")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_BUCKET", "neutral-archive")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_PREFIX", "inputs")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_REGION", "auto")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_ACCESS_KEY_ID", "reader")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_SECRET_ACCESS_KEY", "synthetic-reader-secret")
	if c, err := NodeTaskPreparationReaderFromEnv(); err != nil || c.AccessKeyID != "reader" || c.SessionToken != "" {
		t.Fatal("dedicated reader not selected", err)
	}
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_READER_ACCESS_KEY_ID", "")
	if _, err := NodeTaskPreparationReaderFromEnv(); err == nil {
		t.Fatal("missing reader silently fell back")
	}
}
