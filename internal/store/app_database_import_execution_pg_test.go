package store

import (
	"context"
	"errors"
	"testing"

	"fugue/internal/appdbimport"
	"fugue/internal/model"
)

func TestPostgresAppDatabaseImportHandoffRecoversWithoutReplayingSQL(t *testing.T) {
	t.Parallel()
	t.Run("expired executor", func(t *testing.T) { testPostgresAppDatabaseImportHandoff(t, false) })
	t.Run("tool failure after SQL begins", func(t *testing.T) { testPostgresAppDatabaseImportHandoff(t, true) })
}

func testPostgresAppDatabaseImportHandoff(t *testing.T, completeWithError bool) {
	t.Helper()
	s := newStoreTestDatabase(t, "")
	ctx := context.Background()
	tenant, err := s.CreateTenant("Import handoff")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores: 500, MemoryMebibytes: 2048, StorageGibibytes: 60,
	}); err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "import", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "import", "", model.AppSpec{
		Image: "ghcr.io/example/demo:latest", Replicas: 1, RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{Database: "demo", User: "demo", Password: "secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	upload, err := s.CreateSourceUpload(tenant.ID, "dump.sql", "application/sql", []byte("select 1;"))
	if err != nil {
		t.Fatal(err)
	}
	request := model.AppDatabaseImportJob{
		AppID: app.ID, TenantID: tenant.ID, SourceUploadID: upload.ID,
		Format: model.AppDatabaseImportFormatSQL, Status: model.OperationStatusPending,
	}
	job, err := s.CreateAppDatabaseImportJob(request)
	if err != nil {
		t.Fatal(err)
	}
	expire := func() {
		t.Helper()
		if _, err := s.db.ExecContext(ctx, `UPDATE fugue_meta SET updated_at=clock_timestamp()-interval '61 seconds' WHERE key=$1`, appDatabaseImportAttemptPrefix+job.ID); err != nil {
			t.Fatal(err)
		}
	}
	apiExecution := s.APIDatabaseImportExecution()
	if pending, err := apiExecution.ListPending(ctx, 5); err != nil || len(pending) != 1 || pending[0] != job.ID {
		t.Fatalf("API execution list = %v, %v", pending, err)
	}

	if _, err := s.ClaimAppDatabaseImportAttempt(ctx, AppDatabaseImportWorkerWriter, job.ID, "standby"); !errors.Is(err, ErrConflict) {
		t.Fatalf("standby claim returned %v", err)
	}
	if err := s.SetTenantAccessHold(ctx, tenant.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimAppDatabaseImportAttempt(ctx, AppDatabaseImportAPIWriter, job.ID, "held"); !errors.Is(err, ErrConflict) {
		t.Fatalf("held tenant's selected job could be claimed: %v", err)
	}
	if err := s.SetTenantAccessHold(ctx, tenant.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimAppDatabaseImportAttempt(ctx, AppDatabaseImportAPIWriter, job.ID, "api-1"); err != nil {
		t.Fatal(err)
	}
	sibling, err := s.CreateAppDatabaseImportJob(request)
	if err != nil {
		t.Fatal(err)
	}
	if pending, err := s.ListPendingAppDatabaseImportJobs(1); err != nil || len(pending) != 0 {
		t.Fatalf("pending jobs while the same app is importing = %+v, %v", pending, err)
	}
	if _, err := s.ClaimAppDatabaseImportAttempt(ctx, AppDatabaseImportAPIWriter, sibling.ID, "api-2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("second import for the same database was admitted: %v", err)
	}
	if transferred, err := s.TransferAppDatabaseImportsToWorker(ctx); err != nil || transferred {
		t.Fatalf("busy transfer returned %v, %v", transferred, err)
	}
	expire()
	if err := s.RecoverExpiredAppDatabaseImportAttempts(ctx, AppDatabaseImportAPIWriter, 5); err != nil {
		t.Fatal(err)
	}
	if recovered, err := s.GetAppDatabaseImportJob(app.ID, job.ID); err != nil || recovered.Status != model.OperationStatusPending {
		t.Fatalf("preparation recovery returned %+v, %v", recovered, err)
	}
	for range 2 {
		if transferred, err := s.TransferAppDatabaseImportsToWorker(ctx); err != nil || !transferred {
			t.Fatalf("drained transfer returned %v, %v", transferred, err)
		}
	}
	if _, err := s.ClaimAppDatabaseImportJob(job.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("rollback API claim returned %v", err)
	}
	if pending, err := apiExecution.ListPending(ctx, 5); !errors.Is(err, appdbimport.ErrUnclaimable) || len(pending) != 0 {
		t.Fatalf("old API adapter listed work after transfer: %v, %v", pending, err)
	}
	if _, err := apiExecution.Claim(ctx, job.ID, "old-api-lease"); !errors.Is(err, appdbimport.ErrUnclaimable) {
		t.Fatalf("old API adapter claimed work after transfer: %v", err)
	}
	if err := apiExecution.RecoverExpired(ctx, 5); !errors.Is(err, appdbimport.ErrUnclaimable) {
		t.Fatalf("old API adapter could recover work after transfer: %v", err)
	}
	if _, err := s.ClaimAppDatabaseImportAttempt(ctx, AppDatabaseImportWorkerWriter, job.ID, "worker-1"); err != nil {
		t.Fatal(err)
	}
	connection, err := s.ReadAppDatabaseImportConnection(ctx, AppDatabaseImportWorkerWriter, job.ID, "worker-1")
	if err != nil || connection.Database != "demo" || connection.ServiceID == "" {
		t.Fatalf("live attempt could not resolve its database: %v", err)
	}
	if _, err := s.ReadAppDatabaseImportConnection(ctx, AppDatabaseImportWorkerWriter, sibling.ID, "worker-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("attempt accessed a sibling's database: %v", err)
	}
	// Simulate older metadata changing after claim, bypassing the compatible
	// lifecycle lock to exercise the import executor's last read boundary.
	for _, step := range []struct {
		value string
		want  error
	}{{"true", ErrManagedPostgresDatabaseImportConflict}, {"false", nil}} {
		if _, err := s.db.ExecContext(ctx, `UPDATE fugue_backing_services
SET spec_json=jsonb_set(spec_json,'{postgres,suspended}',$2::jsonb) WHERE id=$1`, connection.ServiceID, step.value); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ReadAppDatabaseImportConnection(ctx, AppDatabaseImportWorkerWriter, job.ID, "worker-1"); !errors.Is(err, step.want) {
			t.Fatalf("read target with suspended=%s: got %v, want %v", step.value, err, step.want)
		}
	}
	archive, err := s.ReadAppDatabaseImportArchive(ctx, AppDatabaseImportWorkerWriter, job.ID, "worker-1", 1024)
	if err != nil || archive.Filename != "dump.sql" || string(archive.Bytes) != "select 1;" {
		t.Fatalf("live attempt did not receive its archive: %v", err)
	}
	if _, err := s.ReadAppDatabaseImportArchive(ctx, AppDatabaseImportWorkerWriter, job.ID, "worker-1", 1); err == nil {
		t.Fatal("archive read ignored its size limit")
	}
	if _, err := s.ReadAppDatabaseImportArchive(ctx, AppDatabaseImportWorkerWriter, sibling.ID, "worker-1", 1024); !errors.Is(err, ErrNotFound) {
		t.Fatalf("attempt read a sibling archive: %v", err)
	}
	if _, err := s.ReadAppDatabaseImportArchive(ctx, AppDatabaseImportAPIWriter, job.ID, "api-1", 1024); !errors.Is(err, ErrConflict) {
		t.Fatalf("old API attempt read the archive: %v", err)
	}
	if err := s.BeginAppDatabaseImportEffect(ctx, AppDatabaseImportAPIWriter, job.ID, "api-1"); !errors.Is(err, ErrConflict) {
		t.Fatalf("old API attempt reached SQL: %v", err)
	}
	if err := s.SetTenantAccessHold(ctx, tenant.ID, true); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginAppDatabaseImportEffect(ctx, AppDatabaseImportWorkerWriter, job.ID, "worker-1"); !errors.Is(err, ErrConflict) {
		t.Fatalf("tenant held since claim reached SQL: %v", err)
	}
	if err := s.SetTenantAccessHold(ctx, tenant.ID, false); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginAppDatabaseImportEffect(ctx, AppDatabaseImportWorkerWriter, job.ID, "worker-1"); err != nil {
		t.Fatal(err)
	}
	otherApp, err := s.CreateApp(tenant.ID, project.ID, "other-import", "", model.AppSpec{
		Image: "ghcr.io/example/demo:latest", Replicas: 1, RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{Database: "other", User: "demo", Password: "secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	unrelated := request
	unrelated.AppID = otherApp.ID
	other, err := s.CreateAppDatabaseImportJob(unrelated)
	if err != nil {
		t.Fatal(err)
	}
	wantLogs := 1
	if completeWithError {
		if _, err := s.CompleteAppDatabaseImportAttempt(ctx, AppDatabaseImportWorkerWriter, job.ID, "worker-1",
			model.OperationStatusFailed, "", "tool failed after attempting SQL"); err != nil {
			t.Fatal(err)
		}
		wantLogs++
	} else {
		expire()
		if err := s.RecoverExpiredAppDatabaseImportAttempts(ctx, AppDatabaseImportWorkerWriter, 5); err != nil {
			t.Fatal(err)
		}
	}
	lost, err := s.GetAppDatabaseImportJob(app.ID, job.ID)
	if err != nil || lost.Status != model.OperationStatusFailed || lost.ErrorMessage != AppDatabaseImportAmbiguousMessage ||
		len(lost.Logs) != wantLogs || lost.Logs[wantLogs-1].Message != AppDatabaseImportAmbiguousMessage {
		t.Fatalf("executing recovery returned %+v, %v", lost, err)
	}
	if completeWithError && lost.Logs[0].Message != "tool failed after attempting SQL" {
		t.Fatalf("client failure detail not retained in the job log: %+v", lost.Logs)
	}
	if _, err := s.CompleteAppDatabaseImportAttempt(ctx, AppDatabaseImportWorkerWriter, job.ID, "worker-1", model.OperationStatusCompleted, "done", ""); !errors.Is(err, ErrConflict) {
		t.Fatalf("expired completion returned %v", err)
	}
	if err := s.AppendAppDatabaseImportAttemptLog(ctx, AppDatabaseImportWorkerWriter, job.ID, "worker-1", "stale"); !errors.Is(err, ErrConflict) {
		t.Fatalf("expired log append returned %v", err)
	}
	obsoleted, err := s.GetAppDatabaseImportJob(app.ID, sibling.ID)
	if err != nil || obsoleted.Status != model.OperationStatusFailed ||
		obsoleted.ErrorMessage != appDatabaseImportQueueInvalidatedMessage || obsoleted.StartedAt != nil {
		t.Fatalf("queued sibling after unknown outcome = %+v, %v", obsoleted, err)
	}
	if _, err := s.ClaimAppDatabaseImportAttempt(ctx, AppDatabaseImportWorkerWriter, sibling.ID, "stale-selection"); !errors.Is(err, ErrConflict) {
		t.Fatalf("an old queue selection claimed a failed sibling: %v", err)
	}
	if _, err := s.ClaimAppDatabaseImportAttempt(ctx, AppDatabaseImportWorkerWriter, other.ID, "unrelated"); err != nil {
		t.Fatalf("another application's queued import was affected: %v", err)
	}
	for _, prior := range []string{job.ID, sibling.ID} {
		retry := request
		retry.RetryOfJobID = prior
		if _, err := s.CreateAppDatabaseImportJob(retry); !errors.Is(err, ErrAppDatabaseImportOutcomeUnknown) {
			t.Fatalf("retry of %s with an unknown predecessor returned %v", prior, err)
		}
	}
	if fresh, err := s.CreateAppDatabaseImportJob(request); err != nil || fresh.Status != model.OperationStatusPending {
		t.Fatalf("a fresh import intent after inspection was not accepted: %+v, %v", fresh, err)
	}
}
