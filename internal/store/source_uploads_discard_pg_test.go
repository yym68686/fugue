package store

import (
	"database/sql"
	"errors"
	"testing"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGDiscardNewSourceUploadRequiresExactCapability(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	proof := model.SourceUpload{ID: "upload_capability", TenantID: "tenant_capability", DownloadToken: "fugue_upload_capability"}

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT id.*FROM fugue_source_uploads.*WHERE id = \$1 AND tenant_id = \$2 AND download_token = \$3.*FOR UPDATE`).
		WithArgs(proof.ID, proof.TenantID, proof.DownloadToken).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	if err := stateStore.DiscardNewSourceUpload(proof); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DiscardNewSourceUpload error = %v, want not found", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGDiscardNewSourceUploadRefusesDurableReference(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	proof := model.SourceUpload{ID: "upload_referenced", TenantID: "tenant_referenced", DownloadToken: "fugue_upload_referenced"}

	mock.ExpectBegin()
	expectPGDiscardSourceUploadCapability(mock, proof)
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_app_database_import_jobs.*FROM fugue_apps.*FROM fugue_operations`).
		WithArgs(proof.ID, proof.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectRollback()

	if err := stateStore.DiscardNewSourceUpload(proof); !errors.Is(err, ErrConflict) {
		t.Fatalf("DiscardNewSourceUpload referenced error = %v, want conflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGDiscardNewSourceUploadDeletesOnlyExactUnreferencedRow(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	proof := model.SourceUpload{ID: "upload_unpublished", TenantID: "tenant_unpublished", DownloadToken: "fugue_upload_unpublished"}

	mock.ExpectBegin()
	expectPGDiscardSourceUploadCapability(mock, proof)
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_app_database_import_jobs.*FROM fugue_apps.*FROM fugue_operations`).
		WithArgs(proof.ID, proof.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectQuery(`(?s)DELETE FROM fugue_source_uploads AS upload.*upload.id = \$1.*upload.tenant_id = \$2.*upload.download_token = \$3.*NOT EXISTS.*fugue_app_database_import_jobs.*RETURNING upload.id`).
		WithArgs(proof.ID, proof.TenantID, proof.DownloadToken).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(proof.ID))
	mock.ExpectCommit()

	if err := stateStore.DiscardNewSourceUpload(proof); err != nil {
		t.Fatalf("discard exact new source upload: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func expectPGDiscardSourceUploadCapability(mock sqlmock.Sqlmock, proof model.SourceUpload) {
	mock.ExpectQuery(`(?s)SELECT id.*FROM fugue_source_uploads.*WHERE id = \$1 AND tenant_id = \$2 AND download_token = \$3.*FOR UPDATE`).
		WithArgs(proof.ID, proof.TenantID, proof.DownloadToken).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(proof.ID))
}
