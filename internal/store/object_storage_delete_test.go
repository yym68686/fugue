package store

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"regexp"
	"testing"

	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestDeleteObjectStoreRetainsOtherStoresAndRequiresRevokedGrants(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two"} {
		if err := s.SaveObjectStore(model.ObjectStore{ID: id, TenantID: "tenant", ProjectID: "project", Name: id, Status: "active"}); err != nil {
			t.Fatal(err)
		}
		if err := s.SaveObjectStorageCredential(model.ObjectStorageCredentialRecord{Credential: model.ObjectStorageCredential{ID: "grant-" + id, StoreID: id, Name: "writer", Status: "active"}}); err != nil {
			t.Fatal(err)
		}
	}
	v, _ := s.GetObjectStore("one")
	if err := s.DeleteObjectStore(v.ID); !errors.Is(err, ErrConflict) {
		t.Fatal("active store removed", err)
	}
	v.Status = "deleting"
	if err := s.SaveObjectStore(v); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteObjectStore(v.ID); !errors.Is(err, ErrConflict) {
		t.Fatal("live grant removed", err)
	}
	recs, _ := s.ListObjectStorageCredentials(v.ID)
	recs[0].Credential.Status = "revoked"
	if err := s.SaveObjectStorageCredential(recs[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteObjectStore(v.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetObjectStore("two"); err != nil {
		t.Fatal("other store removed", err)
	}
	if recs, err := s.ListObjectStorageCredentials("two"); err != nil || len(recs) != 1 {
		t.Fatal("other grants removed", err)
	}
	if _, err := s.GetObjectStore(v.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted store remains", err)
	}
	if err := s.DeleteObjectStore(v.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("missing store not detected", err)
	}
}

func TestDeleteObjectStorePostgresRollsBackCredentialRemovalOnFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://test", db: db, dbReady: true}
	v, _ := json.Marshal(model.ObjectStore{ID: "one", Status: "deleting"})
	rec, _ := json.Marshal(model.ObjectStorageCredentialRecord{Credential: model.ObjectStorageCredential{ID: "grant", StoreID: "one", Status: "revoked"}})
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT body FROM fugue_object_stores WHERE id=$1 FOR UPDATE")).WithArgs("one").WillReturnRows(sqlmock.NewRows([]string{"body"}).AddRow(v))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT body FROM fugue_object_storage_credentials WHERE store_id=$1 FOR UPDATE")).WithArgs("one").WillReturnRows(sqlmock.NewRows([]string{"body"}).AddRow(rec))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM fugue_object_storage_credentials WHERE store_id=$1")).WithArgs("one").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM fugue_object_stores WHERE id=$1")).WithArgs("one").WillReturnError(errors.New("synthetic failure"))
	mock.ExpectRollback()
	if err := s.DeleteObjectStore("one"); err == nil {
		t.Fatal("failed store removal committed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
