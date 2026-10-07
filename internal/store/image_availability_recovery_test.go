package store

import (
	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
	"regexp"
	"strings"
	"testing"
)

func TestRestoreLostImageAvailabilityPostgresOnlyUpdatesMatchingLostIdentity(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db, databaseURL: "postgres://fixture", dbReady: true}
	digest := "sha256:" + strings.Repeat("a", 64)
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE fugue_images SET lifecycle_state=$4, updated_at=$5 WHERE id=$1 AND tenant_id=$2 AND canonical_digest=$3 AND lifecycle_state=$6`)).WithArgs("image_fixture", "tenant_fixture", digest, model.ImageLifecycleAvailable, sqlmock.AnyArg(), model.ImageLifecycleLost).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := s.RestoreLostImageAvailability("image_fixture", "tenant_fixture", digest); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
