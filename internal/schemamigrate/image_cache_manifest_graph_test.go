package schemamigrate

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const imageCacheManifestGraphInspectPattern = `(?s)SELECT format_type\(attribute\.atttypid,.*attribute\.attname = 'referenced_manifests_json'`

func imageCacheManifestGraphRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"data_type", "is_nullable"}).AddRow("jsonb", "YES")
}

func TestApplyImageCacheManifestGraphIsLockedAndIdempotent(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing *sqlmock.Rows
		apply    bool
	}{
		{name: "missing", existing: sqlmock.NewRows([]string{"data_type", "is_nullable"}), apply: true},
		{name: "exact", existing: imageCacheManifestGraphRows()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(`SELECT set_config('lock_timeout', $1, true)`)).
				WithArgs(platformStateLockLimit.String()).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock($1)`)).
				WithArgs(imageCacheManifestGraphLockID).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(imageCacheManifestGraphInspectPattern).WillReturnRows(tc.existing)
			if tc.apply {
				mock.ExpectExec(regexp.QuoteMeta(imageCacheManifestGraphSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(imageCacheManifestGraphInspectPattern).WillReturnRows(imageCacheManifestGraphRows())
			}
			mock.ExpectCommit()
			if err := applyImageCacheManifestGraph(context.Background(), database); err != nil {
				t.Fatalf("apply image-cache manifest graph: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
