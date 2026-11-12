package store

import (
	"context"
	"crypto/sha256"
	"regexp"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"github.com/DATA-DOG/go-sqlmock"
)

// Exercise a valid complete read beyond the former 30-second boundary. This
// represents slow authenticated storage, not a renewed observation timestamp.
func TestCompilerInputFullReadBeyondInteractiveBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("31-second storage deadline regression")
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: "slow-input", PolicyGeneration: "policy"}
	raw, _ := platformconfig.EncodeRuntimeSnapshot(snapshot)
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	s := &Store{databaseURL: "postgres://test", db: db, dbReady: true}
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT format,size_bytes,chunk_size,chunk_count,created_at,COALESCE(to_jsonb(m)->>'storage_location','legacy') FROM fugue_platform_input_manifests m WHERE content_hash=$1`)).WithArgs(digest).
		WillDelayFor(31 * time.Second).
		WillReturnRows(sqlmock.NewRows([]string{"format", "size", "chunk_size", "count", "created", "location"}).AddRow(inputFormatV1, len(raw), len(raw), 1, time.Now(), "legacy"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT ordinal,chunk_hash,data FROM fugue_platform_input_chunks WHERE content_hash=$1 ORDER BY ordinal`)).WithArgs(digest).
		WillReturnRows(sqlmock.NewRows([]string{"ordinal", "chunk_hash", "data"}).AddRow(0, sharedHash(sha256.Sum256(raw)), raw))
	mock.ExpectRollback()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	got, err := s.GetPlatformCompilerInputContext(ctx, digest)
	if err != nil {
		t.Fatal("valid complete read was cut off at the interactive deadline", err)
	}
	if actual, err := platformconfig.RuntimeSnapshotDigest(got); err != nil || actual != digest {
		t.Fatal("slow complete read changed authenticated input", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
