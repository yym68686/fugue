package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestCompilerInputOuterWriterObservesQueueAndCommitOutcomes(t *testing.T) {
	for _, scenario := range []string{"begin", "queue-canceled", "queue-deadline", "commit", "success"} {
		t.Run(scenario, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := &Store{db: db}
			snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: "neutral", PolicyGeneration: "policy"}
			raw, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			want := "other_failures"
			begin := mock.ExpectBegin()
			if scenario == "begin" {
				begin.WillReturnError(errors.New("synthetic begin failure"))
			} else {
				lock := mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(315609238744290)`))
				switch scenario {
				case "queue-canceled":
					lock.WillReturnError(fmt.Errorf("queue: %w", context.Canceled))
					want = "canceled_failures"
				case "queue-deadline":
					lock.WillReturnError(fmt.Errorf("queue: %w", context.DeadlineExceeded))
					want = "deadline_failures"
				default:
					lock.WillReturnResult(sqlmock.NewResult(0, 1))
					mock.ExpectQuery("SELECT format,size_bytes,chunk_size,chunk_count,created_at").WithArgs(digest).
						WillReturnRows(sqlmock.NewRows([]string{"format", "size", "chunk_size", "count", "created", "location"}).AddRow(inputFormatV1, len(raw), inputChunkBytes, 1, time.Now(), "legacy"))
					mock.ExpectQuery(regexp.QuoteMeta(`SELECT to_regclass('fugue_platform_input_archives') IS NOT NULL`)).WillReturnRows(sqlmock.NewRows([]string{"installed"}).AddRow(false))
					hash := sha256.Sum256(raw)
					mock.ExpectQuery("SELECT ordinal,chunk_hash,octet_length").WithArgs(digest).
						WillReturnRows(sqlmock.NewRows([]string{"ordinal", "hash", "size", "actual"}).AddRow(0, "sha256:"+hex.EncodeToString(hash[:]), len(raw), hash[:]))
					commit := mock.ExpectCommit()
					if scenario == "commit" {
						commit.WillReturnError(errors.New("synthetic commit failure"))
					} else {
						want = "successful_returns"
					}
				}
				if scenario == "queue-canceled" || scenario == "queue-deadline" {
					mock.ExpectRollback()
				}
			}
			before := compilerInputWriterStats.snapshot()
			err = s.pgEnsureChunkedInput(t.Context(), snapshot, digest)
			if (err == nil) != (scenario == "success") {
				t.Fatalf("scenario %s: %v", scenario, err)
			}
			after := compilerInputWriterStats.snapshot()
			for _, key := range []string{"attempts", "in_flight", "successful_returns", "deadline_failures", "canceled_failures", "other_failures"} {
				var delta uint64
				if key == "attempts" || key == want {
					delta = 1
				}
				if after[key].(uint64) != before[key].(uint64)+delta {
					t.Fatalf("%s: before=%v after=%v", key, before, after)
				}
			}
			observation, err := s.StorageObservation(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if got := observation.(map[string]any)["compiler_input_writer_activity"]; !reflect.DeepEqual(got, after) {
				t.Fatalf("runtime storage snapshot omitted the writer outcome: %v", got)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompilerInputOuterWriterSnapshotAccountsForConcurrentInflight(t *testing.T) {
	var r compilerInputWriterRecorder
	var ready, done sync.WaitGroup
	release := make(chan struct{})
	for range 32 {
		ready.Add(1)
		done.Add(1)
		go func() { defer done.Done(); finish := r.begin(); ready.Done(); <-release; finish(context.Canceled) }()
	}
	ready.Wait()
	before := r.snapshot()
	if before["attempts"] != uint64(32) || before["in_flight"] != uint64(32) || before["canceled_failures"] != uint64(0) {
		t.Fatal(before)
	}
	close(release)
	done.Wait()
	after := r.snapshot()
	if after["attempts"] != uint64(32) || after["in_flight"] != uint64(0) || after["canceled_failures"] != uint64(32) {
		t.Fatal(after)
	}
}

func TestCompilerInputOuterWriterPanicIsNotASuccessfulReturn(t *testing.T) {
	before := compilerInputWriterStats.snapshot()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected the original nil-database panic to propagate")
			}
		}()
		// A panic must retain its original control flow and must not turn the
		// named error's zero value into a successful-return observation.
		_ = (&Store{}).pgEnsureChunkedInput(t.Context(), platformconfig.RuntimeSnapshot{}, "sha256:"+strings.Repeat("a", 64))
	}()
	after := compilerInputWriterStats.snapshot()
	if after["attempts"].(uint64) != before["attempts"].(uint64)+1 ||
		after["in_flight"] != before["in_flight"] || after["successful_returns"] != before["successful_returns"] ||
		after["other_failures"].(uint64) != before["other_failures"].(uint64)+1 {
		t.Fatalf("before=%v after=%v", before, after)
	}
}
