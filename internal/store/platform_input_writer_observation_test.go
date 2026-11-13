package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"github.com/jackc/pgx/v5/pgconn"
)

func TestCompilerInputOuterWriterObservesQueueAndCommitOutcomes(t *testing.T) {
	for _, scenario := range []string{"begin", "queue-canceled", "queue-deadline", "persist", "commit", "success"} {
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
				mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(315609238744291)`)).WillReturnResult(sqlmock.NewResult(0, 1))
				lock := mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock_shared(315609238744290)`))
				switch scenario {
				case "queue-canceled":
					lock.WillReturnError(fmt.Errorf("queue: %w", context.Canceled))
					want = "canceled_failures"
				case "queue-deadline":
					lock.WillReturnError(fmt.Errorf("queue: %w", context.DeadlineExceeded))
					want = "deadline_failures"
				default:
					lock.WillReturnResult(sqlmock.NewResult(0, 1))
					manifest := mock.ExpectQuery("SELECT format,size_bytes,chunk_size,chunk_count,created_at").WithArgs(digest)
					if scenario == "persist" {
						manifest.WillReturnError(errors.New("synthetic private database failure"))
						break
					}
					manifest.WillReturnRows(sqlmock.NewRows([]string{"format", "size", "chunk_size", "count", "created", "location"}).AddRow(inputFormatV1, len(raw), inputChunkBytes, 1, time.Now(), "legacy"))
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
				if scenario == "queue-canceled" || scenario == "queue-deadline" || scenario == "persist" {
					mock.ExpectRollback()
				}
			}
			before := compilerInputWriterStats.snapshot()
			priorDiagnostic := compilerInputWriterStats.diagnosticSnapshot()
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
			diagnostic := observation.(map[string]any)["compiler_input_writer_diagnostic"].(map[string]any)
			if scenario == "success" {
				if !reflect.DeepEqual(priorDiagnostic["last_failure"], diagnostic["last_failure"]) {
					t.Fatal("a successful return erased the last failure")
				}
			} else {
				failure := diagnostic["last_failure"].(compilerInputWriterFailure)
				wantPhase := map[string]string{"begin": "begin_tx", "queue-canceled": "writer_barrier", "queue-deadline": "writer_barrier", "persist": "persist_verify", "commit": "commit"}[scenario]
				if failure.Phase != wantPhase || failure.WriterStateAtFailure != "active_with_deadline" {
					t.Fatal("failure phase or pre-cleanup context was lost", failure)
				}
				wantSource := map[string]string{"queue-canceled": "canceled_unattributed", "queue-deadline": "deadline_unattributed"}[scenario]
				if wantSource == "" {
					wantSource = "non_deadline_failure"
				}
				if failure.DeadlineSource != wantSource {
					t.Fatal("driver errors must not be attributed to our deferred cancel", failure)
				}
				if (failure.WriterBudgetAfterBeginMS >= 0) != (scenario != "begin") ||
					(failure.WriterBudgetAfterBarrierMS >= 0) != (scenario == "persist" || scenario == "commit") {
					t.Fatal("an unreached checkpoint was reported as observed", failure)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompilerInputWriterDiagnosticDeadlineOwnership(t *testing.T) {
	for _, scenario := range []string{"parent-deadline", "writer-deadline", "parent-cancel", "writer-cancel", "database-cancel"} {
		t.Run(scenario, func(t *testing.T) {
			parent, cancelParent := context.WithTimeout(t.Context(), time.Minute)
			defer cancelParent()
			var writer context.Context
			var cancelWriter context.CancelFunc
			resultErr := error(context.DeadlineExceeded)
			want := ""
			switch scenario {
			case "parent-deadline":
				cancelParent()
				parent, cancelParent = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
				defer cancelParent()
				writer, cancelWriter = context.WithTimeout(parent, 2*time.Minute)
				want = "parent_deadline_observed"
			case "writer-deadline":
				writer, cancelWriter = context.WithDeadline(parent, time.Now().Add(-time.Second))
				want = "writer_budget_deadline_observed"
			default:
				writer, cancelWriter = context.WithTimeout(parent, 2*time.Minute)
				switch scenario {
				case "parent-cancel":
					cancelParent()
					resultErr, want = context.Canceled, "parent_canceled_observed"
				case "writer-cancel":
					cancelWriter()
					resultErr, want = context.Canceled, "writer_canceled_observed"
				case "database-cancel":
					resultErr = &pgconn.PgError{Code: "57014", Message: "private server detail"}
					want = "database_query_canceled"
				}
			}
			defer cancelWriter()
			var recorder compilerInputWriterRecorder
			attempt := recorder.beginObserved(parent)
			attempt.setWriterContext(writer)
			attempt.advance(compilerInputWriterBarrier)
			attempt.stopWork()
			// Cleanup cancellation must not change the previously sampled cause.
			cancelWriter()
			attempt.finish(resultErr)
			failure := recorder.diagnosticSnapshot()["last_failure"].(compilerInputWriterFailure)
			if failure.DeadlineSource != want || failure.Phase != "writer_barrier" {
				t.Fatal(failure)
			}
			if scenario == "database-cancel" && failure.WriterStateAtFailure != "active_with_deadline" {
				t.Fatal("post-return cleanup changed the failed operation's context", failure)
			}
		})
	}
}

func TestCompilerInputWriterDiagnosticIsBoundedAndIndependentOfV1Counters(t *testing.T) {
	var recorder compilerInputWriterRecorder
	if recorder.diagnosticSnapshot()["last_failure"] != nil {
		t.Fatal("empty recorder invented a failure")
	}
	wantKeys := []string{"schema", "attempts", "in_flight", "successful_returns", "deadline_failures", "canceled_failures", "other_failures"}
	for range 100 {
		attempt := recorder.beginObserved(context.Background())
		attempt.finish(errors.New(strings.Repeat("private content must never be retained", 1000)))
	}
	counters := recorder.snapshot()
	if len(counters) != len(wantKeys) || counters["schema"] != "fugue.compiler-input-writer.observation.v1" {
		t.Fatal("the strict v1 counter schema changed", counters)
	}
	for _, key := range wantKeys {
		if _, ok := counters[key]; !ok {
			t.Fatal("v1 field missing", key)
		}
	}
	diagnostic := recorder.diagnosticSnapshot()
	failure := diagnostic["last_failure"].(compilerInputWriterFailure)
	if len(failure.PhaseMS) != int(compilerInputWriterPhaseCount) || failure.WriterBudgetEntryMS != -1 || failure.WriterStateAtFailure != "unavailable" {
		t.Fatal(failure)
	}
	failure.PhaseMS["untrusted"] = 42
	encoded, err := json.Marshal(recorder.diagnosticSnapshot())
	if err != nil || len(encoded) > 2048 || strings.Contains(string(encoded), "private") || strings.Contains(string(encoded), "untrusted") {
		t.Fatal("diagnostics retained error details, unbounded history or a mutable map", len(encoded), err, string(encoded))
	}
	before := recorder.diagnosticSnapshot()
	recorder.begin()(nil)
	if !reflect.DeepEqual(before["last_failure"], recorder.diagnosticSnapshot()["last_failure"]) {
		t.Fatal("success erased failure evidence")
	}
}

func TestCompilerInputWriterDiagnosticBeforeTransaction(t *testing.T) {
	if err := (&Store{}).pgEnsureChunkedInput(t.Context(), platformconfig.RuntimeSnapshot{}, "invalid"); err == nil {
		t.Fatal("invalid input succeeded")
	}
	failure := compilerInputWriterStats.diagnosticSnapshot()["last_failure"].(compilerInputWriterFailure)
	if failure.Phase != "validate" || failure.WriterBudgetAfterBeginMS != -1 || failure.WriterBudgetAfterBarrierMS != -1 {
		t.Fatal("early failure invented a transaction checkpoint", failure)
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
