package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPublicationAdmissionOnlyMarksProducerAdvisoryLockTimeout(t *testing.T) {
	for _, name := range []string{"producer advisory", "operator advisory", "scope lookup"} {
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			raw := &pgconn.PgError{Code: "55P03"}
			mock.ExpectBegin()
			lookup := mock.ExpectQuery("SELECT artifact_kind, scope_key FROM fugue_platform_artifacts").WithArgs("candidate")
			if name == "scope lookup" {
				lookup.WillReturnError(raw)
			} else {
				lookup.WillReturnRows(sqlmock.NewRows([]string{"artifact_kind", "scope_key"}).AddRow("release_set", "neutral"))
				mock.ExpectExec(regexp.QuoteMeta("SELECT pg_advisory_xact_lock($1)")).WillReturnError(raw)
			}
			mock.ExpectRollback()
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			err = pgLockPublicationAdmissionArtifact(t.Context(), tx, "candidate", true, name != "operator advisory")
			var busy *publicationScopeLockBusy
			if errors.As(err, &busy) != (name == "producer advisory") || !errors.Is(err, raw) {
				t.Fatal("incorrectly classified lock timeout", err)
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err = mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProducedShadowPublicationRetriesSamePreparedCandidate(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable PostgreSQL")
	}
	for _, name := range []string{"policy lock", "scope lock", "changed witness", "still held"} {
		t.Run(name, func(t *testing.T) {
			f := newServingFixture(t, dsn, true)
			s := f.s
			candidate := f.candidate(t)
			req, guard := producedShadowIntent(candidate.ID, f.authority.ID, "")
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			ctx, trace := startPlatformMutationTrace(ctx, "test-produced-shadow")
			proof, pins, err := s.prepareCompilerInputPublication(ctx, candidate.ID, "", guard, req)
			if err != nil {
				t.Fatal(err)
			}
			holder, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Rollback()
			var pid int
			if err := holder.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
				t.Fatal(err)
			}
			scope, retryStage := candidate.ScopeKey, "retry-admission-output-scope-lock"
			if name == "policy lock" {
				scope, retryStage = f.authority.ScopeKey, "retry-admission-producer-policy-lock"
			}
			if err := pgLockPromotionScope(ctx, holder, scope, true); err != nil {
				t.Fatal(err)
			}
			// Change the witness before the second transaction sees it, while
			// the advisory lock still excludes both publication attempts.
			if name == "changed witness" {
				if _, err := holder.ExecContext(ctx, "UPDATE fugue_platform_artifacts SET updated_at=updated_at WHERE id=$1", candidate.ID); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() {
				a, release, _, _, err := s.pgReleasePreparedPlatformArtifact(ctx, candidate.ID, req, producerTestPrincipal(), guard, proof, pins, trace)
				if err == nil && (a.ID != candidate.ID || release.ArtifactID != candidate.ID) {
					err = errors.New("release did not use the original signed candidate")
				}
				done <- err
			}()
			if name != "still held" {
				waitForProducedPublicationAdvisoryWait(t, ctx, s.db, pid, true)
				waitForProducedPublicationAdvisoryWait(t, ctx, s.db, pid, false)
				if err := holder.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			err = <-done
			if _, retried := trace.durations[retryStage]; !retried {
				t.Fatal("missing retry receipt for", retryStage, err)
			}
			var busy *publicationScopeLockBusy
			switch name {
			case "changed witness":
				if !errors.Is(err, ErrConflict) {
					t.Fatal("retry did not reject stale preparation", err)
				}
			case "still held":
				if !errors.As(err, &busy) || !isAdmissionLockTimeout(err) {
					t.Fatal("retry concealed sustained contention", err)
				}
			default:
				if err != nil {
					t.Fatal("retry did not recover", err)
				}
				return
			}
			if _, _, found, err := s.GetActivePlatformArtifact(candidate.ArtifactKind, candidate.ScopeKey, "shadow"); err != nil || found {
				t.Fatal("failed retry advanced the shadow lane", found, err)
			}
		})
	}
}

func waitForProducedPublicationAdvisoryWait(t *testing.T, ctx context.Context, db *sql.DB, holder int, want bool) {
	t.Helper()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND wait_event='advisory')`, holder).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("advisory wait state did not appear", want, ctx.Err())
		case <-ticker.C:
		}
	}
}
