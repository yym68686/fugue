package store

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"fugue/internal/platformcontrol"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestConsumerAdmissionLockRetryDoesNotReplayUnmarkedFailures(t *testing.T) {
	for _, original := range []error{compilerInputBarrierBusy(), ErrConflict, context.DeadlineExceeded} {
		calls := 0
		err := retryConsumerAdmissionLock(t.Context(), func(context.Context) error {
			calls++
			return original
		})
		if calls != 1 || err != original {
			t.Fatal("unmarked transaction failure was replayed", original, calls, err)
		}
	}
	busy := &consumerAdmissionLockBusy{compilerInputBarrierBusy()}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	if err := retryConsumerAdmissionLock(ctx, func(context.Context) error {
		calls++
		cancel()
		return busy
	}); err != busy || calls != 1 {
		t.Fatal("canceled admission was replayed", err, calls)
	}
	calls = 0
	if err := retryConsumerAdmissionLock(t.Context(), func(context.Context) error {
		calls++
		return busy
	}); err != busy || calls != 2 {
		t.Fatal("sustained contention did not stop at one retry", err, calls)
	}
}

func TestConsumerAdmissionLockRetryChecksCurrentWitnessesInFreshTransaction(t *testing.T) {
	for _, change := range []bool{false, true} {
		name := "unchanged"
		if change {
			name = "changed after rollback"
		}
		t.Run(name, func(t *testing.T) {
			s, roots, _ := inputActivationFixture(t, 0)
			proof, err := s.prepareCompilerInputActivation(t.Context(), roots)
			if err != nil {
				t.Fatal(err)
			}
			holder, err := s.db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Rollback()
			if _, err := holder.Exec(`SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
				t.Fatal(err)
			}
			ctx, observe := WithPlatformInputFailureObservation(t.Context())
			calls, applied := 0, 0
			err = retryConsumerAdmissionLock(ctx, func(ctx context.Context) error {
				calls++
				err := s.withPreparedCompilerInputActivation(ctx, proof, nil, func(context.Context, *sql.Tx) error {
					applied++
					return nil
				})
				if calls == 1 {
					failure := observe()
					if failure.Check != "admission_writer_busy" || failure.SQLState != "55P03" || applied != 0 {
						t.Fatal("first attempt did not fail before apply", failure, err)
					}
					if releaseErr := holder.Rollback(); releaseErr != nil {
						t.Fatal(releaseErr)
					}
					if change {
						if _, updateErr := s.db.Exec(`UPDATE fugue_platform_artifacts SET updated_at=updated_at WHERE id='activation-root'`); updateErr != nil {
							t.Fatal(updateErr)
						}
					}
				}
				return err
			})
			if calls != 2 {
				t.Fatal("admission did not retry once", calls, err)
			}
			if change {
				if !errors.Is(err, ErrConflict) || applied != 0 || observe().Check != "dependency_changed" {
					t.Fatal("retry accepted or concealed a stale dependency", err, applied, observe())
				}
			} else if err != nil || applied != 1 || observe().Phase != "" {
				t.Fatal("retry did not recover exactly once", err, applied, observe())
			}
		})
	}
}

func TestTrustedHeartbeatPublicationLockRetryAfterRollback(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	claims := platformcontrol.PlatformComponentIdentityClaims{ScopeKey: "neutral-scope"}
	holder, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if err := pgLockPromotionScope(t.Context(), holder, claims.ScopeKey, true); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	calls := 0
	err = retryConsumerAdmissionLock(ctx, func(ctx context.Context) error {
		calls++
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout='50ms'`); err != nil {
			t.Fatal(err)
		}
		rollbackScope, lockErr := pgLockConsumerHeartbeatScope(ctx, tx, claims, platformcontrol.PlatformConsumerHeartbeatEnvelope{})
		if calls == 1 {
			var busy *consumerAdmissionLockBusy
			if !errors.As(lockErr, &busy) || stringSQLState(lockErr) != "55P03" {
				t.Fatal("scope lock timeout was not classified", lockErr)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := holder.Rollback(); err != nil {
				t.Fatal(err)
			}
			return lockErr
		}
		if rollbackScope || lockErr != nil {
			t.Fatal("retry did not acquire the ordinary shared scope", rollbackScope, lockErr)
		}
		return tx.Commit()
	})
	if calls != 2 || err != nil {
		t.Fatal("publication lock did not recover in fresh transaction", calls, err)
	}
}

func TestShadowHeartbeatPublicationLockRetryAfterRollback(t *testing.T) {
	for _, busyAt := range []string{"scope", "consumer"} {
		t.Run(busyAt, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			claims := platformcontrol.PlatformComponentIdentityClaims{ScopeKey: "neutral-scope"}
			scope := regexp.QuoteMeta("SELECT pg_advisory_xact_lock($1)")
			consumer := regexp.QuoteMeta("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
			lockTimeout := &pgconn.PgError{Code: "55P03"}
			mock.ExpectBegin()
			if busyAt == "scope" {
				mock.ExpectExec(scope).WillReturnError(lockTimeout)
			} else {
				mock.ExpectExec(scope).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(consumer).WithArgs("fugue-shadow/" + claims.ConsumerID() + "/" + claims.ScopeKey).WillReturnError(lockTimeout)
			}
			mock.ExpectRollback()
			mock.ExpectBegin()
			mock.ExpectExec(scope).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(consumer).WithArgs("fugue-shadow/" + claims.ConsumerID() + "/" + claims.ScopeKey).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()
			err = retryConsumerAdmissionLock(t.Context(), func(ctx context.Context) error {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				if err := pgLockShadowConsumerHeartbeatScope(ctx, tx, claims); err != nil {
					return err
				}
				return tx.Commit()
			})
			if err != nil {
				t.Fatal("shadow lock did not recover in a fresh transaction", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
