package store

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

// This opt-in test starts and crashes only its newly initialized private
// cluster. It never uses FUGUE_TEST_DATABASE_URL or an existing data directory.
// Synthetic resource bounds exercise accounting, not production byte budgets.
func TestPendingRootResourcePrivatePostgresCrashRecovery(t *testing.T) {
	bin := os.Getenv("FUGUE_RESOURCE_CRASH_POSTGRES_BIN")
	if bin == "" {
		t.Skip("set FUGUE_RESOURCE_CRASH_POSTGRES_BIN to disposable PostgreSQL 17+ tools")
	}
	if !filepath.IsAbs(bin) {
		t.Fatal("PostgreSQL tools directory must be absolute")
	}
	root := t.TempDir()
	data, log := filepath.Join(root, "data"), filepath.Join(root, "postgres.log")
	run := func(t *testing.T, args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, filepath.Join(bin, args[0]), args[1:]...).CombinedOutput()
		if err != nil {
			t.Fatalf("private PostgreSQL %s: %v\n%s", args[0], err, output)
		}
	}
	run(t, "initdb", "-D", data, "-A", "trust", "--no-locale", "--encoding=UTF8", "-U", "resource_test")
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	// No existing cluster or external socket can be selected. A port collision
	// refuses the test; it does not retry against another endpoint.
	options := fmt.Sprintf("-h 127.0.0.1 -p %d -c unix_socket_directories='' -c fsync=on -c full_page_writes=on -c synchronous_commit=on -c autovacuum=off", port)
	start := func(t *testing.T) { run(t, "pg_ctl", "-D", data, "-l", log, "-o", options, "-t", "15", "-w", "start") }
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		status := exec.CommandContext(ctx, filepath.Join(bin, "pg_ctl"), "-D", data, "status")
		if err := status.Run(); err != nil {
			if exit, ok := err.(*exec.ExitError); ok && exit.ExitCode() == 3 {
				return // startup failed before a server was running
			}
			t.Errorf("private cluster cleanup status: %v", err)
			return
		}
		command := exec.CommandContext(ctx, filepath.Join(bin, "pg_ctl"), "-D", data, "-m", "smart", "-t", "15", "-w", "stop")
		if output, err := command.CombinedOutput(); err != nil {
			t.Errorf("private cluster cleanup: %v\n%s", err, output)
		}
	})
	start(t)
	address := url.URL{Scheme: "postgres", User: url.User("resource_test"), Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), Path: "/postgres"}
	parameters := url.Values{"sslmode": {"disable"}, "connect_timeout": {"5"}, "statement_timeout": {"5000"}, "lock_timeout": {"250"}, "temp_file_limit": {"0"}, "work_mem": {"1MB"}}
	address.RawQuery = parameters.Encode()
	open := func(t *testing.T, address string) *Store {
		t.Helper()
		db, err := sql.Open("pgx", address)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(2)
		db.SetMaxIdleConns(0)
		t.Cleanup(func() { _ = db.Close() })
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		var version, actualPort int
		var actualDirectory string
		if err := db.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int,inet_server_port(),current_setting('data_directory')`).Scan(&version, &actualPort, &actualDirectory); err != nil || version < 170000 || actualPort != port || actualDirectory != data {
			t.Fatalf("private server identity/version: %d %d %q %v", version, actualPort, actualDirectory, err)
		}
		return &Store{db: db, databaseURL: address}
	}
	for _, scenario := range []string{"ticket-only", "journal-without-outcome", "terminal-outcome"} {
		t.Run(scenario, func(t *testing.T) {
			admin := open(t, address.String())
			name := "resource_crash_" + strconv.FormatInt(time.Now().UnixNano(), 10)
			if _, err := admin.db.ExecContext(t.Context(), `CREATE DATABASE `+name); err != nil {
				t.Fatal(err)
			}
			_ = admin.db.Close()
			private := address
			private.Path = "/" + name
			s := open(t, private.String())
			if _, err := s.db.ExecContext(t.Context(), schemamigrate.CompilerInputZeroTempSQL); err != nil {
				t.Fatal(err)
			}
			pool, source := resourcePoolFixture(t, s, 1024)
			// Exactly one complete synthetic cost vector fits. No crash gap,
			// missing row or completed outcome can replenish that finite pool.
			pool.SourceVolumeBudgetBytes = 128
			var checks atomic.Int32
			if scenario == "ticket-only" {
				base := source
				source = resourceSourceFunc(func(ctx context.Context, op PendingRootResourceOperation, snapshot PendingRootResourcePoolSnapshot) (PendingRootResourceWitness, error) {
					w, err := base(ctx, op, snapshot)
					if err == nil {
						w.(*resourceWitnessFixture).check = func(context.Context) error {
							if checks.Add(1) == 3 {
								return ErrConflict // journal INSERT rolls back after durable ticket ACK
							}
							return nil
						}
					}
					return w, err
				})
			}
			a := seedResourcePool(t, s, pool, source)
			reservation, err := a.ReservePendingRoot(t.Context(), resourceTestRequest())
			if scenario == "ticket-only" {
				if err == nil || reservation != nil || checks.Load() != 3 {
					t.Fatal("ticket-only failure was not after journal insertion", err, checks.Load())
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if scenario == "terminal-outcome" {
				if err := reservation.(*pendingRootResourceReservation).RecordOutcome(t.Context(), inputarchive.PendingRootCompletionNotConfirmed); err != nil {
					t.Fatal(err)
				}
			}
			read := func(s *Store) (int64, bool, int, int) {
				t.Helper()
				var last int64
				var called bool
				var journals, outcomes int
				if err := s.db.QueryRowContext(t.Context(), `SELECT last_value,is_called,(SELECT count(*) FROM fugue_platform_input_resource_reservations),(SELECT count(*) FROM fugue_platform_input_resource_outcomes) FROM fugue_platform_input_resource_tickets`).Scan(&last, &called, &journals, &outcomes); err != nil {
					t.Fatal(err)
				}
				return last, called, journals, outcomes
			}
			last, called, journals, outcomes := read(s)
			if last != 1 || !called || (scenario == "ticket-only" && journals != 0) || (scenario != "ticket-only" && journals != 1) || (scenario == "terminal-outcome" && outcomes != 1) || (scenario != "terminal-outcome" && outcomes != 0) {
				t.Fatal("unexpected pre-crash durable state", last, called, journals, outcomes)
			}
			// Drop client connections/handles, then exercise actual database WAL
			// recovery. This is not a power-loss, fsync-failure or lost-network-ACK
			// simulation, and supplies no per-operation byte or completion bound.
			reservation, a = nil, nil
			_ = s.db.Close()
			run(t, "pg_ctl", "-D", data, "-m", "immediate", "-t", "15", "-w", "stop")
			start(t)
			reopened := open(t, private.String())
			recoveredLast, recoveredCalled, recoveredJournals, recoveredOutcomes := read(reopened)
			if recoveredLast < last || recoveredLast > 1024 || !recoveredCalled || recoveredJournals != journals || recoveredOutcomes != outcomes {
				t.Fatal("crash recovery lost durable charge/state", recoveredLast, recoveredCalled, recoveredJournals, recoveredOutcomes)
			}
			// PostgreSQL may WAL-log sequence values ahead even with CACHE 1.
			// Treat every skipped ticket as charged; never expect last_value==1.
			a, err = NewPendingRootResourceAdmission(reopened, source, pool)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := a.ReservePendingRoot(t.Context(), resourceTestRequest()); err == nil {
				t.Fatal("crash recovery refunded a charged operation")
			}
			endLast, endCalled, endJournals, endOutcomes := read(reopened)
			if endLast != recoveredLast || endCalled != recoveredCalled || endJournals != journals || endOutcomes != outcomes {
				t.Fatal("refused replay consumed another ticket or changed state")
			}
			t.Logf("recovered ticket high-water %d -> %d; journals=%d outcomes=%d; replay refused", last, recoveredLast, journals, outcomes)
			_ = reopened.db.Close()
		})
	}
}
