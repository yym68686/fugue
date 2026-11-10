package store

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

type resourceSourceFunc func(context.Context, PendingRootResourceOperation, PendingRootResourcePoolSnapshot) (PendingRootResourceWitness, error)

func (f resourceSourceFunc) PreparePendingRootResources(ctx context.Context, operation PendingRootResourceOperation, pool PendingRootResourcePoolSnapshot) (PendingRootResourceWitness, error) {
	return f(ctx, operation, pool)
}

type resourceWitnessFixture struct {
	allocation PendingRootResourceAllocation
	check      func(context.Context) error
}

func (w *resourceWitnessFixture) PendingRootResourceAllocation() PendingRootResourceAllocation {
	return w.allocation
}

func (w *resourceWitnessFixture) CheckPendingRootResources(ctx context.Context) error {
	return w.check(ctx)
}

func resourceTestRequest() inputarchive.PendingRootRequest {
	return pendingAdmissionRequest(inputarchive.PendingRootRegistration{Schema: inputarchive.PendingRootSchema, Owner: "authority/neutral", Generation: "generation-neutral", SourceDigest: "sha256:" + strings.Repeat("a", 64), References: []inputarchive.HotReference{{Kind: "release", ID: "release-neutral"}}})
}

func resourcePoolFixture(t *testing.T, s *Store, max uint32) (PendingRootResourcePool, resourceSourceFunc) {
	t.Helper()
	if _, err := s.db.Exec(schemamigrate.CompilerInputResourceReservationsSQL); err != nil {
		t.Fatal(err)
	}
	storage := PendingRootResourceStorageIdentity{ClusterUID: "cluster-neutral", PGPodUID: "pg-neutral", PVCUID: "pvc-neutral", PVUID: "pv-neutral", LonghornVolumeUID: "volume-neutral"}
	if err := s.db.QueryRow(`SELECT current_database(),(SELECT oid FROM pg_database WHERE datname=current_database()),current_schema()`).Scan(&storage.Database, &storage.DatabaseOID, &storage.Schema); err != nil {
		t.Fatal(err)
	}
	// Deliberately synthetic bounds, not measurements of production or this
	// test's SQL allocation. Real PG below tests durable accounting only.
	pool := PendingRootResourcePool{Schema: PendingRootResourcePoolSchema, Epoch: 1, Storage: storage, EvidenceSHA256: strings.Repeat("a", 64),
		ExpiresAt: time.Now().UTC().Add(4 * time.Minute), MaxReservations: max, SourceVolumeBudgetBytes: 4096,
		Limits:    inputarchive.PendingRootWholeOperationBudgetLimits{MaxNewPhysicalAllocationBoundBytes: 32, MaxPeakWALBytes: 32, MaxGrowthDuringBoundedWaitBytes: 32, MaxFailureExitAllocationBytes: 32, MaxWaitAndExitTimeBoundSeconds: 120, MaxReservationLifetime: 5 * time.Minute},
		Aggregate: inputarchive.PendingRootWholeOperationBounds{NewPhysicalAllocationBoundBytes: 1024, PeakWALBytes: 1024, GrowthDuringBoundedWaitBytes: 1024, FailureExitAllocationBytes: 1024, WaitAndExitTimeBoundSeconds: 120}}
	source := resourceSourceFunc(func(ctx context.Context, operation PendingRootResourceOperation, snapshot PendingRootResourcePoolSnapshot) (PendingRootResourceWitness, error) {
		return &resourceWitnessFixture{allocation: PendingRootResourceAllocation{PoolSHA256: snapshot.SHA256, EvidenceSHA256: strings.Repeat("b", 64), AccountingIncluded: true,
			Budget: inputarchive.PendingRootWholeOperationBudget{Schema: inputarchive.PendingRootWholeOperationBudgetSchema, RequestSHA256: operation.RequestSHA256, ReservationEpoch: snapshot.Pool.Epoch, ExpiresAt: snapshot.Pool.ExpiresAt,
				Proven: true, TempFilesForbidden: true, Bounds: inputarchive.PendingRootWholeOperationBounds{NewPhysicalAllocationBoundBytes: 32, PeakWALBytes: 32, GrowthDuringBoundedWaitBytes: 32, FailureExitAllocationBytes: 32, WaitAndExitTimeBoundSeconds: 120}}}, check: func(ctx context.Context) error { return ctx.Err() }}, nil
	})
	return pool, source
}

func seedResourcePool(t *testing.T, s *Store, pool PendingRootResourcePool, source PendingRootResourceSource) *PendingRootResourceAdmission {
	t.Helper()
	raw, _, err := pool.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO fugue_platform_input_resource_pool(singleton,config_json) VALUES(TRUE,$1)`, raw); err != nil {
		t.Fatal(err)
	}
	a, err := NewPendingRootResourceAdmission(s, source, pool)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func resourceReservationCount(t *testing.T, s *Store) int {
	t.Helper()
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_resource_reservations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestPendingRootResourcePostgresClosedCatalog(t *testing.T) {
	s := pendingPolicyFixture(t)
	resourcePoolFixture(t, s, 1)
	tx, err := beginPendingRootPolicyTx(t.Context(), s.db, true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := schemamigrate.VerifyCompilerInputResourceReservations(t.Context(), tx); err != nil {
		for _, table := range []string{"fugue_platform_input_resource_pool", "fugue_platform_input_resource_reservations", "fugue_platform_input_resource_outcomes"} {
			var definitions string
			_ = s.db.QueryRow(`SELECT COALESCE(string_agg(contype::text||':'||pg_get_constraintdef(oid),';' ORDER BY contype::text||':'||pg_get_constraintdef(oid)),'') FROM pg_constraint WHERE conrelid=to_regclass($1)`, table).Scan(&definitions)
			t.Log(table, definitions)
		}
		t.Fatal(err)
	}
}

func TestPendingRootResourcePostgresRejectsAllocationBearingCatalogDrift(t *testing.T) {
	mutations := map[string]string{
		"extra-index":      `CREATE INDEX resource_extra ON fugue_platform_input_resource_reservations(created_at)`,
		"included-index":   `CREATE INDEX resource_extra ON fugue_platform_input_resource_reservations(ticket) INCLUDE(allocation_json)`,
		"expression-index": `CREATE INDEX resource_extra ON fugue_platform_input_resource_reservations(lower(allocation_json))`,
		"column-default":   `ALTER TABLE fugue_platform_input_resource_reservations ALTER COLUMN created_at SET DEFAULT now()`,
		"extra-column":     `ALTER TABLE fugue_platform_input_resource_outcomes ADD COLUMN extra TEXT`,
		"missing-not-null": `ALTER TABLE fugue_platform_input_resource_reservations ALTER COLUMN created_at DROP NOT NULL`,
		"missing-check": `DO $$ DECLARE target TEXT; BEGIN
 SELECT conname INTO STRICT target FROM pg_constraint WHERE conrelid='fugue_platform_input_resource_reservations'::regclass
 AND contype='c' AND pg_get_constraintdef(oid) LIKE '%octet_length(allocation_json)%';
 EXECUTE format('ALTER TABLE fugue_platform_input_resource_reservations DROP CONSTRAINT %I',target); END $$`,
		"missing-foreign-key": `ALTER TABLE fugue_platform_input_resource_outcomes DROP CONSTRAINT fugue_platform_input_resource_outcomes_reservation_id_fkey`,
		"table-options":       `ALTER TABLE fugue_platform_input_resource_reservations SET(fillfactor=50)`,
		"toast-options":       `ALTER TABLE fugue_platform_input_resource_reservations SET(toast.autovacuum_enabled=false)`,
		"column-storage":      `ALTER TABLE fugue_platform_input_resource_reservations ALTER COLUMN allocation_json SET STORAGE PLAIN`,
		"column-compression":  `ALTER TABLE fugue_platform_input_resource_reservations ALTER COLUMN allocation_json SET COMPRESSION pglz`,
		"sequence-owned":      `ALTER SEQUENCE fugue_platform_input_resource_tickets OWNED BY fugue_platform_input_resource_reservations.ticket`,
		"extra-trigger": `CREATE FUNCTION resource_side_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
 CREATE TRIGGER resource_side_effect BEFORE INSERT ON fugue_platform_input_resource_reservations FOR EACH ROW EXECUTE FUNCTION resource_side_effect()`,
		"rewrite-rule": `CREATE RULE resource_side_effect AS ON INSERT TO fugue_platform_input_resource_outcomes DO ALSO NOTIFY resource_side_effect`,
	}
	for name, mutation := range mutations {
		t.Run(name, func(t *testing.T) {
			for _, charged := range []bool{false, true} {
				s := pendingPolicyFixture(t)
				pool, source := resourcePoolFixture(t, s, 2)
				var preparations atomic.Int32
				counted := resourceSourceFunc(func(ctx context.Context, op PendingRootResourceOperation, snapshot PendingRootResourcePoolSnapshot) (PendingRootResourceWitness, error) {
					preparations.Add(1)
					return source(ctx, op, snapshot)
				})
				a := seedResourcePool(t, s, pool, counted)
				var reservation inputarchive.PendingRootReservation
				if charged {
					var err error
					reservation, err = a.ReservePendingRoot(t.Context(), resourceTestRequest())
					if err != nil {
						t.Fatal(err)
					}
				}
				if _, err := s.db.Exec(mutation); err != nil {
					t.Fatal("disposable mutation", err)
				}
				before := preparations.Load()
				if got, err := a.ReservePendingRoot(t.Context(), resourceTestRequest()); err == nil || got != nil || preparations.Load() != before {
					t.Fatal("drift reached independent preparation/admission", err, charged)
				}
				if charged {
					if reservation.CheckPendingRootReservation(t.Context()) == nil {
						t.Fatal("existing handle accepted catalog drift")
					}
					if reservation.(*pendingRootResourceReservation).RecordOutcome(t.Context(), inputarchive.PendingRootCompletionNotConfirmed) == nil {
						t.Fatal("outcome wrote to a changed allocation contract")
					}
				}
				var called bool
				var outcomes int
				if err := s.db.QueryRow(`SELECT is_called FROM fugue_platform_input_resource_tickets`).Scan(&called); err != nil || called != charged {
					t.Fatal("refusal consumed/refunded ticket", called, charged, err)
				}
				want := 0
				if charged {
					want = 1
				}
				if resourceReservationCount(t, s) != want {
					t.Fatal("refusal changed journal charge")
				}
				if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_resource_outcomes`).Scan(&outcomes); err != nil || outcomes != 0 {
					t.Fatal("refusal fabricated outcome", outcomes, err)
				}
			}
		})
	}
}

func TestPendingRootResourcePostgresCatalogLockExcludesConcurrentDDL(t *testing.T) {
	s := pendingPolicyFixture(t)
	resourcePoolFixture(t, s, 1)
	tx, err := beginPendingRootPolicyTx(t.Context(), s.db, false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := schemamigrate.VerifyCompilerInputResourceReservations(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	conn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), `SET lock_timeout='100ms'`); err != nil {
		t.Fatal(err)
	}
	for _, ddl := range []string{
		`ALTER TABLE fugue_platform_input_resource_outcomes ADD COLUMN extra TEXT`,
		`CREATE INDEX CONCURRENTLY resource_racing ON fugue_platform_input_resource_reservations(created_at)`,
	} {
		_, err := conn.ExecContext(t.Context(), ddl)
		var state interface{ SQLState() string }
		if !errors.As(err, &state) || state.SQLState() != "55P03" {
			t.Fatal("verified write shape changed before transaction end", err)
		}
	}
}

func TestPendingRootResourceBoundsConservativeVolumeAndOverflow(t *testing.T) {
	b := inputarchive.PendingRootWholeOperationBounds{NewPhysicalAllocationBoundBytes: math.MaxUint64, PeakWALBytes: 1}
	if _, ok := resourceVolumeCharge(b); ok {
		t.Fatal("volume charge wrapped around uint64")
	}
	if _, ok := addResourceBounds(b, inputarchive.PendingRootWholeOperationBounds{NewPhysicalAllocationBoundBytes: 1}); ok {
		t.Fatal("aggregate wrapped around uint64")
	}
	first := inputarchive.PendingRootWholeOperationBounds{NewPhysicalAllocationBoundBytes: 2, PeakWALBytes: 3, PeakTempBytes: 5, GrowthDuringBoundedWaitBytes: 7, FailureExitAllocationBytes: 11, WaitAndExitTimeBoundSeconds: 100}
	sum, ok := addResourceBounds(first, first)
	if !ok || sum.WaitAndExitTimeBoundSeconds != 100 {
		t.Fatal("time was added as allocatable bytes", sum)
	}
	if volume, ok := resourceVolumeCharge(sum); !ok || volume != 56 {
		t.Fatal("one source-volume cost component omitted", volume, ok)
	}
}

func TestPendingRootResourcePostgresSerializesAcrossPoolsAndKeepsUncertainCharges(t *testing.T) {
	s := pendingPolicyFixture(t)
	pool, source := resourcePoolFixture(t, s, 16)
	// Per-component aggregate caps would admit sixteen operations, but the
	// shared source-volume cap allows exactly one complete cost vector.
	pool.SourceVolumeBudgetBytes = 128
	a := seedResourcePool(t, s, pool, source)
	otherDB, err := sql.Open("pgx", s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = otherDB.Close() })
	other, err := NewPendingRootResourceAdmission(&Store{db: otherDB, databaseURL: s.databaseURL}, source, pool)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan inputarchive.PendingRootReservation, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			provider := a
			if index%2 == 1 {
				provider = other
			}
			reservation, err := provider.ReservePendingRoot(t.Context(), resourceTestRequest())
			if err == nil {
				results <- reservation
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	var winner inputarchive.PendingRootReservation
	count := 0
	for reservation := range results {
		winner = reservation
		count++
	}
	if count != 1 || resourceReservationCount(t, s) != 1 {
		t.Fatal("cross-process source-volume oversubscription", count)
	}
	if err := winner.CheckPendingRootReservation(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Lost response/crashed process: no outcome at all. A reopened provider
	// sees the durable charge and cannot treat the missing ACK as a refund.
	reopened, err := NewPendingRootResourceAdmission(s, source, pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ReservePendingRoot(t.Context(), resourceTestRequest()); err == nil {
		t.Fatal("reopen erased an unacknowledged reservation")
	}
	r := winner.(*pendingRootResourceReservation)
	if err := r.RecordOutcome(t.Context(), inputarchive.PendingRootCompletionNotConfirmed); err != nil {
		t.Fatal(err)
	}
	if err := winner.CheckPendingRootReservation(t.Context()); err == nil {
		t.Fatal("terminal handle was reusable")
	}
	if err := r.RecordOutcome(t.Context(), inputarchive.PendingRootCommitted); err == nil {
		t.Fatal("later success rewrote uncertain outcome")
	}
	if _, err := reopened.ReservePendingRoot(t.Context(), resourceTestRequest()); err == nil || resourceReservationCount(t, s) != 1 {
		t.Fatal("uncertain completion refunded capacity")
	}
}

func TestPendingRootResourcePostgresOutcomesAndExpiryNeverRefund(t *testing.T) {
	for _, outcome := range []inputarchive.PendingRootOutcome{inputarchive.PendingRootNotAttempted, inputarchive.PendingRootCommitted, inputarchive.PendingRootCompletionNotConfirmed} {
		t.Run(string(outcome), func(t *testing.T) {
			s := pendingPolicyFixture(t)
			pool, source := resourcePoolFixture(t, s, 1)
			a := seedResourcePool(t, s, pool, source)
			reservation, err := a.ReservePendingRoot(t.Context(), resourceTestRequest())
			if err != nil {
				t.Fatal(err)
			}
			r := reservation.(*pendingRootResourceReservation)
			if err := r.RecordOutcome(t.Context(), outcome); err != nil {
				t.Fatal(err)
			}
			if err := r.RecordOutcome(t.Context(), outcome); err != nil {
				t.Fatal("exact outcome ACK was not idempotent", err)
			}
			if _, err := a.ReservePendingRoot(t.Context(), resourceTestRequest()); err == nil {
				t.Fatal("outcome refunded a lifetime reservation")
			}
			for _, statement := range []string{`DELETE FROM fugue_platform_input_resource_reservations`, `TRUNCATE fugue_platform_input_resource_outcomes`, `UPDATE fugue_platform_input_resource_pool SET config_json=config_json`} {
				if _, err := s.db.Exec(statement); err == nil {
					t.Fatal("immutable resource journal changed", statement)
				}
			}
		})
	}
	s := pendingPolicyFixture(t)
	pool, base := resourcePoolFixture(t, s, 1)
	expires := time.Now().Add(1500 * time.Millisecond)
	var attempts atomic.Int32
	source := resourceSourceFunc(func(ctx context.Context, op PendingRootResourceOperation, snapshot PendingRootResourcePoolSnapshot) (PendingRootResourceWitness, error) {
		w, err := base(ctx, op, snapshot)
		if attempts.Add(1) == 1 {
			w.(*resourceWitnessFixture).allocation.Budget.ExpiresAt = expires
		}
		return w, err
	})
	a := seedResourcePool(t, s, pool, source)
	r, err := a.ReservePendingRoot(t.Context(), resourceTestRequest())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(expires) + 10*time.Millisecond)
	if err := r.CheckPendingRootReservation(t.Context()); err == nil {
		t.Fatal("expired handle usable")
	}
	// The existing reservation is validated against creation time for charging,
	// not discarded because its execution lease has expired.
	if _, err := a.ReservePendingRoot(t.Context(), resourceTestRequest()); err == nil || resourceReservationCount(t, s) != 1 {
		t.Fatal("expiry refunded durable resource charge")
	}
}

func TestPendingRootResourcePostgresRejectsDriftAndUnprovenSource(t *testing.T) {
	for _, mode := range []string{"unproven", "wrong-request", "wrong-pool", "missing-evidence", "missing-accounting", "source-revoked", "temp-unproven", "guard-disabled", "pool-rewritten", "journal-truncated"} {
		t.Run(mode, func(t *testing.T) {
			s := pendingPolicyFixture(t)
			pool, base := resourcePoolFixture(t, s, 2)
			source := resourceSourceFunc(func(ctx context.Context, op PendingRootResourceOperation, snapshot PendingRootResourcePoolSnapshot) (PendingRootResourceWitness, error) {
				w, err := base(ctx, op, snapshot)
				fixture := w.(*resourceWitnessFixture)
				switch mode {
				case "unproven":
					fixture.allocation.Budget.Proven = false
				case "wrong-request":
					fixture.allocation.Budget.RequestSHA256 = strings.Repeat("c", 64)
				case "wrong-pool":
					fixture.allocation.PoolSHA256 = strings.Repeat("c", 64)
				case "missing-evidence":
					fixture.allocation.EvidenceSHA256 = ""
				case "missing-accounting":
					fixture.allocation.AccountingIncluded = false
				case "source-revoked":
					fixture.check = func(context.Context) error { return ErrConflict }
				case "temp-unproven":
					fixture.allocation.Budget.TempFilesForbidden = false
				}
				return w, err
			})
			a := seedResourcePool(t, s, pool, source)
			r, err := a.ReservePendingRoot(t.Context(), resourceTestRequest())
			if mode != "guard-disabled" && mode != "pool-rewritten" && mode != "journal-truncated" {
				if err == nil || r != nil || resourceReservationCount(t, s) != 0 {
					t.Fatal("unproven source charged/admitted", mode, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "guard-disabled":
				_, err = s.db.Exec(`ALTER TABLE fugue_platform_input_resource_reservations DISABLE TRIGGER fugue_input_resource_append_guard`)
			case "pool-rewritten":
				_, err = s.db.Exec(`ALTER TABLE fugue_platform_input_resource_pool DISABLE TRIGGER fugue_input_resource_append_guard; UPDATE fugue_platform_input_resource_pool SET config_json=config_json; ALTER TABLE fugue_platform_input_resource_pool ENABLE ALWAYS TRIGGER fugue_input_resource_append_guard`)
			case "journal-truncated":
				_, err = s.db.Exec(`ALTER TABLE fugue_platform_input_resource_reservations DISABLE TRIGGER fugue_input_resource_append_guard; ALTER TABLE fugue_platform_input_resource_outcomes DISABLE TRIGGER fugue_input_resource_append_guard; TRUNCATE fugue_platform_input_resource_reservations CASCADE; ALTER TABLE fugue_platform_input_resource_reservations ENABLE ALWAYS TRIGGER fugue_input_resource_append_guard; ALTER TABLE fugue_platform_input_resource_outcomes ENABLE ALWAYS TRIGGER fugue_input_resource_append_guard`)
			}
			if err != nil {
				t.Fatal(err)
			}
			if r.CheckPendingRootReservation(t.Context()) == nil {
				t.Fatal("schema or native identity drift accepted", mode)
			}
		})
	}
}

func TestPendingRootResourcePostgresPolicyCoordinatorSharesFinitePool(t *testing.T) {
	s := pendingPolicyFixture(t)
	pool, source := resourcePoolFixture(t, s, 1)
	a := seedResourcePool(t, s, pool, source)
	desired := pendingPolicyRaw(t, false)
	c, err := NewPendingRootPolicyCoordinator(s, pendingPolicySourceFunc(func(ctx context.Context, before PendingRootPolicySnapshot) (PendingRootPolicyWitness, error) {
		return &pendingPolicyWitnessFixture{PendingRootPolicyChange{before, desired}, func(ctx context.Context) error { return ctx.Err() }}, nil
	}), a)
	if err != nil {
		t.Fatal(err)
	}
	state, err := c.Reconcile(t.Context())
	if err != nil || state.Revision != 1 {
		t.Fatal("native policy transaction could not use durable reservation", state, err)
	}
	var outcome string
	if err := s.db.QueryRow(`SELECT outcome FROM fugue_platform_input_resource_outcomes`).Scan(&outcome); err != nil || outcome != "committed" {
		t.Fatal("native commit outcome not retained", outcome, err)
	}
	if _, err := a.ReservePendingRoot(t.Context(), resourceTestRequest()); err == nil {
		t.Fatal("policy and root admissions did not share capacity")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.ReservePendingRoot(ctx, resourceTestRequest()); err == nil || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("cancellation admitted")
	}
}

func TestPendingRootResourcePostgresStrictNativeRootAndLostOutcomeACK(t *testing.T) {
	s, registration, _ := pendingExternalRootFixture(t)
	pool, source := resourcePoolFixture(t, s, 2)
	a := seedResourcePool(t, s, pool, source)
	s.ConfigureStrictPendingRootAdmission(pendingSourceVerifierFunc(func(ctx context.Context, request inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
		return pendingSourceWitnessFunc(func(ctx context.Context, request inputarchive.PendingRootRequest) error { return ctx.Err() }), nil
	}), a, pool.Limits, pool.Epoch)
	receipt, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), pendingAdmissionRequest(registration))
	if err != nil || !receipt.Matches(registration) || externalRootCount(t, s) != 1 || resourceReservationCount(t, s) != 1 {
		t.Fatal("strict native-root registration did not use durable reservation", receipt, err)
	}
	// A separate reserved attempt loses its outcome acknowledgement. A canceled
	// audit write must never free capacity, confirm commit, or erase the charge.
	raw, err := a.ReservePendingRoot(t.Context(), resourceTestRequest())
	if err != nil {
		t.Fatal(err)
	}
	r := raw.(*pendingRootResourceReservation)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := r.RecordOutcome(ctx, inputarchive.PendingRootCommitted); err == nil {
		t.Fatal("canceled outcome write claimed an ACK")
	}
	if err := r.CheckPendingRootReservation(t.Context()); err == nil {
		t.Fatal("lost outcome acknowledgement left a reusable local handle")
	}
	if err := r.RecordOutcome(t.Context(), inputarchive.PendingRootCommitted); err == nil {
		t.Fatal("retry after a failed outcome write consumed another allocation")
	}
	var outcomes int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_resource_outcomes`).Scan(&outcomes); err != nil || outcomes != 1 {
		t.Fatal("lost outcome fabricated completion", outcomes, err)
	}
	if _, err := a.ReservePendingRoot(t.Context(), resourceTestRequest()); err == nil || resourceReservationCount(t, s) != 2 {
		t.Fatal("lost outcome ACK freed a durable charge")
	}
}

func TestPendingRootResourcePostgresRolledBackJournalStillCharged(t *testing.T) {
	s := pendingPolicyFixture(t)
	pool, source := resourcePoolFixture(t, s, 1)
	a := seedResourcePool(t, s, pool, source)
	// Revoke the independent source after the real journal INSERT but before
	// its commit. The schema stays closed, the inserted row rolls back, and the
	// previously acknowledged nontransactional ticket must remain charged.
	var checks atomic.Int32
	a.source = resourceSourceFunc(func(ctx context.Context, operation PendingRootResourceOperation, snapshot PendingRootResourcePoolSnapshot) (PendingRootResourceWitness, error) {
		witness, err := source(ctx, operation, snapshot)
		witness.(*resourceWitnessFixture).check = func(ctx context.Context) error {
			if checks.Add(1) == 3 {
				return ErrConflict
			}
			return ctx.Err()
		}
		return witness, err
	})
	if _, err := a.ReservePendingRoot(t.Context(), resourceTestRequest()); err == nil || resourceReservationCount(t, s) != 0 {
		t.Fatal("journal failure acknowledged a reservation")
	}
	var used int
	if err := s.db.QueryRow(`SELECT last_value FROM fugue_platform_input_resource_tickets`).Scan(&used); err != nil || used != 1 {
		t.Fatal("rollback returned its durable ticket", used, err)
	}
	reopened, err := NewPendingRootResourceAdmission(s, source, pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ReservePendingRoot(t.Context(), resourceTestRequest()); err == nil || resourceReservationCount(t, s) != 0 {
		t.Fatal("reopen refunded a rolled-back journal write")
	}
}

func TestPendingRootResourcePostgresEveryJournalCommitOverridesAsyncDefault(t *testing.T) {
	s, registration, _ := pendingExternalRootFixture(t)
	pool, source := resourcePoolFixture(t, s, 2)
	a := seedResourcePool(t, s, pool, source)
	// The same pooled session deliberately starts asynchronous. The real
	// native transaction guard checks synchronous commit for every write tx;
	// business root/reference triggers independently reject async execution.
	// Ledger triggers are not injected: their exact closed catalog is required.
	s.db.SetMaxOpenConns(4) // admission rechecks while the root transaction is open
	if _, err := s.db.Exec(`CREATE FUNCTION pending_native_durability_probe() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF current_setting('synchronous_commit') NOT IN ('on','remote_apply') THEN
  RAISE EXCEPTION 'synthetic asynchronous pending write';
 END IF;
 RETURN NEW;
 END $$;
 CREATE TRIGGER pending_native_durability_probe BEFORE INSERT ON fugue_platform_input_external_roots FOR EACH ROW EXECUTE FUNCTION pending_native_durability_probe();
 CREATE TRIGGER pending_native_durability_probe BEFORE INSERT ON fugue_platform_input_external_root_refs FOR EACH ROW EXECUTE FUNCTION pending_native_durability_probe()`); err != nil {
		t.Fatal(err)
	}
	// Hold and configure all four actual pool connections before returning
	// them. No role, database or server default is changed by the fixture.
	connections := []*sql.Conn{}
	defer func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}()
	for range 4 {
		conn, err := s.db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
		if _, err := conn.ExecContext(t.Context(), `SET synchronous_commit='off'`); err != nil {
			t.Fatal(err)
		}
	}
	for _, conn := range connections {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
	s.ConfigureStrictPendingRootAdmission(pendingSourceVerifierFunc(func(ctx context.Context, request inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
		return pendingSourceWitnessFunc(func(ctx context.Context, request inputarchive.PendingRootRequest) error { return ctx.Err() }), nil
	}), a, pool.Limits, pool.Epoch)
	receipt, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), pendingAdmissionRequest(registration))
	if err != nil || !receipt.Matches(registration) {
		t.Fatal("pending-root business/journal/outcome used asynchronous acknowledgement", receipt, err)
	}
	var outcome, mode string
	if err := s.db.QueryRow(`SELECT outcome,current_setting('synchronous_commit') FROM fugue_platform_input_resource_outcomes`).Scan(&outcome, &mode); err != nil || outcome != "committed" || mode != "off" {
		t.Fatal("outcome missing or native setting changed the shared session", outcome, mode, err)
	}
}
