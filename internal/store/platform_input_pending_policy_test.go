package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

// These providers are deliberately synthetic, confined to disposable schemas.
// Passing native CAS tests never supplies production authority or resources.
type pendingPolicySourceFunc func(context.Context, PendingRootPolicySnapshot) (PendingRootPolicyWitness, error)

func (f pendingPolicySourceFunc) PreparePendingRootPolicy(ctx context.Context, state PendingRootPolicySnapshot) (PendingRootPolicyWitness, error) {
	return f(ctx, state)
}

type pendingPolicyWitnessFixture struct {
	change PendingRootPolicyChange
	check  func(context.Context) error
}

func (w *pendingPolicyWitnessFixture) PendingRootPolicyChange() PendingRootPolicyChange {
	return w.change
}
func (w *pendingPolicyWitnessFixture) CheckPendingRootPolicy(ctx context.Context) error {
	return w.check(ctx)
}

type pendingPolicyAdmissionFunc func(context.Context, PendingRootPolicyChange) (PendingRootPolicyReservation, error)

func (f pendingPolicyAdmissionFunc) ReservePendingRootPolicy(ctx context.Context, change PendingRootPolicyChange) (PendingRootPolicyReservation, error) {
	return f(ctx, change)
}

type pendingPolicyReservationFixture struct {
	check    func(context.Context) error
	outcomes []inputarchive.PendingRootOutcome
}

func (r *pendingPolicyReservationFixture) CheckPendingRootPolicyReservation(ctx context.Context) error {
	return r.check(ctx)
}
func (r *pendingPolicyReservationFixture) RecordPendingRootPolicyOutcome(outcome inputarchive.PendingRootOutcome) {
	r.outcomes = append(r.outcomes, outcome)
}

func pendingPolicyFixture(t *testing.T) *Store {
	t.Helper()
	db, address, _ := hotInventoryTestDatabase(t)
	for _, ddl := range []string{schemamigrate.CompilerInputZeroTempSQL, schemamigrate.CompilerInputPendingAuthoritiesSQL} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	return &Store{db: db, databaseURL: address}
}

func pendingPolicyRaw(t *testing.T, lease bool) string {
	t.Helper()
	policy := inputarchive.PendingRootAuthorityPolicy{Schema: inputarchive.PendingRootAuthorityPolicySchema, Leases: []inputarchive.PendingRootAuthorityLease{}}
	if lease {
		policy.Leases = append(policy.Leases, inputarchive.PendingRootAuthorityLease{Authority: pendingAuthorityFixture(), LeaseID: strings.Repeat("c", 64), ExpiresUnix: time.Now().Add(time.Minute).Unix()})
	}
	raw, err := policy.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func pendingPolicyCoordinatorFixture(t *testing.T, s *Store, desired string) (*PendingRootPolicyCoordinator, *pendingPolicyReservationFixture) {
	t.Helper()
	reservation := &pendingPolicyReservationFixture{check: func(ctx context.Context) error { return ctx.Err() }}
	c, err := NewPendingRootPolicyCoordinator(s, pendingPolicySourceFunc(func(ctx context.Context, before PendingRootPolicySnapshot) (PendingRootPolicyWitness, error) {
		return &pendingPolicyWitnessFixture{PendingRootPolicyChange{before, desired}, func(ctx context.Context) error { return ctx.Err() }}, nil
	}), pendingPolicyAdmissionFunc(func(context.Context, PendingRootPolicyChange) (PendingRootPolicyReservation, error) {
		return reservation, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	return c, reservation
}

func TestPendingRootPolicyRequiresIndependentSourceAndAllocation(t *testing.T) {
	if c, err := NewPendingRootPolicyCoordinator(&Store{}, nil, nil); err == nil || c != nil {
		t.Fatal("unconfigured policy mutation became available", c, err)
	}
	var c *PendingRootPolicyCoordinator
	if _, err := c.Reconcile(t.Context()); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
}

func TestPendingRootPolicyPostgresBootstrapExactReopenAndExplicitRevocation(t *testing.T) {
	s := pendingPolicyFixture(t)
	before, err := s.ReadPendingRootAuthorityPolicy(t.Context())
	if err != nil || before.Revision != 0 || before.Relation == 0 || before.Filenode == 0 || before.PolicyJSON != "" {
		t.Fatal("empty installed policy was not identified", before, err)
	}
	desired := pendingPolicyRaw(t, true)
	c, reservation := pendingPolicyCoordinatorFixture(t, s, desired)
	first, err := c.Reconcile(t.Context())
	if err != nil || first.Revision != 1 || first.PolicyJSON != desired || first.Xmin == "" || first.CTID == "" || len(reservation.outcomes) != 1 || reservation.outcomes[0] != inputarchive.PendingRootCommitted {
		t.Fatal("complete bootstrap did not commit", first, err, reservation.outcomes)
	}
	// Treat the first acknowledgement as lost. A new Store/connection pool and
	// coordinator must use current native state; exact retry is not revision+1.
	db, err := sql.Open("pgx", s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	fresh := &Store{db: db, databaseURL: s.databaseURL}
	c, _ = pendingPolicyCoordinatorFixture(t, fresh, desired)
	again, err := c.Reconcile(t.Context())
	if err != nil || again != first {
		t.Fatal("exact reopened reconciliation rewrote current policy", again, first, err)
	}
	c, _ = pendingPolicyCoordinatorFixture(t, fresh, pendingPolicyRaw(t, false))
	revoked, err := c.Reconcile(t.Context())
	if err != nil || revoked.Revision != 2 || revoked.Xmin == first.Xmin {
		t.Fatal("explicit complete revocation did not advance native policy", revoked, err)
	}
	// Old state is not a reusable write token, even if a provider returns it.
	c.source = pendingPolicySourceFunc(func(context.Context, PendingRootPolicySnapshot) (PendingRootPolicyWitness, error) {
		return &pendingPolicyWitnessFixture{PendingRootPolicyChange{first, desired}, func(context.Context) error { return nil }}, nil
	})
	if _, err := c.Reconcile(t.Context()); !errors.Is(err, ErrConflict) {
		t.Fatal("old native parent restored revoked authority", err)
	}
	current, err := fresh.ReadPendingRootAuthorityPolicy(t.Context())
	if err != nil || current != revoked {
		t.Fatal("stale reconciliation changed revoked policy", current, err)
	}
}

func TestPendingRootPolicyPostgresDriftAndFailureNeverCommitPrefix(t *testing.T) {
	for _, mode := range []string{"source", "budget", "mutated-witness", "same-policy-new-revision", "table-replaced", "guard-disabled", "expired", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			s := pendingPolicyFixture(t)
			seed, _ := pendingPolicyCoordinatorFixture(t, s, pendingPolicyRaw(t, false))
			before, err := seed.Reconcile(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			desired := pendingPolicyRaw(t, true)
			if mode == "expired" {
				p, _ := inputarchive.DecodePendingRootAuthorityPolicy([]byte(desired))
				p.Leases[0].ExpiresUnix = time.Now().Add(-time.Second).Unix()
				raw, _ := p.Canonical()
				desired = string(raw)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			checks := 0
			var witness *pendingPolicyWitnessFixture
			reservation := &pendingPolicyReservationFixture{check: func(context.Context) error { return nil }}
			budgetChecks := 0
			reservation.check = func(context.Context) error {
				budgetChecks++
				if mode == "budget" && budgetChecks == 2 {
					return ErrConflict
				}
				return nil
			}
			source := pendingPolicySourceFunc(func(context.Context, PendingRootPolicySnapshot) (PendingRootPolicyWitness, error) {
				witness = &pendingPolicyWitnessFixture{change: PendingRootPolicyChange{before, desired}}
				witness.check = func(context.Context) error {
					checks++
					if checks == 3 && mode == "source" {
						return ErrConflict
					}
					if checks == 3 && mode == "mutated-witness" {
						witness.change.PolicyJSON = before.PolicyJSON
					}
					return nil
				}
				return witness, nil
			})
			admission := pendingPolicyAdmissionFunc(func(context.Context, PendingRootPolicyChange) (PendingRootPolicyReservation, error) {
				var err error
				switch mode {
				case "same-policy-new-revision":
					_, err = s.db.Exec(`UPDATE fugue_platform_input_pending_authorities SET revision=revision+1`)
				case "table-replaced":
					_, err = s.db.Exec(`ALTER TABLE fugue_platform_input_pending_authorities RENAME TO old_policy`)
					if err == nil {
						_, err = s.db.Exec(schemamigrate.CompilerInputPendingAuthoritiesSQL)
					}
				case "guard-disabled":
					_, err = s.db.Exec(`ALTER TABLE fugue_platform_input_pending_authorities DISABLE TRIGGER fugue_pending_authorities_revision`)
				case "canceled":
					cancel()
				}
				if err != nil {
					t.Fatal(err)
				}
				return reservation, nil
			})
			c, err := NewPendingRootPolicyCoordinator(s, source, admission)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Reconcile(ctx); err == nil {
				t.Fatal("drift acknowledged policy mutation")
			}
			var raw string
			var revision int64
			if mode != "table-replaced" {
				if err := s.db.QueryRow(`SELECT revision,policy_json FROM fugue_platform_input_pending_authorities`).Scan(&revision, &raw); err != nil {
					t.Fatal(err)
				}
				expected := before.Revision
				if mode == "same-policy-new-revision" {
					expected++ // only the explicitly injected independent update
				}
				if revision != expected || raw != before.PolicyJSON {
					t.Fatal("failed policy update committed a prefix", revision, raw)
				}
			}
			if len(reservation.outcomes) != 1 || reservation.outcomes[0] == inputarchive.PendingRootCommitted {
				t.Fatal("failure acknowledged or silently lost its reservation", reservation.outcomes)
			}
		})
	}
}

func TestPendingRootPolicyPostgresConcurrentCompletePoliciesDoNotMergeOrRetry(t *testing.T) {
	s := pendingPolicyFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ready, release := make(chan struct{}, 2), make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		desired := pendingPolicyRaw(t, true)
		if i == 1 {
			desired = strings.ReplaceAll(desired, strings.Repeat("c", 64), strings.Repeat("d", 64))
		}
		c, _ := pendingPolicyCoordinatorFixture(t, s, desired)
		c.source = pendingPolicySourceFunc(func(ctx context.Context, before PendingRootPolicySnapshot) (PendingRootPolicyWitness, error) {
			ready <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &pendingPolicyWitnessFixture{PendingRootPolicyChange{before, desired}, func(context.Context) error { return nil }}, nil
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Reconcile(ctx)
			results <- err
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("concurrent policy preparation did not reach barrier", ctx.Err())
		}
	}
	close(release)
	wg.Wait()
	close(results)
	succeeded := 0
	for err := range results {
		if err == nil {
			succeeded++
		}
	}
	current, err := s.ReadPendingRootAuthorityPolicy(t.Context())
	if err != nil || succeeded != 1 || current.Revision != 1 {
		t.Fatal("concurrent policies merged or silently retried", succeeded, current, err)
	}
}

func TestPendingRootPolicyPostgresSerializesWithFinalRetentionCommit(t *testing.T) {
	s, r, _ := pendingExternalRootFixture(t)
	c, _ := pendingPolicyCoordinatorFixture(t, s, pendingPolicyRaw(t, false))
	checks := 0
	s.ConfigurePendingRootAdmission(pendingSourceVerifierFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
		return pendingSourceWitnessFunc(func(context.Context, inputarchive.PendingRootRequest) error {
			checks++
			if checks == 3 {
				// The actual coordinator on another connection must not cross
				// this retention transaction's final native SHARE lock.
				if _, err := c.Reconcile(t.Context()); err == nil {
					t.Fatal("coordinator crossed final retention authorization lock")
				}
			}
			return nil
		}), nil
	}), pendingAdmissionFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error) {
		return &pendingReservationFixture{check: func(context.Context) error { return nil }}, nil
	}))
	first, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), pendingAdmissionRequest(r))
	if err != nil || !first.Matches(r) || checks != 3 {
		t.Fatal("retention commit failed after blocked coordinator", first, checks, err)
	}
	if after, err := c.Reconcile(t.Context()); err != nil || after.Revision != 2 {
		t.Fatal("coordinator could not revoke after retention commit", after, err)
	}
	if receipt, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), pendingAdmissionRequest(r)); err == nil || receipt.RegistryRevision != 0 || externalRootCount(t, s) != 1 {
		t.Fatal("revocation bypassed or discarded committed retention", receipt, err)
	}
}
