package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"fugue/internal/inputarchive"
)

// Explicit synthetic providers for the disposable native transaction tests;
// neither is production source authentication or a physical budget proof.
type pendingSourceVerifierFunc func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error)

func (f pendingSourceVerifierFunc) VerifyPendingRootSource(ctx context.Context, r inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
	return f(ctx, r)
}

type pendingSourceWitnessFunc func(context.Context, inputarchive.PendingRootRequest) error

func (f pendingSourceWitnessFunc) CheckPendingRootSource(ctx context.Context, r inputarchive.PendingRootRequest) error {
	return f(ctx, r)
}

func (pendingSourceWitnessFunc) PendingRootAuthority() inputarchive.PendingRootAuthority {
	return pendingAuthorityFixture()
}

type pendingAdmissionFunc func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error)

func (f pendingAdmissionFunc) ReservePendingRoot(ctx context.Context, r inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error) {
	return f(ctx, r)
}

type pendingReservationFixture struct {
	check    func(context.Context) error
	outcomes []inputarchive.PendingRootOutcome
}

func (r *pendingReservationFixture) CheckPendingRootReservation(ctx context.Context) error {
	return r.check(ctx)
}
func (r *pendingReservationFixture) RecordPendingRootOutcome(outcome inputarchive.PendingRootOutcome) {
	r.outcomes = append(r.outcomes, outcome)
}

func pendingAdmissionRequest(r inputarchive.PendingRootRegistration) inputarchive.PendingRootRequest {
	return inputarchive.PendingRootRequest{Schema: inputarchive.PendingRootRequestSchema, Registration: r, SourceSchema: "synthetic-private-test-source", Source: []byte(`{"original":"synthetic"}`)}
}

func TestPendingExternalRootPublicEntryFailsClosedBeforeDatabaseAccess(t *testing.T) {
	r := inputarchive.PendingRootRegistration{Schema: inputarchive.PendingRootSchema, Owner: "authority/neutral", Generation: "generation-neutral", SourceDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", References: []inputarchive.HotReference{{Kind: "release", ID: "neutral-release"}}}
	request := pendingAdmissionRequest(r)
	for _, mode := range []string{"none", "source-only", "admission-only", "source-denied", "no-witness", "admission-denied", "no-reservation", "reservation-denied"} {
		t.Run(mode, func(t *testing.T) {
			s := &Store{} // any DB access would panic
			reserved := false
			witness := pendingSourceWitnessFunc(func(context.Context, inputarchive.PendingRootRequest) error { return nil })
			var source inputarchive.PendingRootSourceVerifier = pendingSourceVerifierFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
				if mode == "source-denied" {
					return nil, ErrConflict
				}
				if mode == "no-witness" {
					return nil, nil
				}
				return witness, nil
			})
			var admission inputarchive.PendingRootAdmission = pendingAdmissionFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error) {
				reserved = true
				if mode == "admission-denied" {
					return nil, ErrConflict
				}
				if mode == "no-reservation" {
					return nil, nil
				}
				return &pendingReservationFixture{check: func(context.Context) error { return ErrConflict }}, nil
			})
			if mode == "none" || mode == "admission-only" {
				source = nil
			}
			if mode == "none" || mode == "source-only" {
				admission = nil
			}
			s.ConfigurePendingRootAdmission(source, admission)
			if receipt, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), request); err == nil || receipt.RegistryRevision != 0 {
				t.Fatal(receipt, err)
			}
			if (mode == "source-denied" || mode == "no-witness" || source == nil || admission == nil) && reserved {
				t.Fatal("reserved before authentication/configuration")
			}
		})
	}
}

func TestPendingExternalRootPublicEntryOwnsRequestBeforeCallingProviders(t *testing.T) {
	r := inputarchive.PendingRootRegistration{Schema: inputarchive.PendingRootSchema, Owner: "authority/neutral", Generation: "generation-neutral", SourceDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", References: []inputarchive.HotReference{{Kind: "release", ID: "neutral-release"}}}
	caller := pendingAdmissionRequest(r)
	_, expected, err := caller.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{}
	reserved := false
	source := pendingSourceVerifierFunc(func(_ context.Context, received inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
		// Mutation after entry must not alter the request retained by the store.
		caller.Registration.References[0].ID = "different-release"
		caller.Source[2] = 'X'
		return pendingSourceWitnessFunc(func(_ context.Context, checked inputarchive.PendingRootRequest) error {
			_, actual, err := checked.Canonical()
			if err != nil || actual != expected {
				return ErrConflict
			}
			return nil
		}), nil
	})
	s.ConfigurePendingRootAdmission(source, pendingAdmissionFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error) {
		reserved = true
		return nil, ErrConflict
	}))
	if _, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), caller); !errors.Is(err, ErrConflict) || !reserved {
		t.Fatal("request reused caller-owned memory", reserved, err)
	}
}

func TestPendingExternalRootPostgresAdmissionAndSourceRecheckBeforeCommit(t *testing.T) {
	for _, mode := range []string{"success", "source-drift", "budget-drift", "configuration-drift"} {
		t.Run(mode, func(t *testing.T) {
			s, r, _ := pendingExternalRootFixture(t)
			checks := 0
			reservation := &pendingReservationFixture{check: func(context.Context) error { return nil }}
			witness := pendingSourceWitnessFunc(func(context.Context, inputarchive.PendingRootRequest) error {
				checks++
				if checks == 3 {
					if externalRootCount(t, s) != 0 {
						t.Fatal("uncommitted record visible outside transaction")
					}
					if mode == "source-drift" {
						return ErrConflict
					}
				}
				return nil
			})
			var source inputarchive.PendingRootSourceVerifier = pendingSourceVerifierFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
				return witness, nil
			})
			admission := pendingAdmissionFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error) {
				if mode == "configuration-drift" {
					s.ConfigurePendingRootAdmission(nil, nil)
				}
				return reservation, nil
			})
			if mode == "budget-drift" {
				budgetChecks := 0
				reservation.check = func(context.Context) error {
					budgetChecks++
					if budgetChecks == 2 {
						return ErrConflict
					}
					return nil
				}
			}
			s.ConfigurePendingRootAdmission(source, admission)
			receipt, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), pendingAdmissionRequest(r))
			if mode == "success" {
				if err != nil || !receipt.Matches(r) || externalRootCount(t, s) != 1 || checks != 3 {
					t.Fatal(receipt, checks, err)
				}
				if len(reservation.outcomes) != 1 || reservation.outcomes[0] != inputarchive.PendingRootCommitted {
					t.Fatal(reservation.outcomes)
				}
				// Even exact retry cannot use an old receipt after policy removal.
				s.ConfigurePendingRootAdmission(nil, nil)
				if _, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), pendingAdmissionRequest(r)); !errors.Is(err, ErrConflict) {
					t.Fatal(err)
				}
			} else {
				if err == nil || receipt.RegistryRevision != 0 || externalRootCount(t, s) != 0 {
					t.Fatal("drift committed prefix or acknowledged rollback", receipt, err)
				}
				if len(reservation.outcomes) != 1 || reservation.outcomes[0] != inputarchive.PendingRootCompletionNotConfirmed {
					t.Fatal("ambiguous error released reservation", reservation.outcomes)
				}
			}
		})
	}
}

func TestPendingExternalRootPostgresPreparationDoesNotAuthorizePrivateCommit(t *testing.T) {
	s, r, _ := pendingExternalRootFixture(t)
	proof, err := s.preparePendingCompilerInputExternalRoot(t.Context(), r, pendingAuthorityFixture())
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := s.commitPendingCompilerInputExternalRoot(t.Context(), proof); !errors.Is(err, ErrConflict) || receipt.RegistryRevision != 0 || externalRootCount(t, s) != 0 {
		t.Fatal("bare native preparation bypassed source/allocation admission", receipt, err)
	}
}

func TestPendingExternalRootPostgresCrossConnectionRevocationSerializesWithCommit(t *testing.T) {
	s, r, _ := pendingExternalRootFixture(t)
	checks := 0
	source := pendingSourceVerifierFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
		return pendingSourceWitnessFunc(func(context.Context, inputarchive.PendingRootRequest) error {
			checks++
			if checks != 3 {
				return nil
			}
			// Independent connection models another API/coordinator. Its policy
			// update cannot pass the final native lease row lock during commit.
			tx, err := s.db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err = tx.Exec(`SET LOCAL lock_timeout='50ms'`); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec(`UPDATE fugue_platform_input_pending_authorities SET revision=revision+1`); err == nil || !strings.Contains(err.Error(), "lock timeout") {
				t.Fatal("revocation crossed final native authority lock", err)
			}
			return nil
		}), nil
	})
	reservation := &pendingReservationFixture{check: func(context.Context) error { return nil }}
	s.ConfigurePendingRootAdmission(source, pendingAdmissionFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error) {
		return reservation, nil
	}))
	first, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), pendingAdmissionRequest(r))
	if err != nil || !first.Matches(r) || checks != 3 {
		t.Fatal(first, checks, err)
	}
	// The writer can now revoke after commit; the still-configured API cannot
	// replay an old receipt after that database revision has committed.
	if _, err = s.db.Exec(`UPDATE fugue_platform_input_pending_authorities SET revision=revision+1,policy_json='{"schema":"fugue.compiler-input.pending-root-authority-policy.v1","leases":[]}'`); err != nil {
		t.Fatal(err)
	}
	if receipt, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), pendingAdmissionRequest(r)); err == nil || receipt.RegistryRevision != 0 || externalRootCount(t, s) != 1 {
		t.Fatal("old receipt bypassed native revocation", receipt, err)
	}
}

func TestPendingExternalRootPostgresAuthorityPolicyRequiresExplicitInstallation(t *testing.T) {
	s, r, _ := pendingExternalRootFixture(t)
	for _, mode := range []string{"no-row", "invalid-policy", "wrong-authority", "guard-replaced"} {
		t.Run(mode, func(t *testing.T) {
			tx, err := s.db.BeginTx(t.Context(), &sql.TxOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			switch mode {
			case "no-row":
				_, err = tx.Exec(`ALTER TABLE fugue_platform_input_pending_authorities DISABLE TRIGGER fugue_pending_authorities_guard;DELETE FROM fugue_platform_input_pending_authorities;ALTER TABLE fugue_platform_input_pending_authorities ENABLE ALWAYS TRIGGER fugue_pending_authorities_guard`)
			case "invalid-policy":
				_, err = tx.Exec(`UPDATE fugue_platform_input_pending_authorities SET revision=revision+1,policy_json='{}'`)
			case "guard-replaced":
				_, err = tx.Exec(`ALTER TABLE fugue_platform_input_pending_authorities DISABLE TRIGGER fugue_pending_authorities_revision`)
			}
			if err != nil {
				t.Fatal(err)
			}
			a := pendingAuthorityFixture()
			if mode == "wrong-authority" {
				a.SourceSHA = strings.Repeat("d", 40)
			}
			if _, err := readPendingRootPolicy(t.Context(), tx, a, false); err == nil {
				t.Fatal("missing/invalid policy authorized", r.Owner)
			}
		})
	}
}
