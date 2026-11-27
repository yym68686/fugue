package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestPlatformInputReadPostgresColdRestoreAndFreshPool(t *testing.T) {
	s, objects, plan, _ := consumerPreparationFixture(t, "trusted")
	set, err := s.GetPlatformExpectedConsumerSet(plan.Ticket.Contract.ExpectedSetID)
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.GetPlatformArtifact(set.ReleaseSetID)
	if err != nil {
		t.Fatal(err)
	}
	reads := []PlatformInputReadSelection{{ArtifactID: a.ID, ContentHash: a.ContentHash,
		ReleaseID: set.ArtifactReleaseID, ExpectedConsumerSetID: set.ID}}
	before := consumerInputCounts(t, s)
	if err := s.CheckPlatformInputRead(t.Context(), reads); !errors.Is(err, ErrConflict) || before != consumerInputCounts(t, s) {
		t.Fatal("cold read bypassed verified pins or changed state", err)
	}
	r, err := restoreConsumerInputs(t.Context(), s.db, plan.Ticket, plan.VerificationKey, objects)
	if err != nil || r.Usage.GetRequests != 1 || r.Usage.PutRequests != 0 {
		t.Fatal(r, err)
	}
	objects.failGet = true
	db, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fresh := &Store{db: db, databaseURL: s.databaseURL}
	fresh.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
	if err := fresh.CheckPlatformInputRead(t.Context(), reads); err != nil || before != consumerInputCounts(t, fresh) {
		t.Fatal("fresh uncached reader needed old chunks/GET or changed business state", err)
	}
	if _, err := db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE state='verified'`); err != nil {
		t.Fatal(err)
	}
	if err := fresh.CheckPlatformInputRead(t.Context(), reads); !errors.Is(err, ErrConflict) {
		t.Fatal("reader reused stale verified evidence", err)
	}
}

func TestPlatformInputReadPostgresExactLKGSelectorIncludingAbsence(t *testing.T) {
	for _, change := range []string{"initial-empty", "insert", "update", "delete", "aba", "keyring", "pin", "epoch", "expired"} {
		t.Run(change, func(t *testing.T) {
			s, _, _ := inputActivationFixture(t, 1)
			if _, err := s.ValidatePlatformArtifact("activation-root", []model.PlatformArtifactValidationResult{{Name: "schema", Pass: true}}); err != nil {
				t.Fatal(err)
			}
			reads := []PlatformInputReadSelection{{LKGKind: model.PlatformArtifactKindReleaseSet, LKGScope: "global"}}
			if change != "initial-empty" && change != "insert" {
				insertTaskConfigurationLKG(t, s, "activation-root")
				lkg, err := s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
				if err != nil || lkg == nil {
					t.Fatal(lkg, err)
				}
				reads[0].LKG = lkg
			}
			if err := s.CheckPlatformInputRead(t.Context(), reads); err != nil {
				t.Fatal("initial read", err)
			}
			proof, err := s.prepareCompilerInputDependencyProof(t.Context(), nil, nil, nil, nil, reads)
			if err != nil {
				t.Fatal(err)
			}
			proof, pins, err := s.prepareCompilerInputActivationPins(t.Context(), proof)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "insert":
				insertTaskConfigurationLKG(t, s, "activation-root")
			case "update":
				_, err = s.db.Exec(`UPDATE fugue_platform_lkg_snapshots SET updated_at=updated_at`)
			case "delete", "aba":
				_, err = s.db.Exec(`DELETE FROM fugue_platform_lkg_snapshots`)
				if change == "aba" && err == nil {
					insertTaskConfigurationLKG(t, s, "activation-root")
				}
			case "keyring":
				s.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
			case "pin":
				_, err = s.db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL`)
			case "epoch":
				_, err = s.db.Exec(`UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`)
			case "expired":
				_, err = s.db.Exec(`UPDATE fugue_platform_lkg_snapshots SET expires_at=now()-interval '1 second'`)
			}
			if err != nil {
				t.Fatal(err)
			}
			err = s.withPreparedCompilerInputActivation(t.Context(), proof, pins, func(context.Context, *sql.Tx) error { return nil })
			if (err == nil) != (change == "initial-empty") {
				t.Fatal("reader consumed a changed selector/epoch/pin/keyring", change, err)
			}
			if change == "insert" || change == "delete" || change == "expired" {
				if err := s.CheckPlatformInputRead(t.Context(), reads); !errors.Is(err, ErrConflict) {
					t.Fatal("old observed selection accepted a changed current LKG", err)
				}
			}
		})
	}
}
