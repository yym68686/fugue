package store

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

func publicationPreparationFixture(t *testing.T, mode string) (*Store, *archiveTestObjects, PublicationInputPreparationPlan, model.PlatformArtifactRelease) {
	t.Helper()
	s, _, pins := inputActivationFixture(t, 2)
	first := lkgInputRelease(t, s, "activation-artifact-000")
	if mode != "lkg" {
		if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(first.ID, completePlatformVerificationRequest(first.FencingToken, true), testPlatformPrincipal()); err != nil {
			t.Fatal(err)
		}
	}
	publicationInputValidate(t, s, "activation-artifact-001")
	if mode == "rollback" {
		_ = lkgInputRelease(t, s, "activation-artifact-001")
	}
	for _, p := range pins {
		if _, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: p.Key, Digest: p.Digest, ExpectedToken: p.Token, Release: true, Apply: true}); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		`INSERT INTO fugue_platform_input_chunks(content_hash,ordinal,chunk_hash,data) SELECT content_hash,ordinal,chunk_hash,data FROM fugue_platform_input_hot_chunks`,
		`DELETE FROM fugue_platform_input_hot_chunks`,
		`UPDATE fugue_platform_input_manifests SET storage_location='legacy'`,
	} {
		if _, err := s.db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	objects := &archiveTestObjects{}
	backend := "sha256:" + strings.Repeat("b", 64)
	for _, p := range pins {
		if _, err := copyCompilerInputArchive(t.Context(), s.db, objects, backend, CompilerInputArchiveMigrationOptions{Digest: p.Digest, Apply: true}); err != nil {
			t.Fatal(err)
		}
		if _, err := routeCompilerInputArchive(t.Context(), s.db, objects, backend, p.Digest); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`DROP TABLE fugue_platform_input_chunks`); err != nil {
		t.Fatal(err)
	}
	c := PublicationInputPreparationContract{Version: 1, Mode: mode, ArtifactID: "activation-artifact-001", OperationKey: "neutral-publication-prepare",
		CompilerInputPreparationBudget: CompilerInputPreparationBudget{MaxInputs: 2, MaxRawBytes: 2048, MaxStoredBytes: 2048, MaxGetBytes: 65536, MaxGetRequests: 2, MaxSeconds: 30}}
	switch mode {
	case "release":
		c.Request = model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}
	case "rollback":
		c.RollbackGeneration = "activation-artifact-000"
	case "lkg":
		c.ArtifactID, c.ReleaseID = "", first.ID
	}
	plan, err := s.planPublicationInputPreparation(t.Context(), c, backend)
	if err != nil {
		t.Fatal("cold ordinary publication plan", err)
	}
	return s, objects, plan, first
}

func TestPublicationPreparationPostgresColdRestoreConfirmAndFreshBusinessMutation(t *testing.T) {
	for _, mode := range []string{"release", "rollback", "lkg"} {
		t.Run(mode, func(t *testing.T) {
			s, objects, plan, first := publicationPreparationFixture(t, mode)
			s.db.SetMaxOpenConns(1)
			s.db.SetMaxIdleConns(1)
			invoke := func(fresh *Store) error {
				switch mode {
				case "release":
					_, _, _, _, err := fresh.ReleasePlatformArtifact(plan.Ticket.Contract.ArtifactID, plan.Ticket.Contract.Request, testPlatformPrincipal())
					return err
				case "rollback":
					_, _, _, _, err := fresh.RollbackPlatformArtifact(plan.Ticket.Contract.ArtifactID, model.PlatformArtifactRollbackRequest{ReleaseChannel: "shadow", ToGeneration: plan.Ticket.Contract.RollbackGeneration, Reason: "neutral recovery"}, testPlatformPrincipal())
					return err
				default:
					_, _, _, _, err := fresh.VerifyPlatformArtifactReleaseLKG(first.ID, completePlatformVerificationRequest(first.FencingToken, true), testPlatformPrincipal())
					return err
				}
			}
			before := publicationInputCounts(t, s)
			if err := invoke(s); !errors.Is(err, ErrConflict) || publicationInputCounts(t, s) != before {
				t.Fatal("cold publication changed business state", err)
			}
			r, err := restorePublicationInputs(t.Context(), s.db, plan.Ticket, plan.VerificationKey, objects)
			if err != nil || r.Prepared || r.PublicationActivated || r.RetirementAuthorized || r.Usage.GetRequests != int64(len(plan.Ticket.Inputs)) || r.Usage.PutRequests != 0 || publicationInputCounts(t, s) != before {
				t.Fatal("isolated receipt activated publication", r, err)
			}
			objects.failGet = true
			confirmed, err := RunPublicationInputPreparationConfirmation(t.Context(), s.databaseURL, s.platformArtifactSigningKeyring(), plan.Ticket)
			if err != nil || !confirmed.Prepared || confirmed.PublicationActivated || confirmed.RetirementAuthorized || publicationInputCounts(t, s) != before {
				t.Fatal("trusted confirmation", confirmed, err)
			}
			db, err := openPostgresDatabase(s.databaseURL)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			fresh := &Store{db: db, databaseURL: s.databaseURL}
			fresh.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
			if err := invoke(fresh); err != nil {
				t.Fatal("fresh business transaction without old chunks or GET", err)
			}
		})
	}
}

func TestPublicationPreparationPostgresRefusesStaleBeforeGETAndAfterRestore(t *testing.T) {
	for _, afterRestore := range []bool{false, true} {
		for _, change := range []string{"artifact", "lkg", "epoch", "geometry", "keyring", "pin"} {
			if !afterRestore && (change == "keyring" || change == "pin") {
				continue // keyring authority belongs exclusively to trusted confirmation
			}
			t.Run(change+map[bool]string{false: "-before-get", true: "-after-restore"}[afterRestore], func(t *testing.T) {
				s, objects, plan, _ := publicationPreparationFixture(t, "release")
				if afterRestore {
					if _, err := restorePublicationInputs(t.Context(), s.db, plan.Ticket, plan.VerificationKey, objects); err != nil {
						t.Fatal(err)
					}
				}
				switch change {
				case "keyring":
					s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("changed", "changed", "", "", nil))
				default:
					q := map[string]string{
						"artifact": `UPDATE fugue_platform_artifacts SET updated_at=updated_at WHERE id='activation-artifact-001'`,
						"lkg":      `UPDATE fugue_platform_lkg_snapshots SET updated_at=updated_at`,
						"epoch":    `UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`,
						"geometry": `UPDATE fugue_platform_input_manifests SET created_at=created_at+interval '1 second'`,
						"pin":      `UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE state='verified'`,
					}[change]
					if _, err := s.db.Exec(q); err != nil {
						t.Fatal(err)
					}
				}
				before := publicationInputCounts(t, s)
				if afterRestore {
					r, err := s.confirmPublicationInputPreparation(t.Context(), plan.Ticket)
					if err == nil || r.Prepared || publicationInputCounts(t, s) != before {
						t.Fatal("stale confirmation accepted", r, err)
					}
				} else {
					r, err := restorePublicationInputs(t.Context(), s.db, plan.Ticket, plan.VerificationKey, objects)
					if err == nil || r.Usage.GetRequests != 0 || publicationInputCounts(t, s) != before {
						t.Fatal("stale ticket spent GET or activated", r, err)
					}
				}
			})
		}
	}
}

func TestPublicationPreparationPostgresTicketBindsRequestPurposeAndBudgets(t *testing.T) {
	s, objects, plan, _ := publicationPreparationFixture(t, "release")
	for _, change := range []string{"request", "mode", "backend", "budget", "expired", "tuple", "signature"} {
		t.Run(change, func(t *testing.T) {
			raw, _ := json.Marshal(plan.Ticket)
			var ticket PublicationInputPreparationTicket
			if err := json.Unmarshal(raw, &ticket); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "request":
				ticket.Contract.Request.ReleaseChannel = "full"
			case "mode":
				ticket.Contract.Mode, ticket.Contract.RollbackGeneration = "rollback", "activation-artifact-000"
			case "backend":
				ticket.BackendHash = "sha256:" + strings.Repeat("c", 64)
			case "budget":
				ticket.Contract.MaxGetRequests++
			case "expired":
				ticket.ExpiresAt = time.Now().Add(-time.Second)
			case "tuple":
				ticket.Tuples = ticket.Tuples[:len(ticket.Tuples)-1]
			case "signature":
				ticket.Signature = strings.Repeat("A", 88)
			}
			r, err := restorePublicationInputs(t.Context(), s.db, ticket, plan.VerificationKey, objects)
			if err == nil || r.Usage.GetRequests != 0 || r.Prepared {
				t.Fatal("tampered ticket used", r, err)
			}
		})
	}
	taskKey, err := taskPreparationSigningKey(s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatal(err)
	}
	publicationKey, err := publicationPreparationKey(s.platformArtifactSigningKeyring())
	if err != nil || string(taskKey) == string(publicationKey) {
		t.Fatal("task signing domain reused", err)
	}
}
