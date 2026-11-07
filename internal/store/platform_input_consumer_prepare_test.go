package store

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func consumerPreparationFixture(t *testing.T, mode string) (*Store, *archiveTestObjects, ConsumerInputPreparationPlan, platformcontrol.PlatformComponentIdentityClaims) {
	t.Helper()
	s, _, pins := inputActivationFixture(t, 1)
	release := lkgInputRelease(t, s, "activation-artifact-000")
	set, claims, h := consumerInputEnvelope(t, s, release)
	set, err := normalizePlatformExpectedConsumerSetForStore(set)
	if err != nil {
		t.Fatal(err)
	}
	c := ConsumerInputPreparationContract{Version: 1, Mode: mode, ExpectedSetID: set.ID, OperationKey: "neutral-consumer-prepare", CompilerInputPreparationBudget: CompilerInputPreparationBudget{MaxInputs: 1, MaxRawBytes: 2048, MaxStoredBytes: 2048, MaxGetBytes: 65536, MaxGetRequests: 2, MaxSeconds: 30}}
	if mode == "register" {
		c.Registration = &set
	} else {
		if _, err := s.CreatePlatformExpectedConsumerSet(set); err != nil {
			t.Fatal(err)
		}
		c.Heartbeat = &h
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
	plan, err := s.planConsumerInputPreparation(t.Context(), c, backend)
	if err != nil {
		t.Fatal("cold consumer plan", err)
	}
	return s, objects, plan, claims
}

func TestConsumerPreparationPostgresColdRestoreConfirmFreshBusiness(t *testing.T) {
	for _, mode := range []string{"register", "trusted"} {
		t.Run(mode, func(t *testing.T) {
			s, objects, plan, claims := consumerPreparationFixture(t, mode)
			s.db.SetMaxOpenConns(1)
			s.db.SetMaxIdleConns(1)
			invoke := func(fresh *Store) error {
				c := plan.Ticket.Contract
				if mode == "register" {
					_, err := fresh.CreatePlatformExpectedConsumerSet(*c.Registration)
					return err
				}
				_, err := fresh.AcceptTrustedPlatformConsumerHeartbeatWithAudit(claims, c.ExpectedSetID, *c.Heartbeat, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}, bundleauth.NewKeyring("neutral-audit", "neutral-audit", "", "", nil))
				return err
			}
			before := consumerInputCounts(t, s)
			if err := invoke(s); !errors.Is(err, ErrConflict) || consumerInputCounts(t, s) != before {
				t.Fatal("cold business changed state", err)
			}
			r, err := restoreConsumerInputs(t.Context(), s.db, plan.Ticket, plan.VerificationKey, objects)
			if err != nil || r.Prepared || r.ConsumerActivated || r.RetirementAuthorized || r.Usage.GetRequests != 1 || r.Usage.PutRequests != 0 || consumerInputCounts(t, s) != before {
				t.Fatal("isolated restore advanced consumer", r, err)
			}
			objects.failGet = true
			r, err = RunConsumerInputPreparationConfirmation(t.Context(), s.databaseURL, s.platformArtifactSigningKeyring(), plan.Ticket)
			if err != nil || !r.Prepared || r.ConsumerActivated || r.RetirementAuthorized || consumerInputCounts(t, s) != before {
				t.Fatal("fresh trusted confirm", r, err)
			}
			db, err := openPostgresDatabase(s.databaseURL)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			fresh := &Store{db: db, databaseURL: s.databaseURL}
			fresh.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
			if err := invoke(fresh); err != nil {
				t.Fatal("original business fresh pool no chunks/GET", err)
			}
		})
	}
}

func TestConsumerPreparationPostgresStaleWorkAndConfirmation(t *testing.T) {
	for _, after := range []bool{false, true} {
		for _, change := range []string{"expectation", "artifact", "epoch", "geometry", "pin", "keyring"} {
			if !after && (change == "pin" || change == "keyring") {
				continue
			}
			t.Run(change+map[bool]string{false: "-preGET", true: "-postrestore"}[after], func(t *testing.T) {
				s, objects, p, _ := consumerPreparationFixture(t, "trusted")
				if after {
					if _, err := restoreConsumerInputs(t.Context(), s.db, p.Ticket, p.VerificationKey, objects); err != nil {
						t.Fatal(err)
					}
				}
				q := map[string]string{"expectation": `UPDATE fugue_platform_expected_consumer_sets SET updated_at=updated_at`, "artifact": `UPDATE fugue_platform_artifacts SET updated_at=updated_at`, "epoch": `UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`, "geometry": `UPDATE fugue_platform_input_manifests SET created_at=created_at+interval '1 second'`, "pin": `UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE state='verified'`}[change]
				if q != "" {
					if _, err := s.db.Exec(q); err != nil {
						t.Fatal(err)
					}
				} else {
					s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("changed", "changed", "", "", nil))
				}
				before := consumerInputCounts(t, s)
				if after {
					if r, err := s.confirmConsumerInputPreparation(t.Context(), p.Ticket); err == nil || r.Prepared {
						t.Fatal("stale confirm", r, err)
					}
				} else {
					if r, err := restoreConsumerInputs(t.Context(), s.db, p.Ticket, p.VerificationKey, objects); err == nil || r.Usage.GetRequests != 0 {
						t.Fatal("stale work GET", r, err)
					}
				}
				if consumerInputCounts(t, s) != before {
					t.Fatal("preparation mutated consumer/audit")
				}
			})
		}
	}
}

func TestConsumerPreparationPostgresTicketBindsRequestPurposeAndBudget(t *testing.T) {
	s, _, p, _ := consumerPreparationFixture(t, "trusted")
	for _, change := range []string{"mode", "set", "generation", "budget", "tuple", "backend", "expiry", "signature"} {
		raw, _ := json.Marshal(p.Ticket)
		var bad ConsumerInputPreparationTicket
		_ = json.Unmarshal(raw, &bad)
		switch change {
		case "mode":
			bad.Contract.Mode = "shadow"
		case "set":
			bad.Contract.ExpectedSetID = "foreign"
		case "generation":
			bad.Contract.Heartbeat.ActualGeneration = "foreign"
		case "budget":
			bad.Contract.MaxSeconds++
		case "tuple":
			bad.Tuples[0].Xmin = "7"
		case "backend":
			bad.BackendHash = "sha256:" + strings.Repeat("c", 64)
		case "expiry":
			bad.ExpiresAt = bad.ExpiresAt.Add(time.Second)
		case "signature":
			bad.Signature = base64.StdEncoding.EncodeToString(make([]byte, 64))
		}
		if bad.Validate(p.VerificationKey, time.Now()) == nil {
			t.Fatal("tampered ticket", change)
		}
	}
	publicationKey, err := publicationPreparationKey(s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatal(err)
	}
	if p.Ticket.Validate(base64.StdEncoding.EncodeToString(publicationKey[32:]), time.Now()) == nil {
		t.Fatal("cross-purpose key accepted")
	}
	bad := p.Ticket.Contract
	bad.ExpectedPlanSHA256 = ""
	bad.Mode = "register"
	bad.Registration = &model.PlatformExpectedConsumerSet{}
	if bad.Validate(false) == nil {
		t.Fatal("ambiguous register heartbeat accepted")
	}
}
