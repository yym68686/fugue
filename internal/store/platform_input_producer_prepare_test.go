package store

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func producerPreparationFixture(t *testing.T, action string) (servingFixture, PublicationInputPreparationContract, func(*Store) error) {
	t.Helper()
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable PostgreSQL")
	}
	f := newServingFixture(t, dsn, true)
	s := f.s
	p := producerTestPrincipal()
	c := PublicationInputPreparationContract{Version: 1, Mode: "release", OperationKey: "neutral-producer-prepare",
		CompilerInputPreparationBudget: CompilerInputPreparationBudget{MaxInputs: 32, MaxRawBytes: 8 << 20, MaxStoredBytes: 8 << 20, MaxGetBytes: 16 << 20, MaxGetRequests: 32, MaxSeconds: 30},
		Producer:                       &ProducerInputPreparationIntent{Action: action, PolicyReleaseID: f.authority.ID}}
	if action == "shadow" {
		candidate := f.candidate(t)
		c.ArtifactID = candidate.ID
		c.Request, _ = producedShadowIntent(candidate.ID, f.authority.ID, "")
		return f, c, func(s *Store) error {
			_, _, _, _, err := s.ReleaseProducedPlatformArtifact(candidate.ID, f.authority.ID, "", p)
			return err
		}
	}
	seed := f.candidate(t)
	_, initialShadow, _, _, err := s.ReleaseProducedPlatformArtifact(seed.ID, f.authority.ID, "", p)
	if err != nil {
		t.Fatal(err)
	}
	baseline := prepareTrafficLKGFixture(t, s, "global", "gray", true)
	_, full, _, _, err := s.ReleasePlatformArtifact(baseline.parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "full"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	reportProducedPublication(t, s, baseline.parent, full, true)
	_, full, _, lkg, err := s.VerifyPlatformArtifactReleaseLKG(full.ID, completePlatformVerificationRequest(full.FencingToken, false), testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	candidate := f.candidate(t)
	_, shadow, _, _, err := s.ReleaseProducedPlatformArtifact(candidate.ID, f.authority.ID, initialShadow.ID, p)
	if err != nil {
		t.Fatal(err)
	}
	reportProducedPublication(t, s, candidate, shadow, true)
	c.ArtifactID = candidate.ID
	c.Producer.PreviousReleaseID, c.Producer.PreviousFullReleaseID, c.Producer.BaselineArtifactID = baseline.release.ID, full.ID, lkg.ArtifactID
	if action == "gray" {
		c.Producer.CanaryRuleRef = f.policy.Serving.CanaryRuleRef
		c.Request, _, _ = producedTrafficIntent(candidate.ID, f.authority.ID, "gray", baseline.release.ID, full.ID, lkg.ArtifactID, c.Producer.CanaryRuleRef)
		return f, c, func(s *Store) error {
			_, _, _, _, err := s.ReleaseProducedTrafficArtifact(candidate.ID, f.authority.ID, "gray", baseline.release.ID, full.ID, lkg.ArtifactID, c.Producer.CanaryRuleRef, p)
			return err
		}
	}
	_, gray, _, _, err := s.ReleaseProducedTrafficArtifact(candidate.ID, f.authority.ID, "gray", baseline.release.ID, full.ID, lkg.ArtifactID, f.policy.Serving.CanaryRuleRef, p)
	if err != nil {
		t.Fatal(err)
	}
	ageProducerPublication(t, s, full.ID, 10*time.Second)
	ageProducerPublication(t, s, shadow.ID, 5*time.Second)
	ageProducerPublication(t, s, gray.ID, 2*time.Second)
	reportProducedPublication(t, s, candidate, gray, true)
	c.Producer.PreviousReleaseID = full.ID
	if action == "full" {
		c.Request, _, _ = producedTrafficIntent(candidate.ID, f.authority.ID, "full", full.ID, full.ID, lkg.ArtifactID, "")
		return f, c, func(s *Store) error {
			_, _, _, _, err := s.ReleaseProducedTrafficArtifact(candidate.ID, f.authority.ID, "full", full.ID, full.ID, lkg.ArtifactID, "", p)
			return err
		}
	}
	_, current, _, _, err := s.ReleaseProducedTrafficArtifact(candidate.ID, f.authority.ID, "full", full.ID, full.ID, lkg.ArtifactID, "", p)
	if err != nil {
		t.Fatal(err)
	}
	if action == "verify-recovery" {
		// Recovery verification requires a prior verified producer baseline,
		// including its signed source identity. An operator-only seed is not
		// eligible for that original workflow; do not forge its metadata.
		ageProducerPublication(t, s, current.ID, 2*time.Second)
		reportProducedPublication(t, s, candidate, current, true)
		_, current, _, lkg, err = s.VerifyProducedTrafficLKG(current.ID, f.authority.ID, lkg.ArtifactID, completePlatformVerificationRequest(current.FencingToken, false), p)
		if err != nil {
			t.Fatal(err)
		}
		baseline.parent = candidate
		candidate = f.candidate(t)
		_, shadow, _, _, err = s.ReleaseProducedPlatformArtifact(candidate.ID, f.authority.ID, shadow.ID, p)
		if err != nil {
			t.Fatal(err)
		}
		reportProducedPublication(t, s, candidate, shadow, true)
		_, gray, _, _, err = s.ReleaseProducedTrafficArtifact(candidate.ID, f.authority.ID, "gray", gray.ID, current.ID, lkg.ArtifactID, f.policy.Serving.CanaryRuleRef, p)
		if err != nil {
			t.Fatal(err)
		}
		ageProducerPublication(t, s, current.ID, 10*time.Second)
		ageProducerPublication(t, s, shadow.ID, 5*time.Second)
		ageProducerPublication(t, s, gray.ID, 2*time.Second)
		reportProducedPublication(t, s, candidate, gray, true)
		_, current, _, _, err = s.ReleaseProducedTrafficArtifact(candidate.ID, f.authority.ID, "full", current.ID, current.ID, lkg.ArtifactID, "", p)
		if err != nil {
			t.Fatal(err)
		}
		c.Producer.BaselineArtifactID = lkg.ArtifactID
	}
	rollback := model.PlatformArtifactRollbackRequest{ReleaseChannel: "full", ToGeneration: lkg.Generation, Reason: "neutral producer recovery"}
	if action == "rollback" || action == "verify-recovery" {
		ageProducerPublication(t, s, gray.ID, 120*time.Second)
		ageProducerPublication(t, s, current.ID, 61*time.Second)
		if action == "rollback" {
			c.Mode, c.RollbackGeneration, c.Producer.PreviousReleaseID = "rollback", lkg.Generation, ""
			c.Producer.PreviousFullReleaseID, c.Producer.FailedReleaseID, c.Producer.Rollback = current.ID, current.ID, &rollback
			return f, c, func(s *Store) error {
				_, _, _, _, err := s.RollbackProducedTraffic(candidate.ID, current.ID, f.authority.ID, current.ID, lkg.ArtifactID, rollback, p)
				return err
			}
		}
		_, current, _, _, err = s.RollbackProducedTraffic(candidate.ID, current.ID, f.authority.ID, current.ID, lkg.ArtifactID, rollback, p)
		if err != nil {
			t.Fatal(err)
		}
		candidate = baseline.parent
	}
	ageProducerPublication(t, s, current.ID, 2*time.Second)
	reportProducedPublication(t, s, candidate, current, true)
	verification := completePlatformVerificationRequest(current.FencingToken, false)
	c.Mode, c.ArtifactID, c.ReleaseID = "lkg", "", current.ID
	c.Producer.PreviousReleaseID, c.Producer.PreviousFullReleaseID = "", ""
	c.Producer.Verification = &verification
	return f, c, func(s *Store) error {
		var err error
		if action == "verify-recovery" {
			_, _, _, _, err = s.VerifyProducedTrafficRecoveryLKG(current.ID, f.authority.ID, lkg.ArtifactID, verification, p)
		} else {
			_, _, _, _, err = s.VerifyProducedTrafficLKG(current.ID, f.authority.ID, lkg.ArtifactID, verification, p)
		}
		return err
	}
}

func coldProducerPolicy(t *testing.T, f servingFixture) {
	t.Helper()
	s := f.s
	var token int64
	if err := s.db.QueryRow(`SELECT token FROM fugue_platform_input_local_pins WHERE pin_key='fixture/producer/source'`).Scan(&token); err != nil {
		t.Fatal(err)
	}
	if _, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: "fixture/producer/source", Digest: f.coldDigest, ExpectedToken: token, Release: true, Apply: true}); err != nil {
		t.Fatal(err)
	}
	// Test-only physical cold state. The original bytes and full archive were
	// produced and verified by real writers; no forged pins or archive receipts.
	if _, err := s.db.Exec(`DELETE FROM fugue_platform_input_hot_chunks WHERE content_hash=$1`, f.coldDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_platform_input_manifests SET storage_location='legacy' WHERE content_hash=$1`, f.coldDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := routeCompilerInputArchive(t.Context(), s.db, f.coldObjects, "sha256:"+strings.Repeat("b", 64), f.coldDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DROP TABLE fugue_platform_input_chunks`); err != nil {
		t.Fatal(err)
	}
}

func TestProducerPreparationPostgresColdAllOriginalWorkflows(t *testing.T) {
	for _, action := range []string{"shadow", "gray", "full", "rollback", "verify", "verify-recovery"} {
		t.Run(action, func(t *testing.T) {
			f, c, invoke := producerPreparationFixture(t, action)
			coldProducerPolicy(t, f)
			s := f.s
			before := publicationInputCounts(t, s)
			if err := invoke(s); !errors.Is(err, ErrConflict) || publicationInputCounts(t, s) != before {
				t.Fatal("cold guarded method advanced business state", err)
			}
			plan, err := s.planPublicationInputPreparation(t.Context(), c, "sha256:"+strings.Repeat("b", 64))
			if err != nil {
				t.Fatal("complete original producer intent", err)
			}
			found, lanes := false, 0
			for _, m := range plan.Ticket.Inputs {
				found = found || m.Digest == f.coldDigest
			}
			for _, v := range plan.Ticket.Tuples {
				if v.Table == "fugue_platform_release_lanes" {
					lanes++
				}
			}
			if !found || lanes != 4 {
				t.Fatal("producer authority source or lane witnesses omitted", found, lanes)
			}
			restored, err := restorePublicationInputs(t.Context(), s.db, plan.Ticket, plan.VerificationKey, f.coldObjects)
			if err != nil || restored.Prepared || restored.PublicationActivated || restored.RetirementAuthorized || restored.Usage.GetRequests < 1 || restored.Usage.PutRequests != 0 || publicationInputCounts(t, s) != before {
				t.Fatal("producer restore failed or activated", restored, err)
			}
			f.coldObjects.failGet = true
			confirmed, err := RunPublicationInputPreparationConfirmation(t.Context(), s.databaseURL, s.platformArtifactSigningKeyring(), plan.Ticket)
			if err != nil || !confirmed.Prepared || confirmed.PublicationActivated || publicationInputCounts(t, s) != before {
				t.Fatal("producer fresh confirmation", confirmed, err)
			}
			db, err := openPostgresDatabase(s.databaseURL)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			fresh := &Store{db: db, databaseURL: s.databaseURL}
			fresh.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
			if err := invoke(fresh); err != nil {
				t.Fatal("original guarded method after new pool without old chunks or remote GET", err)
			}
		})
	}
}

func TestProducerPreparationPostgresPolicyFenceAndBusinessAuthorityRemainIndependent(t *testing.T) {
	for _, phase := range []string{"before-get", "after-restore", "after-confirm"} {
		for _, change := range []string{"policy", "absent-target"} {
			t.Run(change+"-"+phase, func(t *testing.T) {
				f, c, invoke := producerPreparationFixture(t, "shadow")
				coldProducerPolicy(t, f)
				plan, err := f.s.planPublicationInputPreparation(t.Context(), c, "sha256:"+strings.Repeat("b", 64))
				if err != nil {
					t.Fatal(err)
				}
				if phase != "before-get" {
					if _, err := restorePublicationInputs(t.Context(), f.s.db, plan.Ticket, plan.VerificationKey, f.coldObjects); err != nil {
						t.Fatal(err)
					}
				}
				if phase == "after-confirm" {
					if _, err := f.s.confirmPublicationInputPreparation(t.Context(), plan.Ticket); err != nil {
						t.Fatal(err)
					}
				}
				if change == "policy" {
					lane := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindPolicySnapshot, f.authority.ScopeKey, "shadow")
					if _, err := f.s.db.Exec(`UPDATE fugue_platform_release_lanes SET frozen=true,version=version+1 WHERE lane_key=$1`, lane); err != nil {
						t.Fatal(err)
					}
				} else {
					lane := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindReleaseSet, "global", "shadow")
					absent := false
					for _, v := range plan.Ticket.Tuples {
						absent = absent || v.Table == "fugue_platform_release_lanes" && v.Key == lane && v.Xmin == "0" && v.CTID == "(0,0)"
					}
					if !absent {
						t.Fatal("missing target lane was not bound")
					}
					if _, err := f.s.db.Exec(`INSERT INTO fugue_platform_release_lanes(lane_key,artifact_kind,scope_key,release_channel,frozen,updated_at) VALUES($1,$2,'global','shadow',true,now())`, lane, model.PlatformArtifactKindReleaseSet); err != nil {
						t.Fatal(err)
					}
				}
				before := publicationInputCounts(t, f.s)
				if phase == "before-get" {
					r, err := restorePublicationInputs(t.Context(), f.s.db, plan.Ticket, plan.VerificationKey, f.coldObjects)
					if err == nil || r.Usage.GetRequests != 0 {
						t.Fatal("changed policy fence spent GET", r, err)
					}
				} else if phase == "after-restore" {
					if r, err := f.s.confirmPublicationInputPreparation(t.Context(), plan.Ticket); err == nil || r.Prepared {
						t.Fatal("changed policy fence confirmed", r, err)
					}
				} else if err := invoke(f.s); !errors.Is(err, ErrConflict) {
					t.Fatal("preparation bypassed original business authority", err)
				}
				if publicationInputCounts(t, f.s) != before {
					t.Fatal("refusal changed business state")
				}
			})
		}
	}
}

func TestProducerPreparationContractDerivesExactOriginalIntent(t *testing.T) {
	c := PublicationInputPreparationContract{Version: 1, Mode: "release", ArtifactID: "neutral-artifact", OperationKey: "neutral-operation",
		CompilerInputPreparationBudget: CompilerInputPreparationBudget{MaxInputs: 1, MaxRawBytes: 1, MaxStoredBytes: 1, MaxGetBytes: 1, MaxGetRequests: 1, MaxSeconds: 1},
		Producer:                       &ProducerInputPreparationIntent{Action: "shadow", PolicyReleaseID: "neutral-policy"}}
	c.Request, _ = producedShadowIntent(c.ArtifactID, c.Producer.PolicyReleaseID, "")
	if err := c.Validate(false); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"request", "policy", "phase", "baseline", "evidence"} {
		raw, _ := json.Marshal(c)
		var bad PublicationInputPreparationContract
		json.Unmarshal(raw, &bad)
		switch field {
		case "request":
			bad.Request.IdempotencyKey = "other"
		case "policy":
			bad.Producer.PolicyReleaseID = "other"
		case "phase":
			bad.Producer.Action = "verify"
		case "baseline":
			bad.Producer.BaselineArtifactID = "other"
		case "evidence":
			bad.Producer.Verification = &model.PlatformArtifactVerifyLKGRequest{}
		}
		if bad.Validate(false) == nil {
			t.Fatal("cross-workflow or changed original intent accepted", field)
		}
	}
}
