package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func consumerInputEnvelope(t *testing.T, s *Store, release model.PlatformArtifactRelease) (model.PlatformExpectedConsumerSet, platformcontrol.PlatformComponentIdentityClaims, platformcontrol.PlatformConsumerHeartbeatEnvelope) {
	t.Helper()
	now := time.Now().UTC()
	a, err := s.GetPlatformArtifact(release.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID: a.ID, ArtifactReleaseID: release.ID, ArtifactKind: a.ArtifactKind,
		Scope: a.Scope, ScopeKey: a.ScopeKey, Generation: a.Generation, PreparedAt: now,
		Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "neutral-consumer", EdgeGroupID: "neutral-group"}}},
	})
	if err != nil || len(set.Consumers) != 1 {
		t.Fatal(set, err)
	}
	keys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "neutral", Keys: map[string]string{"neutral": "synthetic-component-secret"}}
	token, err := platformcontrol.IssuePlatformComponentIdentity(keys, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID: "neutral-credential", Component: set.Consumers[0].Component, NodeID: set.Consumers[0].NodeID, ScopeKey: a.ScopeKey, ArtifactKinds: []string{a.ArtifactKind},
	}, now.Add(-time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(keys, token, now)
	if err != nil {
		t.Fatal(err)
	}
	h, err := platformcontrol.BindPlatformConsumerHeartbeatToExpectedSet(claims, set, platformcontrol.PlatformConsumerHeartbeatEnvelope{
		FencingToken: release.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: 1, IssuedAt: now,
		Nonce: model.NewID("nonce"), GenerationSequence: a.GenerationSequence, ActualGeneration: a.Generation, LKGGeneration: a.Generation, ApplyStatus: "applied", ProbeStatus: "passed",
	})
	if err != nil {
		t.Fatal(err)
	}
	h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
	if err != nil {
		t.Fatal(err)
	}
	return set, claims, h
}

func consumerInputCounts(t *testing.T, s *Store) [4]int64 {
	t.Helper()
	var out [4]int64
	err := s.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_expected_consumer_sets),(SELECT count(*) FROM fugue_platform_consumer_instances),(SELECT count(*) FROM fugue_platform_shadow_consumers),(SELECT count(*) FROM fugue_audit_events)`).Scan(&out[0], &out[1], &out[2], &out[3])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestConsumerInputPostgresColdRegistrationAndFreshAuditedHeartbeat(t *testing.T) {
	s, objects, plan, first := publicationPreparationFixture(t, "release")
	set, claims, h := consumerInputEnvelope(t, s, first)
	before := consumerInputCounts(t, s)
	if _, err := s.CreatePlatformExpectedConsumerSet(set); !errors.Is(err, ErrConflict) || consumerInputCounts(t, s) != before {
		t.Fatal("cold registration changed retained roots", err)
	}
	// This fixture's independent publication preparation really GETs and
	// full-SHA verifies both original inputs after copy/route and DROP chunks.
	// It is not a consumer-specific preparation contract or its acceptance.
	r, err := restorePublicationInputs(t.Context(), s.db, plan.Ticket, plan.VerificationKey, objects)
	if err != nil || r.Usage.GetRequests != 2 || r.Usage.PutRequests != 0 || consumerInputCounts(t, s) != before {
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
	if _, err := fresh.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatal("fresh registration without old chunks/GET", err)
	}
	before = consumerInputCounts(t, fresh)
	if _, err := db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE state='verified'`); err != nil {
		t.Fatal(err)
	}
	audit := bundleauth.NewKeyring("neutral-audit", "neutral-audit", "", "", nil)
	if _, err := fresh.AcceptTrustedPlatformConsumerHeartbeatWithAudit(claims, set.ID, h, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}, audit); !errors.Is(err, ErrConflict) || consumerInputCounts(t, fresh) != before {
		t.Fatal("cold heartbeat advanced cursor/audit", err)
	}
	// Restore durable evidence by the real full-readback protocol, using only
	// the independently restored local representation, with remote GET disabled.
	for _, pin := range plan.Ticket.Inputs {
		if _, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: "consumer/reverify/" + pin.Digest[7:], Digest: pin.Digest, MaxRawBytes: 2048, Apply: true}); err != nil {
			t.Fatal(err)
		}
	}
	// Existing wire binding permits these fields to be omitted; preparation
	// resolves their exact namespace from the expectation in its RR snapshot.
	omitted := h
	omitted.ArtifactKind, omitted.ScopeKey, omitted.DesiredGeneration = "", "", ""
	if got, err := fresh.AcceptTrustedPlatformConsumerHeartbeatWithAudit(claims, set.ID, omitted, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}, audit); err != nil || !got.IdentityVerified {
		t.Fatal("fresh admitted audited heartbeat", got, err)
	}
	accepted := consumerInputCounts(t, fresh)
	if accepted[1] != before[1]+1 || accepted[3] != before[3]+1 {
		t.Fatal("cursor/audit not atomic", before, accepted)
	}
	if _, err := fresh.AcceptTrustedPlatformConsumerHeartbeatWithAudit(claims, set.ID, h, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}, audit); !errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatReplay) || consumerInputCounts(t, fresh) != accepted {
		t.Fatal("replay changed cursor/audit", err)
	}
	h.Sequence++
	h.IssuedAt = time.Now().UTC()
	h.Nonce = model.NewID("nonce")
	h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
	if _, err := fresh.AcceptTrustedPlatformConsumerHeartbeatWithAudit(claims, set.ID, h, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}, bundleauth.Keyring{}); err == nil || consumerInputCounts(t, fresh) != accepted {
		t.Fatal("failed audit committed consumer", err)
	}
}

func TestConsumerInputPostgresEveryReportedGenerationIsIndependent(t *testing.T) {
	for _, slot := range []string{"actual", "candidate", "lkg"} {
		t.Run(slot, func(t *testing.T) {
			s, _, pins := inputActivationFixture(t, 2)
			release := lkgInputRelease(t, s, "activation-artifact-001")
			set, claims, h := consumerInputEnvelope(t, s, release)
			if _, err := s.CreatePlatformExpectedConsumerSet(set); err != nil {
				t.Fatal(err)
			}
			switch slot {
			case "actual":
				h.ActualGeneration = "activation-artifact-000"
			case "candidate":
				h.CandidateGeneration = "activation-artifact-000"
			case "lkg":
				h.LKGGeneration = "activation-artifact-000"
			}
			h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
			proof, _, err := s.prepareCompilerInputConsumer(t.Context(), compilerInputConsumerIntent{expectedID: set.ID, heartbeat: &h})
			if err != nil || len(proof.inputs) != 2 {
				t.Fatal("retained generation omitted", err)
			}
			if _, err := s.db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE pin_key=$1`, pins[0].Key); err != nil {
				t.Fatal(err)
			}
			before := consumerInputCounts(t, s)
			if _, err := s.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, h, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); !errors.Is(err, ErrConflict) || consumerInputCounts(t, s) != before {
				t.Fatal("cold retained generation admitted", err)
			}
		})
	}
}

func TestConsumerInputPostgresRechecksBeforeBusinessAndAudit(t *testing.T) {
	for _, change := range []string{"expectation", "artifact", "release", "pin", "epoch", "keyring"} {
		t.Run(change, func(t *testing.T) {
			s, _, _ := inputActivationFixture(t, 1)
			release := lkgInputRelease(t, s, "activation-artifact-000")
			set, claims, h := consumerInputEnvelope(t, s, release)
			if _, err := s.CreatePlatformExpectedConsumerSet(set); err != nil {
				t.Fatal(err)
			}
			proof, pins, err := s.prepareCompilerInputConsumer(t.Context(), compilerInputConsumerIntent{expectedID: set.ID, heartbeat: &h})
			if err != nil {
				t.Fatal(err)
			}
			q := map[string]string{
				"expectation": `UPDATE fugue_platform_expected_consumer_sets SET updated_at=updated_at`,
				"artifact":    `UPDATE fugue_platform_artifacts SET updated_at=updated_at`,
				"release":     `UPDATE fugue_platform_artifact_releases SET updated_at=updated_at`,
				"pin":         `UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL`,
				"epoch":       `UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`,
			}[change]
			if q != "" {
				if _, err := s.db.Exec(q); err != nil {
					t.Fatal(err)
				}
			} else {
				s.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
			}
			before := consumerInputCounts(t, s)
			called := false
			err = s.consumePreparedCompilerInput(t.Context(), proof, pins, false, func(ctx context.Context, tx *sql.Tx) error {
				_, err := pgLockConsumerHeartbeatScope(ctx, tx, claims, h)
				return err
			}, func(context.Context, *sql.Tx) error { called = true; return nil })
			if err == nil || called || consumerInputCounts(t, s) != before {
				t.Fatal("changed preparation reached business/audit", change, err, called)
			}
		})
	}
}

func TestConsumerInputPostgresKeyRotationDuringAuditRollsBack(t *testing.T) {
	s, _, _ := inputActivationFixture(t, 1)
	release := lkgInputRelease(t, s, "activation-artifact-000")
	set, claims, h := consumerInputEnvelope(t, s, release)
	if _, err := s.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatal(err)
	}
	proof, pins, err := s.prepareCompilerInputConsumer(t.Context(), compilerInputConsumerIntent{expectedID: set.ID, heartbeat: &h})
	if err != nil {
		t.Fatal(err)
	}
	before := consumerInputCounts(t, s)
	audit := bundleauth.NewKeyring("neutral-audit", "neutral-audit", "", "", nil)
	rollback := false
	err = s.consumePreparedCompilerInput(t.Context(), proof, pins, false, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		rollback, err = pgLockConsumerHeartbeatScope(ctx, tx, claims, h)
		return err
	}, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := s.pgAcceptTrustedPlatformConsumerHeartbeatInTransaction(ctx, tx, claims, set.ID, h, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}, &audit, rollback); err != nil {
			return err
		}
		// ABA rotation is a revision change even with exactly the same keys.
		s.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
		return nil
	})
	if !errors.Is(err, ErrConflict) || consumerInputCounts(t, s) != before {
		t.Fatal("rotation committed cursor or audit", err)
	}
}
