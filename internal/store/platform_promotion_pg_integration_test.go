package store

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestFullReleaseSetPostgresAtomicPromotion(t *testing.T) {
	address := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL")
	}
	if !strings.Contains(address, "fugue_test") && !strings.Contains(address, "fugue-pgtest") {
		t.Fatal("requires disposable test database")
	}
	s := New("", address)
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	waitBlocked := func(t *testing.T) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			var count int
			if err := s.db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND wait_event='advisory'`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count > 0 {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("concurrent publisher did not reach scope lock")
	}
	for _, scenario := range []string{"complete", "probe fails while queued", "expires while queued", "topology changes while queued"} {
		t.Run(scenario, func(t *testing.T) {
			scope := "promotion-" + model.NewID("test")
			f := preparePromotionFixture(t, s, scope)
			var beforeReleases, beforeMessages int
			if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_artifact_releases WHERE scope_key=$1`, scope).Scan(&beforeReleases); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_release_messages WHERE scope_key=$1`, scope).Scan(&beforeMessages); err != nil {
				t.Fatal(err)
			}
			lkgBefore, err := s.GetPlatformLKG(f.parent.ArtifactKind, scope)
			if err != nil {
				t.Fatal(err)
			}
			// A real competing transaction owns the publication scope. Queue a full
			// publication and change the facts before it acquires that same scope.
			tx, err := s.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err := pgLockPromotionScope(context.Background(), tx, scope, true); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				_, _, _, _, err := s.ReleasePlatformArtifact(f.parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "full", IdempotencyKey: "full"}, testPlatformPrincipal())
				done <- err
			}()
			waitBlocked(t)
			switch scenario {
			case "probe fails while queued":
				if _, err := tx.Exec(`UPDATE fugue_platform_consumer_instances SET probe_status='failed' WHERE consumer_id=$1 AND artifact_kind=$2 AND scope_key=$3`, f.consumers[0].ConsumerID, f.consumers[0].ArtifactKind, scope); err != nil {
					t.Fatal(err)
				}
			case "expires while queued":
				if _, err := tx.Exec(`UPDATE fugue_platform_consumer_instances SET last_heartbeat_at=$1 WHERE scope_key=$2`, time.Now().Add(-time.Hour), scope); err != nil {
					t.Fatal(err)
				}
			case "topology changes while queued":
				// Emulate the immutable newer revision being committed by the owner of
				// the scope lock, exactly as CreatePlatformExpectedConsumerSet serializes.
				if _, err := tx.Exec(`INSERT INTO fugue_platform_expected_consumer_sets SELECT id||'-new',release_set_id,artifact_release_id,artifact_kind,scope_key,scope_json,expected_generation,topology_revision||'-new',revision+100,requires_consumers,required_cardinality,optional_cardinality,heartbeat_deadline,convergence_deadline,consumers_json,created_at,updated_at FROM fugue_platform_expected_consumer_sets WHERE id=$1`, f.sets[0].ID); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if scenario == "complete" {
					if err != nil {
						t.Fatal("valid full publication failed", err)
					}
				} else if !errors.Is(err, ErrConflict) {
					t.Fatal("queued publisher used stale preflight facts", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("publisher failed to finish")
			}
			var afterReleases, afterMessages int
			if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_artifact_releases WHERE scope_key=$1`, scope).Scan(&afterReleases); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_release_messages WHERE scope_key=$1`, scope).Scan(&afterMessages); err != nil {
				t.Fatal(err)
			}
			_, _, found, err := s.GetActivePlatformArtifact(f.parent.ArtifactKind, scope, "full")
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "complete" {
				if !found || afterReleases != beforeReleases+1 || afterMessages != beforeMessages+1 {
					t.Fatal("successful publish not atomic")
				}
			} else {
				if found || afterReleases != beforeReleases || afterMessages != beforeMessages {
					t.Fatal("failed convergence changed release ledger")
				}
				var lanes int
				if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_release_lanes WHERE artifact_kind=$1 AND scope_key=$2 AND release_channel='full'`, f.parent.ArtifactKind, scope).Scan(&lanes); err != nil || lanes != 0 {
					t.Fatal("failed publication advanced lane", err)
				}
			}
			lkgAfter, err := s.GetPlatformLKG(f.parent.ArtifactKind, scope)
			if err != nil || !reflect.DeepEqual(lkgBefore, lkgAfter) {
				t.Fatal("publication changed verified LKG", err)
			}
		})
	}
	t.Run("expected revision waits for publication scope", func(t *testing.T) {
		scope := "promotion-" + model.NewID("test")
		f := preparePromotionFixture(t, s, scope)
		tx, err := s.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if err := pgLockPromotionScope(context.Background(), tx, scope, true); err != nil {
			t.Fatal(err)
		}
		changed := clonePlatformExpectedConsumerSet(f.sets[0])
		changed.ID += "-next"
		changed.Revision += 100
		changed.TopologyRevision += "-next"
		done := make(chan error, 1)
		go func() { _, err := s.CreatePlatformExpectedConsumerSet(changed); done <- err }()
		waitBlocked(t)
		select {
		case err := <-done:
			t.Fatal("expectation bypassed publication lock", err)
		default:
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("expectation remained blocked after transaction")
		}
	})
	t.Run("trusted identity scope serializes omitted heartbeat scope", func(t *testing.T) {
		scope := "promotion-" + model.NewID("test")
		f := preparePromotionFixture(t, s, scope)
		consumer := f.consumers[0]
		set := f.sets[0]
		now := time.Now().UTC()
		keys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "key", Keys: map[string]string{"key": "synthetic-signing-key"}}
		token, err := platformcontrol.IssuePlatformComponentIdentity(keys, platformcontrol.PlatformComponentIdentityClaims{CredentialID: "credential", Component: consumer.Component, NodeID: consumer.NodeID, ScopeKey: scope, ArtifactKinds: []string{consumer.ArtifactKind}}, now, 5*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		claims, err := platformcontrol.ParsePlatformComponentIdentity(keys, token, now)
		if err != nil {
			t.Fatal(err)
		}
		heartbeat := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: consumer.ConsumerID, Component: consumer.Component, NodeID: consumer.NodeID, ArtifactKind: consumer.ArtifactKind, ScopeKey: scope, ReleaseSetID: f.parent.ID, ExpectedConsumerSetID: set.ID, FencingToken: f.release.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: 2, IssuedAt: now, Nonce: "fedcba9876543210fedcba9876543210", GenerationSequence: consumer.GenerationSequence, DesiredGeneration: consumer.DesiredGeneration, ActualGeneration: consumer.ActualGeneration, LKGGeneration: consumer.LKGGeneration, ApplyStatus: "applied", ProbeStatus: "passed"}
		heartbeat.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
		if err != nil {
			t.Fatal(err)
		}
		heartbeat.ScopeKey = ""
		tx, err := s.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if err := pgLockPromotionScope(context.Background(), tx, " "+strings.ToUpper(scope)+" ", true); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := s.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, heartbeat, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{})
			done <- err
		}()
		waitBlocked(t)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("heartbeat remained blocked")
		}
	})

}
