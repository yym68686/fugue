package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

func TestTrafficLKGAtomicRecovery(t *testing.T) { testTrafficLKGAtomicRecovery(t, "") }

func TestTrafficLKGPostgresAtomicRecovery(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set disposable test database")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testTrafficLKGAtomicRecovery(t, address)
}

func trafficLKGKinds() []string {
	return []string{model.PlatformArtifactKindReleaseSet, model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindPolicySnapshot}
}

func trafficLKGState(t *testing.T, s *Store, scope string) []*model.PlatformLKGSnapshot {
	t.Helper()
	out := []*model.PlatformLKGSnapshot{}
	for _, kind := range trafficLKGKinds() {
		lkg, err := s.GetPlatformLKG(kind, scope)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, lkg)
	}
	return out
}

func testTrafficLKGAtomicRecovery(t *testing.T, address string) {
	for _, scenario := range []string{"complete", "full complete", "shadow", "not explicit", "missing member", "failed probe", "expired", "wrong fence", "unverified", "missing policy", "tampered policy", "new topology", "new publication", "frozen", "full without fresh facts", "queued probe failure"} {
		t.Run(scenario, func(t *testing.T) {
			if scenario == "queued probe failure" && address == "" {
				t.Skip("Postgres concurrency")
			}
			s := New(t.TempDir()+"/state.json", address)
			configureTestPlatformArtifactSigning(s)
			if err := s.Init(); err != nil {
				t.Fatal(err)
			}
			if address != "" {
				t.Cleanup(func() { s.db.Close() })
			}
			scope := "recovery-" + model.NewID("test")
			channel := "gray"
			if scenario == "shadow" {
				channel = "shadow"
			}
			f := prepareTrafficLKGFixture(t, s, scope, channel, scenario == "full without fresh facts" || scenario == "full complete")
			// Keep a real independent route LKG. A gray member's recovery snapshot
			// must neither erase its history nor authorize other groups to serve it.
			if scenario == "complete" {
				child, err := s.GetPlatformArtifact(f.parent.Content["artifact_ids"].([]any)[0].(string))
				if err != nil {
					t.Fatal(err)
				}
				seedVerifiedPlatformLKG(t, s, child)
			}
			before := trafficLKGState(t, s, scope)
			request := completePlatformVerificationRequest(f.release.FencingToken, true)
			if scenario == "not explicit" {
				request.AllowInitialLKG = false
			}
			if scenario == "full without fresh facts" || scenario == "full complete" {
				_, release, _, _, err := s.ReleasePlatformArtifact(f.parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "full"}, testPlatformPrincipal())
				if err != nil {
					t.Fatal(err)
				}
				f.release = release
				request = completePlatformVerificationRequest(release.FencingToken, false)
				if scenario == "full complete" {
					reportTrafficLKGFixture(t, s, f)
				}
			}
			if scenario == "new publication" {
				if _, _, _, _, err := s.ReleasePlatformArtifact(f.parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal()); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "new topology" {
				changed := clonePlatformExpectedConsumerSet(f.sets[0])
				changed.ID += "-new"
				changed.Revision += 10
				changed.TopologyRevision += "-new"
				if _, err := s.CreatePlatformExpectedConsumerSet(changed); err != nil {
					t.Fatal(err)
				}
			}
			if address == "" {
				if err := s.withLockedState(true, func(state *model.State) error {
					switch scenario {
					case "missing member":
						state.ExpectedConsumerSets = state.ExpectedConsumerSets[:2]
					case "failed probe":
						state.PlatformConsumerInstances[0].ProbeStatus = "failed"
					case "expired":
						state.PlatformConsumerInstances[0].LastHeartbeatAt = time.Now().Add(-time.Hour)
					case "wrong fence":
						state.PlatformConsumerInstances[0].FencingToken++
					case "unverified":
						state.PlatformConsumerInstances[0].IdentityVerified = false
					case "frozen":
						state.PlatformReleaseLanes[0].Frozen = true
					case "missing policy", "tampered policy":
						for i := range state.PlatformArtifacts {
							if state.PlatformArtifacts[i].ArtifactKind == model.PlatformArtifactKindPolicySnapshot {
								if scenario == "missing policy" {
									state.PlatformArtifacts = append(state.PlatformArtifacts[:i], state.PlatformArtifacts[i+1:]...)
								} else {
									state.PlatformArtifacts[i].Content["tampered"] = true
								}
								break
							}
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				queries := map[string]string{
					"missing member":  `DELETE FROM fugue_platform_expected_consumer_sets WHERE scope_key=$1 AND artifact_kind='caddy_route_config'`,
					"failed probe":    `UPDATE fugue_platform_consumer_instances SET probe_status='failed' WHERE scope_key=$1`,
					"expired":         `UPDATE fugue_platform_consumer_instances SET last_heartbeat_at=now()-interval '1 hour' WHERE scope_key=$1`,
					"wrong fence":     `UPDATE fugue_platform_consumer_instances SET fencing_token=fencing_token+1 WHERE scope_key=$1`,
					"unverified":      `UPDATE fugue_platform_consumer_instances SET identity_verified=false WHERE scope_key=$1`,
					"frozen":          `UPDATE fugue_platform_release_lanes SET frozen=true WHERE scope_key=$1`,
					"missing policy":  `DELETE FROM fugue_platform_artifacts WHERE scope_key=$1 AND artifact_kind='policy_snapshot'`,
					"tampered policy": `UPDATE fugue_platform_artifacts SET content_json=content_json||'{"tampered":true}'::jsonb WHERE scope_key=$1 AND artifact_kind='policy_snapshot'`,
				}
				if query := queries[scenario]; query != "" {
					if _, err := s.db.Exec(query, scope); err != nil {
						t.Fatal(err)
					}
				}
			}
			verify := func() error {
				_, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(f.release.ID, request, testPlatformPrincipal())
				return err
			}
			var err error
			if scenario == "queued probe failure" {
				tx, txErr := s.db.BeginTx(context.Background(), nil)
				if txErr != nil {
					t.Fatal(txErr)
				}
				defer tx.Rollback()
				if err := pgLockPromotionScope(context.Background(), tx, scope, false); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- verify() }()
				deadline := time.Now().Add(5 * time.Second)
				for {
					var count int
					if err := s.db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND wait_event='advisory'`).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count > 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("verification failed to wait for evidence transaction")
					}
					time.Sleep(10 * time.Millisecond)
				}
				if _, err := tx.Exec(`UPDATE fugue_platform_consumer_instances SET probe_status='failed' WHERE scope_key=$1`, scope); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				select {
				case err = <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("verification stayed blocked")
				}
			} else {
				err = verify()
			}
			if scenario != "complete" && scenario != "full complete" {
				if !errors.Is(err, ErrConflict) {
					t.Fatal("invalid recovery evidence accepted", err)
				}
				if !reflect.DeepEqual(before, trafficLKGState(t, s, scope)) {
					t.Fatal("failed verification changed a recovery pointer")
				}
				r, readErr := s.GetPlatformArtifactRelease(f.release.ID)
				if readErr != nil || r.VerificationState == model.PlatformArtifactVerificationStateVerified {
					t.Fatal("failed verification changed release state", readErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			after := trafficLKGState(t, s, scope)
			for _, lkg := range after {
				if lkg == nil || lkg.VerifiedByReleaseID != f.release.ID || lkg.VerificationEvidenceHash != platformsafety.VerificationEvidenceHash(request) {
					t.Fatal("incomplete or mixed recovery result")
				}
				artifact, err := s.GetPlatformArtifact(lkg.ArtifactID)
				if err != nil || !platformsafety.EvaluatePlatformLKGSnapshot(*lkg, artifact, s.platformArtifactSigningKeyring(), time.Now()).Pass {
					t.Fatal("recovery snapshot is not independently verifiable", err)
				}
			}
			standalone, err := s.GetStandalonePlatformLKG(model.PlatformArtifactKindEdgeRouteBundle, scope)
			if err != nil || scenario == "complete" && !reflect.DeepEqual(standalone, before[1]) || scenario == "full complete" && standalone != nil {
				t.Fatal("member verification replaced standalone serving", err)
			}
			if err := verify(); err != nil || !reflect.DeepEqual(after, trafficLKGState(t, s, scope)) {
				t.Fatal("retry rewrote recovery evidence", err)
			}
		})
	}
}

// Report a new publication through the real identity/replay-checked ingress.
func reportTrafficLKGFixture(t *testing.T, s *Store, f promotionFixture) {
	t.Helper()
	for _, previous := range f.sets {
		set := clonePlatformExpectedConsumerSet(previous)
		set.ID = model.NewID("expected")
		set.Revision += 10
		set.ArtifactReleaseID = f.release.ID
		if _, err := s.CreatePlatformExpectedConsumerSet(set); err != nil {
			t.Fatal(err)
		}
		for _, consumer := range f.consumers {
			if consumer.ArtifactKind != set.ArtifactKind {
				continue
			}
			now := time.Now().UTC()
			keys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "key", Keys: map[string]string{"key": "synthetic-serving-identity"}}
			claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "credential", TokenID: "token", Component: consumer.Component, NodeID: consumer.NodeID, ScopeKey: consumer.ScopeKey, ArtifactKinds: []string{consumer.ArtifactKind}}
			token, err := platformcontrol.IssuePlatformComponentIdentity(keys, claims, now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			claims, err = platformcontrol.ParsePlatformComponentIdentity(keys, token, now)
			if err != nil {
				t.Fatal(err)
			}
			heartbeat := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: consumer.ConsumerID, Component: consumer.Component, NodeID: consumer.NodeID, ArtifactKind: consumer.ArtifactKind, ScopeKey: consumer.ScopeKey, ReleaseSetID: f.parent.ID, ExpectedConsumerSetID: set.ID, FencingToken: f.release.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: consumer.Sequence + 1, IssuedAt: now, Nonce: fmt.Sprintf("%032d", now.UnixNano()), GenerationSequence: consumer.GenerationSequence, DesiredGeneration: consumer.DesiredGeneration, ActualGeneration: consumer.ActualGeneration, LKGGeneration: consumer.LKGGeneration, ApplyStatus: "applied", ProbeStatus: "passed"}
			heartbeat.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, heartbeat, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
				t.Fatal(err)
			}
		}
	}
}
