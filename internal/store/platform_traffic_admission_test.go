package store

import (
	"context"
	"errors"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

func TestLeasedTrafficAdmission(t *testing.T) { testLeasedTrafficAdmission(t, "") }
func TestLeasedTrafficAdmissionPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set disposable test database")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testLeasedTrafficAdmission(t, address)
}

func testLeasedTrafficAdmission(t *testing.T, address string) {
	for _, scenario := range []string{"supported", "failed facts still support recovery", "missing route capability", "missing DNS capability", "missing TLS capability", "stale issued", "missing issued", "stale received", "unverified", "new topology", "empty cohort member", "standalone", "soft override", "rollback", "rollback missing capability", "queued revocation"} {
		t.Run(scenario, func(t *testing.T) {
			if scenario == "queued revocation" && address == "" {
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
			scope := model.NewID("leased-admission")
			now := time.Now().UTC()
			input := platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: scope, Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}, ACMEChallenges: []platformconfig.ACMEChallengeIntent{{ID: "challenge", Zone: "example.test", Hostname: "_acme-challenge.example.test", Value: "synthetic-proof", TTL: 60, ExpiresAt: now.Add(time.Hour)}}}, Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: scope, TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "test", EdgeGroupIDs: []string{"edge-group-a"}}}}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now}}
			f := prepareTrafficLKGFixture(t, s, scope, "shadow", false, input)
			for _, old := range f.consumers {
				claims := platformcontrol.PlatformComponentIdentityClaims{Version: "v1", CredentialID: "credential", TokenID: "token", Component: old.Component, NodeID: old.NodeID, ScopeKey: scope, ArtifactKinds: []string{old.ArtifactKind}}
				identityKeys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "key", Keys: map[string]string{"key": "synthetic-admission-identity"}}
				token, err := platformcontrol.IssuePlatformComponentIdentity(identityKeys, claims, time.Now().UTC(), time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				claims, err = platformcontrol.ParsePlatformComponentIdentity(identityKeys, token, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: old.ConsumerID, Component: old.Component, NodeID: old.NodeID, ArtifactKind: old.ArtifactKind, ScopeKey: scope, ReleaseSetID: old.ReleaseSetID, ExpectedConsumerSetID: old.ExpectedConsumerSetID, FencingToken: old.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: old.Sequence + 1, IssuedAt: time.Now().UTC(), Nonce: model.NewID("capability-nonce"), GenerationSequence: old.GenerationSequence, DesiredGeneration: old.DesiredGeneration, CandidateGeneration: old.DesiredGeneration, ApplyStatus: "staged", ProbeStatus: "shadow_validated", CompatibilityCapabilities: []string{platformcontrol.TrafficReleaseCapabilityV1}}
				if scenario == "failed facts still support recovery" {
					h.ApplyStatus, h.ProbeStatus = "failed", "failed"
				}
				missing := scenario == "missing route capability" && old.ArtifactKind == model.PlatformArtifactKindEdgeRouteBundle || (scenario == "missing DNS capability" || scenario == "soft override" || scenario == "rollback missing capability") && old.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle || scenario == "missing TLS capability" && old.ArtifactKind == model.PlatformArtifactKindCaddyRouteConfig
				if missing {
					h.CompatibilityCapabilities = nil
				}
				h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
				if _, err := s.AcceptTrustedPlatformConsumerHeartbeat(claims, old.ExpectedConsumerSetID, h, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "new topology" || scenario == "empty cohort member" {
				set := clonePlatformExpectedConsumerSet(f.sets[1])
				set.ID = model.NewID("expectation")
				set.Revision += 100
				set.TopologyRevision += "-changed"
				if scenario == "new topology" {
					set.Consumers[0].NodeID = "replacement"
					set.Consumers[0].ConsumerID = "dns-server:replacement"
				} else {
					set.Consumers[0].Cohort = "edge-group-outside"
				}
				if _, err := s.CreatePlatformExpectedConsumerSet(set); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "stale issued" || scenario == "missing issued" || scenario == "stale received" || scenario == "unverified" {
				if address == "" {
					if err := s.withLockedState(true, func(st *model.State) error {
						for i := range st.PlatformConsumerInstances {
							c := &st.PlatformConsumerInstances[i]
							if c.ScopeKey != scope {
								continue
							}
							switch scenario {
							case "stale issued":
								expired := now.Add(-3 * time.Minute)
								c.IssuedAt = &expired
							case "missing issued":
								c.IssuedAt = nil
							case "stale received":
								c.LastHeartbeatAt = now.Add(-3 * time.Minute)
							case "unverified":
								c.IdentityVerified = false
							}
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				} else {
					query := "UPDATE fugue_platform_consumer_instances SET identity_verified=false WHERE scope_key=$1"
					if scenario == "stale issued" {
						query = "UPDATE fugue_platform_consumer_instances SET issued_at=now()-interval '3 minutes' WHERE scope_key=$1"
					}
					if scenario == "missing issued" {
						query = "UPDATE fugue_platform_consumer_instances SET issued_at=NULL WHERE scope_key=$1"
					}
					if scenario == "stale received" {
						query = "UPDATE fugue_platform_consumer_instances SET last_heartbeat_at=now()-interval '3 minutes' WHERE scope_key=$1"
					}
					if _, err := s.db.Exec(query, scope); err != nil {
						t.Fatal(err)
					}
				}
			}
			request := model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=test"}
			if scenario == "soft override" {
				request.SoftOverride = true
				request.Reason = "compatibility cannot be waived"
			}
			id := f.parent.ID
			if scenario == "standalone" {
				id = f.parent.Content["artifact_ids"].([]any)[1].(string)
			}
			before, err := s.ListPlatformReleaseMessages(model.PlatformArtifactKindReleaseSet, scope, time.Time{}, 100)
			if err != nil {
				t.Fatal(err)
			}
			principal := testPlatformPrincipal()
			if scenario == "soft override" {
				principal = testPlatformSoftOverridePrincipal()
			}
			publish := func() error {
				if strings.HasPrefix(scenario, "rollback") {
					_, _, _, _, err := s.RollbackPlatformArtifact(f.parent.ID, model.PlatformArtifactRollbackRequest{ReleaseChannel: "gray", ToGeneration: f.parent.Generation, CanaryRuleRef: "cohort=test", Reason: "explicit recovery"}, principal)
					return err
				}
				_, _, _, _, err := s.ReleasePlatformArtifact(id, request, principal)
				return err
			}
			if scenario == "queued revocation" {
				tx, e := s.db.BeginTx(context.Background(), nil)
				if e != nil {
					t.Fatal(e)
				}
				defer tx.Rollback()
				if e = pgLockPromotionScope(context.Background(), tx, scope, false); e != nil {
					t.Fatal(e)
				}
				done := make(chan error, 1)
				go func() { done <- publish() }()
				deadline := time.Now().Add(5 * time.Second)
				for {
					var waiting int
					if e = s.db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND wait_event='advisory'`).Scan(&waiting); e != nil {
						t.Fatal(e)
					}
					if waiting > 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("admission did not serialize with fact writers")
					}
					time.Sleep(10 * time.Millisecond)
				}
				if _, e = tx.Exec(`UPDATE fugue_platform_consumer_instances SET compatibility_capabilities_json='[]'::jsonb WHERE scope_key=$1`, scope); e != nil {
					t.Fatal(e)
				}
				if e = tx.Commit(); e != nil {
					t.Fatal(e)
				}
				select {
				case err = <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("admission stuck")
				}
			} else {
				err = publish()
			}
			after, e := s.ListPlatformReleaseMessages(model.PlatformArtifactKindReleaseSet, scope, time.Time{}, 100)
			if e != nil {
				t.Fatal(e)
			}
			valid := scenario == "supported" || scenario == "failed facts still support recovery" || scenario == "rollback"
			if valid {
				if err != nil || len(after) != len(before)+1 {
					t.Fatal("supported release rejected", err)
				}
			} else {
				if !errors.Is(err, ErrConflict) || !reflect.DeepEqual(before, after) {
					t.Fatal("failed admission changed release ledger", err)
				}
			}
			for _, kind := range []string{model.PlatformArtifactKindReleaseSet, model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindPolicySnapshot} {
				if lkg, err := s.GetPlatformLKG(kind, scope); err != nil || lkg != nil {
					t.Fatal("capability admission claimed a positive LKG", kind, err)
				}
			}
		})
	}
}
