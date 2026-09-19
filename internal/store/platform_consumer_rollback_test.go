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
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

func TestTrafficConsumerRollback(t *testing.T) { testTrafficConsumerRollback(t, "") }
func TestTrafficConsumerRollbackPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set disposable test database")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testTrafficConsumerRollback(t, address)
}

func testTrafficConsumerRollback(t *testing.T, address string) {
	for _, scenario := range []string{"same lane", "cross lane", "dns member", "tls member", "newer other channel", "replayed sequence", "replayed nonce", "older time", "wrong fence", "wrong generation", "ordinary message", "tampered child", "frozen", "superseded", "new topology", "removed cohort", "queued revocation"} {
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
			scope := "rollback-" + model.NewID("test")
			old := preparePromotionFixture(t, s, scope)
			now := time.Now().UTC()
			compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "new-intent", Scope: scope, Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://new-origin:8080", Enabled: true}}}, Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: scope, TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "test", EdgeGroupIDs: []string{"edge-group-a"}}}}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now}})
			if err != nil {
				t.Fatal(err)
			}
			children := []model.PlatformArtifact{}
			for _, a := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.DNSArtifact, compiled.TLSArtifact} {
				a, err = s.CreatePlatformArtifact(a)
				if err != nil {
					t.Fatal(err)
				}
				a, err = s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "compiler", Pass: true}})
				if err != nil {
					t.Fatal(err)
				}
				children = append(children, a)
			}
			parent, err := s.CreatePlatformArtifact(platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, []string{children[0].ID, children[1].ID, children[2].ID}, now))
			if err != nil {
				t.Fatal(err)
			}
			parent, err = s.ValidatePlatformArtifact(parent.ID, []model.PlatformArtifactValidationResult{{Name: "compiler", Pass: true}})
			if err != nil {
				t.Fatal(err)
			}
			_, release, _, _, err := s.ReleasePlatformArtifact(parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=test"}, testPlatformPrincipal())
			if err != nil {
				t.Fatal(err)
			}
			newer := promotionFixture{parent: parent, release: release}
			for i, child := range children {
				set := clonePlatformExpectedConsumerSet(old.sets[i])
				set.ReleaseSetID = parent.ID
				set.ExpectedGeneration = child.Generation
				for j := range set.Consumers {
					set.Consumers[j].ExpectedGeneration = child.Generation
				}
				newer.sets = append(newer.sets, set)
				consumer := old.consumers[i]
				consumer.GenerationSequence = child.GenerationSequence
				consumer.DesiredGeneration = child.Generation
				consumer.ActualGeneration = child.Generation
				newer.consumers = append(newer.consumers, consumer)
			}
			reportTrafficLKGFixture(t, s, newer)
			channel := "gray"
			if scenario == "cross lane" {
				channel = "full"
			}
			_, rollback, message, _, err := s.RollbackPlatformArtifact(parent.ID, model.PlatformArtifactRollbackRequest{ReleaseChannel: channel, ToGeneration: old.parent.Generation, CanaryRuleRef: "cohort=test", Reason: "recover previous verified artifact"}, testPlatformPrincipal())
			if err != nil {
				t.Fatal(err)
			}
			if message.MessageType != model.PlatformReleaseMessageTypeRollback {
				t.Fatal("fixture lacks explicit rollback")
			}
			memberIndex := 0
			if scenario == "dns member" {
				memberIndex = 1
			}
			if scenario == "tls member" {
				memberIndex = 2
			}
			set := clonePlatformExpectedConsumerSet(old.sets[memberIndex])
			set.ID = model.NewID("rollback-set")
			set.Revision += 100
			set.ArtifactReleaseID = rollback.ID
			if scenario == "removed cohort" {
				for i := range set.Consumers {
					set.Consumers[i].Cohort = "edge-group-outside"
				}
			}
			set, err = s.CreatePlatformExpectedConsumerSet(set)
			if err != nil {
				t.Fatal(err)
			}
			facts, err := s.ListPlatformConsumers(set.ArtifactKind, scope)
			if err != nil || len(facts) != 1 {
				t.Fatal("consumer fixture", err)
			}
			previous := facts[0]
			keys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "key", Keys: map[string]string{"key": "synthetic-rollback-identity"}}
			claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "credential", TokenID: "token", Component: previous.Component, NodeID: previous.NodeID, ScopeKey: scope, ArtifactKinds: []string{previous.ArtifactKind}}
			now = time.Now().UTC()
			token, err := platformcontrol.IssuePlatformComponentIdentity(keys, claims, now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			claims, err = platformcontrol.ParsePlatformComponentIdentity(keys, token, now)
			if err != nil {
				t.Fatal(err)
			}
			h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: previous.ConsumerID, Component: previous.Component, NodeID: previous.NodeID, ArtifactKind: previous.ArtifactKind, ScopeKey: scope, ReleaseSetID: old.parent.ID, ExpectedConsumerSetID: set.ID, FencingToken: rollback.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: previous.Sequence + 1, IssuedAt: now, Nonce: fmt.Sprintf("%032d", now.UnixNano()), GenerationSequence: old.consumers[memberIndex].GenerationSequence, DesiredGeneration: set.ExpectedGeneration, ActualGeneration: set.ExpectedGeneration, LKGGeneration: set.ExpectedGeneration, ApplyStatus: "applied", ProbeStatus: "passed"}
			switch scenario {
			case "replayed sequence":
				h.Sequence = previous.Sequence
			case "replayed nonce":
				h.Nonce = previous.Nonce
			case "older time":
				h.IssuedAt = previous.IssuedAt.Add(-time.Second)
			case "wrong fence":
				h.FencingToken++
			case "wrong generation":
				h.GenerationSequence = 0
			}
			h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "new topology" {
				changed := clonePlatformExpectedConsumerSet(set)
				changed.ID += "-new"
				changed.Revision++
				changed.TopologyRevision += "-new"
				if _, err = s.CreatePlatformExpectedConsumerSet(changed); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "superseded" {
				if _, _, _, _, err = s.RollbackPlatformArtifact(parent.ID, model.PlatformArtifactRollbackRequest{ReleaseChannel: channel, ToGeneration: old.parent.Generation, CanaryRuleRef: "cohort=test", Reason: "newer rollback authority"}, testPlatformPrincipal()); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "newer other channel" {
				if _, _, _, _, err = s.RollbackPlatformArtifact(parent.ID, model.PlatformArtifactRollbackRequest{ReleaseChannel: "full", ToGeneration: parent.Generation, Reason: "newer full authority supersedes earlier gray"}, testPlatformPrincipal()); err != nil {
					t.Fatal(err)
				}
			}
			if address == "" {
				if err = s.withLockedState(true, func(state *model.State) error {
					switch scenario {
					case "ordinary message":
						for i := range state.PlatformReleaseMessages {
							if state.PlatformReleaseMessages[i].ReleaseID == rollback.ID {
								state.PlatformReleaseMessages[i].MessageType = model.PlatformReleaseMessageTypeRelease
							}
						}
					case "tampered child":
						for i := range state.PlatformArtifacts {
							if state.PlatformArtifacts[i].Generation == h.DesiredGeneration {
								state.PlatformArtifacts[i].Content["tampered"] = true
							}
						}
					case "frozen":
						for i := range state.PlatformReleaseLanes {
							if state.PlatformReleaseLanes[i].LaneKey == rollback.LaneKey {
								state.PlatformReleaseLanes[i].Frozen = true
							}
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				switch scenario {
				case "ordinary message":
					_, err = s.db.Exec(`UPDATE fugue_platform_release_messages SET message_type='release' WHERE release_id=$1`, rollback.ID)
				case "tampered child":
					_, err = s.db.Exec(`UPDATE fugue_platform_artifacts SET content_json=content_json||'{"tampered":true}'::jsonb WHERE scope_key=$1 AND generation=$2`, scope, h.DesiredGeneration)
				case "frozen":
					_, err = s.db.Exec(`UPDATE fugue_platform_release_lanes SET frozen=true WHERE lane_key=$1`, rollback.LaneKey)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			accept := func() error {
				_, err := s.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, h, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{})
				return err
			}
			if scenario == "queued revocation" {
				tx, txerr := s.db.BeginTx(context.Background(), nil)
				if txerr != nil {
					t.Fatal(txerr)
				}
				defer tx.Rollback()
				if err := pgLockPromotionScope(context.Background(), tx, scope, false); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- accept() }()
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
						t.Fatal("rollback failed to serialize")
					}
					time.Sleep(10 * time.Millisecond)
				}
				if _, err := tx.Exec(`UPDATE fugue_platform_release_lanes SET frozen=true WHERE lane_key=$1`, rollback.LaneKey); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				select {
				case err = <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("rollback stuck")
				}
			} else {
				err = accept()
			}
			after, readErr := s.ListPlatformConsumers(set.ArtifactKind, scope)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if scenario != "same lane" && scenario != "cross lane" && scenario != "dns member" && scenario != "tls member" {
				if err == nil || !reflect.DeepEqual(facts, after) {
					t.Fatal("rejected rollback mutated consumer fact", err)
				}
				if strings.HasPrefix(scenario, "replayed") || scenario == "older time" {
					if !errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatReplay) {
						t.Fatal("rollback weakened replay validation", err)
					}
				}
				return
			}
			if err != nil || len(after) != 1 || after[0].GenerationSequence != h.GenerationSequence || after[0].Sequence != h.Sequence || after[0].FencingToken != h.FencingToken || after[0].ExpectedConsumerSetID != set.ID {
				t.Fatal("authorized rollback did not converge", err)
			}
			if !errors.Is(accept(), platformcontrol.ErrPlatformConsumerHeartbeatReplay) {
				t.Fatal("replayed rollback accepted")
			}
			h.Sequence++
			h.IssuedAt = time.Now().UTC()
			h.Nonce = fmt.Sprintf("%032d", h.IssuedAt.UnixNano())
			h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
			if err := accept(); err != nil {
				t.Fatal("rollback could not renew its heartbeat", err)
			}
		})
	}
}
