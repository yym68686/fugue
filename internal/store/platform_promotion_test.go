package store

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

type promotionFixture struct {
	parent    model.PlatformArtifact
	release   model.PlatformArtifactRelease
	sets      []model.PlatformExpectedConsumerSet
	consumers []model.PlatformConsumerInstance
}

func preparePromotionFixture(t *testing.T, s *Store, scope string) promotionFixture {
	t.Helper()
	now := time.Now().UTC()
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: scope, Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}}, Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: scope, TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "test", EdgeGroupIDs: []string{"edge-group-a"}}}}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now}})
	if err != nil {
		t.Fatal(err)
	}
	children := []model.PlatformArtifact{}
	for _, child := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.DNSArtifact, compiled.TLSArtifact} {
		stored, err := s.CreatePlatformArtifact(child)
		if err != nil {
			t.Fatal(err)
		}
		stored, err = s.ValidatePlatformArtifact(stored.ID, []model.PlatformArtifactValidationResult{{Name: "compiler", Pass: true, Severity: model.RobustnessSeverityInfo}})
		if err != nil {
			t.Fatal(err)
		}
		children = append(children, stored)
	}
	parent := platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, []string{children[0].ID, children[1].ID, children[2].ID}, now)
	parent, err = s.CreatePlatformArtifact(parent)
	if err != nil {
		t.Fatal(err)
	}
	parent, err = s.ValidatePlatformArtifact(parent.ID, []model.PlatformArtifactValidationResult{{Name: "compiler", Pass: true, Severity: model.RobustnessSeverityInfo}})
	if err != nil {
		t.Fatal(err)
	}
	seedVerifiedPlatformLKG(t, s, parent)
	_, release, found, err := s.GetActivePlatformArtifact(parent.ArtifactKind, parent.ScopeKey, "shadow")
	if err != nil || !found {
		t.Fatal("fixture has no shadow publication", err)
	}
	fixture := promotionFixture{parent: parent, release: release}
	topology := platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a"}}, DNSNodes: []model.DNSNode{{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test"}}}
	for childIndex, child := range children {
		set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: parent.ID, ArtifactReleaseID: release.ID, ArtifactKind: child.ArtifactKind, Scope: child.Scope, ScopeKey: child.ScopeKey, Generation: child.Generation, Revision: int64(childIndex + 1), PreparedAt: now, Topology: topology})
		if err != nil {
			t.Fatal(err)
		}
		set, err = s.CreatePlatformExpectedConsumerSet(set)
		if err != nil {
			t.Fatal(err)
		}
		fixture.sets = append(fixture.sets, set)
		for _, consumer := range set.Consumers {
			claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "credential", TokenID: "token", Component: consumer.Component, NodeID: consumer.NodeID, ScopeKey: scope, ArtifactKinds: []string{child.ArtifactKind}}
			identityKeys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "key", Keys: map[string]string{"key": "synthetic-component-signing-secret"}}
			token, err := platformcontrol.IssuePlatformComponentIdentity(identityKeys, claims, now.Add(-time.Second), 5*time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			claims, err = platformcontrol.ParsePlatformComponentIdentity(identityKeys, token, now)
			if err != nil {
				t.Fatal(err)
			}
			heartbeat := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: consumer.ConsumerID, Component: consumer.Component, NodeID: consumer.NodeID, ArtifactKind: child.ArtifactKind, ScopeKey: scope, ReleaseSetID: parent.ID, ExpectedConsumerSetID: set.ID, FencingToken: release.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: 1, IssuedAt: time.Now().UTC(), Nonce: "0123456789abcdef0123456789abcdef", GenerationSequence: child.GenerationSequence, DesiredGeneration: child.Generation, ActualGeneration: child.Generation, LKGGeneration: child.Generation, ApplyStatus: "applied", ProbeStatus: "passed"}
			heartbeat.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
			if err != nil {
				t.Fatal(err)
			}
			fact, err := s.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, heartbeat, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{})
			if err != nil {
				t.Fatal(err)
			}
			fixture.consumers = append(fixture.consumers, fact)
		}
	}
	return fixture
}

func TestFullReleaseSetRechecksStoredFactsAndPreservesLedgerOnFailure(t *testing.T) {
	for _, scenario := range []string{"complete", "missing member", "failed probe", "expired", "unverified", "wrong fence", "changed topology", "new publication", "soft override"} {
		t.Run(scenario, func(t *testing.T) {
			s := New(t.TempDir() + "/state.json")
			configureTestPlatformArtifactSigning(s)
			if err := s.Init(); err != nil {
				t.Fatal(err)
			}
			f := preparePromotionFixture(t, s, "global")
			if err := s.withLockedState(false, func(state *model.State) error {
				return validateFullReleaseSetInState(state, f.parent, s.platformArtifactSigningKeyring(), time.Now().UTC())
			}); err != nil {
				t.Fatal("initial complete facts rejected", err)
			}
			// Simulate changes after the API preflight, before store publication.
			if scenario == "new publication" {
				if _, _, _, _, err := s.ReleasePlatformArtifact(f.parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=test", IdempotencyKey: "new-gray"}, testPlatformPrincipal()); err != nil {
					t.Fatal(err)
				}
			} else if scenario == "changed topology" {
				changed := clonePlatformExpectedConsumerSet(f.sets[0])
				changed.ID += "-new"
				changed.Revision += 10
				changed.TopologyRevision += "-new"
				if _, err := s.CreatePlatformExpectedConsumerSet(changed); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := s.withLockedState(true, func(state *model.State) error {
					switch scenario {
					case "missing member":
						state.ExpectedConsumerSets = state.ExpectedConsumerSets[:2]
					case "failed probe", "soft override":
						state.PlatformConsumerInstances[0].ProbeStatus = "failed"
					case "expired":
						state.PlatformConsumerInstances[0].LastHeartbeatAt = time.Now().Add(-time.Hour)
					case "unverified":
						state.PlatformConsumerInstances[0].IdentityVerified = false
					case "wrong fence":
						state.PlatformConsumerInstances[0].FencingToken++
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			var releases []model.PlatformArtifactRelease
			var messages []model.PlatformReleaseMessage
			var lanes []model.PlatformReleaseLane
			if err := s.withLockedState(false, func(state *model.State) error {
				releases = append(releases, state.PlatformArtifactReleases...)
				messages = append(messages, state.PlatformReleaseMessages...)
				lanes = append(lanes, state.PlatformReleaseLanes...)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			lkgBefore, err := s.GetPlatformLKG(f.parent.ArtifactKind, f.parent.ScopeKey)
			if err != nil {
				t.Fatal(err)
			}
			request := model.PlatformArtifactReleaseRequest{ReleaseChannel: "full", IdempotencyKey: "full", Reason: "complete traffic promotion"}
			if scenario == "soft override" {
				request.SoftOverride = true
			}
			principal := testPlatformPrincipal()
			if scenario == "soft override" {
				principal = testPlatformSoftOverridePrincipal()
			}
			_, release, _, _, err := s.ReleasePlatformArtifact(f.parent.ID, request, principal)
			if scenario == "complete" {
				if err != nil || release.ReleaseChannel != "full" {
					t.Fatal("complete publication rejected", err)
				}
				_, retry, _, _, err := s.ReleasePlatformArtifact(f.parent.ID, request, testPlatformPrincipal())
				if err != nil || retry.ID != release.ID {
					t.Fatal("idempotent publication changed", err)
				}
			} else {
				if !errors.Is(err, ErrConflict) {
					t.Fatal("stale preflight authorized publication", err)
				}
				if err := s.withLockedState(false, func(state *model.State) error {
					if !reflect.DeepEqual(releases, state.PlatformArtifactReleases) || !reflect.DeepEqual(messages, state.PlatformReleaseMessages) || !reflect.DeepEqual(lanes, state.PlatformReleaseLanes) {
						return fmt.Errorf("failed promotion modified ledger or lane")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			lkgAfter, err := s.GetPlatformLKG(f.parent.ArtifactKind, f.parent.ScopeKey)
			if err != nil || !reflect.DeepEqual(lkgBefore, lkgAfter) {
				t.Fatal("publication changed verified LKG", err)
			}
		})
	}
}
