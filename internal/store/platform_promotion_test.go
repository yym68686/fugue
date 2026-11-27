package store

import (
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
	return prepareTrafficLKGFixture(t, s, scope, "gray", true)
}

func prepareTrafficLKGFixture(t *testing.T, s *Store, scope, channel string, seed bool, inputs ...platformconfig.CompileRequest) promotionFixture {
	t.Helper()
	now := time.Now().UTC()
	requestInput := platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: scope, Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}}, Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: scope, TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "test", EdgeGroupIDs: []string{"edge-group-a"}}}}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now}}
	if len(inputs) > 0 {
		requestInput = inputs[0]
	}
	compiled, err := platformconfig.Compile(requestInput)
	if err != nil {
		t.Fatal(err)
	}
	prepareCompiledInputPin(t, s, compiled)
	policy, err := s.CreatePlatformArtifact(compiled.PolicyArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ValidatePlatformArtifact(policy.ID, []model.PlatformArtifactValidationResult{{Name: "compiler", Pass: true, Severity: model.RobustnessSeverityInfo}}); err != nil {
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
	request := model.PlatformArtifactReleaseRequest{ReleaseChannel: channel, IdempotencyKey: "initial"}
	if channel == "gray" {
		request.CanaryRuleRef = "cohort=test"
	}
	_, release, _, _, err := s.ReleasePlatformArtifact(parent.ID, request, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	fixture := promotionFixture{parent: parent, release: release}
	topology := platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a"}}, DNSNodes: []model.DNSNode{{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test"}}}
	if declared, present, err := platformcontrol.DeclaredTrafficConsumerTopology(parent); err != nil {
		t.Fatal(err)
	} else if present {
		topology = declared
	}
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
			claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "credential", TokenID: "token", Component: consumer.Component, NodeID: consumer.NodeID, AuthorityID: consumer.AuthorityID, ScopeKey: scope, ArtifactKinds: []string{child.ArtifactKind}}
			if consumer.Component == model.PlatformConsumerComponentDNSServer && consumer.AuthorityID != "" {
				claims.CredentialID = "kubernetes:test-system:dns-account:pod-a"
			}
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
	if seed {
		if _, verified, _, _, err := s.VerifyPlatformArtifactReleaseLKG(release.ID, completePlatformVerificationRequest(release.FencingToken, true), testPlatformPrincipal()); err != nil {
			t.Fatal(err)
		} else {
			fixture.release = verified
		}
	}
	return fixture
}
