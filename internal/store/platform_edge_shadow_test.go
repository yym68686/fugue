package store

import (
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestEdgeShadowObservationIsolation(t *testing.T) {
	testEdgeShadowObservationIsolation(t, false)
}

func TestEdgeShadowPostgresObservationIsolation(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set disposable loopback test database")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	testEdgeShadowObservationIsolation(t, true)
}

func testEdgeShadowObservationIsolation(t *testing.T, postgres bool) {
	s := New(t.TempDir() + "/state.json")
	if postgres {
		s = platformStatePostgresFixture(t)
	} else if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	configureTestPlatformArtifactSigning(s)
	f := prepareTrafficLKGFixture(t, s, "neutral-shadow", "gray", true)
	before := append([]model.PlatformConsumerInstance(nil), f.consumers...)
	lkg := trafficLKGState(t, s, f.parent.ScopeKey)
	_, release, _, _, err := s.ReleasePlatformArtifact(f.parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: "isolated-edge-candidate"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	for _, oldSet := range f.sets {
		if oldSet.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
			continue
		}
		t.Run(oldSet.ArtifactKind, func(t *testing.T) {
			set := clonePlatformExpectedConsumerSet(oldSet)
			set.ID += "-isolated-shadow"
			set.ArtifactReleaseID, set.Revision = release.ID, oldSet.Revision+10
			set, err = s.CreatePlatformExpectedConsumerSet(set)
			if err != nil {
				t.Fatal(err)
			}
			member := set.Consumers[0]
			claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "edge-credential", TokenID: "edge-shadow-token", Component: member.Component, NodeID: member.NodeID, ScopeKey: set.ScopeKey, ArtifactKinds: []string{set.ArtifactKind}}
			ring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "identity", Keys: map[string]string{"identity": "synthetic-shadow-identity"}}
			token, err := platformcontrol.IssuePlatformComponentIdentity(ring, claims, time.Now().Add(-time.Second), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			claims, err = platformcontrol.ParsePlatformComponentIdentity(ring, token, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			var child model.PlatformArtifact
			for _, raw := range f.parent.Content["artifact_ids"].([]any) {
				a, err := s.GetPlatformArtifact(raw.(string))
				if err != nil {
					t.Fatal(err)
				}
				if a.ArtifactKind == set.ArtifactKind {
					child = a
				}
			}
			h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: member.ConsumerID, Component: member.Component, NodeID: member.NodeID, ArtifactKind: set.ArtifactKind, ScopeKey: set.ScopeKey, ReleaseSetID: f.parent.ID, ExpectedConsumerSetID: set.ID, FencingToken: release.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: 1, IssuedAt: time.Now().UTC(), Nonce: model.NewID("nonce"), GenerationSequence: child.GenerationSequence, DesiredGeneration: child.Generation, CandidateGeneration: child.Generation, ApplyStatus: "staged", ProbeStatus: "shadow_validated"}
			h.CompatibilityCapabilities = []string{platformcontrol.TrafficReleaseCapabilityV1}
			seal := func(h *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
				h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(*h)
				if err != nil {
					t.Fatal(err)
				}
			}
			preserve := func() {
				t.Helper()
				for _, old := range before {
					rows, err := s.ListPlatformConsumers(old.ArtifactKind, old.ScopeKey)
					if err != nil || len(rows) != 1 || !reflect.DeepEqual(rows[0], old) {
						t.Fatal("shadow replaced serving cursor or registered generation", err)
					}
				}
				if !reflect.DeepEqual(lkg, trafficLKGState(t, s, f.parent.ScopeKey)) {
					t.Fatal("shadow changed positive LKG")
				}
			}
			for _, mode := range []string{"applied", "actual", "group-lkg", "serving-lkg", "expired-lkg", "wrong-candidate", "wrong-fence", "wrong-sequence", "foreign-node", "foreign-parent", "stale"} {
				bad := h
				switch mode {
				case "applied":
					bad.ApplyStatus, bad.ProbeStatus = "applied", "passed"
				case "actual":
					bad.ActualGeneration = child.Generation
				case "group-lkg":
					bad.LKGGeneration = "enclosing-group-generation"
				case "serving-lkg":
					bad.ServingLKG = true
				case "expired-lkg":
					bad.LKGExpired = true
				case "wrong-candidate":
					bad.CandidateGeneration = "another-member"
				case "wrong-fence":
					bad.FencingToken++
				case "wrong-sequence":
					bad.GenerationSequence++
				case "foreign-node":
					bad.NodeID = "another-node"
				case "foreign-parent":
					bad.ReleaseSetID = "another-parent"
				case "stale":
					bad.IssuedAt = time.Now().Add(-time.Hour)
				}
				seal(&bad)
				if _, err := s.AcceptPlatformShadowConsumerHeartbeat(claims, bad, time.Now().UTC(), s.platformArtifactSigningKeyring()); err == nil {
					t.Fatal("invalid edge shadow accepted", mode)
				}
				preserve()
			}
			seal(&h)
			out, err := s.AcceptPlatformShadowConsumerHeartbeat(claims, h, time.Now().UTC(), s.platformArtifactSigningKeyring())
			if err != nil || out.ActualGeneration != "" || out.LKGGeneration != "" || out.CandidateGeneration != child.Generation {
				t.Fatal("exact isolated member observation rejected", out, err)
			}
			preserve()
			if _, err := s.AcceptPlatformShadowConsumerHeartbeat(claims, h, time.Now().UTC(), s.platformArtifactSigningKeyring()); err == nil {
				t.Fatal("shadow replay accepted")
			}
			preserve()
			rows, err := s.ListPlatformShadowConsumers(set.ArtifactKind, set.ScopeKey)
			if err != nil || len(rows) != 1 || rows[0].Sequence != 1 {
				t.Fatal("shadow latest slot/cursor invalid", err)
			}
			var state *model.State
			if s.db != nil {
				tx, err := s.db.BeginTx(t.Context(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				state, err = s.pgReleaseSetSnapshot(t.Context(), tx, f.parent)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if err := s.withLockedState(false, func(v *model.State) error { state = v; return nil }); err != nil {
					t.Fatal(err)
				}
			}
			facts := trafficCapabilityObservations(state, f.parent, child, set, time.Now().UTC())
			found := false
			for _, v := range facts {
				if v.ExpectedConsumerSetID == set.ID {
					found = true
				}
			}
			if !found {
				t.Fatal("exact shadow unavailable to candidate capability admission")
			}
		})
	}
}
