package store

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/testfixture/celldns"
)

// Called from the signed complete DNS lifecycle fixture in memory and PostgreSQL.
func testShadowObservationIsolation(t *testing.T, s *Store, parent model.PlatformArtifact, release model.PlatformArtifactRelease) func() {
	t.Helper()
	ids := parent.Content["artifact_ids"].([]any)
	child, err := s.GetPlatformArtifact(ids[0].(string))
	if err != nil {
		t.Fatal(err)
	}
	topology, _, err := platformcontrol.DeclaredTrafficConsumerTopology(parent)
	if err != nil {
		t.Fatal(err)
	}
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: parent.ID, ArtifactReleaseID: release.ID, ArtifactKind: child.ArtifactKind, Scope: parent.Scope, ScopeKey: parent.ScopeKey, Generation: child.Generation, Revision: 1, PreparedAt: time.Now().UTC(), Topology: topology})
	if err != nil {
		t.Fatal(err)
	}
	set, err = s.CreatePlatformExpectedConsumerSet(set)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.ListPlatformConsumers(child.ArtifactKind, parent.ScopeKey)
	if err != nil || len(before) != 1 {
		t.Fatal("missing real serving baseline", err)
	}
	lkg, err := s.GetPlatformLKG(parent.ArtifactKind, parent.ScopeKey)
	if err != nil {
		t.Fatal(err)
	}
	member := set.Consumers[0]
	claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: before[0].CredentialID, TokenID: "isolated-shadow-token", Component: member.Component, NodeID: member.NodeID, AuthorityID: member.AuthorityID, ScopeKey: parent.ScopeKey, ArtifactKinds: []string{child.ArtifactKind}, IssuedAtUnix: time.Now().Add(-time.Minute).Unix(), ExpiresAtUnix: time.Now().Add(time.Hour).Unix()}
	ring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "identity", Keys: map[string]string{"identity": "synthetic-shadow-identity"}}
	token, e := platformcontrol.IssuePlatformComponentIdentity(ring, claims, time.Now().Add(-time.Second), time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	claims, e = platformcontrol.ParsePlatformComponentIdentity(ring, token, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: member.ConsumerID, Component: member.Component, NodeID: member.NodeID, ArtifactKind: child.ArtifactKind, ScopeKey: parent.ScopeKey, ReleaseSetID: parent.ID, ExpectedConsumerSetID: set.ID, FencingToken: release.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: 1, IssuedAt: time.Now().UTC(), Nonce: model.NewID("nonce"), GenerationSequence: child.GenerationSequence, DesiredGeneration: child.Generation, CandidateGeneration: child.Generation, CompatibilityCapabilities: before[0].CompatibilityCapabilities, ApplyStatus: "staged", ProbeStatus: "shadow_validated"}
	seal := func(h *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
		h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(*h)
		if err != nil {
			t.Fatal(err)
		}
	}
	preserve := func() {
		t.Helper()
		current, e := s.ListPlatformConsumers(child.ArtifactKind, parent.ScopeKey)
		if e != nil || !reflect.DeepEqual(before, current) {
			t.Fatal("shadow changed serving observations", e)
		}
		currentLKG, e := s.GetPlatformLKG(parent.ArtifactKind, parent.ScopeKey)
		if e != nil || !reflect.DeepEqual(lkg, currentLKG) {
			t.Fatal("shadow changed positive LKG", e)
		}
	}
	for _, mode := range []string{"serving", "wrong-fence", "wrong-generation", "wrong-set", "foreign-node", "stale", "tampered-evidence"} {
		bad := h
		switch mode {
		case "serving":
			bad.ApplyStatus = "applied"
			bad.ProbeStatus = "passed"
		case "wrong-fence":
			bad.FencingToken++
		case "wrong-generation":
			bad.GenerationSequence++
		case "wrong-set":
			bad.ExpectedConsumerSetID = "foreign"
		case "foreign-node":
			bad.NodeID = "other"
		case "stale":
			bad.IssuedAt = time.Now().Add(-time.Hour)
		}
		seal(&bad)
		if mode == "tampered-evidence" {
			bad.EvidenceHash = "sha256:bad"
		}
		if _, e := s.AcceptPlatformShadowConsumerHeartbeat(claims, bad, time.Now().UTC(), celldns.Keys()); e == nil {
			t.Fatal("invalid shadow accepted", mode)
		}
		preserve()
	}
	seal(&h)
	if _, e := s.AcceptPlatformShadowConsumerHeartbeat(claims, h, time.Now().UTC(), bundleauth.Keyring{}); e == nil {
		t.Fatal("shadow committed without signed audit")
	}
	empty, e := s.ListPlatformShadowConsumers(child.ArtifactKind, parent.ScopeKey)
	if e != nil || len(empty) != 0 {
		t.Fatal("failed audit left shadow state", e)
	}
	preserve()
	if _, err = s.AcceptPlatformShadowConsumerHeartbeat(claims, h, time.Now().UTC(), celldns.Keys()); err != nil {
		t.Fatal("valid independent observation rejected", err)
	}
	preserve()
	if _, err = s.AcceptPlatformShadowConsumerHeartbeat(claims, h, time.Now().UTC(), celldns.Keys()); err == nil {
		t.Fatal("shadow replay accepted")
	}
	h.Sequence++
	h.Nonce = model.NewID("nonce")
	h.IssuedAt = time.Now().UTC()
	seal(&h)
	var wg sync.WaitGroup
	failures := make([]error, 2)
	for i := range failures {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, failures[i] = s.AcceptPlatformShadowConsumerHeartbeat(claims, h, time.Now().UTC(), celldns.Keys())
		}(i)
	}
	wg.Wait()
	if (failures[0] == nil) == (failures[1] == nil) {
		t.Fatal("concurrent duplicate shadow did not have exactly one winner", failures)
	}
	preserve()
	shadow, err := s.ListPlatformShadowConsumers(child.ArtifactKind, parent.ScopeKey)
	if err != nil || len(shadow) != 1 || shadow[0].Sequence != h.Sequence || shadow[0].ID == before[0].ID {
		t.Fatal("isolated bounded slot invalid", shadow, err)
	}
	// Older serving generation keeps advancing in its own replay domain.
	raw, _ := json.Marshal(before[0])
	var old platformcontrol.PlatformConsumerHeartbeatEnvelope
	if json.Unmarshal(raw, &old) != nil {
		t.Fatal("invalid serving fixture")
	}
	old.Sequence++
	old.Nonce = model.NewID("nonce")
	old.IssuedAt = time.Now().UTC()
	seal(&old)
	h.Sequence++
	h.Nonce = model.NewID("nonce")
	h.IssuedAt = time.Now().UTC()
	seal(&h)
	var concurrent sync.WaitGroup
	results := make([]error, 2)
	concurrent.Add(2)
	go func() {
		defer concurrent.Done()
		_, results[0] = s.AcceptTrustedPlatformConsumerHeartbeat(claims, old.ExpectedConsumerSetID, old, time.Now().UTC(), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{})
	}()
	go func() {
		defer concurrent.Done()
		_, results[1] = s.AcceptPlatformShadowConsumerHeartbeat(claims, h, time.Now().UTC(), celldns.Keys())
	}()
	concurrent.Wait()
	if results[0] != nil || results[1] != nil {
		t.Fatal("shadow and serving could not advance independently", results)
	}
	before, err = s.ListPlatformConsumers(child.ArtifactKind, parent.ScopeKey)
	if err != nil {
		t.Fatal(err)
	}
	preserve()
	// No shadow facts may satisfy serving convergence, even after a capability read.
	if s.db == nil {
		if err = s.withLockedState(false, func(state *model.State) error {
			candidates := trafficCapabilityObservations(state, parent, child, set, time.Now().UTC())
			if len(candidates) != 1 || candidates[0].ExpectedConsumerSetID != set.ID {
				t.Fatal("exact shadow capability missing")
			}
			if !reflect.DeepEqual(state.PlatformConsumerInstances, before) {
				t.Fatal("capability projection mutated serving")
			}
			if e := validateFullReleaseSetInState(state, parent, s.platformArtifactSigningKeyring(), time.Now().UTC()); e == nil {
				t.Fatal("shadow became full convergence")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	return func() {
		h.Sequence++
		h.Nonce = model.NewID("nonce")
		h.IssuedAt = time.Now().UTC()
		seal(&h)
		previous, e := s.ListPlatformShadowConsumers(child.ArtifactKind, parent.ScopeKey)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.AcceptPlatformShadowConsumerHeartbeat(claims, h, time.Now().UTC(), celldns.Keys()); e == nil {
			t.Fatal("superseded shadow observation accepted")
		}
		after, e := s.ListPlatformShadowConsumers(child.ArtifactKind, parent.ScopeKey)
		if e != nil || !reflect.DeepEqual(previous, after) {
			t.Fatal("rejected superseded shadow changed cursor", e)
		}
		preserve()
	}

}
