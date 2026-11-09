package edgecontrol

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/trafficbinding"
)

func publicationInputFixture(t *testing.T, bound bool) (*PersistentGroupStore, *fixtureGroupSigner, string, string, time.Time) {
	t.Helper()
	s, group, now, intent := compileInputFixture(t, true)
	if bound {
		digest := "sha256:" + strings.Repeat("a", 64)
		intent.TrafficRelease = &model.TrafficReleaseBinding{Schema: trafficbinding.Schema,
			ReleaseSetID: "release-set", ReleaseSetDigest: digest, ReleaseSetGeneration: "release-set-generation",
			RouteArtifactID: "route-artifact", RouteArtifactDigest: digest, RouteArtifactGeneration: intent.Generation, RouteArtifactSequence: 1,
			ReleaseID: "release-full", ReleaseChannel: "full", FencingToken: 1, ScopeKey: "global",
			IntentDigest: digest, PolicyDigest: digest, InputSnapshotDigest: digest, CompilerVersion: "compiler",
			ProjectionDigest: trafficbinding.ProjectionDigest(intent)}
	}
	batch := compileInputRun(t, s, group, now, intent)
	signer := &fixtureGroupSigner{keys: map[string][]byte{group: bytes.Repeat([]byte{0x73}, 32)}, validFor: time.Hour}
	result, err := (GroupAuthorityPublisher{Store: s, Signer: signer, Now: func() time.Time { return now }}).Publish(t.Context(), batch)
	if err != nil || result.Published != 1 {
		t.Fatal("native publication", result, err)
	}
	state, err := s.readGroupState(s.groupStatePath(group), group)
	if err != nil {
		t.Fatal(err)
	}
	return s, signer, group, originalExportKeys(t, group, state.Published.Bundle.KeyID, signer.keys[group]), now
}

func TestGroupOriginalPublicationBindsNativeSourceWithoutAdmittingClosure(t *testing.T) {
	for _, bound := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy-unbound", true: "traffic-bound"}[bound], func(t *testing.T) {
			s, _, group, keyDir, _ := publicationInputFixture(t, bound)
			path := s.groupStatePath(group)
			state, _ := s.readGroupState(path, group)
			before, _ := os.ReadFile(path)
			out, err := ExportGroupOriginalEvidence(t.Context(), path, group, keyDir, []string{state.Published.Bundle.Generation})
			if err != nil || len(out.Generations[0].PublicationBindings) != 1 {
				t.Fatal("native original was not bound", err)
			}
			b := out.Generations[0].PublicationBindings[0]
			if b.GroupID != group || b.Generation != state.Published.Bundle.Generation || b.PublicationSequence != state.Published.PublicationSequence ||
				b.CandidateSequence != state.Published.CandidateLedgerSequence || b.OriginalInputDigest != digestJSON(state.RetentionInputs[0]) ||
				b.SignedBundleDigest != state.Published.Digest || b.SigningKeyringRevision != 7 || b.DependencyBindingKnown != bound ||
				out.AdmissionAuthenticated || out.DependencyClosure || out.StateContentsValidated {
				t.Fatal("source binding or proof boundary changed", b)
			}
			commitment := b.Commitment
			b.Commitment = ""
			if commitment != digestJSON(b) {
				t.Fatal("binding commitment mismatch")
			}
			if bound {
				if len(b.References) != 4 || !reflect.DeepEqual(b.TrafficRelease, state.RetentionInputs[0].RouteIntent.TrafficRelease) ||
					b.References[0].ID != "release-set" || b.References[1].ID != "route-artifact" ||
					b.References[2].Kind != "release" || b.References[3].Kind != "input" || b.References[3].ExpectedHash != "sha256:"+strings.Repeat("a", 64) || b.References[3].ID != b.References[3].ExpectedHash {
					t.Fatal("signed dependency references changed", b)
				}
				b.TrafficRelease.ReleaseID = "caller-change"
			} else if b.TrafficRelease != nil || len(b.References) != 0 {
				t.Fatal("unbound source invented dependencies")
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(before, after) {
				t.Fatal("read-only verification changed original source")
			}
		})
	}
}

func TestGroupOriginalPublicationRefusesReboundOrReplayedClaims(t *testing.T) {
	s, signer, group, keyDir, _ := publicationInputFixture(t, true)
	state, _ := s.readGroupState(s.groupStatePath(group), group)
	keys, err := loadGroupBundleSigningKeyring(filepath.Join(keyDir, group+".json"), group)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"input", "candidate-sequence", "publication-sequence", "source-generation", "signed-time", "signed-route", "signature", "key-policy", "recovery-epoch"} {
		t.Run(mode, func(t *testing.T) {
			input := cloneGroupCompileInput(state.RetentionInputs[0])
			pub := state.AuthorityLedger[0]
			body := cloneEdgeRouteBundle(state.Published.Bundle)
			policy := keys
			switch mode {
			case "input":
				input.RouteIntent.Routes[0].Hostname = "rebound.example.invalid"
			case "candidate-sequence":
				pub.CandidateLedgerSequence++
			case "publication-sequence":
				pub.Sequence++
			case "source-generation":
				pub.RouteIntentGeneration = "different-original"
			case "signed-time":
				body.GeneratedAt = body.GeneratedAt.Add(time.Second)
			case "signed-route":
				body.Routes[0].Hostname = "rebound.example.invalid"
				body, err = signer.SignGroupBundle(t.Context(), group, body)
				if err != nil {
					t.Fatal(err)
				}
				pub.PublishedBundleDigest = signedGroupBundleDigest(body)
			case "signature":
				body.Signature = "forged-signature"
				pub.PublishedBundleDigest = signedGroupBundleDigest(body)
			case "key-policy":
				policy.RevokedKeyIDs = []string{body.KeyID}
			case "recovery-epoch":
				pub.RecoveryEpoch = 1
			}
			if proof, err := bindGroupOriginalPublication(group, input, pub, body, 0, policy); err == nil || !reflect.DeepEqual(proof, GroupOriginalPublicationBinding{}) {
				t.Fatal("altered claim acquired a binding", mode)
			}
		})
	}
}

func TestGroupOriginalPublicationSecondaryCannotRescueRevokedIssuance(t *testing.T) {
	s, _, group, _, now := publicationInputFixture(t, false)
	state, _ := s.readGroupState(s.groupStatePath(group), group)
	body := state.Published.Bundle
	oldKey, newKey := strings.Repeat("o", 32), strings.Repeat("n", 32)
	body = bundleauth.SignEdgeRouteBundleWithKeyring(body, bundleauth.NewKeyring(oldKey, "old-key", newKey, "new-key", nil), time.Hour)
	current := bundleauth.NewKeyring(newKey, "new-key", oldKey, "old-key", []string{"old-key"})
	if err := bundleauth.VerifyEdgeRouteBundleWithKeyring(body, current, now); err != nil {
		t.Fatal("fixture lacks an independently valid secondary signature", err)
	}
	policy := groupBundleSigningKey{GroupID: group, generation: 9, PrimaryKeyID: "new-key", PrimaryKey: base64.RawURLEncoding.EncodeToString([]byte(newKey)),
		PreviousKeyID: "old-key", PreviousKey: base64.RawURLEncoding.EncodeToString([]byte(oldKey)), RevokedKeyIDs: []string{"old-key"}}
	if verifyGroupOriginalIssuance(group, body, policy) {
		t.Fatal("new secondary signature authenticated revoked original issuance")
	}
	policy.RevokedKeyIDs = nil
	if !verifyGroupOriginalIssuance(group, body, policy) {
		t.Fatal("still-trusted previous original signature was rejected")
	}
	if verifyGroupOriginalIssuance(group, body, groupBundleSigningKey{}) {
		t.Fatal("empty verifier admitted signature")
	}
}

func TestGroupOriginalPublicationInheritsRecoveryEpochAfterNativeRefresh(t *testing.T) {
	s, signer, group, keyDir, now := publicationInputFixture(t, false)
	if _, err := (GroupAuthorityPublisher{Store: s, Signer: signer}).RefreshPublishedLKG(t.Context(), group, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	state, _ := s.readGroupState(s.groupStatePath(group), group)
	inventory := *state.Inventory
	inventory.Sequence++
	inventory.Generation = "inventory-after-refresh"
	inventory.ObservedAt = now.Add(time.Hour + time.Second)
	if err := s.StoreGroupInventoryCAS(t.Context(), group, inventory.Sequence-1, inventory); err != nil {
		t.Fatal(err)
	}
	intent := routeIntentFixture()
	intent.Generation = "intent-after-refresh"
	batch := compileInputRun(t, s, group, inventory.ObservedAt, intent)
	if result, err := (GroupAuthorityPublisher{Store: s, Signer: signer, Now: func() time.Time { return inventory.ObservedAt }}).Publish(t.Context(), batch); err != nil || result.Published != 1 {
		t.Fatal(result, err)
	}
	state, _ = s.readGroupState(s.groupStatePath(group), group)
	last := state.AuthorityLedger[len(state.AuthorityLedger)-1]
	if last.RecoveryEpoch != 0 || state.Published.RecoveryEpoch != 1 {
		t.Fatal("fixture did not exercise inherited epoch")
	}
	out, err := ExportGroupOriginalEvidence(t.Context(), s.groupStatePath(group), group, keyDir, []string{state.Published.Bundle.Generation})
	if err != nil || len(out.Generations[0].PublicationBindings) != 1 || out.Generations[0].PublicationBindings[0].RecoveryEpoch != 1 || out.AdmissionAuthenticated || out.DependencyClosure {
		t.Fatal("normal publication lost recovery epoch or bypassed admission", err)
	}
}
