package edgecontrol

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestGroupPublicationOriginalSurvivesRefreshSupersessionAndColdReopen(t *testing.T) {
	s, signer, group, keys, now := publicationInputFixture(t, false)
	path := s.groupStatePath(group)
	state, err := s.readGroupState(path, group)
	if err != nil || state.PublicationOriginalsFromSequence != 1 || len(state.PublicationOriginals) != 1 {
		t.Fatal("native publication did not retain its original issuance", err)
	}
	original := cloneGroupPublishedBundle(*state.Published)
	publisher := GroupAuthorityPublisher{Store: s, Signer: signer}
	if _, err := publisher.RefreshPublishedLKG(t.Context(), group, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	state, err = s.readGroupState(path, group)
	if err != nil || len(state.PublicationOriginals) != 2 || !sameGroupCompileValue(state.PublicationOriginals[0], original) {
		t.Fatal("refresh overwrote original signature", err)
	}
	// Supersede both the generation and current pointer; compaction is free to
	// remove redundant candidates, never an exact captured publication body.
	inventory := cloneGroupInventorySnapshot(*state.Inventory)
	inventory.Sequence++
	inventory.Generation = "inventory-successor"
	inventory.ObservedAt = now.Add(2 * time.Hour)
	if err := s.StoreGroupInventoryCAS(t.Context(), group, inventory.Sequence-1, inventory); err != nil {
		t.Fatal(err)
	}
	intent := state.RetentionInputs[0].RouteIntent
	intent.Generation = "intent-successor"
	intent.TrafficRelease = nil
	batch := compileInputRun(t, s, group, inventory.ObservedAt, intent)
	if batch.Succeeded != 1 {
		t.Fatal("superseding compile", batch)
	}
	if _, err := s.readGroupState(path, group); err != nil {
		t.Fatal("read after superseding compile", err)
	}
	publisher.Now = func() time.Time { return inventory.ObservedAt }
	if out, err := publisher.Publish(t.Context(), batch); err != nil || out.Published != 1 {
		t.Fatal("superseding publication", out, err)
	}
	state, err = s.readGroupState(path, group)
	if err != nil || len(state.PublicationOriginals) != 3 {
		t.Fatal(err)
	}
	state.Candidate, state.CandidateHistory = nil, nil
	state.Ledger[0].Bundle, state.Ledger[0].BundleArchived = nil, true
	state.Revision++
	if err := s.writeGroupState(path, state); err != nil {
		t.Fatal(err)
	}
	// Drop every in-memory cache and reopen with capture's process option OFF.
	cold, err := OpenPersistentGroupStore(s.root)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := cold.readGroupState(path, group)
	if err != nil || !sameGroupCompileValue(restored.PublicationOriginals, state.PublicationOriginals) {
		t.Fatal("cold read lost custody", err)
	}
	copy := clonePersistentGroupState(restored)
	copy.PublicationOriginals[0].Bundle.Routes[0].Hostname = "changed.example.invalid"
	if !sameGroupCompileValue(restored.PublicationOriginals[0], original) {
		t.Fatal("returned clone aliases original")
	}
	before, _ := os.ReadFile(path)
	exported, err := ExportGroupOriginalEvidence(t.Context(), path, group, keys, []string{original.Bundle.Generation})
	if err != nil || len(exported.Generations[0].PublicationBindings) != 2 {
		t.Fatal("exact historical issuance no longer reproducible", err)
	}
	if !reflect.DeepEqual(exported.Generations[0].SignedBodies[0].Bundle, original.Bundle) ||
		exported.Generations[0].SignedBodies[0].Source != "publication_originals" || exported.AdmissionAuthenticated || exported.DependencyClosure {
		t.Fatal("export rebound original or claimed admission")
	}
	if result, err := (GroupAuthorityPublisher{Store: cold, Signer: signer}).RefreshPublishedLKG(t.Context(), group, inventory.ObservedAt.Add(time.Hour)); err == nil && result.Status != GroupAuthorityStatusFailed {
		t.Fatal("reopen without trusted capture policy bypassed durable requirement")
	}
	after, _ := os.ReadFile(path)
	afterState, readErr := cold.readGroupState(path, group)
	if readErr != nil || bytes.Equal(before, after) || !sameGroupCompileValue(restored.PublicationOriginals, afterState.PublicationOriginals) ||
		!sameGroupCompileValue(restored.Published, afterState.Published) || afterState.AuthorityLedger[len(afterState.AuthorityLedger)-1].FailureCode != GroupAuthorityFailurePendingRetention {
		t.Fatal("refused refresh lost originals/LKG or mandatory audit", readErr)
	}
	// Current trust still controls authentication of an intact old original.
	keyPath := filepath.Join(keys, group+".json")
	var policy groupBundleSigningKeyringFile
	raw, _ := os.ReadFile(keyPath)
	if json.Unmarshal(raw, &policy) != nil {
		t.Fatal("key policy")
	}
	policy.Generation++
	policy.Group.PreviousKeyID, policy.Group.PreviousKey = policy.Group.PrimaryKeyID, policy.Group.PrimaryKey
	policy.Group.RevokedKeyIDs = []string{policy.Group.PreviousKeyID}
	policy.Group.PrimaryKeyID = "successor-key"
	policy.Group.PrimaryKey = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x27}, 32))
	raw, _ = json.Marshal(policy)
	if err := os.WriteFile(keyPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	exported, err = ExportGroupOriginalEvidence(t.Context(), path, group, keys, []string{original.Bundle.Generation})
	if err != nil || len(exported.Generations[0].PublicationBindings) != 0 || exported.Generations[0].SignedBodies[0].SignatureVerified {
		t.Fatal("retained checksum bypassed current revocation", err)
	}
}

func TestGroupPublicationOriginalRefusesChecksummedGapsAndRebinding(t *testing.T) {
	s, signer, group, _, now := publicationInputFixture(t, true)
	if _, err := (GroupAuthorityPublisher{Store: s, Signer: signer}).RefreshPublishedLKG(t.Context(), group, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	state, err := s.readGroupState(s.groupStatePath(group), group)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"missing", "flag", "duplicate", "leading-gap", "gap", "body", "sequence", "candidate", "recovery", "time", "latest", "shared-bytes"} {
		t.Run(mode, func(t *testing.T) {
			bad := clonePersistentGroupState(state)
			switch mode {
			case "missing":
				bad.PublicationOriginals = nil
			case "flag":
				bad.PublicationOriginalsFromSequence = 0
			case "leading-gap":
				bad.PublicationOriginals = bad.PublicationOriginals[1:]
			case "duplicate":
				bad.PublicationOriginals = append(bad.PublicationOriginals, bad.PublicationOriginals[1])
			case "gap":
				bad.PublicationOriginals = bad.PublicationOriginals[:1]
			case "body":
				bad.PublicationOriginals[0].Bundle.Routes[0].Hostname = "different.example.invalid"
			case "sequence":
				bad.PublicationOriginals[0].PublicationSequence++
			case "candidate":
				bad.PublicationOriginals[0].CandidateLedgerSequence++
			case "recovery":
				bad.PublicationOriginals[0].RecoveryEpoch++
			case "time":
				bad.PublicationOriginals[0].PublishedAt = now.Add(time.Second)
			case "latest":
				bad.Published = &bad.PublicationOriginals[0]
			case "shared-bytes":
				// All inputs and signed outputs draw from ONE 32MiB bound.
				input := cloneGroupCompileInput(bad.RetentionInputs[0])
				input.RouteIntent.Routes[0].OriginStatusReason = string(bytes.Repeat([]byte{'x'}, 7<<20))
				bad.RetentionInputs = []GroupCompileInput{input, input, input, input, input}
			}
			bad.Digest = persistentGroupStateDigest(bad)
			if validateGroupPublicationOriginals(bad) == nil {
				t.Fatal("rechecksummed missing/rebound issuance accepted")
			}
		})
	}
}

func TestGroupPublicationOriginalBoundRefusesWholeCASWithoutPruning(t *testing.T) {
	s, signer, group, _, now := publicationInputFixture(t, true)
	publisher := GroupAuthorityPublisher{Store: s, Signer: signer}
	for i := 1; i < 32; i++ {
		if _, err := publisher.RefreshPublishedLKG(t.Context(), group, now.Add(time.Duration(i)*time.Hour)); err != nil {
			t.Fatal("capture before bound", i, err)
		}
	}
	path := s.groupStatePath(group)
	before, err := s.readGroupState(path, group)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := publisher.RefreshPublishedLKG(t.Context(), group, now.Add(32*time.Hour)); err == nil && result.Status != GroupAuthorityStatusFailed {
		t.Fatal("publication exceeded custody capacity")
	}
	state, err := s.readGroupState(path, group)
	if err != nil || !sameGroupCompileValue(before.PublicationOriginals, state.PublicationOriginals) || !sameGroupCompileValue(before.Published, state.Published) ||
		len(state.PublicationOriginals) != 32 || len(state.AuthorityLedger) != 33 || state.AuthorityLedger[32].FailureCode != GroupAuthorityFailurePendingRetention {
		t.Fatal("bound refusal pruned originals or advanced publication", err)
	}
}

func TestGroupPublicationOriginalDoesNotBackfillLegacyIssuance(t *testing.T) {
	s, signer, group, now := originalExportFixture(t)
	old, err := s.readGroupState(s.groupStatePath(group), group)
	if err != nil || old.PublicationOriginalsFromSequence != 0 || len(old.PublicationOriginals) != 0 {
		t.Fatal("default capture changed legacy format", err)
	}
	upgraded, err := OpenPersistentGroupStoreWithOptions(s.root, PersistentGroupStoreOptions{RetainCompileInputs: true, CompileInputKeyringDir: compileInputTestKeys(t, group)})
	if err != nil {
		t.Fatal(err)
	}
	// Merely reopening and refreshing legacy state never invents an original.
	if _, err := (GroupAuthorityPublisher{Store: upgraded, Signer: signer}).RefreshPublishedLKG(t.Context(), group, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	state, err := upgraded.readGroupState(s.groupStatePath(group), group)
	if err != nil || state.PublicationOriginalsFromSequence != 0 || len(state.PublicationOriginals) != 0 {
		t.Fatal("legacy issuance was backfilled", err)
	}
}
