package edgecontrol

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGroupCompileInputAttestationNativeCASAndCurrentTrust(t *testing.T) {
	s, _, group, keyDir, _ := publicationInputFixture(t, true)
	path := s.groupStatePath(group)
	state, err := s.readGroupState(path, group)
	if err != nil || !state.CompileInputAttestationsRequired {
		t.Fatal("native CAS omitted durable attestation requirement", err)
	}
	keys, err := loadGroupBundleSigningKeyring(filepath.Join(keyDir, group+".json"), group)
	if err != nil {
		t.Fatal(err)
	}
	input := state.RetentionInputs[0]
	if input.Attestation == nil || !verifyGroupCompileInputAttestation(input, keys) || input.Attestation.InputDigest == digestJSON(input) {
		t.Fatal("original input was not independently authenticated")
	}
	// Revocation must block admission without making protected history unreadable.
	reopened, err := OpenPersistentGroupStore(s.root)
	if err != nil {
		t.Fatal(err)
	}
	read, err := reopened.readGroupState(path, group)
	if err != nil || !sameGroupCompileValue(read.RetentionInputs[0], input) {
		t.Fatal("restart changed protected original", err)
	}
	rotated := groupBundleSigningKey{GroupID: group, generation: 8,
		PrimaryKeyID: "next-key", PrimaryKey: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x55}, 32)),
		PreviousKeyID: keys.PrimaryKeyID, PreviousKey: keys.PrimaryKey}
	if !verifyGroupCompileInputAttestation(input, rotated) {
		t.Fatal("still trusted original key lost after rotation")
	}
	rotated.RevokedKeyIDs = []string{keys.PrimaryKeyID}
	if verifyGroupCompileInputAttestation(input, rotated) {
		t.Fatal("revoked original input key remained trusted")
	}
	for _, mode := range []string{"missing", "group", "sequence", "time", "revision", "key", "signature", "future-policy"} {
		t.Run(mode, func(t *testing.T) {
			bad := cloneGroupCompileInput(input)
			policy := keys
			switch mode {
			case "missing":
				bad.Attestation = nil
			case "group":
				bad.Attestation.GroupID = "other-group"
			case "sequence":
				bad.Attestation.Sequence++
			case "time":
				bad.Attestation.CapturedAt = bad.Attestation.CapturedAt.Add(time.Nanosecond)
			case "revision":
				bad.Attestation.KeyringRevision++
			case "key":
				bad.Attestation.KeyID = "other-key"
			case "signature":
				bad.Attestation.Signature = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0}, 32))
			case "future-policy":
				policy.generation--
			}
			if verifyGroupCompileInputAttestation(bad, policy) {
				t.Fatal("changed original authenticated", mode)
			}
		})
	}
}

func TestGroupCompileInputAttestationRejectsRechecksummedSemanticEquivalent(t *testing.T) {
	s, _, group, keyDir, _ := publicationInputFixture(t, true)
	path := s.groupStatePath(group)
	state, _ := s.readGroupState(path, group)
	bad := cloneGroupCompileInput(state.RetentionInputs[0])
	bad.Inventory.ObservedAt = bad.Inventory.ObservedAt.Add(-time.Nanosecond)
	unsigned := cloneGroupCompileInput(bad)
	unsigned.Attestation = nil
	if validateGroupCompileInput(unsigned, group) != nil {
		t.Fatal("fixture must retain native semantic compilation and freshness")
	}
	// Recompute ordinary checksums, but not the original HMAC.
	bad.Attestation.InputDigest = groupCompileInputSourceDigest(bad)
	if validateGroupCompileInput(bad, group) != nil {
		t.Fatal("fixture must pass structural replay checks")
	}
	state.RetentionInputs[0] = bad
	if err := s.writeGroupState(path, state); err != nil {
		t.Fatal(err)
	}
	out, err := ExportGroupOriginalEvidence(t.Context(), path, group, keyDir, []string{state.Published.Bundle.Generation})
	if err != nil || len(out.Generations[0].CompileInputs) != 1 || len(out.Generations[0].PublicationBindings) != 0 || out.AdmissionAuthenticated {
		t.Fatal("rechecksummed original acquired signed publication provenance", err)
	}
}

func TestGroupCompileInputAttestationLegacyPrefixNeverBackfilled(t *testing.T) {
	s, group, now, intent := compileInputFixture(t, true)
	compileInputRun(t, s, group, now, intent)
	path := s.groupStatePath(group)
	state, _ := s.readGroupState(path, group)
	state.RetentionInputs[0].Attestation = nil
	state.CompileInputAttestationsRequired = false
	if err := s.writeGroupState(path, state); err != nil {
		t.Fatal(err)
	}
	legacy := cloneGroupCompileInput(state.RetentionInputs[0])
	reopened, err := OpenPersistentGroupStoreWithOptions(s.root, PersistentGroupStoreOptions{CompileInputKeyringDir: s.compileInputKeyringDir})
	if err != nil {
		t.Fatal(err)
	}
	compileInputRun(t, reopened, group, now, intent)
	intent.Generation = "next-native-source"
	compileInputRun(t, reopened, group, now, intent)
	state, err = reopened.readGroupState(path, group)
	if err != nil || len(state.RetentionInputs) != 2 || !sameGroupCompileValue(state.RetentionInputs[0], legacy) || state.RetentionInputs[1].Attestation == nil || !state.CompileInputAttestationsRequired {
		t.Fatal("upgrade backfilled history or omitted new source", err)
	}
	for _, mode := range []string{"remove-required", "remove-later-seal", "seal-gap"} {
		bad := clonePersistentGroupState(state)
		switch mode {
		case "remove-required":
			bad.CompileInputAttestationsRequired = false
		case "remove-later-seal":
			bad.RetentionInputs[1].Attestation = nil
		case "seal-gap":
			bad.RetentionInputs[0].Attestation = bad.RetentionInputs[1].Attestation
		}
		bad.Digest = persistentGroupStateDigest(bad)
		if validatePersistentGroupState(bad, group) == nil {
			t.Fatal("durable seal requirement bypassed", mode)
		}
	}
}

func TestGroupCompileInputAttestationMissingPolicyRefusesWholeCAS(t *testing.T) {
	if _, err := OpenPersistentGroupStoreWithOptions(privateStateDir(t), PersistentGroupStoreOptions{RetainCompileInputs: true}); err == nil {
		t.Fatal("capture enabled without trusted policy location")
	}
	s, group, now, intent := compileInputFixture(t, true)
	compileInputRun(t, s, group, now, intent)
	path := s.groupStatePath(group)
	before, _ := os.ReadFile(path)
	// Option false cannot append an unsigned source to a sealed group.
	reopened, err := OpenPersistentGroupStore(s.root)
	if err != nil {
		t.Fatal(err)
	}
	intent.Generation = "new-without-policy"
	if b := compileInputRun(t, reopened, group, now, intent); b.Succeeded != 0 || b.Failed != 1 {
		t.Fatal("missing policy admitted successful generation", b)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("missing policy partially committed CAS")
	}
	if err := os.WriteFile(filepath.Join(s.compileInputKeyringDir, group+".json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if b := compileInputRun(t, s, group, now, intent); b.Succeeded != 0 || b.Failed != 1 {
		t.Fatal("invalid policy admitted source", b)
	}
	after, _ = os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("invalid policy partially committed CAS")
	}
}
