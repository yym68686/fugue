package edgecontrol

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestGroupOriginalExportScansLargeStateWithoutExportingUnselectedBodies(t *testing.T) {
	s, signer, group, _ := originalExportFixture(t)
	state, err := s.readGroupState(s.groupStatePath(group), group)
	if err != nil {
		t.Fatal(err)
	}
	keys := originalExportKeys(t, group, state.Published.Bundle.KeyID, signer.keys[group])
	generation := state.Published.Bundle.Generation
	entry := state.Ledger[0]
	entry.Bundle = nil
	entry.BundleArchived = true
	entry.RouteIntentGeneration = strings.Repeat("unselected-source-", 24)
	entry.BundleGeneration = "unselected-generation"
	entry.LastSuccessfulBundleGeneration = entry.BundleGeneration
	for i := len(state.Ledger); i < 9000; i++ {
		entry.Sequence = uint64(i + 1)
		state.Ledger = append(state.Ledger, entry)
	}
	state.Digest = persistentGroupStateDigest(state)
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= groupOriginalRecordBytes || len(raw) > groupOriginalBatchBytes {
		t.Fatal("fixture did not cross the whole-file bound", len(raw))
	}
	path := filepath.Join(s.root, "large-original.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	beforeFiles, _ := os.ReadDir(s.root)
	out, err := ExportGroupOriginalEvidence(t.Context(), path, group, keys, []string{generation, "not-present"})
	if err != nil {
		t.Fatal(err)
	}
	if out.StateDigest != state.Digest || out.EncodedBytes != int64(len(raw)) || out.DecodedBytes != int64(len(raw)) || out.ShadowEntries != 9000 || !out.ChecksumVerified ||
		out.StateContentsValidated || out.AdmissionAuthenticated || out.DependencyClosure || len(out.Generations[0].Shadow) != 1 || len(out.Generations[0].Publications) != 1 ||
		len(out.Generations[0].SignedBodies) != 2 || !out.Generations[0].SignedBodies[0].SignatureVerified || !reflect.DeepEqual(out.Generations[0].SignedBodies[0].PublicationSequences, []uint64{1}) || len(out.Generations[0].SignedBodies[1].PublicationSequences) != 0 || len(out.Generations[1].Shadow) != 0 {
		t.Fatal("original selection or proof boundary lost")
	}
	result, _ := json.Marshal(out)
	if bytes.Contains(result, []byte("unselected-source-")) || bytes.Contains(result, []byte("primary_key")) || len(result) >= groupOriginalRecordBytes {
		t.Fatal("unselected source/key leaked or unbounded output", len(result))
	}
	if refused, err := ExportGroupOriginalEvidence(t.Context(), path, group, keys, []string{entry.BundleGeneration}); err == nil || !reflect.DeepEqual(refused, GroupOriginalExport{}) {
		t.Fatal("a selected artifact exceeding 8MiB was exported")
	}
	after, _ := os.ReadFile(path)
	afterFiles, _ := os.ReadDir(s.root)
	if !bytes.Equal(raw, after) || !reflect.DeepEqual(beforeFiles, afterFiles) {
		t.Fatal("export mutated original filesystem")
	}
	var zipped bytes.Buffer
	z := gzip.NewWriter(&zipped)
	_, _ = z.Write(raw)
	_ = z.Close()
	if err := os.WriteFile(path, zipped.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	compressed, err := ExportGroupOriginalEvidence(t.Context(), path, group, keys, []string{generation, "not-present"})
	if err != nil || compressed.StateDigest != out.StateDigest || compressed.DecodedBytes != out.DecodedBytes || compressed.EncodedBytes != int64(zipped.Len()) || !reflect.DeepEqual(compressed.Generations, out.Generations) {
		t.Fatal("compressed scan changed original evidence", err)
	}
}

func TestGroupOriginalExportRetainsMissingHistoricalWitnessAndCurrentRevocation(t *testing.T) {
	s, signer, group, now := originalExportFixture(t)
	path := s.groupStatePath(group)
	old, _ := s.readGroupState(path, group)
	keys := originalExportKeys(t, group, old.Published.Bundle.KeyID, signer.keys[group])
	if _, err := (GroupAuthorityPublisher{Store: s, Signer: signer}).RefreshPublishedLKG(t.Context(), group, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	state, _ := s.readGroupState(path, group)
	state.Candidate = nil
	state.CandidateHistory = nil
	state.Ledger[0].Bundle = nil
	state.Ledger[0].BundleArchived = true
	state.Revision++
	if err := s.writeGroupState(path, state); err != nil {
		t.Fatal(err)
	}
	out, err := ExportGroupOriginalEvidence(t.Context(), path, group, keys, []string{old.Published.Bundle.Generation})
	if err != nil {
		t.Fatal(err)
	}
	g := out.Generations[0]
	if len(g.Publications) != 2 || len(g.SignedBodies) != 1 || !reflect.DeepEqual(g.SignedBodies[0].PublicationSequences, []uint64{state.Published.PublicationSequence}) || g.Shadow[0].Bundle != nil || !g.Shadow[0].BundleArchived || len(g.CompileInputs) != 0 {
		t.Fatal("refresh invented original publication or compile source")
	}
	keyPath := filepath.Join(keys, group+".json")
	var f groupBundleSigningKeyringFile
	raw, _ := os.ReadFile(keyPath)
	if json.Unmarshal(raw, &f) != nil {
		t.Fatal("key fixture")
	}
	f.Generation++
	f.Group.RevokedKeyIDs = []string{f.Group.PrimaryKeyID}
	raw, _ = json.Marshal(f)
	if err := os.WriteFile(keyPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	// Keep a non-revoked primary while revoking the original issuance key.
	f.Group.PreviousKeyID = f.Group.PrimaryKeyID
	f.Group.PreviousKey = f.Group.PrimaryKey
	f.Group.PrimaryKeyID = "new-key"
	f.Group.PrimaryKey = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x44}, 32))
	raw, _ = json.Marshal(f)
	if err := os.WriteFile(keyPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	out, err = ExportGroupOriginalEvidence(t.Context(), path, group, keys, []string{old.Published.Bundle.Generation})
	if err != nil || out.Generations[0].SignedBodies[0].SignatureVerified {
		t.Fatal("current revocation bypassed", err)
	}
}

func TestGroupOriginalExportRefusesCorruptionAndNoPartialReport(t *testing.T) {
	for _, mode := range []string{"checksum", "unknown", "duplicate", "reordered", "trailing", "single-frame", "expanded-bound", "encoded-bound", "gzip-tail", "gzip-multistream", "wrong-group", "canceled", "symlink", "fifo", "public-file", "repeated-selection"} {
		t.Run(mode, func(t *testing.T) {
			s, signer, group, _ := originalExportFixture(t)
			path := s.groupStatePath(group)
			state, _ := s.readGroupState(path, group)
			keys := originalExportKeys(t, group, state.Published.Bundle.KeyID, signer.keys[group])
			generations := []string{state.Published.Bundle.Generation}
			raw, _ := os.ReadFile(path)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch mode {
			case "checksum":
				raw = bytes.Replace(raw, []byte(`"revision":`), []byte(`"revision":9`), 1)
			case "unknown":
				raw = append([]byte(`{"unknown":true,`), raw[1:]...)
			case "duplicate":
				raw = append([]byte(`{"schema":"duplicate",`), raw[1:]...)
			case "reordered":
				var fields map[string]json.RawMessage
				_ = json.Unmarshal(raw, &fields)
				raw, _ = json.Marshal(fields)
			case "trailing":
				raw = append(raw, []byte(`{}`)...)
			case "single-frame":
				raw = []byte(`{"schema":"` + strings.Repeat("x", groupOriginalRecordBytes) + `"}`)
			case "expanded-bound":
				raw = bytes.Repeat([]byte{' '}, groupOriginalBatchBytes+1)
				fallthrough
			case "gzip-tail", "gzip-multistream":
				var b bytes.Buffer
				z := gzip.NewWriter(&b)
				_, _ = z.Write(raw)
				_ = z.Close()
				raw = b.Bytes()
				if mode == "gzip-tail" {
					raw = append(raw, 0)
				}
				if mode == "gzip-multistream" {
					raw = append(raw, raw...)
				}
			case "encoded-bound":
				raw = bytes.Repeat([]byte{' '}, groupOriginalBatchBytes+1)
			case "wrong-group":
				group = "other-group"
			case "canceled":
				cancel()
			case "symlink":
				p := filepath.Join(s.root, "link")
				if err := os.Symlink(path, p); err != nil {
					t.Fatal(err)
				}
				path = p
			case "fifo":
				path = filepath.Join(s.root, "fifo")
				if err := syscall.Mkfifo(path, 0o600); err != nil {
					t.Fatal(err)
				}
			case "public-file":
				if err := os.Chmod(path, 0o644); err != nil {
					t.Fatal(err)
				}
			case "repeated-selection":
				generations = append(generations, generations[0])
			}
			if mode != "symlink" && mode != "fifo" && mode != "public-file" {
				if err := os.WriteFile(path, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			out, err := ExportGroupOriginalEvidence(ctx, path, group, keys, generations)
			if err == nil || !reflect.DeepEqual(out, GroupOriginalExport{}) {
				t.Fatal("unsafe input produced partial evidence", mode, err)
			}
		})
	}
}

func TestGroupOriginalExportRejectsReboundPublicationAndInvalidSignature(t *testing.T) {
	for _, mode := range []string{"candidate-binding", "signature"} {
		t.Run(mode, func(t *testing.T) {
			s, signer, group, _ := originalExportFixture(t)
			path := s.groupStatePath(group)
			state, err := s.readGroupState(path, group)
			if err != nil {
				t.Fatal(err)
			}
			keys := originalExportKeys(t, group, state.Published.Bundle.KeyID, signer.keys[group])
			generation := state.Published.Bundle.Generation
			state.Candidate, state.CandidateHistory = nil, nil
			if mode == "candidate-binding" {
				state.AuthorityLedger[0].RouteIntentGeneration = "different-source"
			} else {
				state.Published.Bundle.Signature = "forged-signature"
				state.Published.Bundle.Signatures = nil
				state.Published.Digest = signedGroupBundleDigest(state.Published.Bundle)
				state.AuthorityLedger[0].PublishedBundleDigest = state.Published.Digest
			}
			// The native checksum is not an authentication substitute.
			state.Digest = persistentGroupStateDigest(state)
			raw, err := json.Marshal(state)
			if err != nil || os.WriteFile(path, raw, 0o600) != nil {
				t.Fatal("fixture write", err)
			}
			out, err := ExportGroupOriginalEvidence(t.Context(), path, group, keys, []string{generation})
			if mode == "candidate-binding" {
				if err == nil || !reflect.DeepEqual(out, GroupOriginalExport{}) {
					t.Fatal("recomputed checksum hid a broken original binding")
				}
			} else if err != nil || len(out.Generations[0].SignedBodies) != 1 || out.Generations[0].SignedBodies[0].SignatureVerified || !out.ChecksumVerified || out.AdmissionAuthenticated {
				t.Fatal("native checksum authenticated a forged original", err)
			}
		})
	}
}

func originalExportFixture(t *testing.T) (*PersistentGroupStore, *fixtureGroupSigner, string, time.Time) {
	t.Helper()
	group := "edge-group-neutral"
	now := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	s, signer, _, _ := groupPromotionFixture(t, group, now)
	return s, signer, group, now
}

func originalExportKeys(t *testing.T, group, keyID string, key []byte) string {
	t.Helper()
	dir := privateStateDir(t)
	keys := groupBundleSigningKeyringFile{Schema: GroupBundleSigningKeyringSchemaV1, Generation: 7,
		Group: groupBundleSigningKey{GroupID: group, PrimaryKeyID: keyID, PrimaryKey: base64.RawURLEncoding.EncodeToString(key)}}
	raw, _ := json.Marshal(keys)
	if err := os.WriteFile(filepath.Join(dir, group+".json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}
