package edgecontrol

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPersistentCompressionPreservesAuthorityAndRollbackReader(t *testing.T) {
	groupID := "edge-group-country-us"
	store, _, _, _ := groupPromotionFixture(t, groupID, time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC))
	path := store.groupStatePath(groupID)
	before, err := store.readGroupState(path, groupID)
	if err != nil {
		t.Fatal(err)
	}
	plain, _ := os.ReadFile(path)
	if !bytes.HasPrefix(plain, []byte("{")) {
		t.Fatal("read-compatible release enabled compression without configuration")
	}
	store.compressStateWrites = true
	if err := store.writeGroupState(path, before); err != nil {
		t.Fatal(err)
	}
	compressed, _ := os.ReadFile(path)
	if !bytes.HasPrefix(compressed, []byte{0x1f, 0x8b}) || len(compressed) >= len(plain) {
		t.Fatal("compressed format was not written")
	}
	rollback, err := OpenPersistentGroupStore(store.root)
	if err != nil {
		t.Fatal(err)
	}
	after, err := rollback.readGroupState(path, groupID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("reader changed authority, ledger, candidate or checksum", err)
	}
	if err := rollback.writeGroupState(path, after); err != nil {
		t.Fatal("read-compatible rollback cannot continue writing", err)
	}
	continued, _ := os.ReadFile(path)
	if !bytes.HasPrefix(continued, []byte{0x1f, 0x8b}) {
		t.Fatal("rollback silently reverted a committed compressed format")
	}
	for name, corrupt := range map[string][]byte{
		"truncated":    compressed[:len(compressed)-8],
		"trailing":     append(append([]byte(nil), compressed...), 'x'),
		"concatenated": append(append([]byte(nil), compressed...), compressed...),
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, corrupt, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := rollback.readGroupState(path, groupID); err == nil {
				t.Fatal("invalid compressed state was accepted")
			}
		})
	}
	var tampered bytes.Buffer
	writer := gzip.NewWriter(&tampered)
	changed := before
	changed.Revision++
	raw, _ := json.Marshal(changed)
	_, _ = writer.Write(raw)
	_ = writer.Close()
	_ = os.WriteFile(path, tampered.Bytes(), 0o600)
	if _, err := rollback.readGroupState(path, groupID); err == nil {
		t.Fatal("compressed state bypassed checksum authentication")
	}
}

func TestPersistentCompressionRetainsMetadataBeyondLegacyCeiling(t *testing.T) {
	groupID := "edge-group-country-us"
	state := persistentGroupState{Schema: persistentGroupStateSchemaV1, GroupID: groupID, Revision: 1}
	for index := 0; index < 60000; index++ {
		state.Ledger = append(state.Ledger, GroupShadowLedgerEntry{
			Schema: GroupShadowLedgerSchemaV1, GroupID: groupID, Sequence: uint64(index + 1),
			Status: GroupShadowStatusCompiled, BundleArchived: true, Authority: "none",
			RouteIntentGeneration: strings.Repeat("generation", 140),
			InputDigest:           "sha256:" + strings.Repeat("a", 64), BundleGeneration: "retained-generation",
			LastSuccessfulBundleGeneration: "retained-generation", RecordedAt: time.Date(2026, 10, 14, 0, 0, index, 0, time.UTC),
		})
	}
	if err := compactPersistentGroupStateForSize(&state); err == nil {
		t.Fatal("fixture did not reproduce metadata-only durable ceiling")
	}
	if err := compactPersistentGroupStateForLimit(&state, maxDecodedPersistentGroupStateBytes); err != nil {
		t.Fatal(err)
	}
	store, err := OpenPersistentGroupStoreWithOptions(privateStateDir(t), PersistentGroupStoreOptions{CompressStateWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	path := store.groupStatePath(groupID)
	if err := store.writeGroupState(path, state); err != nil {
		t.Fatal(err)
	}
	reopened, _ := OpenPersistentGroupStore(store.root)
	after, err := reopened.readGroupState(path, groupID)
	if err != nil || len(after.Ledger) != len(state.Ledger) || !reflect.DeepEqual(after.Ledger, state.Ledger) {
		t.Fatal("compressed continuation lost ledger evidence", err)
	}
}

func TestPersistentCompressionPrivateSnapshotReplay(t *testing.T) {
	snapshotPath := os.Getenv("FUGUE_STORAGE_REPLAY_SNAPSHOT")
	if snapshotPath == "" {
		t.Skip("private read-only authority snapshot not supplied")
	}
	groupID := "edge-group-country-us"
	store, err := OpenPersistentGroupStoreWithOptions(privateStateDir(t), PersistentGroupStoreOptions{CompressStateWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatal("private snapshot unavailable")
	}
	path := store.groupStatePath(groupID)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := store.readGroupState(path, groupID)
	if err != nil {
		t.Fatal("private snapshot did not validate", err)
	}
	if err := store.writeGroupState(path, before); err != nil {
		t.Fatal("private snapshot did not persist losslessly", err)
	}
	rollback, _ := OpenPersistentGroupStore(store.root)
	after, err := rollback.readGroupState(path, groupID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("private snapshot changed through compression", err)
	}
	t.Logf("validated shadow_entries=%d authority_entries=%d revision=%d", len(after.Ledger), len(after.AuthorityLedger), after.Revision)
}
