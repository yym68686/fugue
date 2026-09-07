package edgecontrol

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// Retained audit rows dominate mature stores even when current inventory is small.
func benchmarkPersistentStore(b *testing.B, entries int) (*PersistentGroupStore, string) {
	b.Helper()
	root := b.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		b.Fatal(err)
	}
	store, err := OpenPersistentGroupStore(root)
	if err != nil {
		b.Fatal(err)
	}
	groupID := "edge-group-benchmark"
	state := persistentGroupState{Schema: persistentGroupStateSchemaV1, GroupID: groupID, Revision: 1,
		Inventory: &GroupInventorySnapshot{Schema: GroupInventorySchemaV1, GroupID: groupID, Sequence: 1, Generation: "inventory-1"}}
	for i := 0; i < entries; i++ {
		generation := fmt.Sprintf("route-generation-%064d", i)
		state.Ledger = append(state.Ledger, GroupShadowLedgerEntry{
			Schema: GroupShadowLedgerSchemaV1, GroupID: groupID, Sequence: uint64(i + 1),
			Status: GroupShadowStatusCompiled, BundleArchived: true, Authority: "none",
			RouteIntentGeneration: generation, BundleGeneration: generation, LastSuccessfulBundleGeneration: generation,
			InputDigest: fmt.Sprintf("sha256:%064x", i), RecordedAt: time.Unix(int64(i+1), 0).UTC(),
		})
	}
	if err := store.writeGroupState(store.groupStatePath(groupID), state); err != nil {
		b.Fatal(err)
	}
	return store, groupID
}

func BenchmarkPersistentGroupInventory(b *testing.B) {
	for _, entries := range []int{100, 35000} {
		b.Run(fmt.Sprintf("rows-%d", entries), func(b *testing.B) {
			store, group := benchmarkPersistentStore(b, entries)
			ctx := context.Background()
			if _, err := store.ReadGroupInventory(ctx, group); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := store.ReadGroupInventory(ctx, group); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPersistentGroupInventoryWrite(b *testing.B) {
	store, group := benchmarkPersistentStore(b, 35000)
	ctx := context.Background()
	if _, err := store.ReadGroupInventory(ctx, group); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sequence := uint64(i + 1)
		err := store.StoreGroupInventoryCAS(ctx, group, sequence, GroupInventorySnapshot{
			Schema: GroupInventorySchemaV1, GroupID: group, Sequence: sequence + 1, Generation: "inventory-next"})
		if err != nil {
			b.Fatal(err)
		}
	}
}
