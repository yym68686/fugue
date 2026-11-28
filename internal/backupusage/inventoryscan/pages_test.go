package inventoryscan

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"fugue/internal/backupusage"
)

func inventoryPagesFixture() checkpoint {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scan := &scanState{Generation: "inventory-synthetic", StartedAt: now, Wanted: map[string]bool{}, References: map[string]backupusage.InventoryObject{}, Totals: map[string]backupusage.InventoryTotals{"all": {Count: 12000}}}
	for i := 0; i < 12000; i++ {
		key := fmt.Sprintf("objects/%06d", i)
		scan.Wanted[key] = true
		scan.References[key] = backupusage.InventoryObject{Key: key, Size: int64(i), ObservedAt: now}
	}
	return checkpoint{Version: 1, Current: scan}
}
func TestInventoryPagesPreserveContentAndWriteOnlyChangedPartition(t *testing.T) {
	cp := inventoryPagesFixture()
	before, err := encodePages(cp)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodePages(before, nil)
	if err != nil || !reflect.DeepEqual(decoded, cp) {
		t.Fatal("roundtrip differs", err)
	}
	o := cp.Current.References["objects/000123"]
	o.Size++
	cp.Current.References[o.Key] = o
	cp.Current.Cursor = "next-page"
	cp.Current.Pages++
	after, err := encodePages(cp)
	if err != nil {
		t.Fatal(err)
	}
	changed, size := 0, 0
	for key, page := range after.Pages {
		if !bytes.Equal(page, before.Pages[key]) {
			changed++
			size += len(page)
		}
	}
	if changed != 1 || size > 64<<10 {
		t.Fatalf("one entry rewrote %d pages / %d bytes", changed, size)
	}
	cp.Complete = cp.Current
	cp.Current = nil
	complete, err := encodePages(cp)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after.Pages, complete.Pages) {
		t.Fatal("completion rewrote immutable generation pages")
	}
	for _, mode := range []string{"missing", "corrupt", "extra"} {
		broken, err := encodePages(cp)
		if err != nil {
			t.Fatal(err)
		}
		for key, page := range broken.Pages {
			if mode == "missing" {
				delete(broken.Pages, key)
			} else if mode == "corrupt" {
				broken.Pages[key] = append(append([]byte{}, page...), byte(' '))
			} else {
				broken.Pages["foreign/page"] = []byte("{}")
			}
			break
		}
		if _, err := decodePages(broken, nil); err == nil {
			t.Fatal("accepted", mode)
		}
	}
}
