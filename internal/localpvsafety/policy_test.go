package localpvsafety

import (
	"testing"
	"time"
)

func TestRequiredFreeBytesUsesLargerAbsoluteOrPercentReserve(t *testing.T) {
	t.Parallel()

	if got := RequiredFreeBytes(20 << 30); got != 5<<30 {
		t.Fatalf("20Gi required free = %d, want %d", got, int64(5<<30))
	}
	if got := RequiredFreeBytes(100 << 30); got != 10<<30 {
		t.Fatalf("100Gi required free = %d, want %d", got, int64(10<<30))
	}
}

func TestHasCapacityHeadroomRejectsInvalidAndLowFreeCapacity(t *testing.T) {
	t.Parallel()

	if HasCapacityHeadroom(100<<30, 1<<30) {
		t.Fatal("expected one GiB free in a 100GiB volume group to fail")
	}
	if !HasCapacityHeadroom(100<<30, 12<<30) {
		t.Fatal("expected twelve GiB free in a 100GiB volume group to pass")
	}
	if HasCapacityHeadroom(0, 0) || HasCapacityHeadroom(10, 11) {
		t.Fatal("expected invalid capacity observations to fail")
	}
}

func TestFilesystemCapacityConvergedAllowsFormattingOverhead(t *testing.T) {
	t.Parallel()

	if FilesystemCapacityConverged(28<<30, 40<<30) {
		t.Fatal("expected a 28Gi filesystem not to satisfy a 40Gi provisioned volume")
	}
	if !FilesystemCapacityConverged(38<<30, 40<<30) {
		t.Fatal("expected a 38Gi filesystem to satisfy a 40Gi provisioned volume")
	}
}

func TestIsFreshUsesTTL(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 27, 8, 0, 0, 0, time.UTC)
	if !IsFresh(now.Add(-30*time.Minute), now, time.Hour) {
		t.Fatal("expected recent inventory to be fresh")
	}
	if IsFresh(now.Add(-2*time.Hour), now, time.Hour) {
		t.Fatal("expected old inventory to be stale")
	}
	if IsFresh(now.Add(10*time.Minute), now, time.Hour) {
		t.Fatal("expected an implausibly future-dated inventory to be stale")
	}
}
