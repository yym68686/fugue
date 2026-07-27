package imagecacheusage

import "testing"

func TestConservativeUsedPercentCountsReservedBlocksAsUnavailable(t *testing.T) {
	t.Parallel()

	const (
		capacity  = int64(1_000)
		available = int64(100)
		reported  = 70.0
	)
	if got := ConservativeUsedPercent(reported, capacity, available); got != 90 {
		t.Fatalf("conservative used percent = %.2f, want 90", got)
	}
}

func TestConservativeUsedPercentNeverLowersReportedValue(t *testing.T) {
	t.Parallel()

	if got := ConservativeUsedPercent(91, 1_000, 200); got != 91 {
		t.Fatalf("conservative used percent = %.2f, want reported 91", got)
	}
}

func TestConservativeUsedPercentRejectsInvalidCapacityEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		capacity  int64
		available int64
	}{
		{name: "missing capacity", capacity: 0, available: 0},
		{name: "negative available", capacity: 1_000, available: -1},
		{name: "available exceeds capacity", capacity: 1_000, available: 1_001},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ConservativeUsedPercent(42, tt.capacity, tt.available); got != 42 {
				t.Fatalf("conservative used percent = %.2f, want unchanged 42", got)
			}
		})
	}
}
