package pgimage

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestImageDrainObservationFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, want            string
		idle, unknown, oldest int
		valid                 bool
	}{
		{"idle connections", "eligible", 0, 0, 0, true},
		{"recent transaction", "eligible", 0, 0, 500, true},
		{"long transaction", "blocked", 0, 0, 15000, true},
		{"idle transaction", "blocked", 1, 0, 700, true},
		{"unreadable session", "unavailable", 0, 1, 0, true},
		{"stats disabled or replica", "unavailable", 0, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := fmt.Sprintf(`{"sampled_at":"2026-01-01T00:00:00Z","sessions":4,"active_transactions":1,"idle_in_transaction":%d,"unknown":%d,"oldest_transaction_ms":%d,"valid":%t,"marker":"witness","marker_from_file":true}`, tc.idle, tc.unknown, tc.oldest, tc.valid)
			got := DecodeDrainObservation([]byte(raw), 15)
			if got.Status != tc.want {
				t.Fatalf("expected %s, got %+v", tc.want, got)
			}
			if tc.want == "eligible" && (!got.MarkerFromFile || got.LoadedMarker != "witness") {
				t.Fatalf("did not retain config acknowledgement: %+v", got)
			}
			safe, err := json.Marshal(got)
			if err != nil || strings.Contains(string(safe), "witness") {
				t.Fatalf("unexpected public observation: %s %v", safe, err)
			}
		})
	}
	if got := DecodeDrainObservation([]byte(`{"valid":true}`), 10); got.Status != "unavailable" {
		t.Fatalf("incomplete SQL response accepted: %+v", got)
	}
}
