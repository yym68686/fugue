package qualityrollup

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/store"
)

func TestCompatibilityWriterHonorsPersistedOwner(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, owner  string
		runs, failed int64
	}{
		{name: "before handover", runs: 1},
		{name: "after handover", owner: dedicatedWriter},
		{name: "unknown owner", owner: "invalid", runs: 1, failed: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "state.json")
			storage := store.New(path)
			if err := storage.Init(); err != nil {
				t.Fatal(err)
			}
			if tc.owner != "" {
				ok, err := storage.UpdateObservationCheckpoint(ctx, writerCheckpoint, func([]byte) ([]byte, error) {
					return []byte(tc.owner), nil
				})
				if err != nil || !ok {
					t.Fatalf("write owner: acquired=%t err=%v", ok, err)
				}
			}
			restarted := New(store.New(path), nil)
			restarted.runOnce(ctx, time.Now().UTC())
			if restarted.runCount != tc.runs || restarted.errorCount != tc.failed {
				t.Fatalf("runs=%d failures=%d, want %d/%d", restarted.runCount, restarted.errorCount, tc.runs, tc.failed)
			}
		})
	}
}
