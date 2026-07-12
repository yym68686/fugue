package backupschedule

import (
	"testing"
	"time"
)

func TestNext(t *testing.T) {
	t.Parallel()

	after := time.Date(2026, 6, 13, 10, 17, 30, 0, time.FixedZone("test", 8*60*60))
	tests := []struct {
		name     string
		schedule string
		want     time.Time
	}{
		{name: "hourly", schedule: "0 * * * *", want: time.Date(2026, 6, 13, 3, 0, 0, 0, time.UTC)},
		{name: "hourly shortcut", schedule: "@hourly", want: time.Date(2026, 6, 13, 3, 0, 0, 0, time.UTC)},
		{name: "minute step", schedule: "*/15 * * * *", want: time.Date(2026, 6, 13, 2, 30, 0, 0, time.UTC)},
		{name: "hour step", schedule: "0 */6 * * *", want: time.Date(2026, 6, 13, 6, 0, 0, 0, time.UTC)},
		{name: "list and range", schedule: "5,35 9-17 * * 1-5", want: time.Date(2026, 6, 15, 9, 5, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Next(tt.schedule, after)
			if err != nil {
				t.Fatalf("next schedule: %v", err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("expected %s, got %s", tt.want, got)
			}
		})
	}
}

func TestNextAdvancesExactBoundary(t *testing.T) {
	t.Parallel()

	after := time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)
	got, err := Next("0 */6 * * *", after)
	if err != nil {
		t.Fatalf("next schedule: %v", err)
	}
	want := time.Date(2026, 6, 13, 18, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("expected %s, got %s", want, got)
	}
}

func TestInvalidSchedulesAreRejected(t *testing.T) {
	t.Parallel()

	for _, schedule := range []string{
		"",
		"bad schedule",
		"0 0 0 * * *",
		"60 * * * *",
		"*/0 * * * *",
		"1/9223372036854775807 * * * *",
		"0 0 31 2 *",
		"@daily",
		"CRON_TZ=Asia/Shanghai 0 * * * *",
	} {
		if got, err := Next(schedule, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); err == nil || !got.IsZero() {
			t.Fatalf("expected %q to be rejected, got %s err=%v", schedule, got, err)
		}
	}
}
