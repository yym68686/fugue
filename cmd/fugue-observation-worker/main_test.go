package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/store"
)

func TestProgressMetricsOnlyReportReadbackFromStorage(t *testing.T) {
	var output bytes.Buffer
	progress := store.EdgeQualityRollupWorkerProgress{Owner: store.EdgeQualityRollupWorkerWriter,
		Watermarks: map[string]time.Time{"5m": time.Unix(1795788000, 0), "unexpected": time.Now()}}
	writeProgressMetrics(&output, progress, nil)
	for _, sample := range []string{
		`fugue_edge_quality_rollup_progress_storage_available 1.000000`,
		`fugue_edge_quality_rollup_persisted_writer_owner{owner="worker"} 1.000000`,
		`fugue_edge_quality_rollup_committed_window_end_timestamp_seconds{window="5m"} 1795788000.000000`,
	} {
		if !strings.Contains(output.String(), sample) {
			t.Fatalf("missing metric %q in %s", sample, output.String())
		}
	}
	if strings.Contains(output.String(), "unexpected") {
		t.Fatal("an unknown window would create an unbounded series")
	}
	output.Reset()
	writeProgressMetrics(&output, progress, errors.New("synthetic database failure"))
	if !strings.Contains(output.String(), "fugue_edge_quality_rollup_progress_storage_available 0.000000") ||
		strings.Contains(output.String(), "persisted_writer_owner") || strings.Contains(output.String(), "synthetic database failure") {
		t.Fatalf("failed storage was published as progress: %s", output.String())
	}
}
