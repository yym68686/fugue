package api

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestProjectNodeDeepHealthFreshnessMarksHistoricalPassStale(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC)
	result := model.NodeDeepHealthResult{
		ObservedOnly:    true,
		OverallStatus:   model.NodeDeepHealthStatusPass,
		QuarantineState: model.NodeQuarantineStateClear,
		ReportedAt:      now.Add(-24 * time.Hour),
	}

	projected := projectNodeDeepHealthFreshness(result, now)
	if projected.OverallStatus != model.NodeDeepHealthStatusWarning ||
		projected.QuarantineState != model.NodeQuarantineStateDegraded ||
		projected.QuarantineReason != nodeDeepHealthReportStaleReason {
		t.Fatalf("historical pass was not marked stale: %+v", projected)
	}
	if len(projected.Checks) != 1 || projected.Checks[0].Name != "report_freshness" ||
		projected.Checks[0].Status != model.NodeDeepHealthStatusWarning {
		t.Fatalf("missing freshness evidence: %+v", projected.Checks)
	}
	if !projected.ObservedOnly {
		t.Fatal("freshness projection must not turn observation into active quarantine")
	}
}

func TestProjectNodeDeepHealthFreshnessPreservesFreshResult(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC)
	result := model.NodeDeepHealthResult{
		ObservedOnly:    true,
		OverallStatus:   model.NodeDeepHealthStatusPass,
		QuarantineState: model.NodeQuarantineStateClear,
		ReportedAt:      now.Add(-5 * time.Minute),
	}

	projected := projectNodeDeepHealthFreshness(result, now)
	if projected.OverallStatus != model.NodeDeepHealthStatusPass || projected.QuarantineState != model.NodeQuarantineStateClear || len(projected.Checks) != 0 {
		t.Fatalf("fresh result changed: %+v", projected)
	}
}
