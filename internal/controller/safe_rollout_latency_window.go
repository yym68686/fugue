package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseflow"
)

// A sticky client can move its entire request cohort at a canary step. Keep
// recent absolute/error checks in their original window and retain every
// comparable recent regression. Only a pure sample deficit may consult the
// separately configured, bounded history for these exact two release IDs.
func safeRolloutLatencyComparison(ctx context.Context, q releaseflow.ReleaseGateLatencyCohortsQuerier, state *safeRolloutState, policy model.AppReleaseGatePolicy, recentWindow time.Duration) (map[string]any, []string, []string, error) {
	seconds := policy.LatencyComparisonWindowSeconds
	if seconds < 0 || seconds > 3600 || (seconds > 0 && time.Duration(seconds)*time.Second < recentWindow) {
		return nil, nil, nil, fmt.Errorf("invalid latency comparison window")
	}
	cohorts, err := q.QueryReleaseGateLatencyCohorts(ctx, state.CandidateApp.ID, state.Candidate.ID, state.StableRelease.ID, recentWindow)
	if err != nil {
		return nil, nil, nil, err
	}
	metrics, failures, warnings := releaseflow.ReleaseGateLatencyCohortComparison(cohorts, state.Candidate.ID, state.StableRelease.ID, policy)
	metrics["window_seconds"] = int(recentWindow.Seconds())
	if seconds == 0 || time.Duration(seconds)*time.Second == recentWindow || len(failures) != 1 ||
		!strings.HasPrefix(failures[0], "matched latency cohort request count ") || !strings.Contains(failures[0], "below minimum") {
		return metrics, failures, warnings, nil
	}
	longer, err := q.QueryReleaseGateLatencyCohorts(ctx, state.CandidateApp.ID, state.Candidate.ID, state.StableRelease.ID, time.Duration(seconds)*time.Second)
	if err != nil {
		return nil, nil, nil, err
	}
	comparison, extendedFailures, extendedWarnings := releaseflow.ReleaseGateLatencyCohortComparison(longer, state.Candidate.ID, state.StableRelease.ID, policy)
	comparison["window_seconds"] = seconds
	comparison["recent_window"] = metrics
	extendedWarnings = append(extendedWarnings, fmt.Sprintf("recent matched latency samples insufficient; used configured %ds comparison history; recent absolute and error checks remain mandatory", seconds))
	return comparison, extendedFailures, extendedWarnings, nil
}
