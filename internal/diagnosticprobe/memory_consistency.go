package diagnosticprobe

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
)

// Successful transport is not agreement between independently sampled sources.
// Preserve the original series and describe conflicts, never fix counters by
// replacing missing or contradictory samples with inferred values.
func appendMemoryConsistency(report *livediagnostics.ProbeReport) {
	type oomFact struct {
		namespace, pod, container string
		ended                     time.Time
	}
	facts := []oomFact{}
	for _, e := range report.Evidence {
		if e.Source != "kubernetes-objects" {
			continue
		}
		var data struct {
			Items []map[string]json.RawMessage `json:"items"`
		}
		if json.Unmarshal(e.Data, &data) != nil {
			continue
		}
		for _, item := range data.Items {
			var meta struct{ Name, Namespace string }
			if json.Unmarshal(item["metadata"], &meta) != nil {
				continue
			}
			var statuses []struct {
				Name      string
				LastState struct {
					Terminated *struct {
						Reason     string
						FinishedAt time.Time
					}
				}
			}
			if json.Unmarshal(item["status.containerStatuses"], &statuses) != nil {
				continue
			}
			for _, s := range statuses {
				term := s.LastState.Terminated
				if term != nil && term.Reason == "OOMKilled" {
					facts = append(facts, oomFact{meta.Namespace, meta.Name, s.Name, term.FinishedAt})
				}
			}
		}
	}
	conflicts := []string{}
	for _, e := range report.Evidence {
		if e.Source != "prometheus" {
			continue
		}
		var data map[string]struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []json.RawMessage `json:"value"`
			} `json:"result"`
		}
		if json.Unmarshal(e.Data, &data) != nil {
			continue
		}
		for _, row := range data["oom_events_6h"].Result {
			if len(row.Value) != 2 {
				continue
			}
			var at float64
			var raw string
			if json.Unmarshal(row.Value[0], &at) != nil || json.Unmarshal(row.Value[1], &raw) != nil {
				continue
			}
			value, err := strconv.ParseFloat(raw, 64)
			if err != nil || value != 0 {
				continue
			}
			observed := time.UnixMilli(int64(at * 1000))
			for _, f := range facts {
				if row.Metric["namespace"] == f.namespace && row.Metric["pod"] == f.pod && row.Metric["container"] == f.container && !f.ended.IsZero() && f.ended.Before(observed) && observed.Sub(f.ended) < 6*time.Hour {
					conflicts = append(conflicts, fmt.Sprintf("%s/%s/%s: zero six-hour OOM series conflicts with Kubernetes OOMKilled at %s; compare container lifecycle and source coverage", f.namespace, f.pod, f.container, f.ended.UTC().Format(time.RFC3339)))
				}
			}
		}
	}
	if len(conflicts) == 0 {
		return
	}
	unique := []string{}
	seen := map[string]bool{}
	for _, s := range conflicts {
		if !seen[s] {
			seen[s] = true
			unique = append(unique, s)
		}
		if len(unique) == 16 {
			break
		}
	}
	report.Quality.Status = "degraded"
	appendGap(report, "memory evidence sources disagree; successful collection does not establish no OOM")
	raw, _ := json.Marshal(map[string]any{"schema": "fugue.diagnostic.memory_consistency.v1", "consistency": "conflicting", "causal_attribution": "not established", "conflicts": unique, "scope": "recent Kubernetes OOM terminations compared with independently sampled six-hour OOM series; original identities and values retained"})
	if len(raw) > 16<<10 {
		return
	}
	report.Evidence = append(report.Evidence, livediagnostics.Evidence{Name: "memory-consistency", Source: "cross-source-consistency", ObservedAt: report.FinishedAt, Status: "degraded", Data: raw})
}

// Keep this helper small enough to make the no-query-content contract explicit.
func databaseQueryCommand(raw string) *string {
	command := strings.ToUpper(strings.TrimSpace(raw))
	switch command {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "COMMIT", "BEGIN", "ROLLBACK":
		return &command
	}
	return nil
}
