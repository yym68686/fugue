package diagnosticprobe

import (
	"encoding/json"
	"fugue/internal/livediagnostics"
	"strings"
	"testing"
	"time"
)

func TestMemoryConsistencyDoesNotTreatZeroAsProofOfNoOOM(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		count        string
		want         bool
	}{{"conflict", "OOMKilled", "0", true}, {"positive", "OOMKilled", "1", false}, {"other-exit", "Error", "0", false}} {
		t.Run(tc.name, func(t *testing.T) {
			at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			pods, _ := json.Marshal(map[string]any{"items": []any{map[string]any{"metadata": map[string]string{"name": "db-1", "namespace": "system"}, "status.containerStatuses": []any{map[string]any{"name": "postgres", "lastState": map[string]any{"terminated": map[string]any{"reason": tc.reason, "finishedAt": at.Add(-time.Minute)}}}}}}})
			metrics, _ := json.Marshal(map[string]any{"oom_events_6h": map[string]any{"result": []any{map[string]any{"metric": map[string]string{"namespace": "system", "pod": "db-1", "container": "postgres", "id": "/unique-cgroup"}, "value": []any{at.Unix(), tc.count}}}}})
			r := livediagnostics.ProbeReport{FinishedAt: at, Quality: livediagnostics.EvidenceQuality{Status: "complete"}, Evidence: []livediagnostics.Evidence{{Source: "kubernetes-objects", Data: pods}, {Source: "prometheus", Data: metrics}}}
			appendMemoryConsistency(&r)
			if (r.Quality.Status == "degraded") != tc.want {
				t.Fatalf("unexpected consistency %+v", r.Quality)
			}
			if string(r.Evidence[1].Data) != string(metrics) {
				t.Fatal("raw metrics were changed")
			}
		})
	}
}
func TestDatabaseCommandNeverExportsUnrecognizedText(t *testing.T) {
	secret := "select\ncredential-value"
	if databaseQueryCommand(secret) != nil {
		t.Fatal("non-command exported")
	}
	if got := databaseQueryCommand("INSERT"); got == nil || *got != "INSERT" {
		t.Fatal("command missing")
	}
	if strings.Contains(postgresObservationSQL, " AS query ") {
		t.Fatal("raw SQL output")
	}
}
