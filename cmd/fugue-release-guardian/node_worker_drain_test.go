package main

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
)

func TestNodeWorkerDrainCompletionMustMatchExactGrant(t *testing.T) {
	grant := edgegroupfront.WorkerDrainGrant{
		Serving: edgegroupfront.ActivationState{Generation: 913}, Deadline: time.Unix(10000, 0),
	}
	valid := strings.Join([]string{
		"# TYPE fugue_edge_front_worker_drain_capability gauge",
		"fugue_edge_front_worker_drain_capability 1",
		"fugue_edge_front_worker_drain_last_generation 913",
		"fugue_edge_front_worker_drain_last_deadline_seconds 10000",
		"fugue_edge_front_worker_drain_last_remaining 0",
		"fugue_edge_front_worker_drain_last_completed 1", "",
	}, "\n")
	if !nodeWorkerDrainMetricsMatch([]byte(valid), grant) {
		t.Fatal("exact mounted and completed grant was not recognized")
	}
	for _, bad := range []string{
		strings.Replace(valid, "generation 913", "generation 912", 1),
		strings.Replace(valid, "seconds 10000", "seconds 10001", 1),
		strings.Replace(valid, "remaining 0", "remaining 1", 1),
		strings.Replace(valid, "completed 1", "completed 0", 1),
		valid + "fugue_edge_front_worker_drain_last_completed 1\n",
	} {
		if nodeWorkerDrainMetricsMatch([]byte(bad), grant) {
			t.Fatalf("accepted incomplete or foreign grant acknowledgement: %s", bad)
		}
	}
}
