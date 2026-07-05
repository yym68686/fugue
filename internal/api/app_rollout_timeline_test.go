package api

import "testing"

func TestRolloutTimelineDrainSummaryCombinesLogsAndKubernetesEvents(t *testing.T) {
	drain := rolloutTimelineDrainSummary(
		[]map[string]any{
			{
				"timestamp": "2026-07-05T00:00:00Z",
				"pod":       "app-demo-abc",
				"message":   "2026/07/05 00:00:00 fugue_drain_start pod=app-demo-abc namespace=tenant-demo ports=8080 timeout_seconds=600 quiet_period_seconds=2 poll_interval_ms=200",
			},
			{
				"timestamp": "2026-07-05T00:00:03Z",
				"pod":       "app-demo-abc",
				"message":   "2026/07/05 00:00:03 fugue_drain_sample active_connections=2 states=ESTABLISHED=2 waited_ms=3000",
			},
			{
				"timestamp": "2026-07-05T00:00:05Z",
				"pod":       "app-demo-abc",
				"message":   "2026/07/05 00:00:05 fugue_drain_complete reason=idle waited_ms=5000 active_connections=0 max_active_connections=2 observer_errors=0",
			},
		},
		[]map[string]any{
			{
				"timestamp":     "2026-07-05T00:00:06Z",
				"reason":        "SuccessfulDelete",
				"message":       "Deleted pod: app-demo-abc",
				"involved_kind": "ReplicaSet",
				"involved_name": "app-demo",
			},
			{
				"timestamp":     "2026-07-05T00:01:00Z",
				"reason":        "timeout",
				"message":       "reason=timeout waited_ms=600000 active_connections=2 max_active_connections=9 observer_errors=0",
				"involved_kind": "Pod",
				"involved_name": "app-demo-def",
			},
		},
	)

	if len(drain) != 2 {
		t.Fatalf("expected two drain records, got %#v", drain)
	}
	first := drain[0]
	if first["pod"] != "app-demo-abc" || first["old_pod_drain_start"] != "2026-07-05T00:00:00Z" {
		t.Fatalf("unexpected first drain record start: %#v", first)
	}
	if first["last_sample"] != "2026-07-05T00:00:03Z" || first["last_sample_active_connections"] != "2" {
		t.Fatalf("unexpected first drain record sample: %#v", first)
	}
	if first["final_reason"] != "idle" || first["waited_ms"] != "5000" || first["max_active_connections"] != "2" {
		t.Fatalf("unexpected first drain final result: %#v", first)
	}
	if first["old_pod_actual_disappear_time"] != "2026-07-05T00:00:06Z" {
		t.Fatalf("unexpected old pod disappear time: %#v", first)
	}

	second := drain[1]
	if second["pod"] != "app-demo-def" || second["final_reason"] != "timeout" || second["final_result_source"] != "kubernetes_event" {
		t.Fatalf("unexpected kubernetes event drain final result: %#v", second)
	}
}
