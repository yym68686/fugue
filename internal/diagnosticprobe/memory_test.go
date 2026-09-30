package diagnosticprobe

import (
	"context"
	"encoding/json"
	"testing"

	"fugue/internal/livediagnostics"
)

func TestMemoryProfileRequiresExactBoundedContainer(t *testing.T) {
	for _, tc := range []struct {
		req       livediagnostics.ProbeRequest
		collector Collector
	}{
		{livediagnostics.ProbeRequest{DurationSeconds: 90}, Collector{CaptureSeconds: 60, IntervalSeconds: 120}},
		{livediagnostics.ProbeRequest{ContainerID: "containerd://fixture", DurationSeconds: 60}, Collector{CaptureSeconds: 60, IntervalSeconds: 120}},
		{livediagnostics.ProbeRequest{ContainerID: "containerd://fixture", DurationSeconds: 90}, Collector{CaptureSeconds: 60, IntervalSeconds: 30}},
		{livediagnostics.ProbeRequest{ContainerID: "containerd://fixture", DurationSeconds: 180}, Collector{CaptureSeconds: 121, IntervalSeconds: 180}},
		{livediagnostics.ProbeRequest{ContainerID: "containerd://fixture", Target: livediagnostics.Target{ProcessName: "fugue-edge"}, DurationSeconds: 90}, Collector{CaptureSeconds: 60, IntervalSeconds: 120}},
	} {
		if _, err := processMemoryProfile(context.Background(), tc.req, tc.collector); err == nil {
			t.Fatal("accepted ambiguous target or unbounded capture")
		}
	}
}

func TestMemoryProfileEvidencePreservesIdentityAndReportsMissingCoverage(t *testing.T) {
	for _, mode := range []string{"complete", "wrong-container", "wrong-pid", "duplicate-pid", "wrong-schema", "missing-go", "missing-allocs", "short-window", "warning"} {
		t.Run(mode, func(t *testing.T) {
			v := map[string]any{"schema": "fugue.diagnostic.memory_profile.v1", "target_container_id": "containerd://fixture", "target_pids": []int{12, 13}, "go_runtime_profile_available": true, "go_inuse_space_top": "heap", "go_alloc_space_delta_top": "allocs", "peak_cgroup_memory_bytes": int64(9007199254740993), "samples": []map[string]string{{"observed_at": "2026-01-01T00:00:00Z"}, {"observed_at": "2026-01-01T00:01:00Z"}}}
			switch mode {
			case "wrong-container":
				v["target_container_id"] = "containerd://another"
			case "wrong-pid":
				v["target_pids"] = []int{12, 14}
			case "duplicate-pid":
				v["target_pids"] = []int{12, 12}
			case "wrong-schema":
				v["schema"] = "other"
			case "missing-go":
				v["go_runtime_profile_available"] = false
			case "missing-allocs":
				delete(v, "go_alloc_space_delta_top")
			case "short-window":
				v["samples"] = []map[string]string{{"observed_at": "2026-01-01T00:00:00Z"}, {"observed_at": "2026-01-01T00:00:10Z"}}
			case "warning":
				v["warnings"] = []string{"target cgroup unavailable"}
			}
			raw, _ := json.Marshal(v)
			got, err := memoryProfileEvidence(raw, "containerd://fixture", map[int]string{12: "1", 13: "2"}, 60)
			switch mode {
			case "wrong-container", "wrong-pid", "duplicate-pid", "wrong-schema":
				if err == nil {
					t.Fatal("accepted incorrect sampler identity")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (len(got.Gaps) == 0) != (mode == "complete") {
				t.Fatalf("incorrect evidence completeness: %+v", got.Gaps)
			}
			if got.Value.(map[string]any)["peak_cgroup_memory_bytes"] != json.Number("9007199254740993") {
				t.Fatal("lost memory counter precision")
			}
		})
	}
}
