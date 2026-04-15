package api

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCollectRuntimeLogsFallsBackToPreviousContainerLogs(t *testing.T) {
	t.Parallel()

	fake := newFakeAppLogsClient()
	pod := fakePod("demo-abc", "Running", time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC), "demo")
	pod.Metadata.Namespace = "tenant-123"
	fake.setLogLines("tenant-123", "demo-abc", "demo", true, "previous line 1", "previous line 2")

	logs, warnings := collectRuntimeLogs(context.Background(), fake, "tenant-123", []kubePodInfo{pod}, "demo", 20, false)
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings, got %+v", warnings)
	}
	if !strings.Contains(logs, "==> demo-abc (previous) <==") || !strings.Contains(logs, "previous line 1") {
		t.Fatalf("expected previous log fallback, got %q", logs)
	}
}

func TestCollectRuntimeLogsFallsBackToFailureSummaryWhenLogsAreGone(t *testing.T) {
	t.Parallel()

	fake := newFakeAppLogsClient()
	pod := fakePod("demo-evicted", "Failed", time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC), "demo")
	pod.Metadata.Namespace = "tenant-123"
	pod.Spec.NodeName = "gcp1"
	pod.Status.Reason = "Evicted"
	pod.Status.Message = "The node had condition: [DiskPressure]."

	logs, warnings := collectRuntimeLogs(context.Background(), fake, "tenant-123", []kubePodInfo{pod}, "demo", 20, false)
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings, got %+v", warnings)
	}
	if !strings.Contains(logs, "pod demo-evicted on node gcp1 failed: Evicted: The node had condition: [DiskPressure].") {
		t.Fatalf("expected failure summary fallback, got %q", logs)
	}
}
