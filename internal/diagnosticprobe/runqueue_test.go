package diagnosticprobe

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/livediagnostics"
)

func TestSchedulerPairsExcludeBlockedTimeAndSortCrossCPUCaptures(t *testing.T) {
	// The wakeup appears after switch-in in the per-CPU output stream. Only
	// 3 ms of its 103 ms blocked interval was spent waiting for a CPU.
	raw := `10.000000000: sched:sched_switch: prev_comm=worker prev_pid=42 prev_prio=120 prev_state=S ==> next_comm=idle next_pid=0 next_prio=120
10.103000000: sched:sched_switch: prev_comm=idle prev_pid=0 prev_prio=120 prev_state=R ==> next_comm=worker next_pid=42 next_prio=120
10.100000000: sched:sched_wakeup: comm=worker pid=42 prio=120 target_cpu=001
10.104000000: sched:sched_switch: prev_comm=worker prev_pid=42 prev_prio=120 prev_state=R+ ==> next_comm=other next_pid=12 next_prio=120
10.109000000: sched:sched_switch: prev_comm=other prev_pid=12 prev_prio=120 prev_state=S ==> next_comm=worker next_pid=42 next_prio=120
10.110000000: sched:sched_wakeup: comm=outside pid=99 prio=120 target_cpu=000
`
	events, lost, invalid, cut := parseSchedulerEvents([]byte(raw))
	if lost != 0 || invalid != 0 || cut || len(events) != 6 {
		t.Fatalf("capture decode: %d %d %d %v", len(events), lost, invalid, cut)
	}
	out := schedulerLatencies(events, map[int]schedulerThread{42: {PID: 40, Start: "1"}})
	if out["completed_pairs"] != 2 || out["sum_thread_wait_ns"] != int64(8000000) || out["max_wait_ns"] != int64(5000000) || out["p50_wait_ns"] != int64(3000000) || out["p95_wait_ns"] != int64(5000000) || out["conflicting_transitions"] != 0 {
		t.Fatalf("blocked time or another thread entered runqueue time: %+v", out)
	}
	pairs := out["longest_pairs"].([]schedulerPair)
	if pairs[0].Reason != "preemption" || pairs[1].Reason != "wakeup" || pairs[0].PID != 40 {
		t.Fatalf("lost provenance: %+v", pairs)
	}
}

func TestSchedulerUnpairedAndConflictingTransitionsAreExplicit(t *testing.T) {
	out := schedulerLatencies([]schedulerEvent{{At: 1, Kind: "sched_switch", Next: 42}, {At: 2, Kind: "sched_wakeup", PID: 42}, {At: 3, Kind: "sched_wakeup", PID: 42}, {At: 4, Kind: "sched_switch", Next: 42}, {At: 5, Kind: "sched_wakeup", PID: 42}}, map[int]schedulerThread{42: {PID: 42, Start: "1"}})
	if out["completed_pairs"] != 0 || out["unpaired_switch_ins"] != 2 || out["conflicting_transitions"] != 1 || out["pending_at_end"] != 1 {
		t.Fatalf("fabricated latency from an incomplete pair: %+v", out)
	}
	if _, exists := out["max_wait_ns"]; exists {
		t.Fatal("no pairs reported as zero latency")
	}
	examples := out["conflict_examples"].([]map[string]any)
	if len(examples) != 1 || examples[0]["tid"] != 42 || len(examples[0]["preceding_events"].([]schedulerEvent)) != 2 {
		t.Fatalf("conflict context is missing: %+v", examples)
	}
}

func TestSchedulerEventLimitsAndMalformedLossCannotDisappear(t *testing.T) {
	valid := "1.123456789: sched:sched_wakeup: comm=worker pid=42 prio=120 target_cpu=000\n"
	events, lost, invalid, cut := parseSchedulerEvents([]byte(valid + "PERF_RECORD_LOST lost 4\nmalformed\n999999999999999.123456789: sched:sched_wakeup: pid=42 prio=120\n"))
	if len(events) != 1 || lost != 1 || invalid != 2 || cut {
		t.Fatalf("loss hidden %d %d %d %v", len(events), lost, invalid, cut)
	}
	_, _, _, cut = parseSchedulerEvents([]byte(strings.Repeat(valid, 32769)))
	if !cut {
		t.Fatal("unbounded scheduler events")
	}
}

func TestSchedulerFiltersIncludeExternalWakersAndNoGlobalSettingChange(t *testing.T) {
	args := schedulerPerfArgs(map[int]schedulerThread{42: {}, 41: {}}, 10, "/tmp/data")
	want := []string{"record", "-a", "-e", "sched:sched_switch", "--filter", "prev_pid == 41 || next_pid == 41 || prev_pid == 42 || next_pid == 42", "-e", "sched:sched_wakeup", "--filter", "pid == 41 || pid == 42"}
	if !reflect.DeepEqual(args[:len(want)], want) {
		t.Fatalf("incorrect target filters %v", args)
	}
	for _, arg := range args {
		if arg == "-G" || arg == "-p" {
			t.Fatal("external waker events would be lost")
		}
	}
	if _, err := processRunqueueLatency(context.Background(), livediagnostics.ProbeRequest{DurationSeconds: 60}, Collector{CaptureSeconds: 20, IntervalSeconds: 120}); err == nil {
		t.Fatal("capture without target permitted")
	}
	if _, err := schedulerTracefs(t.TempDir()); err == nil {
		t.Fatal("absent tracefs silently accepted")
	}
}

func TestSchedulerThreadsRejectReusedProcessAndBoundThreadSet(t *testing.T) {
	root := t.TempDir()
	stat := "42 (worker) S 1 2 3 4 5 6 7 8 9 10 111 222 13 14 15 16 7 18 9000 20\n"
	if err := os.MkdirAll(filepath.Join(root, "42/task/42"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"42/stat", "42/task/42/stat"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(stat), 0600); err != nil {
			t.Fatal(err)
		}
	}
	threads, err := schedulerThreads(root, map[int]string{42: "9000"})
	if err != nil || threads[42].Start != "9000" {
		t.Fatalf("thread identity: %+v %v", threads, err)
	}
	if _, err = schedulerThreads(root, map[int]string{42: "99"}); err == nil {
		t.Fatal("reused PID accepted")
	}
}

func TestSchedulerFailedRecordingStillReportsTruncation(t *testing.T) {
	file := filepath.Join(t.TempDir(), "perf.data")
	f, err := os.Create(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(schedulerCaptureLimit + 1024); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	result := map[string]any{}
	cut, gaps := schedulerRecordingQuality(result, file, false, errors.New("signal: terminated"))
	if !cut || result["capture_bytes"] != int64(schedulerCaptureLimit+1024) || len(gaps) != 2 {
		t.Fatalf("failed capture hid its bound: %+v %v %v", result, cut, gaps)
	}
	if err := os.Truncate(file, 100); err != nil {
		t.Fatal(err)
	}
	cut, gaps = schedulerRecordingQuality(result, file, false, nil)
	if cut || len(gaps) != 0 {
		t.Fatalf("complete capture degraded: %v %v", cut, gaps)
	}
	cut, gaps = schedulerRecordingQuality(result, file, true, nil)
	if !cut || len(gaps) != 1 {
		t.Fatal("output truncation ignored")
	}
}
