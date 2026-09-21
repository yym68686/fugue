package diagnosticprobe

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
)

const schedulerCaptureLimit = 8 << 20

type schedulerThread struct {
	PID   int    `json:"pid"`
	Start string `json:"start_ticks"`
}

// Select only threads belonging to the frozen process lifetimes. Newly created
// threads are deliberately outside this capture's kernel filter.
func schedulerThreads(proc string, processes map[int]string) (map[int]schedulerThread, error) {
	out := map[int]schedulerThread{}
	for pid, start := range processes {
		base := filepath.Join(proc, strconv.Itoa(pid))
		raw, err := readBounded(filepath.Join(base, "stat"), 16<<10)
		fact, parseErr := parseProcessStat(pid, raw)
		if err != nil || parseErr != nil || fact.StartTicks != start {
			return nil, errors.New("scheduler process lifetime changed")
		}
		entries, err := os.ReadDir(filepath.Join(base, "task"))
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			tid, err := strconv.Atoi(entry.Name())
			if err != nil || tid <= 0 {
				continue
			}
			raw, err := readBounded(filepath.Join(base, "task", entry.Name(), "stat"), 16<<10)
			fact, parseErr := parseProcessStat(tid, raw)
			if err != nil || parseErr != nil {
				return nil, errors.New("scheduler thread identity unavailable")
			}
			out[tid] = schedulerThread{PID: pid, Start: fact.StartTicks}
			if len(out) > 128 {
				return nil, errors.New("scheduler capture exceeds 128 frozen threads")
			}
		}
		raw, err = readBounded(filepath.Join(base, "stat"), 16<<10)
		fact, parseErr = parseProcessStat(pid, raw)
		if err != nil || parseErr != nil || fact.StartTicks != start {
			return nil, errors.New("scheduler process changed during thread discovery")
		}
	}
	if len(out) == 0 {
		return nil, errors.New("scheduler capture has no target threads")
	}
	return out, nil
}

func schedulerTracefs(proc string) (string, error) {
	for _, suffix := range []string{"sys/kernel/tracing", "sys/kernel/debug/tracing"} {
		base := filepath.Join(proc, "1/root", suffix)
		valid := true
		for _, event := range []string{"sched_switch", "sched_wakeup"} {
			id, err := readBounded(filepath.Join(base, "events/sched", event, "id"), 64)
			n, parseErr := strconv.Atoi(strings.TrimSpace(id))
			if err != nil || parseErr != nil || n <= 0 {
				valid = false
				break
			}
		}
		if valid {
			return base, nil
		}
	}
	return "", errors.New("existing host scheduler tracepoints are unavailable; no filesystem or global scheduler settings were changed")
}

func schedulerPerfArgs(threads map[int]schedulerThread, seconds int, output string) []string {
	tids := make([]int, 0, len(threads))
	for tid := range threads {
		tids = append(tids, tid)
	}
	sort.Ints(tids)
	switches, wakes := []string{}, []string{}
	for _, tid := range tids {
		switches = append(switches, fmt.Sprintf("prev_pid == %d || next_pid == %d", tid, tid))
		wakes = append(wakes, fmt.Sprintf("pid == %d", tid))
	}
	// A wakeup runs in the waker's context, so cgroup filtering would lose
	// precisely the cross-cgroup wakeups needed to establish runnable latency.
	// Thread identity is read directly from procfs. Symbol/mapping synthesis
	// adds unrelated system-wide metadata to this event-only capture.
	return []string{"record", "-a", "-e", "sched:sched_switch", "--filter", strings.Join(switches, " || "), "-e", "sched:sched_wakeup", "--filter", strings.Join(wakes, " || "), "--synth=no", "--clockid", "CLOCK_MONOTONIC", "--max-size", "8M", "--mmap-pages", "64", "--no-buildid", "--no-buildid-cache", "--no-buildid-mmap", "-o", output, "--", "sleep", strconv.Itoa(seconds)}
}

func schedulerRecordingQuality(result map[string]any, file string, cut bool, recordErr error) (bool, []string) {
	gaps := []string{}
	if info, err := os.Stat(file); err == nil {
		result["capture_bytes"] = info.Size()
		cut = cut || info.Size() >= schedulerCaptureLimit
	} else {
		gaps = append(gaps, "scheduler capture file unavailable")
	}
	if recordErr != nil {
		gaps = append(gaps, "scheduler recording failed: "+boundedError(recordErr))
	}
	if cut {
		gaps = append(gaps, "scheduler recording reached a capture or output bound")
	}
	return cut, gaps
}

func processRunqueueLatency(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	if (req.ContainerID == "") == (req.Target.ProcessName == "") || c.CaptureSeconds < 5 || c.CaptureSeconds > 20 || c.CaptureSeconds+15 > req.DurationSeconds || c.IntervalSeconds < req.DurationSeconds {
		return nil, errors.New("scheduler capture requires one process or container target, 5-20 seconds and 15 seconds analysis headroom")
	}
	processes, err := profileProcessIdentities(ctx, req)
	if err != nil {
		return nil, err
	}
	before, err := schedulerThreads(hostProc, processes)
	if err != nil {
		return nil, err
	}
	tracefs, err := schedulerTracefs(hostProc)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "fugue-scheduler-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	file := filepath.Join(dir, "perf.data")
	started := time.Now().UTC()
	_, cut, recording, err := observedCommandEnvironment(ctx, 16<<10, 192<<20, []string{"TRACEFS_PATH=" + tracefs}, "perf", schedulerPerfArgs(before, c.CaptureSeconds, file)...)
	finished := time.Now().UTC()
	result := map[string]any{"schema": "fugue.process-runqueue-latency.v1", "started_at": started, "finished_at": finished, "clock": "CLOCK_MONOTONIC", "requested_seconds": c.CaptureSeconds, "target_threads": before, "capture_limit_bytes": schedulerCaptureLimit, "record_resources": recording,
		"scope": "completed wakeup-to-switch-in and runnable-switch-out-to-switch-in pairs for frozen thread lifetimes; blocked time is excluded; unpaired boundary events and new threads are not assigned zero latency"}
	cut, gaps := schedulerRecordingQuality(result, file, cut, err)
	gaps = append(gaps, recording.Missing...)
	if err != nil || cut {
		return partialValue{Value: result, Gaps: gaps, Truncated: cut}, nil
	}
	if finished.Sub(started) < time.Duration(c.CaptureSeconds)*time.Second {
		gaps = append(gaps, "scheduler capture ended before requested duration")
	}
	after, identityErr := schedulerThreads(hostProc, processes)
	stable := map[int]schedulerThread{}
	for tid, identity := range before {
		if after[tid] == identity {
			stable[tid] = identity
		}
	}
	result["stable_thread_count"], result["changed_or_exited_thread_count"] = len(stable), len(before)-len(stable)
	added := 0
	for tid := range after {
		if _, ok := before[tid]; !ok {
			added++
		}
	}
	result["new_threads_outside_capture"] = added
	if identityErr != nil || len(stable) != len(before) || added > 0 {
		gaps = append(gaps, "target thread coverage changed; only stable frozen thread lifetimes are analyzed")
	}
	raw, scriptCut, decoding, err := observedCommand(ctx, 4<<20, 192<<20, "perf", "script", "--ns", "--show-lost-events", "-i", file, "-F", "time,event,trace")
	result["script_resources"] = decoding
	gaps = append(gaps, decoding.Missing...)
	cut = cut || scriptCut
	if err != nil {
		gaps = append(gaps, "scheduler decoding failed: "+boundedError(err))
	}
	events, lost, invalid, eventCut := parseSchedulerEvents(raw)
	cut = cut || eventCut
	result["captured_events"], result["lost_record_notifications"], result["unparsed_lines"] = len(events), lost, invalid
	latencies := schedulerLatencies(events, stable)
	result["latencies"] = latencies
	conflicts := latencies["conflicting_transitions"].(int)
	if conflicts > 0 {
		gaps = append(gaps, "conflicting scheduler transitions prevent complete latency attribution")
	}
	if len(events) == 0 {
		gaps = append(gaps, "no scheduler events captured; absent events do not establish zero latency")
	}
	if lost > 0 || invalid > 0 {
		gaps = append(gaps, "lost or unparsed scheduler events prevent complete latency attribution")
	}
	if cut {
		gaps = append(gaps, "scheduler capture or decoding reached its bound")
	}
	// A missing transition can fabricate a long pair. Retain the capture facts,
	// but withhold durations altogether when event-stream continuity is lost.
	if lost > 0 || invalid > 0 || cut || err != nil || conflicts > 0 {
		delete(result, "latencies")
	}
	if len(gaps) > 0 {
		return partialValue{Value: result, Gaps: gaps, Truncated: cut}, nil
	}
	return result, nil
}

type schedulerEvent struct {
	At              int64
	Kind            string
	PID, Prev, Next int
	Runnable        bool
}

var schedulerHeader = regexp.MustCompile(`^\s*([0-9]+)\.([0-9]{9}):\s+sched:(sched_switch|sched_wakeup):\s+(.*)$`)
var schedulerWake = regexp.MustCompile(`(?:^|\s)pid=([0-9]+)\s`)
var schedulerSwitch = regexp.MustCompile(`\bprev_pid=([0-9]+)\s+prev_prio=[0-9]+\s+prev_state=([^\s]+)\s+==>.*\bnext_pid=([0-9]+)\s`)

func parseSchedulerEvents(raw []byte) ([]schedulerEvent, int, int, bool) {
	events := []schedulerEvent{}
	lost, invalid := 0, 0
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	scanner.Buffer(make([]byte, 4096), 32<<10)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if strings.Contains(line, "PERF_RECORD_LOST") || strings.Contains(line, "LOST") {
			lost++
			continue
		}
		m := schedulerHeader.FindStringSubmatch(line)
		if len(m) == 0 {
			invalid++
			continue
		}
		sec, err := strconv.ParseInt(m[1], 10, 64)
		nsec, _ := strconv.ParseInt(m[2], 10, 64)
		if err != nil || sec > (1<<63-1-nsec)/1e9 {
			invalid++
			continue
		}
		e := schedulerEvent{At: sec*1e9 + nsec, Kind: m[3]}
		if e.Kind == "sched_wakeup" {
			v := schedulerWake.FindStringSubmatch(m[4] + " ")
			if len(v) == 0 {
				invalid++
				continue
			}
			e.PID, err = strconv.Atoi(v[1])
			if err != nil || e.PID <= 0 {
				invalid++
				continue
			}
		} else {
			v := schedulerSwitch.FindStringSubmatch(m[4] + " ")
			if len(v) == 0 {
				invalid++
				continue
			}
			e.Prev, err = strconv.Atoi(v[1])
			if err != nil {
				invalid++
				continue
			}
			e.Next, err = strconv.Atoi(v[3])
			if err != nil {
				invalid++
				continue
			}
			e.Runnable = v[2] == "R" || v[2] == "R+"
		}
		if len(events) >= 32768 {
			return events, lost, invalid, true
		}
		events = append(events, e)
	}
	if scanner.Err() != nil {
		invalid++
	}
	// Events arrive from per-CPU buffers; ordering by the shared monotonic clock
	// is necessary for a wakeup and switch-in on different CPUs.
	sort.SliceStable(events, func(i, j int) bool { return events[i].At < events[j].At })
	return events, lost, invalid, false
}

type schedulerPair struct {
	TID    int    `json:"tid"`
	PID    int    `json:"pid"`
	Reason string `json:"reason"`
	Start  int64  `json:"runnable_monotonic_ns"`
	End    int64  `json:"running_monotonic_ns"`
	Wait   int64  `json:"wait_ns"`
}

func schedulerLatencies(events []schedulerEvent, threads map[int]schedulerThread) map[string]any {
	pending := map[int]schedulerPair{}
	pairs := []schedulerPair{}
	unpaired, conflicts := 0, 0
	for _, e := range events {
		if e.Kind == "sched_wakeup" {
			if identity, ok := threads[e.PID]; ok {
				if _, exists := pending[e.PID]; exists {
					conflicts++
					delete(pending, e.PID)
					continue
				}
				pending[e.PID] = schedulerPair{TID: e.PID, PID: identity.PID, Reason: "wakeup", Start: e.At}
			}
			continue
		}
		if _, ok := threads[e.Next]; ok {
			if pair, exists := pending[e.Next]; exists && e.At >= pair.Start {
				pair.End, pair.Wait = e.At, e.At-pair.Start
				pairs = append(pairs, pair)
			} else {
				unpaired++
			}
			delete(pending, e.Next)
		}
		if identity, ok := threads[e.Prev]; ok {
			if _, exists := pending[e.Prev]; exists {
				conflicts++
			}
			delete(pending, e.Prev)
			if e.Runnable {
				pending[e.Prev] = schedulerPair{TID: e.Prev, PID: identity.PID, Reason: "preemption", Start: e.At}
			}
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Wait > pairs[j].Wait })
	total := int64(0)
	for _, pair := range pairs {
		total += pair.Wait
	}
	out := map[string]any{"completed_pairs": len(pairs), "unpaired_switch_ins": unpaired, "pending_at_end": len(pending), "conflicting_transitions": conflicts, "sum_thread_wait_ns": total, "longest_pairs": pairs[:min(len(pairs), 20)]}
	if len(pairs) > 0 {
		out["max_wait_ns"] = pairs[0].Wait
		for _, q := range []int{50, 95, 99} {
			rank := (len(pairs)*q + 99) / 100
			out[fmt.Sprintf("p%d_wait_ns", q)] = pairs[len(pairs)-rank].Wait
		}
	}
	return out
}
