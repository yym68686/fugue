package diagnosticprobe

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
)

const faultCaptureLimit = 4 << 20

type processMapping struct {
	start, end, offset               uint64
	permissions, device, inode, path string
}

type faultProcess struct {
	start string
	major uint64
	maps  []processMapping
}

type faultEvent struct {
	At      time.Time `json:"at"`
	PID     int       `json:"pid"`
	TID     int       `json:"tid"`
	Address string    `json:"fault_address"`
	IP      string    `json:"instruction_address"`
	Mapping int       `json:"mapping_id"`
	address uint64
}

func processPageFaults(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	if req.Target.Type != livediagnostics.TargetNodeProcess || req.Target.ProcessName == "" || req.ContainerID != "" || c.CaptureSeconds < 5 || c.CaptureSeconds > 30 || c.CaptureSeconds+15 > req.DurationSeconds || c.IntervalSeconds < req.DurationSeconds {
		return nil, errors.New("page-fault capture requires one host process target, 5-30 seconds and 15 seconds analysis headroom")
	}
	identities, err := profileProcessIdentities(ctx, req)
	if err != nil {
		return nil, err
	}
	before, err := faultProcesses(hostProc, identities)
	if err != nil {
		return nil, err
	}
	cgroup, err := faultTargetCgroup(hostProc, "/sys/fs/cgroup", identities)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "fugue-faults-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	dataFile := filepath.Join(dir, "perf.data")
	started := time.Now().UTC()
	args := []string{"record", "-a", "-G", cgroup, "-e", "major-faults", "-c", "1", "-d", "-T", "--clockid", "CLOCK_REALTIME", "--max-size", "4M", "--mmap-pages", "64", "--no-buildid", "--no-buildid-cache", "--no-buildid-mmap", "-o", dataFile, "--", "sleep", strconv.Itoa(c.CaptureSeconds)}
	_, cut, recordResources, recordErr := observedCommand(ctx, 16<<10, 192<<20, "perf", args...)
	finished := time.Now().UTC()
	result := map[string]any{"schema": "fugue.process-page-faults.v1", "event": "major-faults", "sample_period": 1, "clock": "CLOCK_REALTIME", "started_at": started, "finished_at": finished, "requested_seconds": c.CaptureSeconds, "capture_cgroup": cgroup, "target_identities": identities, "capture_limit_bytes": faultCaptureLimit, "record_resources": recordResources,
		"scope": "captured major faults for frozen process IDs; mapping attribution requires identical address ranges, file identity and offsets before and after capture; fault addresses do not measure individual IO or lock duration"}
	gaps := []string{}
	truncated := cut
	if recordErr != nil {
		result["record_error"] = boundedError(recordErr)
		return partialValue{Value: result, Gaps: []string{"major-fault recording failed: " + boundedError(recordErr)}}, nil
	}
	if info, err := os.Stat(dataFile); err != nil {
		return nil, err
	} else {
		result["capture_bytes"] = info.Size()
		if info.Size() >= faultCaptureLimit {
			truncated = true
			gaps = append(gaps, "fault capture reached its data bound")
		}
	}
	if finished.Sub(started) < time.Duration(c.CaptureSeconds)*time.Second {
		gaps = append(gaps, "fault capture ended before its requested duration")
	}
	after, err := faultProcesses(hostProc, identities)
	if err != nil {
		gaps = append(gaps, "target state after capture unavailable: "+boundedError(err))
	}
	if current, err := faultTargetCgroup(hostProc, "/sys/fs/cgroup", identities); err != nil || current != cgroup {
		gaps = append(gaps, "target cgroup membership changed during capture")
	}
	raw, scriptCut, scriptResources, scriptErr := observedCommand(ctx, 2<<20, 192<<20, "perf", "script", "--ns", "--show-lost-events", "-i", dataFile, "-F", "pid,tid,time,addr,ip")
	result["script_resources"] = scriptResources
	truncated = truncated || scriptCut
	if scriptErr != nil {
		result["script_error"] = boundedError(scriptErr)
		gaps = append(gaps, "major-fault decoding failed: "+boundedError(scriptErr))
	}
	events, lost, invalid, recordsCut := parseFaultEvents(raw)
	truncated = truncated || recordsCut
	result["lost_record_notifications"], result["unparsed_lines"] = lost, invalid
	if lost > 0 {
		gaps = append(gaps, "perf reported lost records; fault counts are incomplete")
	}
	if invalid > 0 {
		gaps = append(gaps, "perf fault output contains unparsed records")
	}
	retained := []faultEvent{}
	groups := []map[string]any{}
	groupIDs := map[string]int{}
	outside, unresolved := 0, 0
	for _, event := range events {
		initial, ok := before[event.PID]
		if !ok {
			outside++
			continue
		}
		current, stable := after[event.PID]
		mapping, matched := stableFaultMapping(initial.maps, current.maps, event.address)
		if !stable || current.start != initial.start || !matched {
			unresolved++
			event.Mapping = -1
		} else {
			key := fmt.Sprintf("%d/%x/%x/%x/%s/%s/%s", event.PID, mapping.start, mapping.end, mapping.offset, mapping.device, mapping.inode, mapping.permissions)
			id, found := groupIDs[key]
			if !found {
				if len(groups) >= 256 {
					truncated = true
					unresolved++
					event.Mapping = -1
					retained = append(retained, event)
					continue
				}
				id = len(groups)
				groupIDs[key] = id
				groups = append(groups, map[string]any{"id": id, "pid": event.PID, "start": fmt.Sprintf("0x%x", mapping.start), "end": fmt.Sprintf("0x%x", mapping.end), "file_offset": fmt.Sprintf("0x%x", mapping.offset), "device": mapping.device, "inode": mapping.inode, "permissions": mapping.permissions, "path": safeText(mapping.path), "samples": 0})
			}
			groups[id]["samples"] = groups[id]["samples"].(int) + 1
			event.Mapping = id
		}
		if event.At.Before(started.Add(-time.Second)) || event.At.After(finished.Add(time.Second)) {
			gaps = append(gaps, "fault timestamp falls outside the recorded realtime window")
		}
		retained = append(retained, event)
	}
	deltas := map[int]uint64{}
	for pid, initial := range before {
		current, ok := after[pid]
		if !ok || current.start != initial.start || current.major < initial.major {
			gaps = append(gaps, "target identity or major-fault counter changed")
			continue
		}
		deltas[pid] = current.major - initial.major
	}
	if len(retained) == 0 {
		for _, delta := range deltas {
			if delta > 0 {
				gaps = append(gaps, "no events captured although process fault counters advanced in the surrounding window")
				break
			}
		}
	}
	result["events"], result["mappings"] = retained, groups
	result["target_samples"], result["outside_target_samples"], result["unresolved_mappings"] = len(retained), outside, unresolved
	result["surrounding_process_major_fault_deltas"] = deltas
	if unresolved > 0 {
		gaps = append(gaps, "some fault addresses have no stable mapping attribution")
	}
	if truncated {
		gaps = append(gaps, "fault evidence hit a capture or report bound")
	}
	gaps = append(gaps, recordResources.Missing...)
	gaps = append(gaps, scriptResources.Missing...)
	if len(gaps) > 0 {
		return partialValue{Value: result, Gaps: gaps, Truncated: truncated}, nil
	}
	return result, nil
}

func faultProcesses(procRoot string, identities map[int]string) (map[int]faultProcess, error) {
	result := map[int]faultProcess{}
	for pid, start := range identities {
		root := filepath.Join(procRoot, strconv.Itoa(pid))
		stat, err := readBounded(filepath.Join(root, "stat"), 16<<10)
		if err != nil {
			return result, err
		}
		fact, err := parseProcessStat(pid, stat)
		if err != nil || fact.StartTicks != start {
			return result, errors.New("target process identity changed")
		}
		raw, err := readBounded(filepath.Join(root, "maps"), 128<<10)
		if err != nil {
			return result, err
		}
		maps, err := parseProcessMappings(raw)
		if err != nil {
			return result, err
		}
		stat, err = readBounded(filepath.Join(root, "stat"), 16<<10)
		confirmed, parseErr := parseProcessStat(pid, stat)
		if err != nil || parseErr != nil || confirmed.StartTicks != start {
			return result, errors.New("target process changed while reading its mappings")
		}
		result[pid] = faultProcess{start: start, major: fact.MajorFaults, maps: maps}
	}
	return result, nil
}

func faultTargetCgroup(procRoot, cgroupRoot string, identities map[int]string) (string, error) {
	if len(identities) == 0 || len(identities) > 16 {
		return "", errors.New("fault capture requires 1-16 frozen processes")
	}
	selected := ""
	for pid := range identities {
		raw, err := readBounded(filepath.Join(procRoot, strconv.Itoa(pid), "cgroup"), 16<<10)
		if err != nil {
			return "", err
		}
		if !strings.HasPrefix(strings.TrimSpace(raw), "0::/") || strings.Count(strings.TrimSpace(raw), "\n") != 0 {
			return "", errors.New("fault capture requires a single cgroup-v2 membership")
		}
		path := strings.TrimPrefix(filepath.Clean("/"+strings.TrimPrefix(strings.TrimSpace(raw), "0::")), "/")
		if path == "" || selected != "" && selected != path {
			return "", errors.New("fault targets must share one non-root cgroup")
		}
		selected = path
	}
	raw, err := readBounded(filepath.Join(cgroupRoot, selected, "cgroup.procs"), 64<<10)
	if err != nil {
		return "", err
	}
	members := map[string]bool{}
	for _, pid := range strings.Fields(raw) {
		members[pid] = true
	}
	for pid := range identities {
		if !members[strconv.Itoa(pid)] {
			return "", errors.New("resolved cgroup does not contain every frozen target")
		}
	}
	return selected, nil
}

func parseProcessMappings(raw string) ([]processMapping, error) {
	maps := []processMapping{}
	for _, line := range strings.Split(raw, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 5 || len(maps) >= 2048 {
			return nil, errors.New("invalid or excessive process mapping records")
		}
		start, end, ok := strings.Cut(fields[0], "-")
		a, e1 := strconv.ParseUint(start, 16, 64)
		b, e2 := strconv.ParseUint(end, 16, 64)
		offset, e3 := strconv.ParseUint(fields[2], 16, 64)
		if !ok || e1 != nil || e2 != nil || e3 != nil || a >= b {
			return nil, errors.New("invalid process mapping addresses")
		}
		maps = append(maps, processMapping{a, b, offset, fields[1], fields[3], fields[4], strings.Join(fields[5:], " ")})
	}
	sort.Slice(maps, func(i, j int) bool { return maps[i].start < maps[j].start })
	for i := 1; i < len(maps); i++ {
		if maps[i].start < maps[i-1].end {
			return nil, errors.New("overlapping process mappings")
		}
	}
	return maps, nil
}

func stableFaultMapping(before, after []processMapping, address uint64) (processMapping, bool) {
	find := func(maps []processMapping) (processMapping, bool) {
		i := sort.Search(len(maps), func(i int) bool { return maps[i].end > address })
		if i == len(maps) || address < maps[i].start {
			return processMapping{}, false
		}
		return maps[i], true
	}
	a, ok := find(before)
	b, current := find(after)
	return a, ok && current && a == b
}

func parseFaultEvents(raw []byte) ([]faultEvent, int, int, bool) {
	events := []faultEvent{}
	lost, invalid := 0, 0
	cut := false
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	scanner.Buffer(make([]byte, 4096), 16<<10)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.Contains(line, "PERF_RECORD_LOST") {
			lost++
			continue
		}
		f := strings.Fields(line)
		if len(f) != 4 {
			invalid++
			continue
		}
		pid, tid, hasPID := strings.Cut(f[0], "/")
		p, e1 := strconv.Atoi(pid)
		t, e2 := strconv.Atoi(tid)
		seconds, nanos, hasTime := strings.Cut(strings.TrimSuffix(f[1], ":"), ".")
		s, e3 := strconv.ParseInt(seconds, 10, 64)
		ns, e4 := strconv.ParseInt(nanos, 10, 64)
		addr, e5 := strconv.ParseUint(f[2], 16, 64)
		ip, e6 := strconv.ParseUint(f[3], 16, 64)
		if !hasPID || !hasTime || !strings.HasSuffix(f[1], ":") || len(nanos) != 9 || p <= 0 || t <= 0 || s < 0 || ns < 0 || ns >= 1e9 || e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || e6 != nil {
			invalid++
			continue
		}
		if len(events) >= 8192 {
			cut = true
			continue
		}
		events = append(events, faultEvent{At: time.Unix(s, ns).UTC(), PID: p, TID: t, Address: fmt.Sprintf("0x%x", addr), IP: fmt.Sprintf("0x%x", ip), Mapping: -1, address: addr})
	}
	if scanner.Err() != nil {
		invalid++
		cut = true
	}
	return events, lost, invalid, cut
}
