package diagnosticprobe

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/livediagnostics"
)

const hostProc = "/host/proc"

func readBounded(path string, limit int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return "", err
	}
	if int64(len(b)) > limit {
		return "", errors.New("source byte budget exceeded")
	}
	return string(b), nil
}
func nodeSnapshot(req livediagnostics.ProbeRequest) (any, error) {
	if _, err := os.Stat(hostProc); err != nil {
		return nil, errors.New("host process observation is not available for this capability profile")
	}
	values := map[string]any{"node": req.Target.Node, "sources": map[string]any{}, "missing_sources": map[string]string{}}
	for _, name := range []string{"pressure/cpu", "pressure/io", "pressure/memory", "stat", "loadavg", "diskstats", "uptime", "meminfo", "sys/kernel/random/boot_id", "sys/kernel/sched_schedstats"} {
		s, err := readBounded(filepath.Join(hostProc, name), 128<<10)
		if err != nil {
			values["missing_sources"].(map[string]string)[name] = boundedError(err)
		} else {
			values["sources"].(map[string]any)[name] = strings.TrimSpace(s)
		}
	}
	if missing := values["missing_sources"].(map[string]string); len(missing) > 0 {
		gaps := []string{}
		for name := range missing {
			gaps = append(gaps, "node source unavailable: "+name)
		}
		sort.Strings(gaps)
		return partialValue{Value: values, Gaps: gaps}, nil
	}
	return values, nil
}

type processFact struct {
	PID             int      `json:"pid"`
	Command         string   `json:"command"`
	StartTicks      string   `json:"start_ticks"`
	UserTicks       uint64   `json:"user_ticks"`
	SystemTicks     uint64   `json:"system_ticks"`
	Threads         string   `json:"threads"`
	Cgroup          string   `json:"cgroup"`
	RunNanoseconds  uint64   `json:"run_nanoseconds"`
	WaitNanoseconds uint64   `json:"runqueue_wait_nanoseconds"`
	Slices          uint64   `json:"timeslices"`
	ReadBytes       uint64   `json:"read_bytes"`
	WriteBytes      uint64   `json:"write_bytes"`
	Missing         []string `json:"missing"`
}

func processScheduling(req livediagnostics.ProbeRequest) (any, error) {
	entries, err := os.ReadDir(hostProc)
	if err != nil {
		return nil, err
	}
	facts := []processFact{}
	scanned := 0
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || !e.IsDir() {
			continue
		}
		scanned++
		if scanned > 4096 {
			break
		}
		path := filepath.Join(hostProc, e.Name())
		stat, err := readBounded(filepath.Join(path, "stat"), 16<<10)
		if err != nil {
			continue
		}
		f, err := parseProcessStat(pid, stat)
		if err != nil {
			continue
		}
		cg, _ := readBounded(filepath.Join(path, "cgroup"), 16<<10)
		f.Cgroup = strings.TrimSpace(cg)
		if req.ContainerID != "" {
			id := req.ContainerID
			if p := strings.Index(id, "://"); p >= 0 {
				id = id[p+3:]
			}
			if !strings.Contains(cg, id) {
				continue
			}
		}
		if req.Target.ProcessName != "" {
			exe, _ := os.Readlink(filepath.Join(path, "exe"))
			name := filepath.Base(exe)
			if name != req.Target.ProcessName && f.Command != req.Target.ProcessName && !(req.Target.ProcessName == "k3s" && f.Command == "k3s-server") {
				continue
			}
		}
		sched, err := readBounded(filepath.Join(path, "schedstat"), 4096)
		if err == nil {
			v := strings.Fields(sched)
			if len(v) >= 3 {
				f.RunNanoseconds, _ = strconv.ParseUint(v[0], 10, 64)
				f.WaitNanoseconds, _ = strconv.ParseUint(v[1], 10, 64)
				f.Slices, _ = strconv.ParseUint(v[2], 10, 64)
			}
		} else {
			f.Missing = append(f.Missing, "schedstat")
		}
		iostat, err := readBounded(filepath.Join(path, "io"), 4096)
		if err == nil {
			for _, line := range strings.Split(iostat, "\n") {
				v := strings.Fields(line)
				if len(v) != 2 {
					continue
				}
				n, _ := strconv.ParseUint(v[1], 10, 64)
				if v[0] == "read_bytes:" {
					f.ReadBytes = n
				}
				if v[0] == "write_bytes:" {
					f.WriteBytes = n
				}
			}
		} else {
			f.Missing = append(f.Missing, "io")
		}
		facts = append(facts, f)
	}
	if len(facts) == 0 && (req.ContainerID != "" || req.Target.ProcessName != "") {
		return nil, errors.New("frozen target process was not found in the current process observation")
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].PID < facts[j].PID })
	return map[string]any{"processes": facts, "scanned_processes": scanned, "scan_truncated": scanned > 4096, "scheduler_scope": "main thread; counters are cumulative, compare only identical pid and start_ticks", "note": "zero wait counters do not prove no contention when kernel scheduler statistics are disabled"}, nil
}
func parseProcessStat(pid int, s string) (processFact, error) {
	end := strings.LastIndex(s, ")")
	start := strings.Index(s, "(")
	if start < 0 || end < start {
		return processFact{}, errors.New("invalid process stat")
	}
	v := strings.Fields(s[end+1:])
	if len(v) < 20 {
		return processFact{}, errors.New("incomplete process stat")
	}
	user, e1 := strconv.ParseUint(v[11], 10, 64)
	system, e2 := strconv.ParseUint(v[12], 10, 64)
	if e1 != nil || e2 != nil {
		return processFact{}, fmt.Errorf("invalid process CPU counters")
	}
	return processFact{PID: pid, Command: s[start+1 : end], StartTicks: v[19], UserTicks: user, SystemTicks: system, Threads: v[17], Missing: []string{}}, nil
}
