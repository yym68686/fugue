package diagnosticprobe

import (
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
)

type nodeWindow struct {
	Sources map[string]string `json:"sources"`
}

type processWindow struct {
	Processes []processFact `json:"processes"`
}

type processDelta struct {
	PID         int     `json:"pid"`
	StartTicks  string  `json:"start_ticks"`
	Command     string  `json:"command"`
	CPUTicks    uint64  `json:"cpu_ticks"`
	CPUCores    float64 `json:"average_cpu_cores"`
	Cgroup      string  `json:"cgroup"`
	MinorFaults uint64  `json:"minor_faults"`
	MajorFaults uint64  `json:"major_faults"`
}

func cpuCounters(stat string) ([]uint64, int, error) {
	var ticks []uint64
	cpus := 0
	for _, line := range strings.Split(stat, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		if parts[0] == "cpu" {
			if len(parts) < 9 {
				return nil, 0, errors.New("node CPU counters are incomplete")
			}
			// guest time is already included in user/nice; do not count it twice.
			for _, s := range parts[1:9] {
				x, err := strconv.ParseUint(s, 10, 64)
				if err != nil {
					return nil, 0, errors.New("invalid node CPU counter")
				}
				ticks = append(ticks, x)
			}
		} else if strings.HasPrefix(parts[0], "cpu") {
			if _, err := strconv.Atoi(strings.TrimPrefix(parts[0], "cpu")); err == nil {
				cpus++
			}
		}
	}
	if len(ticks) != 8 || cpus == 0 {
		return nil, 0, errors.New("node CPU topology is unavailable")
	}
	return ticks, cpus, nil
}

func summarizeNodeWindow(first, last livediagnostics.Evidence) (map[string]any, error) {
	var before, after nodeWindow
	if err := json.Unmarshal(first.Data, &before); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(last.Data, &after); err != nil {
		return nil, err
	}
	bootID := before.Sources["sys/kernel/random/boot_id"]
	if bootID == "" || bootID != after.Sources["sys/kernel/random/boot_id"] || !last.ObservedAt.After(first.ObservedAt) {
		return nil, errors.New("node reboot or invalid sample window prevents a counter comparison")
	}
	a, cpus, err := cpuCounters(before.Sources["stat"])
	if err != nil {
		return nil, err
	}
	b, endCPUs, err := cpuCounters(after.Sources["stat"])
	if err != nil || cpus != endCPUs {
		return nil, errors.New("node CPU topology changed or is unavailable")
	}
	var total uint64
	deltas := make([]uint64, len(a))
	for i := range a {
		if b[i] < a[i] {
			return nil, errors.New("node CPU counters moved backwards")
		}
		deltas[i] = b[i] - a[i]
		total += deltas[i]
	}
	if total == 0 {
		return nil, errors.New("node CPU counters did not advance")
	}
	percentages := map[string]float64{}
	for i, key := range []string{"user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"} {
		percentages[key] = float64(deltas[i]) * 100 / float64(total)
	}
	return map[string]any{"start": first.ObservedAt, "end": last.ObservedAt, "duration_seconds": last.ObservedAt.Sub(first.ObservedAt).Seconds(), "boot_id": bootID, "logical_cpus": cpus, "cpu_tick_delta": total, "cpu_time_percent": percentages}, nil
}

func appendWindowSummary(report *livediagnostics.ProbeReport) {
	var nodes, processes []livediagnostics.Evidence
	for _, evidence := range report.Evidence {
		if len(evidence.Data) == 0 {
			continue
		}
		switch evidence.Source {
		case "node-snapshot":
			nodes = append(nodes, evidence)
		case "process-scheduling":
			processes = append(processes, evidence)
		}
	}
	if len(nodes) < 2 {
		return
	}
	value, err := summarizeNodeWindow(nodes[0], nodes[len(nodes)-1])
	if err != nil {
		report.Quality.Status = "degraded"
		appendGap(report, "node window: "+err.Error())
		return
	}
	if len(processes) >= 2 {
		first, last := processes[0], processes[len(processes)-1]
		// Do not mix substantially different collection windows.
		if first.ObservedAt.Sub(nodes[0].ObservedAt).Abs() < time.Second && last.ObservedAt.Sub(nodes[len(nodes)-1].ObservedAt).Abs() < time.Second {
			var a, b processWindow
			if json.Unmarshal(first.Data, &a) == nil && json.Unmarshal(last.Data, &b) == nil {
				before := map[int]processFact{}
				for _, fact := range a.Processes {
					before[fact.PID] = fact
				}
				deltas := []processDelta{}
				for _, fact := range b.Processes {
					old, ok := before[fact.PID]
					if !ok || old.StartTicks != fact.StartTicks || old.UserTicks > fact.UserTicks || old.SystemTicks > fact.SystemTicks || old.MinorFaults > fact.MinorFaults || old.MajorFaults > fact.MajorFaults {
						continue
					}
					ticks := fact.UserTicks - old.UserTicks + fact.SystemTicks - old.SystemTicks
					deltas = append(deltas, processDelta{PID: fact.PID, StartTicks: fact.StartTicks, Command: fact.Command, CPUTicks: ticks, CPUCores: float64(ticks) * float64(value["logical_cpus"].(int)) / float64(value["cpu_tick_delta"].(uint64)), Cgroup: fact.Cgroup, MinorFaults: fact.MinorFaults - old.MinorFaults, MajorFaults: fact.MajorFaults - old.MajorFaults})
				}
				sort.Slice(deltas, func(i, j int) bool { return deltas[i].CPUTicks > deltas[j].CPUTicks })
				if len(deltas) > 30 {
					deltas = deltas[:30]
				}
				value["top_process_cpu"] = deltas
				value["process_scope"] = "processes present with identical PID/start time in both snapshots; CPU cores normalized against node ticks; newly started or exited processes are excluded"
				value["fault_scope"] = "per-process fault deltas excluding child processes; top-process ordering is by CPU, not fault count; a major fault alone does not identify the backing file or prove a request's cause"
			}
		}
	}
	data, err := json.Marshal(value)
	if err != nil || len(data) > 16<<10 {
		return
	}
	report.Evidence = append(report.Evidence, livediagnostics.Evidence{Name: "node-window", Source: "derived-counter-delta", ObservedAt: nodes[len(nodes)-1].ObservedAt, Status: "complete", Data: data})
}
