package diagnosticprobe

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type commandProcess struct {
	PID        int    `json:"pid"`
	StartTicks string `json:"start_ticks"`
	Command    string `json:"command"`
	Stage      string `json:"stage,omitempty"`
	RSSBytes   uint64 `json:"rss_bytes"`
}

type commandMemorySample struct {
	At        time.Time        `json:"at"`
	Processes []commandProcess `json:"processes"`
	RSSBytes  uint64           `json:"rss_bytes"`
}

type commandObservation struct {
	Samples          []commandMemorySample `json:"samples"`
	RSSLimitBytes    uint64                `json:"rss_limit_bytes"`
	PeakRSSBytes     uint64                `json:"peak_rss_bytes"`
	StoppedForMemory bool                  `json:"stopped_for_memory"`
	Missing          []string              `json:"missing"`
}

// Observe only the launched command and its descendants. No target command
// lines or environment values are exported. The sampler is killed as a group
// before its RSS consumes the whole diagnostic container memory allowance.
func observedCommand(ctx context.Context, limit int, rssLimit uint64, name string, args ...string) ([]byte, bool, commandObservation, error) {
	return observedCommandEnvironment(ctx, limit, rssLimit, nil, name, args...)
}

func observedCommandEnvironment(ctx context.Context, limit int, rssLimit uint64, environment []string, name string, args ...string) ([]byte, bool, commandObservation, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), environment...)
	configureCommandCancellation(cmd)
	cmd.WaitDelay = time.Second
	out, stderr := &boundedOutput{limit: limit}, &boundedOutput{limit: 4096}
	cmd.Stdout, cmd.Stderr = out, stderr
	obs := commandObservation{RSSLimitBytes: rssLimit, Samples: []commandMemorySample{}, Missing: []string{}}
	if err := cmd.Start(); err != nil {
		return nil, false, obs, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			if obs.StoppedForMemory {
				err = errors.New("sampler process tree exceeded diagnostic RSS budget")
			}
			if err != nil && len(stderr.data) > 0 {
				err = fmt.Errorf("%w: %s", err, safeText(string(stderr.data)))
			}
			return out.data, out.truncated, obs, err
		case <-ticker.C:
			sample, err := commandMemoryAt("/proc", cmd.Process.Pid)
			if err != nil {
				if len(obs.Missing) == 0 {
					obs.Missing = append(obs.Missing, boundedError(err))
				}
				continue
			}
			obs.PeakRSSBytes = max(obs.PeakRSSBytes, sample.RSSBytes)
			// Retain phase transitions and a one-second cadence, bounded to 512.
			if len(obs.Samples) < 512 && (len(obs.Samples) == 0 || sample.At.Sub(obs.Samples[len(obs.Samples)-1].At) >= time.Second || processStagesChanged(obs.Samples[len(obs.Samples)-1], sample) || sample.RSSBytes >= rssLimit) {
				obs.Samples = append(obs.Samples, sample)
			}
			if sample.RSSBytes >= rssLimit && !obs.StoppedForMemory {
				obs.StoppedForMemory = true
				_ = cmd.Cancel()
			}
		}
	}
}

func processStagesChanged(a, b commandMemorySample) bool {
	if len(a.Processes) != len(b.Processes) {
		return true
	}
	for i, p := range a.Processes {
		q := b.Processes[i]
		if p.PID != q.PID || p.StartTicks != q.StartTicks || p.Stage != q.Stage {
			return true
		}
	}
	return false
}

func commandMemoryAt(root string, pid int) (commandMemorySample, error) {
	out := commandMemorySample{At: time.Now().UTC(), Processes: []commandProcess{}}
	queue := []int{pid}
	seen := map[int]bool{}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		if seen[p] {
			continue
		}
		seen[p] = true
		if len(seen) > 64 {
			return out, errors.New("sampler process tree exceeded observation limit")
		}
		dir := filepath.Join(root, strconv.Itoa(p))
		stat, err := readBounded(filepath.Join(dir, "stat"), 16<<10)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return out, err
		}
		fact, err := parseProcessStat(p, stat)
		if err != nil {
			return out, err
		}
		status, err := readBounded(filepath.Join(dir, "status"), 32<<10)
		if err != nil {
			continue
		}
		row := commandProcess{PID: p, StartTicks: fact.StartTicks, Command: fact.Command}
		for _, line := range strings.Split(status, "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				fields := strings.Fields(line)
				if len(fields) == 3 {
					v, err := strconv.ParseUint(fields[1], 10, 64)
					if err == nil {
						row.RSSBytes = v * 1024
					}
				}
			}
		}
		if fact.Command == "perf" {
			args, _ := readBounded(filepath.Join(dir, "cmdline"), 4096)
			fields := strings.Split(args, "\x00")
			if len(fields) > 1 {
				switch fields[1] {
				case "record", "script", "report":
					row.Stage = fields[1]
				}
			}
		}
		out.Processes = append(out.Processes, row)
		out.RSSBytes += row.RSSBytes
		tasks, err := os.ReadDir(filepath.Join(dir, "task"))
		if err != nil && !os.IsNotExist(err) {
			return out, err
		}
		if len(tasks) > 128 {
			return out, errors.New("sampler thread observation limit exceeded")
		}
		for _, task := range tasks {
			children, err := readBounded(filepath.Join(dir, "task", task.Name(), "children"), 16<<10)
			if err != nil && !os.IsNotExist(err) {
				return out, err
			}
			for _, child := range strings.Fields(children) {
				v, err := strconv.Atoi(child)
				if err == nil && v > 1 {
					queue = append(queue, v)
				}
			}
		}
	}
	return out, nil
}
