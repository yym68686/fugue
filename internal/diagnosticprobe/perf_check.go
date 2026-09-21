package diagnosticprobe

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/livediagnostics"
)

func perfCaptureCheck(ctx context.Context, req livediagnostics.ProbeRequest) (any, error) {
	if req.DurationSeconds < 30 || (req.ContainerID == "" && req.Target.ProcessName == "") {
		return nil, errors.New("perf comparison requires an exact process target and at least 30 seconds")
	}
	before, err := profileProcessIdentities(ctx, req)
	if err != nil {
		return nil, err
	}
	pids := make([]int, 0, len(before))
	for pid := range before {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	pidNames := []string{}
	for _, pid := range pids {
		pidNames = append(pidNames, strconv.Itoa(pid))
	}
	cg, err := readBounded(filepath.Join(hostProc, strconv.Itoa(pids[0]), "cgroup"), 16<<10)
	if err != nil {
		return nil, err
	}
	_, path, _ := strings.Cut(strings.TrimSpace(cg), "0::")
	path = strings.TrimPrefix(filepath.Clean("/"+path), "/")
	data, err := readBounded(filepath.Join("/sys/fs/cgroup", path, "cgroup.procs"), 32<<10)
	if err != nil {
		return nil, err
	}
	for _, pid := range pidNames {
		if !strings.Contains("\n"+data, "\n"+pid+"\n") {
			return nil, errors.New("cgroup membership does not match frozen PID set")
		}
	}
	dir, err := os.MkdirTemp("", "fugue-perf-check-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	results := []any{}
	gaps := []string{}
	for _, mode := range []string{"cgroup", "pid"} {
		file := filepath.Join(dir, mode+".data")
		args := []string{"record", "-e", "cpu-clock", "-F", "19", "--no-buildid-mmap", "-o", file}
		if mode == "cgroup" {
			args = append(args, "-a", "-G", path)
		} else {
			args = append(args, "-p", strings.Join(pidNames, ","))
		}
		args = append(args, "--", "sleep", "5")
		_, stderr, truncated, err := diagnosticCommandEvidence(ctx, 16<<10, "perf", args...)
		row := map[string]any{"mode": mode, "record_stderr": stderr}
		if err != nil {
			row["error"] = boundedError(err)
			gaps = append(gaps, mode+" record failed")
		}
		if truncated {
			gaps = append(gaps, mode+" record output truncated")
		}
		if info, err := os.Stat(file); err == nil {
			row["data_bytes"] = info.Size()
		}
		raw, reportErr, cut, err := diagnosticCommandEvidence(ctx, 64<<10, "perf", "report", "--stdio", "--no-children", "--call-graph", "none", "--percent-limit", "0", "-i", file)
		row["report"], row["report_stderr"] = safeText(string(raw)), reportErr
		if err != nil {
			row["report_error"] = boundedError(err)
			gaps = append(gaps, mode+" report failed")
		}
		if cut {
			gaps = append(gaps, mode+" report truncated")
		}
		results = append(results, row)
	}
	after, err := profileProcessIdentities(ctx, req)
	if err != nil || len(before) != len(after) {
		gaps = append(gaps, "process identities changed during comparison")
	}
	for pid, start := range before {
		if after[pid] != start {
			gaps = append(gaps, "process identity changed during comparison")
			break
		}
	}
	value := map[string]any{"pids": pids, "cgroup": path, "captures": results, "preflight": perfPreflight()}
	if len(gaps) > 0 {
		return partialValue{Value: value, Gaps: gaps}, nil
	}
	return value, nil
}
