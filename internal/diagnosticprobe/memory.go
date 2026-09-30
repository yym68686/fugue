package diagnosticprobe

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
)

// Use the existing bounded sampler through the signed package ABI. An exact
// container target also supports independently deployed authority cell workers.
func processMemoryProfile(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	if c.CaptureSeconds < 5 || c.CaptureSeconds > 120 || c.CaptureSeconds+30 > req.DurationSeconds || c.IntervalSeconds < req.DurationSeconds || req.ContainerID == "" || req.Target.ProcessName != "" {
		return nil, errors.New("memory capture requires an exact container, 5-120 seconds, 30 seconds analysis headroom and one capture per session")
	}
	before, err := profileProcessIdentities(ctx, req)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "fugue-memory-profile-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	args := []string{"--kind", "memory-profile", "--duration", strconv.Itoa(c.CaptureSeconds), "--sample-interval-ms", "1000", "--container-id", req.ContainerID, "--output-dir", dir}
	raw, truncated, resources, err := observedCommandEnvironment(ctx, 2<<20, 192<<20, []string{"GOMEMLIMIT=128MiB"}, "/usr/local/bin/fugue-diagnostic-agent", args...)
	if err != nil {
		detail := boundedError(err)
		var failure struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(raw, &failure) == nil && failure.Error != "" {
			detail = safeText(failure.Error)
		}
		return partialValue{Value: map[string]any{"sampler_error": detail, "sampler_resources": resources}, Gaps: []string{"memory sampler failed: " + detail}}, nil
	}
	if truncated {
		return nil, errors.New("memory sampler exceeded its output budget")
	}
	after, err := profileProcessIdentities(ctx, req)
	if err != nil || len(before) != len(after) {
		return nil, errors.New("target process set changed during memory capture")
	}
	for pid, start := range before {
		if after[pid] != start {
			return nil, errors.New("target process identity changed during memory capture")
		}
	}
	value, err := memoryProfileEvidence(raw, req.ContainerID, before, c.CaptureSeconds)
	if err != nil {
		return nil, err
	}
	value.Value.(map[string]any)["sampler_resources"] = resources
	value.Gaps = append(value.Gaps, resources.Missing...)
	if len(value.Gaps) > 0 {
		return value, nil
	}
	return value.Value, nil
}

func memoryProfileEvidence(raw []byte, container string, processes map[int]string, seconds int) (partialValue, error) {
	var report struct {
		Schema      string `json:"schema"`
		Container   string `json:"target_container_id"`
		PIDs        []int  `json:"target_pids"`
		GoProfile   bool   `json:"go_runtime_profile_available"`
		Heap        string `json:"go_inuse_space_top"`
		Allocations string `json:"go_alloc_space_delta_top"`
		Samples     []struct {
			At time.Time `json:"observed_at"`
		} `json:"samples"`
		Warnings []string `json:"warnings"`
	}
	if json.Unmarshal(raw, &report) != nil || report.Schema != "fugue.diagnostic.memory_profile.v1" || report.Container != container || len(report.PIDs) != len(processes) {
		return partialValue{}, errors.New("memory sampler returned another target or an invalid report")
	}
	seen := map[int]bool{}
	for _, pid := range report.PIDs {
		if processes[pid] == "" || seen[pid] {
			return partialValue{}, errors.New("memory sampler process identity differs from the frozen target")
		}
		seen[pid] = true
	}
	var value map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return partialValue{}, err
	}
	result := partialValue{Value: value}
	for _, warning := range report.Warnings {
		result.Gaps = append(result.Gaps, safeText(warning))
	}
	if !report.GoProfile || report.Heap == "" || report.Allocations == "" {
		result.Gaps = append(result.Gaps, "Go heap or allocation profile unavailable")
	}
	if len(report.Samples) < 2 || report.Samples[0].At.IsZero() || report.Samples[len(report.Samples)-1].At.Sub(report.Samples[0].At) < time.Duration(seconds-2)*time.Second {
		result.Gaps = append(result.Gaps, "memory sampling did not cover the requested window")
	}
	return result, nil
}
