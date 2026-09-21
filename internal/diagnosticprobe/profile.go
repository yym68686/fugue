package diagnosticprobe

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"

	"fugue/internal/livediagnostics"
)

func processCPUProfile(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	if c.CaptureSeconds < 5 || c.CaptureSeconds > 30 || c.CaptureSeconds+15 > req.DurationSeconds || c.IntervalSeconds < req.DurationSeconds {
		return nil, errors.New("CPU capture requires 5-30 seconds, 15 seconds analysis headroom and a single capture per session")
	}
	if (req.ContainerID == "") == (req.Target.ProcessName == "") {
		return nil, errors.New("CPU capture requires one frozen process or container target")
	}
	before, err := profileProcessIdentities(ctx, req)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "fugue-profile-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	args := []string{"--kind", "cpu-profile", "--duration", strconv.Itoa(c.CaptureSeconds), "--frequency", "19", "--output-dir", dir}
	if c.CallGraph != "" {
		if c.CallGraph != "fp" && c.CallGraph != "dwarf,8192" {
			return nil, errors.New("unsupported CPU call graph mode")
		}
		args = append(args, "--call-graph", c.CallGraph)
	}
	if req.ContainerID != "" {
		args = append(args, "--container-id", req.ContainerID)
	} else {
		args = append(args, "--process-name", req.Target.ProcessName)
	}
	raw, truncated, observation, err := observedCommandEnvironment(ctx, 8<<20, 192<<20, []string{"GOMEMLIMIT=128MiB"}, "/usr/local/bin/fugue-diagnostic-agent", args...)
	if err != nil {
		detail := boundedError(err)
		var failure struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(raw, &failure) == nil && failure.Error != "" {
			detail = safeText(failure.Error)
		}
		return partialValue{Value: map[string]any{"sampler_error": detail, "preflight": perfPreflight(), "sampler_resources": observation}, Gaps: []string{"CPU sampler failed: " + boundedError(errors.New(detail))}}, nil
	}
	if truncated {
		return nil, errors.New("CPU sampler output exceeded the transport budget")
	}
	after, err := profileProcessIdentities(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(before) != len(after) {
		return nil, errors.New("target process set changed during CPU capture")
	}
	for pid, start := range before {
		if after[pid] != start {
			return nil, errors.New("target process identity changed during CPU capture")
		}
	}
	value, err := profileEvidence(raw)
	if err != nil {
		return nil, err
	}
	if partial, ok := value.(partialValue); ok {
		partial.Value.(map[string]any)["sampler_resources"] = observation
		return partial, nil
	}
	value.(map[string]any)["sampler_resources"] = observation
	if len(observation.Missing) > 0 {
		return partialValue{Value: value, Gaps: observation.Missing}, nil
	}
	return value, nil
}

func profileProcessIdentities(ctx context.Context, req livediagnostics.ProbeRequest) (map[int]string, error) {
	v, err := processSchedulingAt(ctx, req, hostProc)
	if err != nil {
		return nil, err
	}
	if p, ok := v.(partialValue); ok {
		v = p.Value
	}
	facts := v.(map[string]any)["processes"].([]processFact)
	if len(facts) == 0 || len(facts) > 16 {
		return nil, errors.New("CPU capture requires 1-16 matching target processes")
	}
	result := map[int]string{}
	for _, f := range facts {
		result[f.PID] = f.StartTicks
	}
	return result, nil
}

func profileEvidence(raw []byte) (any, error) {
	var result map[string]any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if err := d.Decode(&result); err != nil {
		return nil, errors.New("CPU sampler did not return a valid report")
	}
	if result["schema"] != "fugue.diagnostic.cpu_profile.v1" {
		return nil, errors.New("CPU sampler returned an unexpected schema")
	}
	// The bounded structured sample table preserves every leaf count. Raw
	// duplicate text can be megabytes and is not needed to establish coverage.
	delete(result, "raw_script")
	delete(result, "perf_report")
	gaps := []string{}
	truncated := false
	if warnings, ok := result["warnings"].([]any); ok {
		for _, warning := range warnings {
			if s, ok := warning.(string); ok {
				gaps = append(gaps, safeText(s))
			}
		}
	}
	count := func(key string) int64 { n, _ := result[key].(json.Number); v, _ := n.Int64(); return v }
	if count("samples") <= 0 {
		gaps = append(gaps, "no CPU samples were captured")
	}
	if count("lost_samples") > 0 {
		gaps = append(gaps, "CPU samples were lost")
	}
	if count("unresolved_user_samples") > 0 || count("unresolved_kernel_samples") > 0 {
		gaps = append(gaps, "some sampled symbols could not be resolved")
	}
	if paths, ok := result["stack_paths"].(map[string]any); ok {
		observed, _ := paths["observed_samples"].(json.Number)
		n, _ := observed.Int64()
		truncated, _ = paths["truncated"].(bool)
		if truncated || n < count("stack_samples") {
			truncated = true
			gaps = append(gaps, "structured stack path coverage is incomplete")
		}
	}
	if len(gaps) > 0 {
		return partialValue{Value: result, Gaps: gaps, Truncated: truncated}, nil
	}
	return result, nil
}
