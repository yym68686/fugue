package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultDurationSeconds = 60
	defaultFrequency       = 19
	maxDurationSeconds     = 120
	maxFrequency           = 99
	maxCgroupSearchEntries = 20000
	hostCgroupRoot         = "/sys/fs/cgroup"
)

type options struct {
	kind        string
	duration    int
	frequency   int
	containerID string
	outputDir   string
}

type report struct {
	Schema              string           `json:"schema"`
	Kind                string           `json:"kind"`
	StartedAt           time.Time        `json:"started_at"`
	FinishedAt          time.Time        `json:"finished_at"`
	DurationSeconds     int              `json:"duration_seconds"`
	Frequency           int              `json:"frequency"`
	TargetPID           int              `json:"target_pid"`
	TargetPIDs          []int            `json:"target_pids"`
	TargetContainerID   string           `json:"target_container_id"`
	TargetProcess       string           `json:"target_process,omitempty"`
	TargetProcesses     []string         `json:"target_processes,omitempty"`
	TargetCgroup        string           `json:"target_cgroup,omitempty"`
	ProcessBefore       processSnapshot  `json:"process_before"`
	ProcessAfter        processSnapshot  `json:"process_after"`
	CgroupBefore        cgroupSnapshot   `json:"cgroup_before"`
	CgroupAfter         cgroupSnapshot   `json:"cgroup_after"`
	CPUUsage            cpuUsage         `json:"cpu_usage"`
	Samples             int              `json:"samples"`
	UserSamples         int              `json:"user_samples"`
	KernelSamples       int              `json:"kernel_samples"`
	LostSamples         int              `json:"lost_samples"`
	LeafFunctions       []functionSample `json:"leaf_functions"`
	CumulativeFunctions []functionSample `json:"cumulative_functions"`
	PerfReport          string           `json:"perf_report,omitempty"`
	RawScript           string           `json:"raw_script,omitempty"`
	Warnings            []string         `json:"warnings,omitempty"`
}

type functionSample struct {
	Function string `json:"function"`
	Samples  int    `json:"samples"`
}

type processSnapshot struct {
	RSSBytes uint64 `json:"rss_bytes"`
	Threads  uint64 `json:"threads"`
	OpenFDs  uint64 `json:"open_fds"`
}

type cgroupSnapshot struct {
	UsageUsec     uint64 `json:"usage_usec"`
	UserUsec      uint64 `json:"user_usec"`
	SystemUsec    uint64 `json:"system_usec"`
	Periods       uint64 `json:"periods"`
	Throttled     uint64 `json:"throttled_periods"`
	ThrottledUsec uint64 `json:"throttled_usec"`
	MemoryBytes   uint64 `json:"memory_bytes"`
	MemoryPeak    uint64 `json:"memory_peak_bytes"`
	PIDs          uint64 `json:"pids"`
}

type cpuUsage struct {
	ElapsedSeconds    float64 `json:"elapsed_seconds"`
	CPUSeconds        float64 `json:"cpu_seconds"`
	UserCPUSeconds    float64 `json:"user_cpu_seconds"`
	SystemCPUSeconds  float64 `json:"system_cpu_seconds"`
	AverageMillicores float64 `json:"average_millicores"`
	ThrottledPeriods  uint64  `json:"throttled_periods"`
	ThrottledSeconds  float64 `json:"throttled_seconds"`
}

func main() {
	var opts options
	flag.StringVar(&opts.kind, "kind", "cpu-profile", "diagnostic probe kind")
	flag.IntVar(&opts.duration, "duration", defaultDurationSeconds, "profile duration in seconds")
	flag.IntVar(&opts.frequency, "frequency", defaultFrequency, "CPU sampling frequency in Hz")
	flag.StringVar(&opts.containerID, "container-id", "", "target container ID")
	flag.StringVar(&opts.outputDir, "output-dir", "/tmp/fugue-diagnostic", "temporary output directory")
	flag.Parse()

	if err := run(opts); err != nil {
		writeFailure(err)
		os.Exit(1)
	}
}

func run(opts options) error {
	if strings.TrimSpace(opts.kind) != "cpu-profile" {
		return fmt.Errorf("unsupported diagnostic kind %q", opts.kind)
	}
	if opts.duration < 5 || opts.duration > maxDurationSeconds {
		return fmt.Errorf("duration must be between 5 and %d seconds", maxDurationSeconds)
	}
	if opts.frequency < 1 || opts.frequency > maxFrequency {
		return fmt.Errorf("frequency must be between 1 and %d Hz", maxFrequency)
	}
	if strings.TrimSpace(opts.containerID) == "" {
		return errors.New("container-id is required")
	}
	if err := os.MkdirAll(opts.outputDir, 0o700); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	pids, processNames, cgroupPath, err := findContainerProcesses(strings.TrimSpace(opts.containerID))
	if err != nil {
		return err
	}
	cgroupRoot, cgroupPath, err := resolveCgroupRoot(cgroupPath, normalizeContainerID(opts.containerID))
	if err != nil {
		return err
	}
	processBefore, err := readProcessSnapshot(pids)
	if err != nil {
		return fmt.Errorf("read target process snapshot: %w", err)
	}
	cgroupBefore, err := readCgroupSnapshot(cgroupRoot)
	if err != nil {
		return fmt.Errorf("read target cgroup snapshot: %w", err)
	}

	started := time.Now().UTC()
	dataPath := filepath.Join(opts.outputDir, "perf.data")
	perfArgs := []string{
		"record", "-a", "-e", "cpu-clock", "-F", strconv.Itoa(opts.frequency),
		"--call-graph", "dwarf,8192", "-G", strings.TrimPrefix(cgroupPath, "/"), "-o", dataPath, "--",
		"sleep", strconv.Itoa(opts.duration),
	}
	if output, err := runCommand(context.Background(), "perf", perfArgs...); err != nil {
		return fmt.Errorf("perf record failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	finished := time.Now().UTC()
	processAfter, processErr := readProcessSnapshot(pids)
	cgroupAfter, cgroupErr := readCgroupSnapshot(cgroupRoot)

	targetRoot := filepath.Join("/host/proc", strconv.Itoa(pids[0]), "root")
	rawScript, err := runCommand(context.Background(), "perf", "script", "--symfs", targetRoot, "-i", dataPath, "-F", "ip,sym,dso")
	if err != nil {
		return fmt.Errorf("perf script failed: %w", err)
	}

	functions, cumulativeFunctions, samples, userSamples, kernelSamples := sampledFunctions(rawScript)
	warnings := make([]string, 0, 3)
	if userSamples == 0 {
		warnings = append(warnings, "no user-space samples were captured")
	}
	leafAddresses, cumulativeAddresses := sampledAddressCounts(rawScript)
	if len(leafAddresses) > 0 {
		executable := filepath.Join("/host/proc", strconv.Itoa(pids[0]), "exe")
		if allUnknownFunctions(functions) {
			symbols, symbolWarnings := symbolizeAddresses(leafAddresses, executable)
			warnings = append(warnings, symbolWarnings...)
			if len(symbols) > 0 && !allUnknownFunctions(symbols) {
				functions = symbols
			}
		}
		if allUnknownFunctions(cumulativeFunctions) {
			symbols, symbolWarnings := symbolizeAddresses(cumulativeAddresses, executable)
			warnings = append(warnings, symbolWarnings...)
			if len(symbols) > 0 && !allUnknownFunctions(symbols) {
				cumulativeFunctions = symbols
			}
		}
	}
	perfReport, reportErr := runCommand(context.Background(), "perf", "report", "--stdio", "--no-children", "--symfs", targetRoot, "--sort", "comm,dso,symbol", "-i", dataPath)
	if reportErr != nil {
		warnings = append(warnings, "perf report unavailable: "+reportErr.Error())
	}
	if len(rawScript) > 2<<20 {
		rawScript = rawScript[:2<<20]
		warnings = append(warnings, "raw perf script was truncated at 2 MiB")
	}
	if len(perfReport) > 1<<20 {
		perfReport = perfReport[:1<<20]
		warnings = append(warnings, "perf report was truncated at 1 MiB")
	}
	if processErr != nil {
		warnings = append(warnings, "target process snapshot after sampling unavailable: "+processErr.Error())
	}
	if cgroupErr != nil {
		warnings = append(warnings, "target cgroup snapshot after sampling unavailable: "+cgroupErr.Error())
	}

	value := report{
		Schema:              "fugue.diagnostic.cpu_profile.v1",
		Kind:                opts.kind,
		StartedAt:           started,
		FinishedAt:          finished,
		DurationSeconds:     opts.duration,
		Frequency:           opts.frequency,
		TargetPID:           pids[0],
		TargetPIDs:          pids,
		TargetContainerID:   opts.containerID,
		TargetProcess:       processNames[0],
		TargetProcesses:     processNames,
		TargetCgroup:        cgroupPath,
		ProcessBefore:       processBefore,
		ProcessAfter:        processAfter,
		CgroupBefore:        cgroupBefore,
		CgroupAfter:         cgroupAfter,
		CPUUsage:            cpuUsageDelta(cgroupBefore, cgroupAfter, finished.Sub(started)),
		Samples:             samples,
		UserSamples:         userSamples,
		KernelSamples:       kernelSamples,
		LostSamples:         parseLostSamples(perfReport),
		LeafFunctions:       functions,
		CumulativeFunctions: cumulativeFunctions,
		PerfReport:          string(perfReport),
		RawScript:           string(rawScript),
		Warnings:            warnings,
	}
	return json.NewEncoder(os.Stdout).Encode(value)
}

func findContainerProcesses(containerID string) ([]int, []string, string, error) {
	entries, err := os.ReadDir("/host/proc")
	if err != nil {
		return nil, nil, "", fmt.Errorf("read host proc: %w", err)
	}
	containerID = normalizeContainerID(containerID)
	if !validContainerID(containerID) {
		return nil, nil, "", errors.New("container ID is not a valid runtime identifier")
	}
	pids := make([]int, 0, 4)
	selectedCgroup := ""
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "" || entry.Name()[0] < '0' || entry.Name()[0] > '9' {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 1 {
			continue
		}
		cgroup, err := os.ReadFile(filepath.Join("/host/proc", entry.Name(), "cgroup"))
		if err != nil || !bytes.Contains(cgroup, []byte(containerID)) {
			continue
		}
		pids = append(pids, pid)
		if selectedCgroup == "" {
			selectedCgroup = cgroupV2Path(cgroup)
		}
	}
	if len(pids) == 0 {
		return nil, nil, "", fmt.Errorf("no process found for container %q", containerID)
	}
	sort.Ints(pids)
	names := make([]string, 0, len(pids))
	for _, pid := range pids {
		name, _ := os.ReadFile(filepath.Join("/host/proc", strconv.Itoa(pid), "comm"))
		names = append(names, strings.TrimSpace(string(name)))
	}
	return pids, names, selectedCgroup, nil
}

func normalizeContainerID(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	for _, prefix := range []string{"docker://", "containerd://", "cri-o://"} {
		value = strings.TrimPrefix(value, prefix)
	}
	return value
}

func validContainerID(value string) bool {
	if len(value) < 12 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func cgroupV2Path(data []byte) string {
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) == 3 && parts[0] == "0" && parts[1] == "" {
			return strings.TrimSpace(parts[2])
		}
	}
	return ""
}

func readProcessSnapshot(pids []int) (processSnapshot, error) {
	var snapshot processSnapshot
	read := 0
	for _, pid := range pids {
		root := filepath.Join("/host/proc", strconv.Itoa(pid))
		status, err := os.ReadFile(filepath.Join(root, "status"))
		if err != nil {
			continue
		}
		read++
		values := parseKeyValueLines(status)
		snapshot.RSSBytes += values["VmRSS"] * 1024
		snapshot.Threads += values["Threads"]
		if fdEntries, err := os.ReadDir(filepath.Join(root, "fd")); err == nil {
			snapshot.OpenFDs += uint64(len(fdEntries))
		}
	}
	if read == 0 {
		return processSnapshot{}, errors.New("target processes are no longer available")
	}
	return snapshot, nil
}

func resolveCgroupRoot(reportedPath, containerID string) (string, string, error) {
	return resolveCgroupRootAt(hostCgroupRoot, reportedPath, containerID)
}

func resolveCgroupRootAt(root, reportedPath, containerID string) (string, string, error) {
	if strings.TrimSpace(containerID) == "" {
		return "", "", errors.New("container ID is required to resolve its cgroup")
	}
	candidate := filepath.Join(root, strings.TrimPrefix(filepath.Clean("/"+reportedPath), "/"))
	if info, err := os.Stat(filepath.Join(candidate, "cpu.stat")); err == nil && !info.IsDir() {
		return candidate, reportedPath, nil
	}
	visited := 0
	var found string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || found != "" {
			return nil
		}
		visited++
		if visited > maxCgroupSearchEntries {
			return errors.New("cgroup search limit exceeded")
		}
		if !entry.IsDir() || !strings.Contains(strings.ToLower(entry.Name()), containerID) {
			return nil
		}
		if info, statErr := os.Stat(filepath.Join(path, "cpu.stat")); statErr == nil && !info.IsDir() {
			found = path
		}
		return nil
	})
	if err != nil {
		return "", "", fmt.Errorf("resolve target cgroup: %w", err)
	}
	if found == "" {
		return "", "", fmt.Errorf("no cgroup found for container %q", containerID)
	}
	relative, err := filepath.Rel(root, found)
	if err != nil {
		return "", "", err
	}
	return found, "/" + filepath.ToSlash(relative), nil
}

func readCgroupSnapshot(root string) (cgroupSnapshot, error) {
	cpuStat, err := os.ReadFile(filepath.Join(root, "cpu.stat"))
	if err != nil {
		return cgroupSnapshot{}, err
	}
	values := parseKeyValueLines(cpuStat)
	snapshot := cgroupSnapshot{
		UsageUsec:     values["usage_usec"],
		UserUsec:      values["user_usec"],
		SystemUsec:    values["system_usec"],
		Periods:       values["nr_periods"],
		Throttled:     values["nr_throttled"],
		ThrottledUsec: values["throttled_usec"],
	}
	snapshot.MemoryBytes, _ = readUintFile(filepath.Join(root, "memory.current"))
	snapshot.MemoryPeak, _ = readUintFile(filepath.Join(root, "memory.peak"))
	snapshot.PIDs, _ = readUintFile(filepath.Join(root, "pids.current"))
	return snapshot, nil
}

func parseKeyValueLines(data []byte) map[string]uint64 {
	values := make(map[string]uint64)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err == nil {
			values[strings.TrimSuffix(fields[0], ":")] = value
		}
	}
	return values
}

func readUintFile(path string) (uint64, error) {
	value, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(value)), 10, 64)
}

func cpuUsageDelta(before, after cgroupSnapshot, elapsed time.Duration) cpuUsage {
	seconds := elapsed.Seconds()
	usageUsec := subtractUint64(after.UsageUsec, before.UsageUsec)
	value := cpuUsage{
		ElapsedSeconds:   seconds,
		CPUSeconds:       float64(usageUsec) / 1e6,
		UserCPUSeconds:   float64(subtractUint64(after.UserUsec, before.UserUsec)) / 1e6,
		SystemCPUSeconds: float64(subtractUint64(after.SystemUsec, before.SystemUsec)) / 1e6,
		ThrottledPeriods: subtractUint64(after.Throttled, before.Throttled),
		ThrottledSeconds: float64(subtractUint64(after.ThrottledUsec, before.ThrottledUsec)) / 1e6,
	}
	if seconds > 0 {
		value.AverageMillicores = value.CPUSeconds / seconds * 1000
	}
	return value
}

func subtractUint64(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func sampledFunctions(raw []byte) ([]functionSample, []functionSample, int, int, int) {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 4096), 4<<20)
	leaves := make(map[string]int)
	cumulative := make(map[string]int)
	samples := 0
	userSamples := 0
	kernelSamples := 0
	needLeaf := true
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			needLeaf = true
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		address := strings.TrimSpace(fields[0])
		if !isHexAddress(address) {
			continue
		}
		frame := strings.TrimSpace(strings.TrimPrefix(line, fields[0]))
		if frame == "" {
			frame = "0x" + address + " [unknown]"
		}
		kernel := strings.HasPrefix(address, "ffffffff") || strings.Contains(frame, "[kernel.kallsyms]") || strings.Contains(frame, "[kernel.vmlinux]")
		cumulative[frame]++
		if needLeaf {
			samples++
			leaves[frame]++
			if kernel {
				kernelSamples++
			} else {
				userSamples++
			}
		}
		needLeaf = false
	}
	return sortedFunctionSamples(leaves), sortedFunctionSamples(cumulative), samples, userSamples, kernelSamples
}

func sortedFunctionSamples(counts map[string]int) []functionSample {
	functions := make([]functionSample, 0, len(counts))
	for function, samples := range counts {
		functions = append(functions, functionSample{Function: function, Samples: samples})
	}
	sort.Slice(functions, func(i, j int) bool {
		if functions[i].Samples != functions[j].Samples {
			return functions[i].Samples > functions[j].Samples
		}
		return functions[i].Function < functions[j].Function
	})
	if len(functions) > 100 {
		functions = functions[:100]
	}
	return functions
}

func allUnknownFunctions(functions []functionSample) bool {
	if len(functions) == 0 {
		return true
	}
	for _, function := range functions {
		name := strings.TrimSpace(strings.ToLower(function.Function))
		if name != "?" && !strings.Contains(name, "unknown") {
			return false
		}
	}
	return true
}

func sampledAddressCounts(raw []byte) (map[string]int, map[string]int) {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	leaf := make(map[string]int)
	cumulative := make(map[string]int)
	needLeaf := true
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			needLeaf = true
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 || !isHexAddress(fields[0]) {
			continue
		}
		address := fields[0]
		cumulative[address]++
		if needLeaf {
			leaf[address]++
		}
		needLeaf = false
	}
	return leaf, cumulative
}

func symbolizeAddresses(addresses map[string]int, executable string) ([]functionSample, []string) {
	keys := make([]string, 0, len(addresses))
	for address := range addresses {
		keys = append(keys, address)
	}
	sort.Strings(keys)
	input := bytes.Buffer{}
	for _, address := range keys {
		fmt.Fprintf(&input, "0x%s\n", address)
	}
	output, err := runCommandWithInput(context.Background(), input.Bytes(), "go", "tool", "addr2line", executable)
	if err != nil {
		return nil, []string{"optional Go symbolization unavailable: " + err.Error()}
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	counts := make(map[string]int)
	for index, address := range keys {
		function := "<unknown>"
		lineIndex := index * 2
		if lineIndex < len(lines) && strings.TrimSpace(lines[lineIndex]) != "" {
			function = strings.TrimSpace(lines[lineIndex])
		}
		counts[function] += addresses[address]
	}
	return sortedFunctionSamples(counts), nil
}

func parseLostSamples(perfReport []byte) int {
	for _, line := range strings.Split(string(perfReport), "\n") {
		const prefix = "# Total Lost Samples:"
		if !strings.HasPrefix(strings.TrimSpace(line), prefix) {
			continue
		}
		value, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), prefix)))
		if err == nil && value >= 0 {
			return value
		}
	}
	return 0
}

func isHexAddress(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') && (char < 'A' || char > 'F') {
			return false
		}
	}
	return true
}

func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	return runCommandWithInput(ctx, nil, name, args...)
}

func runCommandWithInput(ctx context.Context, input []byte, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	if input != nil {
		command.Stdin = bytes.NewReader(input)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if err != nil {
		combined := append([]byte(nil), stdout.Bytes()...)
		combined = append(combined, stderr.Bytes()...)
		return combined, err
	}
	return stdout.Bytes(), nil
}

func writeFailure(err error) {
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"schema": "fugue.diagnostic.cpu_profile.v1",
		"error":  err.Error(),
	})
}
