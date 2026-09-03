package main

import (
	"bufio"
	"bytes"
	"context"
	"debug/elf"
	"debug/gosym"
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
	Schema                  string           `json:"schema"`
	Kind                    string           `json:"kind"`
	StartedAt               time.Time        `json:"started_at"`
	FinishedAt              time.Time        `json:"finished_at"`
	DurationSeconds         int              `json:"duration_seconds"`
	Frequency               int              `json:"frequency"`
	TargetPID               int              `json:"target_pid"`
	TargetPIDs              []int            `json:"target_pids"`
	TargetContainerID       string           `json:"target_container_id"`
	TargetProcess           string           `json:"target_process,omitempty"`
	TargetProcesses         []string         `json:"target_processes,omitempty"`
	TargetCgroup            string           `json:"target_cgroup,omitempty"`
	ProcessBefore           processSnapshot  `json:"process_before"`
	ProcessAfter            processSnapshot  `json:"process_after"`
	CgroupBefore            cgroupSnapshot   `json:"cgroup_before"`
	CgroupAfter             cgroupSnapshot   `json:"cgroup_after"`
	CPUUsage                cpuUsage         `json:"cpu_usage"`
	Samples                 int              `json:"samples"`
	UserSamples             int              `json:"user_samples"`
	KernelSamples           int              `json:"kernel_samples"`
	OtherSamples            int              `json:"other_samples"`
	ResolvedUserSamples     int              `json:"resolved_user_samples"`
	UnresolvedUserSamples   int              `json:"unresolved_user_samples"`
	ResolvedKernelSamples   int              `json:"resolved_kernel_samples"`
	UnresolvedKernelSamples int              `json:"unresolved_kernel_samples"`
	StackSamples            int              `json:"stack_samples"`
	StackUserSamples        int              `json:"stack_user_samples"`
	StackKernelSamples      int              `json:"stack_kernel_samples"`
	LostSamples             int              `json:"lost_samples"`
	LeafFunctions           []functionSample `json:"leaf_functions"`
	CumulativeFunctions     []functionSample `json:"cumulative_functions"`
	PerfReport              string           `json:"perf_report,omitempty"`
	RawScript               string           `json:"raw_script,omitempty"`
	Warnings                []string         `json:"warnings,omitempty"`
}

type functionSample struct {
	Function string  `json:"function"`
	Samples  int     `json:"samples"`
	Percent  float64 `json:"percent,omitempty"`
	PID      int     `json:"pid,omitempty"`
	Command  string  `json:"command,omitempty"`
	Mode     string  `json:"mode,omitempty"`
	DSO      string  `json:"dso,omitempty"`
	Source   string  `json:"source,omitempty"`
}

type perfReportEntry struct {
	Samples       int
	SystemPercent float64
	UserPercent   float64
	PID           int
	Command       string
	DSO           string
	Symbol        string
	Mode          string
	Address       uint64
}

type goSymbolizer struct {
	table    *gosym.Table
	loadBase uint64
}

type processSnapshot struct {
	RSSBytes         uint64 `json:"rss_bytes"`
	Threads          uint64 `json:"threads"`
	OpenFDs          uint64 `json:"open_fds"`
	OpenFDsAvailable bool   `json:"open_fds_available"`
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
		"--call-graph", "dwarf,8192", "--no-buildid-mmap", "-G", strings.TrimPrefix(cgroupPath, "/"), "-o", dataPath, "--",
		"sleep", strconv.Itoa(opts.duration),
	}
	if output, err := runCommand(context.Background(), "perf", perfArgs...); err != nil {
		return fmt.Errorf("perf record failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	finished := time.Now().UTC()
	processAfter, processErr := readProcessSnapshot(pids)
	cgroupAfter, cgroupErr := readCgroupSnapshot(cgroupRoot)

	targetRoot := filepath.Join("/host/proc", strconv.Itoa(pids[0]), "root")
	warnings := make([]string, 0, 6)
	rawScript, scriptErr := runCommand(context.Background(), "perf", "script", "--symfs", targetRoot, "-i", dataPath, "-F", "ip,sym,dso")
	_, cumulativeFunctions, stackSamples, stackUserSamples, stackKernelSamples := sampledFunctions(rawScript)
	if scriptErr != nil {
		warnings = append(warnings, "call-stack export unavailable: "+scriptErr.Error())
		rawScript = nil
		cumulativeFunctions = nil
		stackSamples = 0
		stackUserSamples = 0
		stackKernelSamples = 0
	}

	machineReport, machineErr := runPerfMachineReport(dataPath, targetRoot)
	if machineErr != nil && !isEmptyPerfData(machineReport) {
		return fmt.Errorf("perf machine report failed: %w: %s", machineErr, strings.TrimSpace(string(machineReport)))
	}
	entries, expectedSamples, err := parsePerfMachineReport(machineReport)
	if err != nil {
		return fmt.Errorf("parse perf machine report: %w", err)
	}
	functions, samples, userSamples, kernelSamples, otherSamples, resolvedUserSamples, resolvedKernelSamples := summarizePerfEntries(entries, pids)
	if expectedSamples >= 0 && samples != expectedSamples {
		return fmt.Errorf("perf machine report sample mismatch: header=%d rows=%d", expectedSamples, samples)
	}
	if samples == 0 {
		warnings = append(warnings, "no CPU samples were captured during the diagnostic window")
	} else if userSamples == 0 {
		warnings = append(warnings, "no user-space CPU samples were captured")
	}
	if stackSamples < samples {
		warnings = append(warnings, fmt.Sprintf("call stacks available for %d of %d samples; the leaf profile remains complete", stackSamples, samples))
	}
	if resolvedUserSamples < userSamples {
		warnings = append(warnings, fmt.Sprintf("symbols resolved for %d of %d user-space samples", resolvedUserSamples, userSamples))
	}
	if resolvedKernelSamples < kernelSamples {
		warnings = append(warnings, fmt.Sprintf("symbols resolved for %d of %d kernel-space samples", resolvedKernelSamples, kernelSamples))
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
		Schema:                  "fugue.diagnostic.cpu_profile.v1",
		Kind:                    opts.kind,
		StartedAt:               started,
		FinishedAt:              finished,
		DurationSeconds:         opts.duration,
		Frequency:               opts.frequency,
		TargetPID:               pids[0],
		TargetPIDs:              pids,
		TargetContainerID:       opts.containerID,
		TargetProcess:           processNames[0],
		TargetProcesses:         processNames,
		TargetCgroup:            cgroupPath,
		ProcessBefore:           processBefore,
		ProcessAfter:            processAfter,
		CgroupBefore:            cgroupBefore,
		CgroupAfter:             cgroupAfter,
		CPUUsage:                cpuUsageDelta(cgroupBefore, cgroupAfter, finished.Sub(started)),
		Samples:                 samples,
		UserSamples:             userSamples,
		KernelSamples:           kernelSamples,
		OtherSamples:            otherSamples,
		ResolvedUserSamples:     resolvedUserSamples,
		UnresolvedUserSamples:   userSamples - resolvedUserSamples,
		ResolvedKernelSamples:   resolvedKernelSamples,
		UnresolvedKernelSamples: kernelSamples - resolvedKernelSamples,
		StackSamples:            stackSamples,
		StackUserSamples:        stackUserSamples,
		StackKernelSamples:      stackKernelSamples,
		LostSamples:             parseLostSamples(perfReport),
		LeafFunctions:           functions,
		CumulativeFunctions:     cumulativeFunctions,
		PerfReport:              string(perfReport),
		RawScript:               string(rawScript),
		Warnings:                warnings,
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
	fdRead := 0
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
			fdRead++
		}
	}
	if read == 0 {
		return processSnapshot{}, errors.New("target processes are no longer available")
	}
	snapshot.OpenFDsAvailable = fdRead == read
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

const perfFieldSeparator = "\x1f"

func runPerfMachineReport(dataPath, targetRoot string) ([]byte, error) {
	return runCommand(
		context.Background(),
		"perf", "report",
		"--stdio", "--stdio-color", "never", "--no-children", "--call-graph", "none",
		"--percent-limit", "0", "--field-separator", perfFieldSeparator,
		"--fields", "overhead,sample,overhead_sys,overhead_us,tgid,comm,dso,symbol",
		"--sort", "tgid,comm,dso,symbol", "--symfs", targetRoot, "-i", dataPath,
	)
}

func isEmptyPerfData(output []byte) bool {
	return bytes.Contains(bytes.ToLower(output), []byte("data has no samples"))
}

func parsePerfMachineReport(raw []byte) ([]perfReportEntry, int, error) {
	entries := make([]perfReportEntry, 0, 64)
	expectedSamples := parsePerfSampleTotal(raw)
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 4096), 4<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, perfFieldSeparator) {
			continue
		}
		fields := strings.Split(line, perfFieldSeparator)
		if len(fields) < 8 {
			continue
		}
		samples, err := strconv.Atoi(strings.TrimSpace(fields[1]))
		if err != nil || samples <= 0 {
			continue
		}
		systemPercent, err := parsePerfPercent(fields[2])
		if err != nil {
			return nil, expectedSamples, fmt.Errorf("invalid system overhead %q", strings.TrimSpace(fields[2]))
		}
		userPercent, err := parsePerfPercent(fields[3])
		if err != nil {
			return nil, expectedSamples, fmt.Errorf("invalid user overhead %q", strings.TrimSpace(fields[3]))
		}
		mode, symbol, address := parsePerfSymbol(fields[7])
		if mode == "other" {
			switch {
			case userPercent > 0 && systemPercent == 0:
				mode = "user"
			case systemPercent > 0 && userPercent == 0:
				mode = "kernel"
			}
		}
		entries = append(entries, perfReportEntry{
			Samples:       samples,
			SystemPercent: systemPercent,
			UserPercent:   userPercent,
			PID:           parsePerfPID(fields[4]),
			Command:       strings.TrimSpace(fields[5]),
			DSO:           strings.TrimSpace(fields[6]),
			Symbol:        symbol,
			Mode:          mode,
			Address:       address,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, expectedSamples, err
	}
	if expectedSamples > 0 && len(entries) == 0 {
		return nil, expectedSamples, errors.New("report contains samples but no parseable histogram rows")
	}
	return entries, expectedSamples, nil
}

func parsePerfSampleTotal(raw []byte) int {
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "# Samples:") {
			continue
		}
		fields := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, "# Samples:")))
		if len(fields) == 0 {
			break
		}
		value, err := strconv.Atoi(fields[0])
		if err == nil && value >= 0 {
			return value
		}
	}
	return -1
}

func parsePerfPercent(value string) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(value), "%")), 64)
}

func parsePerfPID(value string) int {
	value = strings.TrimSpace(value)
	for _, separator := range []string{"/", ":"} {
		if index := strings.Index(value, separator); index >= 0 {
			value = value[:index]
		}
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(value))
	return pid
}

func parsePerfSymbol(value string) (string, string, uint64) {
	value = strings.TrimSpace(value)
	mode := "other"
	if len(value) >= 3 && value[0] == '[' && value[2] == ']' {
		switch value[1] {
		case '.':
			mode = "user"
		case 'k':
			mode = "kernel"
		}
		value = strings.TrimSpace(value[3:])
	}
	addressValue := strings.TrimPrefix(value, "0x")
	address, err := strconv.ParseUint(addressValue, 16, 64)
	if err != nil || !strings.HasPrefix(value, "0x") {
		address = 0
	}
	if value == "" {
		value = "[unknown]"
	}
	return mode, value, address
}

type functionSampleKey struct {
	Function string
	PID      int
	Command  string
	Mode     string
	DSO      string
}

type functionSampleAggregate struct {
	Samples int
	Sources map[string]int
}

type processGoSymbolizer struct {
	DSO      string
	Resolver *goSymbolizer
}

func summarizePerfEntries(entries []perfReportEntry, targetPIDs []int) ([]functionSample, int, int, int, int, int, int) {
	symbolizers := loadProcessGoSymbolizers(targetPIDs)
	counts := make(map[functionSampleKey]*functionSampleAggregate)
	total := 0
	user := 0
	kernel := 0
	other := 0
	resolvedUser := 0
	resolvedKernel := 0
	for _, entry := range entries {
		total += entry.Samples
		switch entry.Mode {
		case "user":
			user += entry.Samples
		case "kernel":
			kernel += entry.Samples
		default:
			other += entry.Samples
		}

		function := entry.Symbol
		source := ""
		resolved := entry.Address == 0 && !isUnknownSymbol(function)
		if entry.Mode == "user" && entry.Address != 0 {
			if symbolizer := matchingGoSymbolizer(symbolizers, entry); symbolizer != nil {
				if name, file, line, ok := symbolizer.ResolveDSOOffset(entry.Address); ok {
					function = name
					if file != "" && line > 0 {
						source = file + ":" + strconv.Itoa(line)
					}
					resolved = true
				}
			}
		}
		if entry.Mode == "user" && resolved {
			resolvedUser += entry.Samples
		}
		if entry.Mode == "kernel" && resolved {
			resolvedKernel += entry.Samples
		}
		key := functionSampleKey{Function: function, PID: entry.PID, Command: entry.Command, Mode: entry.Mode, DSO: entry.DSO}
		aggregate := counts[key]
		if aggregate == nil {
			aggregate = &functionSampleAggregate{Sources: make(map[string]int)}
			counts[key] = aggregate
		}
		aggregate.Samples += entry.Samples
		if source != "" {
			aggregate.Sources[source] += entry.Samples
		}
	}

	functions := make([]functionSample, 0, len(counts))
	for key, aggregate := range counts {
		percent := 0.0
		if total > 0 {
			percent = float64(aggregate.Samples) / float64(total) * 100
		}
		functions = append(functions, functionSample{
			Function: key.Function,
			Samples:  aggregate.Samples,
			Percent:  percent,
			PID:      key.PID,
			Command:  key.Command,
			Mode:     key.Mode,
			DSO:      key.DSO,
			Source:   mostFrequentSource(aggregate.Sources),
		})
	}
	sort.Slice(functions, func(i, j int) bool {
		if functions[i].Samples != functions[j].Samples {
			return functions[i].Samples > functions[j].Samples
		}
		if functions[i].Function != functions[j].Function {
			return functions[i].Function < functions[j].Function
		}
		return functions[i].DSO < functions[j].DSO
	})
	if len(functions) > 100 {
		functions = functions[:100]
	}
	return functions, total, user, kernel, other, resolvedUser, resolvedKernel
}

func mostFrequentSource(counts map[string]int) string {
	selected := ""
	selectedCount := 0
	for source, count := range counts {
		if count > selectedCount || count == selectedCount && source < selected {
			selected = source
			selectedCount = count
		}
	}
	return selected
}

func isUnknownSymbol(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "" || value == "?" || strings.Contains(value, "unknown")
}

func loadProcessGoSymbolizers(pids []int) map[int]processGoSymbolizer {
	result := make(map[int]processGoSymbolizer)
	for _, pid := range pids {
		executableLink, err := os.Readlink(filepath.Join("/host/proc", strconv.Itoa(pid), "exe"))
		if err != nil || !filepath.IsAbs(executableLink) {
			continue
		}
		executableLink = strings.TrimSuffix(executableLink, " (deleted)")
		executable := filepath.Join("/host/proc", strconv.Itoa(pid), "root", strings.TrimPrefix(filepath.Clean(executableLink), "/"))
		resolver, err := newGoSymbolizer(executable)
		if err != nil {
			continue
		}
		result[pid] = processGoSymbolizer{DSO: filepath.Base(executableLink), Resolver: resolver}
	}
	return result
}

func matchingGoSymbolizer(symbolizers map[int]processGoSymbolizer, entry perfReportEntry) *goSymbolizer {
	candidate, ok := symbolizers[entry.PID]
	if !ok && len(symbolizers) == 1 {
		for _, value := range symbolizers {
			candidate = value
			ok = true
		}
	}
	if !ok || candidate.Resolver == nil || filepath.Base(strings.TrimSpace(entry.DSO)) != candidate.DSO {
		return nil
	}
	return candidate.Resolver
}

func newGoSymbolizer(path string) (*goSymbolizer, error) {
	file, err := elf.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	text := file.Section(".text")
	pcln := file.Section(".gopclntab")
	if text == nil || pcln == nil {
		return nil, errors.New("ELF does not contain Go line tables")
	}
	pclnData, err := pcln.Data()
	if err != nil {
		return nil, err
	}
	var symtabData []byte
	if symtab := file.Section(".gosymtab"); symtab != nil {
		symtabData, _ = symtab.Data()
	}
	table, err := gosym.NewTable(symtabData, gosym.NewLineTable(pclnData, text.Addr))
	if err != nil {
		return nil, err
	}
	loadBase := text.Addr
	for _, program := range file.Progs {
		if program.Type == elf.PT_LOAD && program.Vaddr < loadBase {
			loadBase = program.Vaddr
		}
	}
	return &goSymbolizer{table: table, loadBase: loadBase}, nil
}

func (s *goSymbolizer) ResolveDSOOffset(offset uint64) (string, string, int, bool) {
	if s == nil || s.table == nil {
		return "", "", 0, false
	}
	candidates := []uint64{offset + s.loadBase}
	if s.loadBase != 0 {
		candidates = append(candidates, offset)
	}
	for _, pc := range candidates {
		file, line, function := s.table.PCToLine(pc)
		if function != nil && function.Name != "" {
			return function.Name, file, line, true
		}
	}
	return "", "", 0, false
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
