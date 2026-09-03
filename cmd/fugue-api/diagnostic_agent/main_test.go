package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSampledFunctionsSeparatesLeafAndCumulativeFrames(t *testing.T) {
	raw := []byte("\n          513920 encoding/json.appendString+0x40 (/app/service)\n          5042a5 service.Proxy+0x35 (/app/service)\n          ffffffff12345678 entry_SYSCALL_64 ([kernel.kallsyms])\n\n          ffffffff87654321 schedule ([kernel.kallsyms])\n          4788b7 runtime.park_m+0x17 (/app/service)\n\n")
	leaves, cumulative, samples, users, kernel := sampledFunctions(raw)
	if samples != 2 || users != 1 || kernel != 1 {
		t.Fatalf("unexpected counts samples=%d user=%d kernel=%d", samples, users, kernel)
	}
	if leaves[0].Function != "encoding/json.appendString+0x40 (/app/service)" || leaves[0].Samples != 1 {
		t.Fatalf("unexpected leaf addresses: %+v", leaves)
	}
	if len(cumulative) != 5 {
		t.Fatalf("unexpected cumulative addresses: %+v", cumulative)
	}
}

func TestCPUUsageDelta(t *testing.T) {
	before := cgroupSnapshot{UsageUsec: 1_000_000, UserUsec: 700_000, SystemUsec: 300_000, Throttled: 4, ThrottledUsec: 20_000}
	after := cgroupSnapshot{UsageUsec: 2_500_000, UserUsec: 1_800_000, SystemUsec: 700_000, Throttled: 6, ThrottledUsec: 50_000}
	usage := cpuUsageDelta(before, after, 10_000_000_000)
	if usage.CPUSeconds != 1.5 || usage.AverageMillicores != 150 || usage.ThrottledPeriods != 2 || usage.ThrottledSeconds != 0.03 {
		t.Fatalf("unexpected CPU usage delta: %+v", usage)
	}
}

func TestCgroupV2Path(t *testing.T) {
	value := []byte("0::/kubepods.slice/pod123/container456\n")
	if got := cgroupV2Path(value); got != "/kubepods.slice/pod123/container456" {
		t.Fatalf("unexpected cgroup path %q", got)
	}
}

func TestParseLostSamples(t *testing.T) {
	if got := parseLostSamples([]byte("# Samples: 12\n# Total Lost Samples: 3\n")); got != 3 {
		t.Fatalf("expected 3 lost samples, got %d", got)
	}
}

func TestParsePerfMachineReportPreservesEverySample(t *testing.T) {
	raw := []byte(strings.Join([]string{
		"# Samples: 4  of event 'cpu-clock'",
		"75.00%" + perfFieldSeparator + "3" + perfFieldSeparator + "0.00%" + perfFieldSeparator + "75.00%" + perfFieldSeparator + "123" + perfFieldSeparator + "service" + perfFieldSeparator + "service" + perfFieldSeparator + "[.] runtime.execute" + perfFieldSeparator + "- -",
		"25.00%" + perfFieldSeparator + "1" + perfFieldSeparator + "25.00%" + perfFieldSeparator + "0.00%" + perfFieldSeparator + "123" + perfFieldSeparator + "service" + perfFieldSeparator + "[unknown]" + perfFieldSeparator + "[k] 0xffffffff12345678" + perfFieldSeparator + "- -",
	}, "\n"))
	entries, expected, err := parsePerfMachineReport(raw)
	if err != nil {
		t.Fatal(err)
	}
	if expected != 4 || len(entries) != 2 {
		t.Fatalf("unexpected parsed report expected=%d entries=%+v", expected, entries)
	}
	functions, samples, users, kernel, other, resolvedUsers, resolvedKernel := summarizePerfEntries(entries, nil)
	if samples != 4 || users != 3 || kernel != 1 || other != 0 || resolvedUsers != 3 || resolvedKernel != 0 {
		t.Fatalf("unexpected summary samples=%d users=%d kernel=%d other=%d resolved_users=%d resolved_kernel=%d", samples, users, kernel, other, resolvedUsers, resolvedKernel)
	}
	if functions[0].Function != "runtime.execute" || functions[0].Samples != 3 || functions[0].Mode != "user" || functions[0].Percent != 75 {
		t.Fatalf("unexpected functions: %+v", functions)
	}
}

func TestParsePerfMachineReportRejectsMissingRows(t *testing.T) {
	_, _, err := parsePerfMachineReport([]byte("# Samples: 2  of event 'cpu-clock'\n"))
	if err == nil {
		t.Fatal("expected a non-empty report without histogram rows to fail")
	}
}

func TestGoSymbolizerResolvesStrippedExecutableOffset(t *testing.T) {
	executable := os.Getenv("FUGUE_DIAGNOSTIC_SYMBOL_FIXTURE")
	wantedSuffix := ""
	if executable == "" {
		dir := t.TempDir()
		sourcePath := filepath.Join(dir, "fixture.go")
		source := "package main\n\n//go:noinline\nfunc diagnosticFixture() int { return 42 }\nfunc main() { println(diagnosticFixture()) }\n"
		if err := os.WriteFile(sourcePath, []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
		executable = filepath.Join(dir, "fixture")
		command := exec.Command("go", "build", "-trimpath", "-ldflags=-s -w", "-o", executable, sourcePath)
		command.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH=amd64", "GOCACHE="+filepath.Join(dir, "go-cache"))
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build fixture: %v: %s", err, output)
		}
		wantedSuffix = ".diagnosticFixture"
	}
	resolver, err := newGoSymbolizer(executable)
	if err != nil {
		t.Fatal(err)
	}
	var entry uint64
	wantedName := ""
	for _, function := range resolver.table.Funcs {
		if function.Entry >= resolver.loadBase && (wantedSuffix == "" || strings.HasSuffix(function.Name, wantedSuffix)) {
			entry = function.Entry
			wantedName = function.Name
			break
		}
	}
	if entry < resolver.loadBase {
		t.Fatalf("fixture function entry %#x is below load base %#x", entry, resolver.loadBase)
	}
	name, file, line, ok := resolver.ResolveDSOOffset(entry - resolver.loadBase)
	if !ok || name != wantedName || file == "" || line <= 0 {
		t.Fatalf("unexpected symbolization ok=%t name=%q file=%q line=%d", ok, name, file, line)
	}
}

func TestResolveCgroupRootFallsBackToContainerID(t *testing.T) {
	root := t.TempDir()
	containerID := "abcdef1234567890"
	resolved := filepath.Join(root, "kubepods.slice", "cri-containerd-"+containerID+".scope")
	if err := os.MkdirAll(resolved, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(resolved, "cpu.stat"), []byte("usage_usec 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gotRoot, gotPath, err := resolveCgroupRootAt(root, "/missing/"+containerID, containerID)
	if err != nil {
		t.Fatal(err)
	}
	if gotRoot != resolved || gotPath != "/kubepods.slice/cri-containerd-"+containerID+".scope" {
		t.Fatalf("unexpected cgroup resolution root=%q path=%q", gotRoot, gotPath)
	}
}

func TestHexAddressValidation(t *testing.T) {
	for _, value := range []string{"513920", "ffffffff12345678", "ABCDEF"} {
		if !isHexAddress(value) {
			t.Fatalf("expected valid hexadecimal address %q", value)
		}
	}
	for _, value := range []string{"", "0x123", "123g", "symbol"} {
		if isHexAddress(value) {
			t.Fatalf("expected invalid hexadecimal address %q", value)
		}
	}
}
