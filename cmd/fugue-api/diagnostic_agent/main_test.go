package main

import (
	"os"
	"path/filepath"
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
