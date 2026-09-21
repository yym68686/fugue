package diagnosticprobe

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/livediagnostics"
)

func TestFaultEventsRetainRealtimeAddressesAndReportLoss(t *testing.T) {
	raw := "  123/456  1790000000.123456789:  7f000010  402010\n\n123/456 1790000000.223456789: 7f000020 402020\nPERF_RECORD_LOST lost 12\nmalformed record\n"
	events, lost, invalid, cut := parseFaultEvents([]byte(raw))
	if len(events) != 2 || lost != 1 || invalid != 1 || cut {
		t.Fatalf("lost or malformed evidence hidden: %d %d %d %v", len(events), lost, invalid, cut)
	}
	e := events[0]
	if e.PID != 123 || e.TID != 456 || e.address != 0x7f000010 || e.Address != "0x7f000010" || e.IP != "0x402010" || !e.At.Equal(time.Unix(1790000000, 123456789)) {
		t.Fatalf("incorrect fault identity/address/time: %+v", e)
	}
	events, _, invalid, cut = parseFaultEvents([]byte(strings.Repeat("123/456 1790000000.123456789: 7f000010 402010\n", 8193)))
	if len(events) != 8192 || invalid != 0 || !cut {
		t.Fatal("fault report did not enforce its record bound")
	}
	for _, row := range []string{"123/456 1.12: 10 20", "123/456 1.123456789 10 20", "123/456 1.123456789: not-hex 20", "0/456 1.123456789: 10 20"} {
		if events, _, invalid, _ := parseFaultEvents([]byte(row)); len(events) != 0 || invalid != 1 {
			t.Fatalf("accepted malformed record: %q", row)
		}
	}
}

func TestFaultMappingRequiresStableRangesOffsetsAndFileIdentity(t *testing.T) {
	raw := "00400000-00410000 r-xp 00000000 08:01 123 /usr/bin/server\n7f000000-7f100000 rw-s 00001000 08:01 456 /var/lib/store/db\n7f200000-7f300000 rw-p 00000000 00:00 0\n"
	maps, err := parseProcessMappings(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range []uint64{0x7f000000, 0x7f0fffff} {
		m, ok := stableFaultMapping(maps, maps, address)
		if !ok || m.path != "/var/lib/store/db" || m.inode != "456" || m.offset != 4096 {
			t.Fatalf("fault mapped to wrong backing file: %+v %v", m, ok)
		}
	}
	for _, address := range []uint64{0, 0x7f100000, 0x7f300000} {
		if _, ok := stableFaultMapping(maps, maps, address); ok {
			t.Fatalf("unmapped/end-exclusive address accepted: %x", address)
		}
	}
	for _, replace := range []string{strings.Replace(raw, "08:01 456", "08:01 999", 1), strings.Replace(raw, "00001000", "00002000", 1), strings.Replace(raw, "7f100000", "7f180000", 1)} {
		after, err := parseProcessMappings(replace)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := stableFaultMapping(maps, after, 0x7f000010); ok {
			t.Fatal("changing mapping attributed as stable")
		}
	}
	if _, err := parseProcessMappings("10-30 r--p 0 00:00 0\n20-40 r--p 0 00:00 0\n"); err == nil {
		t.Fatal("overlapping mappings accepted")
	}
}

func TestFaultCgroupResolutionVerifiesEveryFrozenPID(t *testing.T) {
	dir := t.TempDir()
	proc, cgroups := filepath.Join(dir, "proc"), filepath.Join(dir, "cgroups")
	for _, pid := range []string{"42", "44"} {
		if err := os.MkdirAll(filepath.Join(proc, pid), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(proc, pid, "cgroup"), []byte("0::/../../system.slice/service.scope\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(cgroups, "system.slice/service.scope")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "cgroup.procs"), []byte("42\n44\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ids := map[int]string{42: "old", 44: "old"}
	if got, err := faultTargetCgroup(proc, cgroups, ids); err != nil || got != "system.slice/service.scope" {
		t.Fatalf("invalid cgroup resolution: %q %v", got, err)
	}
	if err := os.WriteFile(filepath.Join(path, "cgroup.procs"), []byte("42\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := faultTargetCgroup(proc, cgroups, ids); err == nil {
		t.Fatal("partial membership accepted")
	}
	if _, err := faultTargetCgroup(proc, cgroups, nil); err == nil {
		t.Fatal("unfrozen system-wide capture accepted")
	}
}

func TestFaultCaptureRejectsUnsafeWindowsAndTargets(t *testing.T) {
	req := livediagnostics.ProbeRequest{DurationSeconds: 45, Target: livediagnostics.Target{Type: livediagnostics.TargetNodeProcess, ProcessName: "k3s"}}
	for _, c := range []Collector{{CaptureSeconds: 31, IntervalSeconds: 120}, {CaptureSeconds: 4, IntervalSeconds: 120}, {CaptureSeconds: 20, IntervalSeconds: 10}} {
		if _, err := processPageFaults(context.Background(), req, c); err == nil {
			t.Fatal("unbounded fault request accepted")
		}
	}
	req.Target.Type = livediagnostics.TargetPlatformComponent
	if _, err := processPageFaults(context.Background(), req, Collector{CaptureSeconds: 20, IntervalSeconds: 120}); err == nil {
		t.Fatal("non-host target accepted")
	}
}
