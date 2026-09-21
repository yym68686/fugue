package diagnosticprobe

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestCommandMemoryOnlyExportsBoundedProcessFacts(t *testing.T) {
	root := t.TempDir()
	for _, pid := range []int{20, 21} {
		dir := filepath.Join(root, fmt.Sprint(pid))
		if err := os.MkdirAll(filepath.Join(dir, "task", fmt.Sprint(pid)), 0700); err != nil {
			t.Fatal(err)
		}
		files := map[string]string{"stat": fmt.Sprintf("%d (perf) S 1 2 3 4 5 6 7 8 9 10 111 222 13 14 15 16 7 18 9000 20", pid), "status": "VmRSS:\t4096 kB\n", "cmdline": "perf\x00report\x00credential=must-not-export\x00"}
		for name, data := range files {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
		}
		children := ""
		if pid == 20 {
			children = "21"
		}
		if err := os.WriteFile(filepath.Join(dir, "task", fmt.Sprint(pid), "children"), []byte(children), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Go can launch children from a thread other than the process leader.
	if err := os.MkdirAll(filepath.Join(root, "20/task/22"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "20/task/20/children"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "20/task/22/children"), []byte("21"), 0600); err != nil {
		t.Fatal(err)
	}
	sample, err := commandMemoryAt(root, 20)
	if err != nil || len(sample.Processes) != 2 || sample.RSSBytes != 8<<20 || sample.Processes[1].Stage != "report" {
		t.Fatalf("incorrect memory facts: %+v %v", sample, err)
	}
	if strings.Contains(fmt.Sprint(sample), "credential") {
		t.Fatal("command arguments leaked")
	}
}

func TestObservedCommandStopsOnlyOwnedProcessGroupAtMemoryBudget(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires proc process RSS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, obs, err := observedCommand(ctx, 1024, 1, "sh", "-c", "sleep 20 & wait")
	if err == nil || !obs.StoppedForMemory || obs.PeakRSSBytes == 0 || len(obs.Samples) == 0 {
		t.Fatalf("memory supervisor did not stop sampler: %+v %v", obs, err)
	}
}
