package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/platformconfig"
)

// Explicitly opt-in: synthetic data, disposable loopback database and a local
// Docker container only. This does not discover credentials or production hosts.
func TestBoundedCompilerInputMemoryExperiment(t *testing.T) {
	container := os.Getenv("FUGUE_MEMORY_TEST_CONTAINER")
	if container == "" {
		t.Skip("local memory experiment not enabled")
	}
	if !strings.HasPrefix(container, "fugue-memory-") {
		t.Fatal("requires named disposable memory container")
	}
	for _, mib := range []int{26, 60, 100, 150} {
		for _, chunks := range []bool{false, true} {
			s := boundedContentTestStore(t)
			s.db.SetMaxOpenConns(1)
			s.db.SetMaxIdleConns(1)
			s.storagePolicy.value.CompilerInputChunks = chunks
			var pid int
			if err := s.db.QueryRow(`SELECT pg_backend_pid()`).Scan(&pid); err != nil {
				t.Fatal(err)
			}
			snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("memory-%d-%t-%d", mib, chunks, time.Now().UnixNano()), PolicyGeneration: "policy"}
			row := platformconfig.OriginObservation{Ref: strings.Repeat("synthetic-origin-", 8), RuntimeID: strings.Repeat("synthetic-runtime-", 8), Status: "active", ObservedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			one, _ := json.Marshal(row)
			for i := 0; i < (mib<<20)/(len(one)+1); i++ {
				snapshot.Origins = append(snapshot.Origins, row)
			}
			digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			command := exec.Command("docker", "exec", container, "sh", "-c", fmt.Sprintf("while test -r /proc/%d/status; do awk '/^VmRSS:/{rss=$2} /^RssAnon:/{print rss,$2}' /proc/%d/status; sleep 0.02; done", pid, pid))
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = command.Start(); err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var samples [][2]int64
			done := make(chan struct{})
			ready := make(chan struct{})
			var once sync.Once
			go func() {
				defer close(done)
				scan := bufio.NewScanner(stdout)
				for scan.Scan() {
					fields := strings.Fields(scan.Text())
					if len(fields) == 2 {
						rss, _ := strconv.ParseInt(fields[0], 10, 64)
						anon, _ := strconv.ParseInt(fields[1], 10, 64)
						mu.Lock()
						samples = append(samples, [2]int64{rss, anon})
						mu.Unlock()
						once.Do(func() { close(ready) })
					}
				}
			}()
			select {
			case <-ready:
			case <-time.After(10 * time.Second):
				s.db.Close()
				command.Process.Kill()
				t.Fatal("local memory sampler did not start")
			}
			time.Sleep(100 * time.Millisecond)
			start := time.Now()
			err = s.EnsurePlatformCompilerInput(snapshot, digest)
			elapsed := time.Since(start)
			time.Sleep(100 * time.Millisecond)
			s.db.Close()
			<-done
			command.Wait()
			var peakRSS, peakAnon int64
			for _, sample := range samples {
				peakRSS = max(peakRSS, sample[0])
				peakAnon = max(peakAnon, sample[1])
			}
			result := map[string]any{"mode": map[bool]string{false: "legacy-jsonb", true: "binary-chunks"}[chunks], "approx_input_mib": mib, "samples": len(samples), "postgres_peak_rss_kib": peakRSS, "postgres_peak_anon_kib": peakAnon, "elapsed_ms": elapsed.Milliseconds(), "success": err == nil}
			if len(samples) > 0 {
				result["baseline_kib"] = samples[0]
				result["after_kib"] = samples[len(samples)-1]
			}
			raw, _ := json.Marshal(result)
			t.Log(string(raw))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}
