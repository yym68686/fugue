package diagnosticprobe

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/livediagnostics"
)

func TestPostgresCollectorRejectsAmbiguousAndUnboundedTargets(t *testing.T) {
	for _, req := range []livediagnostics.ProbeRequest{{}, {Target: livediagnostics.Target{Type: livediagnostics.TargetNode}}} {
		if _, err := postgresMemoryWindow(context.Background(), req, Collector{}); err == nil {
			t.Fatal("ambiguous target accepted")
		}
	}
	// The fixed SQL must never export query text or data arguments.
	for _, forbidden := range []string{"SELECT *", "AS query,", "pg_terminate", "pg_cancel", "pg_log_backend", "UPDATE ", "INSERT ", "DELETE "} {
		if strings.Contains(postgresObservationSQL, forbidden) {
			t.Fatalf("unexpected SQL capability %s", forbidden)
		}
	}
	if !strings.Contains(postgresObservationSQL, "LIMIT 129") || !strings.Contains(postgresObservationSQL, "md5(query)") {
		t.Fatal("query bounds missing")
	}
}
func TestRawProfileChunksPreserveAllBytesAndStayLogBounded(t *testing.T) {
	raw := []byte(strings.Repeat("synthetic", 40000))
	chunks := profileChunks(raw)
	var decoded []byte
	for _, chunk := range chunks {
		if len(chunk) > 32<<10 {
			t.Fatal("unbounded log line")
		}
		b, err := base64.StdEncoding.DecodeString(chunk)
		if err != nil {
			t.Fatal(err)
		}
		decoded = append(decoded, b...)
	}
	if string(decoded) != string(raw) {
		t.Fatal("profile corrupted")
	}
}
func TestRawProfileRejectsOtherEndpoints(t *testing.T) {
	for _, path := range []string{"/v1/status", "/v1/profile/heap?gc=1", "/v1/profile/goroutine"} {
		if _, err := rawRuntimeProfile(context.Background(), livediagnostics.ProbeRequest{}, Collector{Path: path}); err == nil {
			t.Fatal("unexpected runtime endpoint accepted")
		}
	}
}
func TestMemoryCheckpointHasIndependentCaptureWithoutClaimingAnalysis(t *testing.T) {
	dir := t.TempDir()
	v := map[string]any{"schema": "fugue.diagnostic.memory_profile.v1", "target_container_id": "containerd://fixture", "target_pids": []int{12}, "samples": []map[string]string{{"observed_at": "2026-01-01T00:00:00Z"}, {"observed_at": "2026-01-01T00:01:00Z"}}}
	raw, _ := json.Marshal(v)
	path := filepath.Join(dir, "memory-checkpoint.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	saved, err := readBounded(path, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	got, err := memoryProfileEvidence([]byte(saved), "containerd://fixture", map[int]string{12: "1"}, 60)
	if err != nil || len(got.Gaps) == 0 {
		t.Fatalf("checkpoint must retain missing analysis: %+v %v", got, err)
	}
	if _, ok := got.Value.(map[string]any)["samples"]; !ok {
		t.Fatal("capture lost")
	}
}

func TestPostgresCgroupResolvesNamespaceRelativePathByIdentity(t *testing.T) {
	root := t.TempDir()
	id := strings.Repeat("a", 64)
	dir := filepath.Join(root, "kubepods.slice", "pod.slice", "cri-containerd-"+id+".scope")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte("27\n42\n"), 0600)
	os.WriteFile(filepath.Join(dir, "memory.current"), []byte("100"), 0600)
	got, err := postgresCgroupAt(root, 42, id)
	if err != nil || got != dir {
		t.Fatalf("cgroup not resolved %s %v", got, err)
	}
	if _, err := postgresCgroupAt(root, 43, id); err == nil {
		t.Fatal("accepted wrong PID membership")
	}
}
