package volumeretire

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

func TestHostExecutorSafetyAndRecovery(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("node executor requires a Unix host")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is required for node executor integration")
	}
	if out, err := exec.Command(python, "-B", "-m", "unittest", "host_test.py").CombinedOutput(); err != nil {
		t.Fatalf("node executor regression: %v\n%s", err, out)
	}
}

func TestInventoryFiltersStreamWithoutLosingReferences(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 required")
	}
	input := `{"kind":"Job","metadata":{"name":"unrelated"},"spec":{"text":"unrelated"}}
{"kind":"Job","metadata":{"name":"historical"},"spec":{"persistentVolumeClaim":{"claimName":"archive-db-2"}}}
{"kind":"PersistentVolume","metadata":{"name":"shared-parent"},"spec":{"local":{"path":"/var/lib/storage"}}}
{"kind":"StatefulSet","metadata":{"name":"db"},"spec":{"volumeClaimTemplates":[{"metadata":{"name":"archive"}}]}}
`
	cmd := exec.Command(python, "-B", "-c", InventoryScript, "archive-db-2", "/var/lib/storage/archive", "target-volume")
	cmd.Stdin = strings.NewReader(input)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("inventory: %v %s", err, stderr.String())
	}
	var result struct {
		Items []map[string]any `json:"items"`
	}
	if json.Unmarshal(out, &result) != nil || len(result.Items) != 3 {
		t.Fatalf("lost references: %s", out)
	}
	cmd = exec.Command(python, "-B", "-c", InventoryScript, "archive-db-2", "/var/lib/storage/archive", "target-volume")
	cmd.Stdin = strings.NewReader("invalid-json\n")
	if err := cmd.Run(); err == nil {
		t.Fatal("malformed inventory silently accepted")
	}
}
