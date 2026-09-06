package api

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestClusterNodeHotPathsScriptDrainsLargeInventory(t *testing.T) {
	t.Parallel()
	// Exceed pipe buffers and include duplicates so that premature downstream
	// closure and a limit applied before deduplication both fail this test.
	output, err := runNodeDiagnosisScriptFixture(t, clusterNodeDiagnosisHotPathsScript, map[string]string{
		"chroot": "#!/bin/sh\nawk 'BEGIN { for (i=50000; i>0; i--) { printf \"%d\\t/var/lib/fixture/%08d\\n\",i,i; printf \"%d\\t/var/lib/fixture/%08d\\n\",i,i } }'\n",
	})
	if err != nil {
		t.Fatalf("inventory failed: %v\n%s", err, output)
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != clusterNodeDiagnosisHotPathLimit {
		t.Fatalf("got %d rows, want %d", len(lines), clusterNodeDiagnosisHotPathLimit)
	}
	for i, line := range lines {
		value := 50000 - i
		want := fmt.Sprintf("%d\t/var/lib/fixture/%08d", value, value)
		if line != want {
			t.Fatalf("row %d: got %q, want %q", i, line, want)
		}
	}
}

func TestClusterNodeHotPathsScriptPreservesCollectionFailure(t *testing.T) {
	t.Parallel()
	_, err := runNodeDiagnosisScriptFixture(t, clusterNodeDiagnosisHotPathsScript, map[string]string{
		"chroot": "#!/bin/sh\nprintf '123\\t/var/lib/fixture\\n'\nexit 7\n",
	})
	if err == nil {
		t.Fatal("expected the collector failure to remain visible")
	}
}

func TestClusterNodeJournalScriptIncludesImageGCFailures(t *testing.T) {
	t.Parallel()
	output, err := runNodeDiagnosisScriptFixture(t, clusterNodeDiagnosisJournalScript, map[string]string{
		"chroot": "#!/bin/sh\nexec /bin/sh -c \"$4\"\n",
		"journalctl": `#!/bin/sh
# Model journalctl's matching-before-limit semantics. The old command only
# selected the last 400 unfiltered records and lost every failure below.
limit=400
pattern=.
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n) limit="$2"; shift ;;
    --grep) pattern="$2"; shift ;;
  esac
  shift
done
cat <<'LOG' | grep -Ei "$pattern" | tail -n "$limit"
2026-01-01T00:00:00Z image_gc_manager.go: Disk usage on image filesystem is over the high threshold
2026-01-01T00:00:01Z Image garbage collection failed multiple times in a row
2026-01-01T00:00:02Z Insufficient free disk space on the node's image filesystem
2026-01-01T00:00:03Z FreeDiskSpaceFailed: freed 0 bytes
2026-01-01T00:00:04Z ImageGCFailed
2026-01-01T00:00:05Z eviction manager: attempting to reclaim ephemeral-storage
2026-01-01T00:00:06Z ordinary unrelated message
` + strings.Repeat("2026-01-01T00:01:00Z ordinary unrelated message\n", 500) + `LOG
`,
	})
	if err != nil {
		t.Fatalf("journal collection failed: %v\n%s", err, output)
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != 6 || strings.Contains(string(output), "ordinary unrelated") {
		t.Fatalf("unexpected filtered journal: %s", output)
	}
}

func runNodeDiagnosisScriptFixture(t *testing.T, script string, programs map[string]string) ([]byte, error) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash unavailable")
	}
	dir := t.TempDir()
	for name, contents := range programs {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bash, "-c", script)
	cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return cmd.CombinedOutput()
}
