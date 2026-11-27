package releasesourceaudit

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRetiredReleaseModesHaveZeroRepositoryHits(t *testing.T) {
	t.Helper()
	banned := []string{
		"Ownership" + "Adoption",
		"ownership" + "Adoption",
		"adoption" + "Receipt",
		"adoption" + "-receipt",
		"Bootstrap" + "Runtime",
		"bootstrap" + "Runtime",
		"Bootstrap" + "LKG",
		"bootstrap" + "Lkg",
		"force" + "-conflicts",
		"InitialExplicit" + "Bootstrap",
		"receiptBound" + "Historical",
		"allowsHistorical" + "Restarts",
		"legacy" + "SourceTag",
		"adopt" + "ing",
		"composite" + "coordinator",
		"release" + "contract",
		"composite" + "-release-transactions",
		"Composite" + "Release",
		"component" + "manifest",
		"component" + "-release-plan",
		"component" + "_release_plan",
		"component" + "_plan.observe",
		"fugue-" + "component-plan",
		"Component" + "ReleasePlan",
	}
	command := exec.Command("git", "ls-files", "-z")
	command.Dir = "../.."
	raw, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range bytes.Split(raw, []byte{0}) {
		if len(name) == 0 {
			continue
		}
		content, err := os.ReadFile(filepath.Join("../..", string(name)))
		if err != nil {
			t.Fatal(err)
		}
		for _, token := range banned {
			if bytes.Contains(content, []byte(token)) {
				t.Fatalf("retired release token %q remains in %s", token, name)
			}
		}
	}
}
