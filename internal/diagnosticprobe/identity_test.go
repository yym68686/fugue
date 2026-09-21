package diagnosticprobe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/livediagnostics"
)

func TestExecutableIdentityReadsActualGoELFWithoutExecutingIt(t *testing.T) {
	if testing.Short() {
		t.Skip("building a Linux ELF fixture")
	}
	dir := t.TempDir()
	source := filepath.Join(dir, "main.go")
	if err := os.WriteFile(source, []byte("package main\nfunc main(){}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "fixture")
	cmd := exec.Command("go", "build", "-o", output, source)
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build ELF fixture: %v %s", err, raw)
	}
	result, err := executableIdentity(output)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(result["go_version"].(string), "go") {
		t.Fatalf("missing build identity: %+v", result)
	}
	lineTable := false
	for _, section := range result["sections"].([]map[string]any) {
		if section["name"] == ".gopclntab" {
			lineTable = true
			if section["go_text_start"] == nil || section["header_hex"] == nil {
				t.Fatal("missing declared Go text origin")
			}
		}
	}
	if !lineTable {
		t.Fatalf("missing actual Go line table metadata: %+v", result)
	}
	procRoot := filepath.Join(dir, "proc")
	process := filepath.Join(procRoot, "42")
	if err := os.MkdirAll(process, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(output, filepath.Join(process, "exe")); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		"stat":   "42 (fixture) S 1 2 3 4 5 6 7 8 9 10 111 222 13 14 15 16 7 18 9000 20",
		"cgroup": "0::/service", "maps": "", "schedstat": "1 2 3",
	} {
		if err := os.WriteFile(filepath.Join(process, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	value, err := processIdentities(context.Background(), livediagnostics.ProbeRequest{Target: livediagnostics.Target{ProcessName: "fixture"}}, procRoot, "example.org/missing")
	partial, ok := value.(partialValue)
	if err != nil || !ok || !strings.Contains(strings.Join(partial.Gaps, " "), "requested Go dependency unavailable") {
		t.Fatalf("missing embedded dependency was treated as complete: %v %v", value, err)
	}
	if err := os.WriteFile(output, []byte("not an ELF executable"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := executableIdentity(output); err == nil {
		t.Fatal("invalid executable was treated as symbol evidence")
	}
}

func TestExecutableCollectorRequiresFrozenTarget(t *testing.T) {
	if _, err := processIdentities(context.Background(), livediagnostics.ProbeRequest{}, t.TempDir()); err == nil {
		t.Fatal("node-wide executable reads were accepted without a process target")
	}
}
