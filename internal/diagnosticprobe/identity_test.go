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
		}
	}
	if !lineTable {
		t.Fatalf("missing actual Go line table metadata: %+v", result)
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
