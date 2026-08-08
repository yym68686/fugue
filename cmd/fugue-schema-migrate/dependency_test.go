package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSchemaMigratorDoesNotLinkSharedProductPackages(t *testing.T) {
	repositoryRoot := filepath.Join("..", "..")
	command := exec.Command("go", "list", "-deps", "./cmd/fugue-schema-migrate")
	command.Dir = repositoryRoot
	raw, err := command.Output()
	if err != nil {
		t.Fatalf("list schema migrator dependencies: %v", err)
	}
	dependencies := make(map[string]struct{})
	for _, line := range strings.Split(string(raw), "\n") {
		dependencies[strings.TrimSpace(line)] = struct{}{}
	}
	for _, forbidden := range []string{
		"fugue/internal/store",
		"fugue/internal/api",
		"fugue/internal/controller",
		"fugue/cmd/fugue-api",
		"fugue/cmd/fugue-controller",
	} {
		if _, exists := dependencies[forbidden]; exists {
			t.Fatalf("schema migrator links forbidden shared product package %q", forbidden)
		}
	}
	if _, exists := dependencies["fugue/internal/schemamigrate"]; !exists {
		t.Fatal("schema migrator does not link its schema-owned migration package")
	}
}
