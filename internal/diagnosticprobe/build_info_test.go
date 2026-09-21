package diagnosticprobe

import (
	"encoding/json"
	"runtime/debug"
	"strings"
	"testing"
)

func TestGoBuildSelectionPreservesReplacementsWithoutBuildSecrets(t *testing.T) {
	build := &debug.BuildInfo{Deps: []*debug.Module{
		{Path: "example.org/store", Version: "v1.2.3", Sum: "h1:original", Replace: &debug.Module{Path: "example.org/store-fork", Version: "v1.2.4", Sum: "h1:replacement"}},
		{Path: "example.org/local", Version: "v1.0.0", Replace: &debug.Module{Path: "/private/build/workspace"}},
		{Path: "example.org/unselected", Version: "v3.0.0"},
	}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "source-revision"}, {Key: "vcs.modified", Value: "true"}, {Key: "-ldflags", Value: "private-build-secret"}}}
	result, missing := selectedGoBuild(build, []string{"example.org/store", "example.org/local", "example.org/missing"})
	if len(missing) != 1 || missing[0] != "example.org/missing" {
		t.Fatalf("missing dependency hidden: %v", missing)
	}
	raw, _ := json.Marshal(result)
	for _, forbidden := range []string{"private-build-secret", "/private/build/workspace", "example.org/unselected"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("excluded build metadata exported: %s", raw)
		}
	}
	for _, required := range []string{"v1.2.3", "v1.2.4", "h1:replacement", "source-revision", `"local_replacement":true`, `"vcs.modified":"true"`} {
		if !strings.Contains(string(raw), required) {
			t.Fatalf("provenance lost: %s", raw)
		}
	}
}

func TestGoBuildSelectionBoundsAndMissingMetadata(t *testing.T) {
	for _, selection := range [][]string{{"example.org/a", "example.org/a"}, {"bad\nselector"}, {strings.Repeat("x", 257)}, make([]string, 33)} {
		if validateGoModules(selection) == nil {
			t.Fatalf("accepted invalid dependency selection: %q", selection)
		}
	}
	if err := validateGoModules([]string{"go.etcd.io/etcd/server/v3", "go.etcd.io/bbolt"}); err != nil {
		t.Fatal(err)
	}
	_, missing := selectedGoBuild(&debug.BuildInfo{Deps: []*debug.Module{{Path: "example.org/store", Version: strings.Repeat("x", 257)}}}, []string{"example.org/store"})
	if len(missing) != 1 {
		t.Fatal("oversized metadata silently appeared complete")
	}
}
