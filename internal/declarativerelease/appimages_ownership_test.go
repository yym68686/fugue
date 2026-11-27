package declarativerelease

import (
	"os"
	"slices"
	"testing"
)

func TestApplicationImageReleaseSourceOwnership(t *testing.T) {
	f, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	registry, err := DecodeRegistry(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"internal/appimages/remote_registry.go",
		"internal/appimages/usage_snapshots.go",
		"internal/appimages/distributed_evidence.go",
	} {
		var got []string
		for _, component := range registry.Components {
			if slices.ContainsFunc(component.SourceRoots, func(root string) bool {
				return pathMatchesRoot(path, root)
			}) {
				got = append(got, component.ID)
			}
		}
		slices.Sort(got)
		if want := []string{"api", "controller"}; !slices.Equal(got, want) {
			t.Errorf("source owners for %s = %v, want %v", path, got, want)
		}
	}
}
