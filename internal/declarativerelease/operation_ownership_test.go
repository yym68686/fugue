package declarativerelease

import (
	"os"
	"slices"
	"testing"
)

func TestQueueAndReadModelReleaseSourceOwnership(t *testing.T) {
	f, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	registry, err := DecodeRegistry(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		want []string
	}{
		{"internal/operationqueue/selection.go", []string{"controller"}},
		{"internal/operationview/repository.go", []string{"api"}},
		{"internal/operationview/contract/contract.go", []string{"api", "controller", "observation-worker", "ssh-control"}},
	} {
		var got []string
		for _, component := range registry.Components {
			for _, root := range component.SourceRoots {
				if pathMatchesRoot(tc.path, root) {
					got = append(got, component.ID)
					break
				}
			}
		}
		slices.Sort(got)
		if !slices.Equal(got, tc.want) {
			t.Errorf("source owners for %s = %v, want %v", tc.path, got, tc.want)
		}
	}
}
