package declarativerelease

import (
	"os"
	"slices"
	"testing"
)

func TestBuildAdmissionReleaseSourceOwnership(t *testing.T) {
	f, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	registry, err := DecodeRegistry(f)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		path string
		want []string
	}{
		{"internal/buildadmission/placement.go", []string{"controller"}},
		{"internal/buildadmission/contract/checkpoints.go", []string{"api", "controller", "observation-worker", "ssh-control"}},
		{"internal/store/builder_admission_checkpoint.go", []string{"api", "controller", "observation-worker", "ssh-control"}},
	}
	for _, tc := range cases {
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
