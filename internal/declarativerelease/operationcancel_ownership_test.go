package declarativerelease

import (
	"os"
	"slices"
	"testing"
)

func TestOperationCancellationReleaseSourceOwnership(t *testing.T) {
	f, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	registry, err := DecodeRegistry(f)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, component := range registry.Components {
		for _, root := range component.SourceRoots {
			if pathMatchesRoot("internal/operationcancel/cancel.go", root) {
				got = append(got, component.ID)
				break
			}
		}
	}
	slices.Sort(got)
	want := []string{"api", "controller", "observation-worker", "ssh-control"}
	if !slices.Equal(got, want) {
		t.Fatalf("operation cancellation source owners = %v, want %v", got, want)
	}
}
