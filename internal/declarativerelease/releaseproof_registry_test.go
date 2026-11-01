package declarativerelease

import (
	"os"
	"reflect"
	"testing"
)

func TestSharedReleaseProofSelectsBothRuntimeObservers(t *testing.T) {
	f, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r, err := DecodeRegistry(f)
	if err != nil {
		t.Fatal(err)
	}
	p, err := BuildPlan(r, testSHA1, testSHA2, []string{"internal/releaseproof/readiness.go", "deploy/releases/api/intent.json", "deploy/releases/controller/intent.json"})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, release := range p.Releases {
		if !reflect.DeepEqual(release.ChangedPaths, []string{"internal/releaseproof/readiness.go"}) {
			t.Fatalf("shared readiness gate missing from %s lane: %v", release.ComponentID, release.ChangedPaths)
		}
		got = append(got, release.ComponentID)
	}
	if !reflect.DeepEqual(got, []string{"api", "controller"}) {
		t.Fatalf("readiness gate change must rebuild both observers only: %v", got)
	}
}
