package declarativerelease

import (
	"bytes"
	"os"
	"reflect"
	"testing"
)

func TestAPIReleaseOwnsDNSReceiptReplaySource(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	registry, err := DecodeRegistry(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	// The API replays the DNS executor's actual decisions before admitting
	// quality evidence. A shared algorithm correction must be publishable
	// through the API lane with its own explicit intent and predecessor fence.
	path := "internal/dnsserver/platform_serving_wire.go"
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{
		path, "deploy/releases/api/intent.json",
	})
	if err != nil {
		t.Fatal("API cannot release its DNS replay dependency", err)
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ComponentID != "api" ||
		!reflect.DeepEqual(plan.Releases[0].ChangedPaths, []string{path}) {
		t.Fatalf("DNS replay dependency is absent from the API release: %+v", plan.Releases)
	}
}
