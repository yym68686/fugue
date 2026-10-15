package declarativerelease

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestRetiredDNSExecutorsCannotBeSelectedByActiveCodeReleases(t *testing.T) {
	read := func(path string) []byte {
		t.Helper()
		raw, err := os.ReadFile("../../" + path)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	active, err := DecodeRegistry(bytes.NewReader(read("deploy/releases/components.json")))
	if err != nil {
		t.Fatal(err)
	}
	retired, err := DecodeRegistry(bytes.NewReader(read("deploy/releases/retired-dns-components.json")))
	if err != nil {
		t.Fatal(err)
	}
	var declaration struct {
		Workloads []struct {
			Name string `json:"name"`
		} `json:"workloads"`
	}
	if err := json.Unmarshal(read("deploy/environments/production/dns-slot-retirement/intent.json"), &declaration); err != nil {
		t.Fatal(err)
	}
	stopped := map[string]bool{}
	for _, workload := range declaration.Workloads {
		stopped[workload.Name] = true
	}
	for _, component := range active.Components {
		if stopped[component.Workload.Name] {
			t.Fatal("retired executor is still an active release target", component.ID)
		}
	}
	for _, component := range retired.Components {
		if !stopped[component.Workload.Name] {
			t.Fatal("retired catalog has no explicit retirement declaration", component.ID)
		}
		plan, err := BuildPlan(active, testSHA1, testSHA2, []string{component.IntentPath, component.ManifestPath})
		if err != nil || len(plan.Releases) != 0 {
			t.Fatal("historical template can reactivate a retired executor", component.ID, err)
		}
	}
}
