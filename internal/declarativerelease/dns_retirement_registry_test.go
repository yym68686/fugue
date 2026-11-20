package declarativerelease

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
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
	const retirement = "deploy/environments/production/dns-slot-retirement/"
	history, err := filepath.Glob("../../" + retirement + "history/*.json")
	if err != nil {
		t.Fatal(err)
	}
	declarations := []string{retirement + "intent.json"}
	for _, path := range history {
		declarations = append(declarations, retirement+"history/"+filepath.Base(path))
	}
	stopped := map[string]bool{}
	generations := map[int]bool{}
	for _, path := range declarations {
		var declaration struct {
			Schema     string `json:"schema"`
			Generation int    `json:"generation"`
			Workloads  []struct {
				Name string `json:"name"`
			} `json:"workloads"`
		}
		if err := json.Unmarshal(read(path), &declaration); err != nil {
			t.Fatal(err)
		}
		if declaration.Schema != "fugue.dns-slot-retirement/v1" || declaration.Generation <= 0 ||
			generations[declaration.Generation] || len(declaration.Workloads) == 0 {
			t.Fatal("invalid or repeated DNS retirement declaration", path)
		}
		generations[declaration.Generation] = true
		for _, workload := range declaration.Workloads {
			if workload.Name == "" {
				t.Fatal("unnamed retired DNS workload", path)
			}
			stopped[workload.Name] = true
		}
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
