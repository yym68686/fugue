package declarativerelease

import (
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAPIAndDNSReleaseQueuesRetainPendingPredecessors(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	jobs := yamlMappingValue(t, document.Content[0], "jobs")
	for name, group := range map[string]string{
		"deploy_api":         "fugue-production-api",
		"deploy_edge_client": "${{ matrix.concurrency }}",
	} {
		t.Run(name, func(t *testing.T) {
			job := yamlMappingValue(t, jobs, name)
			concurrency := yamlMappingValue(t, job, "concurrency")
			if yamlMappingValue(t, concurrency, "group").Value != group ||
				yamlMappingValue(t, concurrency, "cancel-in-progress").Value != "false" ||
				yamlMappingValue(t, concurrency, "queue").Value != "max" {
				t.Fatal("a successor must not replace a pending immutable predecessor")
			}
		})
	}
}
