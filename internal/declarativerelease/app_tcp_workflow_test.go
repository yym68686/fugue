package declarativerelease

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAppTCPTransportIsExplicitCASConfigurationNotImageRollout(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var doc yaml.Node
	if err = yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	job := yamlMappingValue(t, yamlMappingValue(t, doc.Content[0], "jobs"), "app_tcp_transport")
	condition := yamlMappingValue(t, job, "if").Value
	if !strings.Contains(condition, "refs/heads/main") || !strings.Contains(condition, "github.event_name == 'push'") {
		t.Fatal("TCP network writes require a main push")
	}
	encoded, err := yaml.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	source := string(encoded)
	for _, required := range []string{"scripts.test_reconcile_app_tcp_transport", "git diff --quiet HEAD origin/main", "scripts/reconcile_app_tcp_transport.py", "--apply --evidence", "fugue-production-app-tcp-transport", "cancel-in-progress: false"} {
		if !strings.Contains(source, required) {
			t.Fatalf("TCP transport lost explicit configuration fence: %s", required)
		}
	}
	for _, forbidden := range []string{"docker build", "docker push", "rollout restart", "install_fugue_ha", "--force"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("TCP transport cannot mutate code/workloads: %s", forbidden)
		}
	}
}
