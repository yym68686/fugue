package declarativerelease

import (
	"encoding/json"
	"os"
	"testing"
)

func TestRuntimeAgentCanaryCannotExecuteExistingBusinessWorkloads(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/releases/runtime-agent-canary/resources.json")
	if err != nil {
		t.Fatal(err)
	}
	var set struct {
		Items []map[string]any `json:"items"`
	}
	if err = json.Unmarshal(raw, &set); err != nil {
		t.Fatal(err)
	}
	var deployment, pvc map[string]any
	for _, r := range set.Items {
		switch r["kind"] {
		case "Deployment":
			deployment = r
		case "PersistentVolumeClaim":
			pvc = r
		default:
			t.Fatal("unexpected authority-bearing canary resource", r["kind"])
		}
	}
	if deployment == nil || pvc == nil {
		t.Fatal("Agent must have independent execution and persistent state")
	}
	meta := pvc["metadata"].(map[string]any)
	if meta["annotations"].(map[string]any)["fugue.pro/release-retain-on-rollback"] != "true" {
		t.Fatal("rollback would erase durable Agent authority floors")
	}
	spec := deployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	if spec["automountServiceAccountToken"] != false {
		t.Fatal("canary gained Kubernetes execution authority")
	}
	container := spec["containers"].([]any)[0].(map[string]any)
	env := map[string]any{}
	for _, r := range container["env"].([]any) {
		v := r.(map[string]any)
		env[v["name"].(string)] = v
	}
	if env["FUGUE_AGENT_APPLY_WITH_KUBECTL"].(map[string]any)["value"] != "false" || env["FUGUE_AGENT_EDGE_TRUST_FILE"] == nil || env["FUGUE_AGENT_EDGE_CHECKPOINT_FILE"] == nil {
		t.Fatal("canary execution/trust boundary missing")
	}
	for _, name := range []string{"FUGUE_AGENT_RUNTIME_ID", "FUGUE_AGENT_RUNTIME_KEY"} {
		if env[name].(map[string]any)["valueFrom"] == nil || env[name].(map[string]any)["value"] != nil {
			t.Fatal("runtime identity was embedded in manifest")
		}
	}
	for _, v := range spec["volumes"].([]any) {
		volume := v.(map[string]any)
		if volume["hostPath"] != nil || volume["emptyDir"] != nil {
			t.Fatal("canary must neither access host state nor discard its durable checkpoint")
		}
	}
}
