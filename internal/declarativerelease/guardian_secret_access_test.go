package declarativerelease

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestGuardianCanReadRequiredWorkerSecretMetadata(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/releases/guardian/resources.json")
	if err != nil {
		t.Fatal(err)
	}
	var resources struct {
		Items []struct {
			Kind     string
			Metadata struct{ Name string }
			Rules    []struct {
				Resources, ResourceNames, Verbs []string
			}
		}
	}
	if err := json.Unmarshal(raw, &resources); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{}
	for _, item := range resources.Items {
		if item.Kind != "Role" || item.Metadata.Name != "fugue-release-guardian" {
			continue
		}
		for _, rule := range item.Rules {
			if slices.Contains(rule.Resources, "secrets") && slices.Contains(rule.Verbs, "get") {
				if len(rule.ResourceNames) == 0 {
					t.Fatal("Guardian secret reads must remain restricted to named resources")
				}
				for _, name := range rule.ResourceNames {
					allowed[name] = true
				}
			}
		}
	}
	raw, err = os.ReadFile("../../deploy/releases/edge-groups.json")
	if err != nil {
		t.Fatal(err)
	}
	groups, err := DecodeEdgeGroupRegistry(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range groups.Groups {
		raw, err := os.ReadFile(filepath.Join("../..", group.Worker.ManifestPath))
		if err != nil {
			t.Fatal(err)
		}
		raw, err = MaterializeManifestTemplate(raw, group.Worker.ManifestVariables)
		if err != nil {
			t.Fatal(err)
		}
		names, err := ReferencedRequiredSecrets(raw)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			if !allowed[name] {
				t.Errorf("Guardian cannot read required Secret %q for %s", name, group.Worker.ID)
			}
		}
	}
}
