package declarativerelease

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestSSHControlEnrollmentDoesNotGrantServingMutationAuthority(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/releases/ssh-control/resources.json")
	if err != nil {
		t.Fatal(err)
	}
	resources, err := DecodeResourceSet(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	foundObserver, foundDeployment := false, false
	for _, item := range resources.Items {
		switch stringField(item, "kind") {
		case "ClusterRole":
			foundObserver = true
			rules, _ := item["rules"].([]any)
			for _, rawRule := range rules {
				rule := rawRule.(map[string]any)
				for _, verb := range rule["verbs"].([]any) {
					if verb != "get" && verb != "list" {
						t.Fatal("initial SSH controller enrollment can mutate resources", verb)
					}
				}
				for _, resource := range rule["resources"].([]any) {
					if resource == "secrets" || resource == "pods/exec" || resource == "*" {
						t.Fatal("initial enrollment exposes credentials or execution authority", resource)
					}
				}
			}
		case "Deployment":
			foundDeployment = true
			workload := Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-ssh-control", Container: "ssh-control", Replicas: 1, RolloutMode: "rolling"}
			if _, err := resources.Primary(workload); err != nil {
				t.Fatal(err)
			}
			spec, _ := objectField(item, "spec")
			template, _ := objectField(spec, "template")
			pod, _ := objectField(template, "spec")
			volumes := pod["volumes"].([]any)
			for _, rawVolume := range volumes {
				volume := rawVolume.(map[string]any)
				if secret, ok := volume["secret"].(map[string]any); ok && secret["optional"] != true {
					t.Fatal("read-only controller requires unprovisioned SSH secrets at startup")
				}
			}
		}
	}
	if !foundObserver || !foundDeployment {
		t.Fatal("controller release lacks its explicit observation boundary")
	}
	if strings.Contains(string(raw), "private_key") || strings.Contains(string(raw), "hostPath") {
		t.Fatal("controller manifest embeds private material or host access")
	}
}
