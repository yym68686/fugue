package declarativerelease

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"fugue/internal/edgetopology"
)

func TestNeutralControlCandidateHasIndependentStateAndNoCoreIssuer(t *testing.T) {
	file, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	registry, err := DecodeRegistry(file)
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	topologyFile, err := os.Open("../../deploy/edge/topology.json")
	if err != nil {
		t.Fatal(err)
	}
	topology, err := edgetopology.Decode(topologyFile)
	topologyFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, component := range registry.Components {
		if !strings.HasPrefix(component.ID, "edge-control-") {
			continue
		}
		items := materializedResourceSetItems(t, component.ManifestPath, component.ManifestVariables)
		for _, item := range items {
			if item["kind"] != "Deployment" {
				continue
			}
			meta := item["metadata"].(map[string]any)
			if meta["annotations"].(map[string]any)["fugue.pro/authority-transition-role"] != "isolated-candidate" {
				continue
			}
			checked++
			observedService := false
			for _, probe := range component.Health {
				if probe.Type == "service-http" {
					t.Fatal("isolated Control cannot rely on an unauthorized API-server proxy path")
				}
				if probe.Type == "service-http-via-workload" && probe.Name == component.Workload.Name && probe.Path == "/readyz" && probe.Expected == `"ready":true` && probe.SourceWorkload != "" && probe.SourceContainer != "" {
					observedService = true
				}
			}
			if !observedService {
				t.Fatal("candidate lacks an explicitly allowed in-cluster Service observer")
			}
			group := meta["labels"].(map[string]any)["fugue.io/authority-cell-id"].(string)
			if !strings.HasPrefix(group, "cell-") {
				t.Fatal("neutral scope is absent")
			}
			template := item["spec"].(map[string]any)["template"].(map[string]any)
			spec := template["spec"].(map[string]any)
			selectors := spec["nodeSelector"].(map[string]any)
			if len(selectors) != 1 || selectors["kubernetes.io/hostname"] == nil || spec["automountServiceAccountToken"] != false || spec["hostNetwork"] == true {
				t.Fatal("candidate placement or identity is implicit")
			}
			node := selectors["kubernetes.io/hostname"].(string)
			member := false
			for _, edge := range topology.Edges {
				member = member || edge.ID == node && edge.AuthorityCellID == group
			}
			if !member {
				t.Fatal("candidate node is outside the declared neutral cell")
			}
			annotations := template["metadata"].(map[string]any)["annotations"].(map[string]any)
			var policy map[string]any
			if json.Unmarshal([]byte(annotations["fugue.pro/consumer-identity"].(string)), &policy) != nil || policy["authority_id"] != group || policy["component"] != "edge-control" {
				t.Fatal("credential policy not cell-bound")
			}
			for _, c := range spec["containers"].([]any) {
				container := c.(map[string]any)
				token := false
				for _, e := range container["env"].([]any) {
					env := e.(map[string]any)
					if env["name"] == "FUGUE_EDGE_CONTROL_ROUTE_INTENT_ISSUER_FILE" {
						t.Fatal("candidate mounts Core signing authority")
					}
					token = token || env["name"] == "FUGUE_EDGE_CONTROL_ROUTE_INTENT_POD_TOKEN_FILE"
				}
				if !token {
					t.Fatal("bound Pod exchange missing")
				}
				for _, p := range container["ports"].([]any) {
					if p.(map[string]any)["hostPort"] != nil {
						t.Fatal("candidate owns a host port")
					}
				}
			}
			state, identity, independentKeys := false, false, 0
			for _, v := range spec["volumes"].([]any) {
				vol := v.(map[string]any)
				if vol["name"] == "authority-state" {
					state = vol["persistentVolumeClaim"].(map[string]any)["claimName"] == component.Workload.Name+"-state"
				}
				if vol["name"] == "control-pod-identity" {
					identity = vol["projected"] != nil
				}
				if strings.HasSuffix(vol["name"].(string), "-keyring") {
					secret := vol["secret"].(map[string]any)
					if secret["optional"] != true || !strings.HasPrefix(secret["secretName"].(string), "fugue-"+group+"-") {
						t.Fatal("keys depend on a legacy authority")
					}
					independentKeys++
				}
			}
			if !state || !identity || independentKeys != 4 {
				t.Fatal("candidate state or independent configuration missing")
			}
			for _, r := range items {
				if r["kind"] == "Service" {
					s := r["spec"].(map[string]any)
					if s["type"] != "ClusterIP" || s["externalIPs"] != nil || s["loadBalancerIP"] != nil {
						t.Fatal("candidate exposes public traffic")
					}
				}
				if r["kind"] == "PersistentVolumeClaim" && r["metadata"].(map[string]any)["annotations"].(map[string]any)["fugue.pro/release-retain-on-rollback"] != "true" {
					t.Fatal("failed code could erase authority state")
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("neutral Control candidate declaration is missing")
	}
}
