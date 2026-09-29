package declarativerelease

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"fugue/internal/edgetopology"
)

func TestNeutralWorkerCandidateCannotReplaceLegacyIdentityOrTransport(t *testing.T) {
	f, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	registry, err := DecodeRegistry(f)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	f, err = os.Open("../../deploy/edge/topology.json")
	if err != nil {
		t.Fatal(err)
	}
	topology, err := edgetopology.Decode(f)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, component := range registry.Components {
		if !strings.HasPrefix(component.ID, "edge-worker-") {
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
			if component.Transition != nil || component.Delivery != nil {
				t.Fatal("isolated worker must not invoke a retained Front transition")
			}
			cell := meta["labels"].(map[string]any)["fugue.io/authority-cell-id"].(string)
			template := item["spec"].(map[string]any)["template"].(map[string]any)
			annotations := template["metadata"].(map[string]any)["annotations"].(map[string]any)
			var identity map[string]any
			if annotations["fugue.io/edge-heartbeat-fenced"] != "true" || json.Unmarshal([]byte(annotations["fugue.pro/consumer-identity"].(string)), &identity) != nil || identity["authority_id"] != cell || identity["component"] != "edge-worker" {
				t.Fatal("candidate has no fenced cell identity")
			}
			spec := template["spec"].(map[string]any)
			selector := spec["nodeSelector"].(map[string]any)
			if len(selector) != 1 || spec["hostNetwork"] == true || spec["automountServiceAccountToken"] != false {
				t.Fatal("candidate placement or credentials are implicit")
			}
			member := false
			for _, edge := range topology.Edges {
				member = member || selector["kubernetes.io/hostname"] == edge.ID && edge.AuthorityCellID == cell
			}
			if !member {
				t.Fatal("candidate does not belong to its declared cell")
			}
			for _, raw := range spec["containers"].([]any) {
				container := raw.(map[string]any)
				for _, raw := range container["ports"].([]any) {
					if raw.(map[string]any)["hostPort"] != nil {
						t.Fatal("candidate owns a public host port")
					}
				}
				for _, raw := range container["env"].([]any) {
					env := raw.(map[string]any)
					if env["name"] == "FUGUE_EDGE_TOKEN" || env["name"] == "FUGUE_EDGE_DESIRED_STATE_URL" {
						t.Fatal("candidate has legacy Core identity")
					}
				}
			}
			state, podIdentity := false, false
			for _, raw := range spec["volumes"].([]any) {
				volume := raw.(map[string]any)
				if volume["hostPath"] != nil {
					t.Fatal("candidate shares host authority or legacy credentials")
				}
				if volume["name"] == "cell-state" {
					state = volume["persistentVolumeClaim"].(map[string]any)["claimName"] == component.Workload.Name+"-state"
				}
				podIdentity = podIdentity || volume["name"] == "platform-consumer-identity" && volume["projected"] != nil
				if volume["name"] == "route-reader" || volume["name"] == "bundle-verifier" || volume["name"] == "inventory-identity" {
					if !strings.HasPrefix(volume["secret"].(map[string]any)["secretName"].(string), "fugue-"+cell+"-") {
						t.Fatal("candidate shares legacy cell credentials")
					}
				}
			}
			if !state || !podIdentity {
				t.Fatal("candidate state or Pod identity is absent")
			}
			for _, r := range items {
				switch r["kind"] {
				case "Service":
					s := r["spec"].(map[string]any)
					ports := s["ports"].([]any)
					if s["type"] != "ClusterIP" || s["externalIPs"] != nil || s["loadBalancerIP"] != nil || len(ports) != 1 || ports[0].(map[string]any)["name"] != "health" {
						t.Fatal("candidate exposes serving transport")
					}
				case "PersistentVolumeClaim":
					if r["metadata"].(map[string]any)["annotations"].(map[string]any)["fugue.pro/release-retain-on-rollback"] != "true" {
						t.Fatal("code rollback may discard cell state")
					}
				case "NetworkPolicy":
					s := r["spec"].(map[string]any)
					ingress := s["ingress"].([]any)
					if len(ingress) != 1 {
						t.Fatal("candidate ingress is not observer-only")
					}
					entry := ingress[0].(map[string]any)
					if len(entry["ports"].([]any)) != 1 || fmt.Sprint(entry["ports"].([]any)[0].(map[string]any)["port"]) != "7832" {
						t.Fatal("candidate allows business traffic")
					}
					peer := entry["from"].([]any)[0].(map[string]any)
					if peer["namespaceSelector"] == nil || peer["podSelector"].(map[string]any)["matchLabels"].(map[string]any)["app.kubernetes.io/component"] != "api" {
						t.Fatal("candidate observer is not bounded")
					}
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("neutral worker candidate declaration is absent")
	}
}
