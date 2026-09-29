package declarativerelease

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"fugue/internal/edgetopology"
)

func TestNeutralDNSCandidateIsPrivateAndKeepsIndependentState(t *testing.T) {
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
		if component.Artifact.BuildPackage != "./cmd/fugue-dns" || component.Workload.Kind != "Deployment" {
			continue
		}
		checked++
		items := materializedResourceSetItems(t, component.ManifestPath, component.ManifestVariables)
		if component.Transition != nil || component.Delivery != nil || component.Workload.RolloutMode != "recreate" || component.Workload.Replicas != 1 {
			t.Fatal("candidate can mutate existing serving authority")
		}
		var selector map[string]any
		stateClaim := ""
		validationPorts := map[string]bool{}
		for _, item := range items {
			if item["kind"] != "Deployment" {
				continue
			}
			meta := item["metadata"].(map[string]any)
			if meta["annotations"].(map[string]any)["fugue.pro/authority-transition-role"] != "isolated-candidate" {
				t.Fatal("unmarked DNS staging workload")
			}
			cell := meta["labels"].(map[string]any)["fugue.io/authority-cell-id"]
			spec := item["spec"].(map[string]any)
			selector = spec["selector"].(map[string]any)["matchLabels"].(map[string]any)
			template := spec["template"].(map[string]any)
			templateMeta := template["metadata"].(map[string]any)
			labels := templateMeta["labels"].(map[string]any)
			for k, v := range selector {
				if labels[k] != v {
					t.Fatal("candidate selector differs from workload")
				}
			}
			var identity map[string]any
			if json.Unmarshal([]byte(templateMeta["annotations"].(map[string]any)["fugue.pro/consumer-identity"].(string)), &identity) != nil || identity["component"] != "dns-server" || identity["authority_id"] != cell || labels["fugue.io/edge-group-id"] != cell {
				t.Fatal("DNS authority lacks exact Pod policy")
			}
			pod := template["spec"].(map[string]any)
			placement := pod["nodeSelector"].(map[string]any)
			independentDNS := false
			for _, resource := range items {
				if resource["kind"] != "Service" {
					continue
				}
				metadata := resource["metadata"].(map[string]any)
				serviceLabels, _ := metadata["labels"].(map[string]any)
				if serviceLabels["app.kubernetes.io/managed-by"] != "fugue-dns-validation" {
					continue
				}
				annotations, _ := metadata["annotations"].(map[string]any)
				service := resource["spec"].(map[string]any)
				if independentDNS || annotations["transport.fugue.dev/authority-id"] != cell || annotations["transport.fugue.dev/node-id"] != placement["kubernetes.io/hostname"] || !reflect.DeepEqual(service["selector"], selector) {
					t.Fatal("private DNS validation member is ambiguous or mismatched")
				}
				independentDNS = true
				for _, raw := range service["ports"].([]any) {
					port := raw.(map[string]any)
					validationPorts[fmt.Sprint(port["protocol"])+"/"+fmt.Sprint(port["targetPort"])] = true
				}
			}
			member := false
			for _, edge := range topology.Edges {
				member = member || edge.ID == placement["kubernetes.io/hostname"] && (independentDNS || edge.AuthorityCellID == cell)
			}
			if len(placement) != 1 || !member || pod["hostNetwork"] == true || pod["automountServiceAccountToken"] != false {
				t.Fatal("DNS placement or credential boundary is implicit")
			}
			containers := pod["containers"].([]any)
			if len(containers) != 1 {
				t.Fatal("DNS code couples unrelated executor")
			}
			c := containers[0].(map[string]any)
			for _, raw := range c["ports"].([]any) {
				if raw.(map[string]any)["hostPort"] != nil {
					t.Fatal("candidate owns public port")
				}
			}
			env := map[string]map[string]any{}
			for _, raw := range c["env"].([]any) {
				e := raw.(map[string]any)
				env[e["name"].(string)] = e
			}
			if env["FUGUE_DNS_TOKEN"] != nil || env["FUGUE_EDGE_TOKEN"] != nil || env["FUGUE_EDGE_GROUP_ID"]["value"] != cell || env["FUGUE_DNS_PLATFORM_TOKEN_FILE"]["value"] == "" {
				t.Fatal("candidate can impersonate legacy DNS inventory")
			}
			if env["FUGUE_DNS_NODE_ID"]["valueFrom"].(map[string]any)["fieldRef"].(map[string]any)["fieldPath"] != "spec.nodeName" {
				t.Fatal("candidate invented physical identity")
			}
			identityVolume := false
			for _, raw := range pod["volumes"].([]any) {
				v := raw.(map[string]any)
				if v["hostPath"] != nil {
					t.Fatal("candidate shares host DNS cache")
				}
				if v["persistentVolumeClaim"] != nil {
					stateClaim = v["persistentVolumeClaim"].(map[string]any)["claimName"].(string)
				}
				identityVolume = identityVolume || v["name"] == "platform-consumer-identity" && v["projected"] != nil
			}
			if stateClaim != component.Workload.Name+"-state" || !identityVolume {
				t.Fatal("missing independent state or projected identity")
			}
			for _, field := range []string{"readinessProbe", "livenessProbe"} {
				if c[field].(map[string]any)["httpGet"].(map[string]any)["path"] != "/livez" {
					t.Fatal("configuration absence blocks candidate process recovery")
				}
			}
		}
		retained, privateService, privatePolicy := false, false, false
		for _, item := range items {
			switch item["kind"] {
			case "PersistentVolumeClaim":
				m := item["metadata"].(map[string]any)
				retained = m["name"] == stateClaim && m["annotations"].(map[string]any)["fugue.pro/release-retain-on-rollback"] == "true"
			case "Service":
				s := item["spec"].(map[string]any)
				ports := s["ports"].([]any)
				if s["type"] != "ClusterIP" || s["externalIPs"] != nil || s["loadBalancerIP"] != nil || !reflect.DeepEqual(s["selector"], selector) {
					t.Fatal("candidate owns public or foreign Service transport")
				}
				for _, raw := range ports {
					if raw.(map[string]any)["nodePort"] != nil {
						t.Fatal("candidate owns node port")
					}
				}
				privateService = privateService || len(ports) == 1 && ports[0].(map[string]any)["name"] == "health"
			case "NetworkPolicy":
				s := item["spec"].(map[string]any)
				ingress := s["ingress"].([]any)
				if len(ingress) != 1 {
					t.Fatal("unbounded candidate ingress")
				}
				i := ingress[0].(map[string]any)
				ports := i["ports"].([]any)
				from := i["from"].([]any)
				if len(ports) != 1+len(validationPorts) || len(from) != 1 {
					t.Fatal("candidate exposes undeclared transport")
				}
				seenPorts := map[string]bool{}
				for _, raw := range ports {
					port := raw.(map[string]any)
					key := fmt.Sprint(port["protocol"]) + "/" + fmt.Sprint(port["port"])
					if seenPorts[key] || key != "TCP/7834" && !validationPorts[key] {
						t.Fatal("candidate permits undeclared ingress port")
					}
					seenPorts[key] = true
				}
				peer := from[0].(map[string]any)
				privatePolicy = reflect.DeepEqual(s["podSelector"].(map[string]any)["matchLabels"], selector) && peer["namespaceSelector"] != nil && peer["podSelector"].(map[string]any)["matchLabels"].(map[string]any)["app.kubernetes.io/component"] == "api"
			}
		}
		if !retained || !privateService || !privatePolicy {
			t.Fatal("candidate state or transport isolation incomplete")
		}
		plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{"internal/dnsserver/service.go", component.IntentPath, component.ManifestPath})
		if err != nil || len(plan.Releases) != 1 || plan.Releases[0].ComponentID != component.ID {
			t.Fatal("DNS candidate rolls unrelated executors", err)
		}
	}
	if checked == 0 {
		t.Fatal("no neutral DNS candidate declared")
	}
}
