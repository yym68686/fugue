package declarativerelease

import (
	"os"
	"strings"
	"testing"
)

func TestPublicFrontObservationMountsDoNotGrantWorkerActivationAuthority(t *testing.T) {
	raw, err := os.ReadFile("../../internal/edge/component/resources.inventory-producer.group.json")
	if err != nil {
		t.Fatal(err)
	}
	group := edgeGroupFixture("gamma", "edge-group-metro-gamma")
	materialized, err := MaterializeManifestTemplate(raw, group.Worker.ManifestVariables)
	if err != nil {
		t.Fatal(err)
	}
	set, err := DecodeResourceSet(strings.NewReader(string(materialized)))
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, item := range set.Items {
		if item["kind"] != "DaemonSet" {
			continue
		}
		pod := item["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
		paths := map[string]string{}
		for _, rawVolume := range pod["volumes"].([]any) {
			volume := rawVolume.(map[string]any)
			if host, ok := volume["hostPath"].(map[string]any); ok {
				paths[stringField(volume, "name")] = stringField(host, "path")
			}
		}
		if paths["network-observations"] != "/var/lib/fugue/edge-owned/"+group.GroupID+"/network-observations" || paths["activation-state"] == paths["network-observations"] {
			t.Fatal("observation mount must be separate from activation authority", paths)
		}
		for _, rawContainer := range pod["containers"].([]any) {
			container := rawContainer.(map[string]any)
			name := stringField(container, "name")
			if name != "edge" && name != "edge-front" {
				continue
			}
			enabled, mounted := false, false
			for _, rawEnv := range container["env"].([]any) {
				env := rawEnv.(map[string]any)
				if stringField(env, "name") == "FUGUE_EDGE_FRONT_NETWORK_SOCKET" {
					enabled = stringField(env, "value") == "/var/run/fugue-network/front.sock"
				}
			}
			for _, rawMount := range container["volumeMounts"].([]any) {
				mount := rawMount.(map[string]any)
				if stringField(mount, "name") == "network-observations" {
					mounted = stringField(mount, "mountPath") == "/var/run/fugue-network" && mount["readOnly"] == (name == "edge")
				}
			}
			if !enabled || !mounted {
				t.Fatal("Front must own observation directory, worker may only connect", name)
			}
			checked++
		}
	}
	if checked != 3 {
		t.Fatal("incomplete Front and worker-slot coverage", checked)
	}
}
