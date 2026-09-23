package declarativerelease

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// The initial isolated DNS lane may coexist with the selected legacy lane,
// but it must never share that lane's listener, cache or inventory credential.
func TestProductionDNSCandidateHasIsolatedExecutionAndRelease(t *testing.T) {
	read := func(name string) []byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join("../..", name))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	registry, err := DecodeRegistry(bytes.NewReader(read("deploy/releases/components.json")))
	if err != nil {
		t.Fatal(err)
	}
	edges, err := DecodeEdgeGroupRegistry(bytes.NewReader(read(registry.EdgeGroupRegistryPath)))
	if err != nil {
		t.Fatal(err)
	}
	registry, err = MergeEdgeGroupRegistry(registry, edges)
	if err != nil {
		t.Fatal(err)
	}
	var transport struct {
		Listeners []struct {
			Selector map[string]string `json:"selector"`
		} `json:"listeners"`
	}
	if err := json.Unmarshal(read("deploy/environments/production/dns-transport/transport.json"), &transport); err != nil {
		t.Fatal(err)
	}
	if len(transport.Listeners) == 0 {
		t.Fatal("no current DNS listeners to check for selector isolation")
	}
	cacheOwners := map[string]string{}
	found := 0
	for _, component := range registry.Components {
		manifest, err := MaterializeManifestTemplate(read(component.ManifestPath), component.ManifestVariables)
		if err != nil {
			t.Fatal(err)
		}
		resources, err := DecodeResourceSet(bytes.NewReader(manifest))
		if err != nil {
			t.Fatal(err)
		}
		candidate := component.Artifact.BuildPackage == "./cmd/fugue-dns"
		for _, resource := range resources.Items {
			if stringField(resource, "kind") != "DaemonSet" {
				continue
			}
			spec, _ := objectField(resource, "spec")
			template, _ := objectField(spec, "template")
			podSpec, _ := objectField(template, "spec")
			containers, _ := podSpec["containers"].([]any)
			for _, raw := range containers {
				container, _ := raw.(map[string]any)
				if stringField(container, "name") != "dns" {
					continue
				}
				if candidate {
					found++
					if component.Workload.RolloutMode != "on-delete" || podSpec["hostNetwork"] == true || len(containers) != 1 {
						t.Fatal("candidate can replace the live listener or couples unrelated executors", component.ID)
					}
					ports, _ := container["ports"].([]any)
					for _, raw := range ports {
						port, _ := raw.(map[string]any)
						if _, exists := port["hostPort"]; exists {
							t.Fatal("candidate claims a node listener", component.ID)
						}
					}
					metadata, _ := objectField(template, "metadata")
					labels := ensureReadStringMap(metadata, "labels")
					for _, listener := range transport.Listeners {
						matches := true
						for key, value := range listener.Selector {
							matches = matches && labels[key] == value
						}
						if matches {
							t.Fatal("unverified candidate is selected by an existing listener", component.ID)
						}
					}
					env, _ := container["env"].([]any)
					hasIdentity := false
					for _, raw := range env {
						entry, _ := raw.(map[string]any)
						switch stringField(entry, "name") {
						case "FUGUE_DNS_TOKEN", "FUGUE_EDGE_TOKEN":
							t.Fatal("candidate can overwrite selected legacy inventory", component.ID)
						case "FUGUE_DNS_PLATFORM_TOKEN_FILE":
							hasIdentity = stringField(entry, "value") != ""
						}
					}
					if !hasIdentity || len(resources.Items) != 2 || stringField(resources.Items[1], "kind") != "ServiceAccount" {
						t.Fatal("candidate lacks explicit identity or changes unrelated resources", component.ID)
					}
					plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{"internal/dnsserver/service.go", component.IntentPath, component.ManifestPath, component.Artifact.Dockerfile})
					if err != nil || len(plan.Releases) != 1 || plan.Releases[0].ComponentID != component.ID {
						t.Fatal("candidate code change activates another runtime lane", err)
					}
				}
				volumes, _ := podSpec["volumes"].([]any)
				for _, raw := range volumes {
					volume, _ := raw.(map[string]any)
					if stringField(volume, "name") != "dns-cache" {
						continue
					}
					host, _ := objectField(volume, "hostPath")
					path := stringField(host, "path")
					// Existing country lanes may use the same path on disjoint
					// nodes; every isolated candidate requires its own path.
					if owner, exists := cacheOwners[path]; exists && (candidate || owner == "candidate") {
						t.Fatal("overlapping DNS slots share durable state", path)
					}
					if candidate {
						if path == "" {
							t.Fatal("candidate has no durable cache")
						}
						cacheOwners[path] = "candidate"
					} else {
						cacheOwners[path] = component.ID
					}
				}
			}
		}
	}
	if found == 0 {
		t.Fatal("no independently releasable DNS candidate")
	}
}
