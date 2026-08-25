package declarativerelease

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestThirdEdgeGroupIsPureDataAndPlansIndependently(t *testing.T) {
	baseFile, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	base, err := DecodeRegistry(baseFile)
	_ = baseFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	edgeFile, err := os.Open("../../deploy/releases/edge-groups.json")
	if err != nil {
		t.Fatal(err)
	}
	edge, err := DecodeEdgeGroupRegistry(edgeFile)
	_ = edgeFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	virtual := edgeGroupFixture("gamma", "edge-group-metro-gamma")
	virtual.Client.Artifact = edge.Groups[0].Client.Artifact
	virtual.Client.SourceRoots = append([]string(nil), edge.Groups[0].Client.SourceRoots...)
	virtual.Control.Artifact = edge.Groups[0].Control.Artifact
	virtual.Worker.Artifact = edge.Groups[0].Worker.Artifact
	virtual.Control.SourceRoots = append([]string(nil), edge.Groups[0].Control.SourceRoots...)
	virtual.Worker.SourceRoots = append([]string(nil), edge.Groups[0].Worker.SourceRoots...)
	edge.Groups = append(edge.Groups, virtual)
	sort.Slice(edge.Groups, func(i, j int) bool { return edge.Groups[i].ID < edge.Groups[j].ID })
	if err := edge.Validate(); err != nil {
		t.Fatalf("third data-defined group: %v", err)
	}
	registry, err := MergeEdgeGroupRegistry(base, edge)
	if err != nil {
		t.Fatal(err)
	}

	byID := make(map[string]Component, len(registry.Components))
	for _, component := range registry.Components {
		byID[component.ID] = component
	}
	control := byID[virtual.Control.ID]
	client := byID[virtual.Client.ID]
	worker := byID[virtual.Worker.ID]
	if client.ID != "edge-client-gamma" || client.Concurrency != "fugue-production-edge-client-gamma" || virtual.FaultDomainID != "fault-domain-test-gamma" || virtual.EdgePoolID != "edge-pool-test-gamma" || virtual.Labels["country"] != "test" ||
		control.Workload.Name != "edge-control-gamma" || control.Concurrency != "fugue-production-edge-control-gamma" ||
		worker.Concurrency != "fugue-production-edge-worker-gamma" {
		t.Fatalf("third group did not get independent resources and Lease: client=%+v control=%+v worker=%+v", client, control, worker)
	}
	transition := worker.Transition.EdgeGroupAB
	if transition.GroupID != virtual.GroupID || transition.FrontName != "edge-gamma-front" ||
		transition.WorkerAName != "edge-gamma-worker-a" || transition.WorkerBName != "edge-gamma-worker-b" {
		t.Fatalf("third group inventory/bundle transition is not isolated: %+v", transition)
	}
	for _, component := range []Component{control, worker} {
		raw, readErr := os.ReadFile(filepath.Join("../..", component.ManifestPath))
		if readErr != nil {
			t.Fatalf("read shared %s template: %v", component.ID, readErr)
		}
		materialized, materializeErr := MaterializeManifestTemplate(raw, component.ManifestVariables)
		if materializeErr != nil {
			t.Fatalf("materialize third-group %s resources: %v", component.ID, materializeErr)
		}
		identities, identityErr := ResourceSetIdentities(materialized)
		if identityErr != nil || len(identities) == 0 {
			t.Fatalf("third-group %s resources are invalid: identities=%+v err=%v", component.ID, identities, identityErr)
		}
		for _, identity := range identities {
			if strings.Contains(identity.Name, "-de") || strings.Contains(identity.Name, "-us") {
				t.Fatalf("third-group %s inherited a configured group resource: %+v", component.ID, identity)
			}
		}
	}
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{
		"internal/edge/service.go",
		virtual.Worker.IntentPath,
	})
	if err != nil {
		t.Fatalf("plan third group: %v", err)
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ComponentID != virtual.Worker.ID {
		t.Fatalf("third group release plan was not isolated: %+v", plan.Releases)
	}
}

func TestProductionEdgeWorkersHaveGenericPublicRouteCanary(t *testing.T) {
	baseFile, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	base, err := DecodeRegistry(baseFile)
	_ = baseFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	edgeFile, err := os.Open("../../deploy/releases/edge-groups.json")
	if err != nil {
		t.Fatal(err)
	}
	edge, err := DecodeEdgeGroupRegistry(edgeFile)
	_ = edgeFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	merged, err := MergeEdgeGroupRegistry(base, edge)
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]Component, len(merged.Components))
	for _, component := range merged.Components {
		byID[component.ID] = component
	}
	for _, group := range edge.Groups {
		count := 0
		var workerCanary HealthProbe
		for _, probe := range group.Worker.Health {
			if probe.Type != "public-route-http" {
				continue
			}
			count++
			workerCanary = probe
			if err := probe.validate(); err != nil {
				t.Fatalf("group %s public route canary is invalid: %v", group.ID, err)
			}
			if probe.Name != group.GroupID {
				t.Fatalf("group %s public route canary is not group-bound: %+v", group.ID, probe)
			}
		}
		if count != 1 {
			t.Fatalf("group %s must define exactly one generic public route canary, got %d", group.ID, count)
		}
		controlCount := 0
		authorityCount := 0
		for _, probe := range byID[group.Control.ID].Health {
			if probe.Type == "public-route-http" {
				controlCount++
				if !reflect.DeepEqual(probe, workerCanary) {
					t.Fatalf("group %s control canary did not derive exactly from worker data: got=%+v want=%+v", group.ID, probe, workerCanary)
				}
			}
			if probe.Type == "service-http" && probe.Name == group.Control.Workload.Name && probe.Path == "/v1/authority/groups/"+group.GroupID+"/readyz" {
				authorityCount++
			}
		}
		if controlCount != 1 {
			t.Fatalf("group %s control must inherit exactly one public route canary, got %d", group.ID, controlCount)
		}
		if authorityCount != 0 {
			t.Fatalf("group %s inactive control candidate must not gate LKG health on candidate authority readiness, got %d probes", group.ID, authorityCount)
		}
	}
}

func TestProductionGoAndWorkflowDoNotNameConfiguredGroups(t *testing.T) {
	edgeFile, err := os.Open("../../deploy/releases/edge-groups.json")
	if err != nil {
		t.Fatal(err)
	}
	edge, err := DecodeEdgeGroupRegistry(edgeFile)
	_ = edgeFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range edge.Groups {
		for _, forbidden := range []string{group.Control.ID, group.Worker.ID} {
			if strings.Contains(string(workflow), forbidden) {
				t.Fatalf("workflow contains configured group %q", forbidden)
			}
		}
	}
	for _, group := range edge.Groups {
		for _, component := range []Component{group.Control, group.Worker} {
			raw, readErr := os.ReadFile(filepath.Join("../..", component.ManifestPath))
			if readErr != nil {
				t.Fatalf("read %s shared manifest: %v", component.ID, readErr)
			}
			materialized, materializeErr := MaterializeManifestTemplate(raw, component.ManifestVariables)
			if materializeErr != nil {
				t.Fatalf("materialize %s: %v", component.ID, materializeErr)
			}
			identities, identityErr := ResourceSetIdentities(materialized)
			if identityErr != nil {
				t.Fatalf("decode %s identities: %v", component.ID, identityErr)
			}
			foundPrimary := false
			for _, identity := range identities {
				foundPrimary = foundPrimary || (identity.APIVersion == component.Workload.APIVersion && identity.Kind == component.Workload.Kind &&
					identity.Namespace == component.Workload.Namespace && identity.Name == component.Workload.Name)
			}
			if !foundPrimary {
				t.Fatalf("%s shared manifest omitted primary workload: %+v", component.ID, identities)
			}
			for _, other := range edge.Groups {
				if other.ID != group.ID && (strings.Contains(string(materialized), other.GroupID) ||
					strings.Contains(string(materialized), other.Control.Workload.Name) || strings.Contains(string(materialized), other.Worker.Workload.Name)) {
					t.Fatalf("%s materialized resources contain %s identity", component.ID, other.ID)
				}
			}
		}
	}
	for _, root := range []string{"../edge", "../edgecontrol", "../edgegroupfront"} {
		err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			for _, group := range edge.Groups {
				if strings.Contains(string(raw), group.GroupID) {
					t.Fatalf("product Go file %s names configured group %q", path, group.GroupID)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSharedEdgeWorkerManifestRollsOneGroupPerIntent(t *testing.T) {
	baseFile, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	base, err := DecodeRegistry(baseFile)
	_ = baseFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	edgeFile, err := os.Open("../../deploy/releases/edge-groups.json")
	if err != nil {
		t.Fatal(err)
	}
	edge, err := DecodeEdgeGroupRegistry(edgeFile)
	_ = edgeFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	registry, err := MergeEdgeGroupRegistry(base, edge)
	if err != nil {
		t.Fatal(err)
	}
	selected := edge.Groups[0].Worker
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{selected.ManifestPath, selected.IntentPath})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ComponentID != selected.ID {
		t.Fatalf("shared manifest did not remain single-group: %+v", plan.Releases)
	}
}

func TestUSEdgeWorkerGuardianDeliveryBindsExactProductionLKG(t *testing.T) {
	baseFile, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	base, err := DecodeRegistry(baseFile)
	closeErr := baseFile.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("decode base registry: %v close: %v", err, closeErr)
	}
	edgeFile, err := os.Open("../../deploy/releases/edge-groups.json")
	if err != nil {
		t.Fatal(err)
	}
	edge, err := DecodeEdgeGroupRegistry(edgeFile)
	closeErr = edgeFile.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("decode edge registry: %v close: %v", err, closeErr)
	}
	var worker Component
	for _, group := range edge.Groups {
		if group.ID == "us" {
			worker = group.Worker
			break
		}
	}
	if worker.ID != "edge-worker-us" || worker.Delivery == nil || worker.Delivery.Writer != "guardian" ||
		worker.Delivery.Group != "us" || worker.Delivery.DependencyService != "edge-control-us" {
		t.Fatalf("US Edge Worker delivery is not Guardian-scoped: %+v", worker)
	}
	intentFile, err := os.Open("../../" + worker.IntentPath)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := DecodeIntent(intentFile)
	closeErr = intentFile.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("decode US Worker intent: %v close: %v", err, closeErr)
	}
	const lkgSHA = "9a3119c8bb32fd556e4c07aa711c2649e26c0a9c"
	const lkgImage = "sha256:b622c44463fcbb21ea63a92b8f55e00348eacc17bdec372abd352f001d32b03f"
	if intent.Generation != 30 || intent.ExpectedPreviousConfigSHA != lkgSHA || intent.ExpectedPreviousManifestSHA != lkgSHA ||
		intent.ExpectedPreviousOCIRevision != lkgSHA || intent.ExpectedPreviousImageDigest != lkgImage ||
		intent.SupersedesFailedConfigSHA != "f9949602a8537a021de769a444b685f0459f8605" {
		t.Fatalf("US Edge Worker intent does not bind the exact live LKG: %+v", intent)
	}
	registry, err := MergeEdgeGroupRegistry(base, edge)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{"deploy/releases/edge-groups.json", worker.IntentPath})
	if err != nil {
		t.Fatal(err)
	}
	prior := intent
	prior.Generation = 29
	prior.SupersedesFailedConfigSHA = "d4aee8852ecc8cafda577616469524abf6e7202e"
	bound, err := BindIntents(registry, plan, map[string]Intent{worker.ID: intent}, map[string]Intent{worker.ID: prior}, nil,
		map[string]Intent{intent.SupersedesFailedConfigSHA: prior})
	if err != nil || len(bound.Releases) != 1 || bound.Releases[0].ComponentID != worker.ID || bound.Releases[0].Delivery == nil ||
		bound.Releases[0].Delivery.Writer != "guardian" {
		t.Fatalf("US Edge Worker Guardian migration expanded the planner: releases=%+v err=%v", bound.Releases, err)
	}
}

func TestEdgeWorkerTemplatePreservesExternalCaddyAndTenantNodePlacement(t *testing.T) {
	edgeFile, err := os.Open("../../deploy/releases/edge-groups.json")
	if err != nil {
		t.Fatal(err)
	}
	edge, err := DecodeEdgeGroupRegistry(edgeFile)
	_ = edgeFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	const caddyImage = "docker.io/library/caddy@sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d"
	for _, group := range edge.Groups {
		for _, target := range group.Worker.ArtifactTargets {
			if target.Container == "caddy" {
				t.Fatalf("group %s treats external Caddy as a Fugue artifact", group.ID)
			}
		}
		raw, readErr := os.ReadFile(filepath.Join("../..", group.Worker.ManifestPath))
		if readErr != nil {
			t.Fatal(readErr)
		}
		materialized, materializeErr := MaterializeManifestTemplate(raw, group.Worker.ManifestVariables)
		if materializeErr != nil {
			t.Fatal(materializeErr)
		}
		set, decodeErr := DecodeResourceSet(strings.NewReader(string(materialized)))
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		for _, item := range set.Items {
			if stringField(item, "kind") != "DaemonSet" {
				continue
			}
			metadata, metadataErr := objectField(item, "metadata")
			itemSpec, specErr := objectField(item, "spec")
			template, templateErr := objectField(itemSpec, "template")
			spec, podSpecErr := objectField(template, "spec")
			if metadataErr != nil || specErr != nil || templateErr != nil || podSpecErr != nil {
				t.Fatalf("group %s DaemonSet structure is invalid", group.ID)
			}
			foundTenant := false
			tolerations, _ := spec["tolerations"].([]any)
			for _, rawToleration := range tolerations {
				toleration, _ := rawToleration.(map[string]any)
				foundTenant = foundTenant || (stringField(toleration, "key") == "fugue.io/tenant" && stringField(toleration, "operator") == "Exists" && stringField(toleration, "effect") == "NoSchedule")
			}
			if !foundTenant {
				t.Fatalf("group %s DaemonSet %s cannot schedule on the existing tenant-tainted Edge node", group.ID, stringField(metadata, "name"))
			}
			containers, _ := spec["containers"].([]any)
			for _, rawContainer := range containers {
				container, _ := rawContainer.(map[string]any)
				if stringField(container, "name") == "caddy" {
					if stringField(container, "image") != caddyImage {
						t.Fatalf("group %s Caddy image is not independently pinned: %+v", group.ID, container)
					}
					security, _ := container["securityContext"].(map[string]any)
					capabilities, _ := security["capabilities"].(map[string]any)
					if fmt.Sprint(capabilities["add"]) != "[NET_BIND_SERVICE]" || fmt.Sprint(capabilities["drop"]) != "[ALL]" || security["allowPrivilegeEscalation"] != false {
						t.Fatalf("group %s Caddy cannot bind its declared low ports with the minimum capability: %+v", group.ID, security)
					}
				}
			}
		}
	}
}

func TestEdgeWorkerTemplateOmitsAPIServerDefaultedEmptyEnvValues(t *testing.T) {
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
	for _, item := range set.Items {
		if item["kind"] != "DaemonSet" {
			continue
		}
		spec, _ := item["spec"].(map[string]any)
		template, _ := spec["template"].(map[string]any)
		templateSpec, _ := template["spec"].(map[string]any)
		containers, _ := templateSpec["containers"].([]any)
		for _, rawContainer := range containers {
			container := rawContainer.(map[string]any)
			environment, _ := container["env"].([]any)
			for _, rawEnv := range environment {
				env := rawEnv.(map[string]any)
				if value, exists := env["value"]; exists && value == "" {
					t.Fatalf("%s/%s contains an explicit empty EnvVar value that the API server drops", item["kind"], item["metadata"].(map[string]any)["name"])
				}
			}
		}
	}
}

func TestEdgeWorkerTemplateSeparatesProcessLivenessFromServingReadiness(t *testing.T) {
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
		metadata, _ := item["metadata"].(map[string]any)
		name := stringField(metadata, "name")
		if name != group.Worker.Transition.EdgeGroupAB.WorkerAName && name != group.Worker.Transition.EdgeGroupAB.WorkerBName {
			continue
		}
		spec, _ := item["spec"].(map[string]any)
		template, _ := spec["template"].(map[string]any)
		templateSpec, _ := template["spec"].(map[string]any)
		containers, _ := templateSpec["containers"].([]any)
		for _, rawContainer := range containers {
			container, _ := rawContainer.(map[string]any)
			if stringField(container, "name") != "edge" {
				continue
			}
			liveness, _ := container["livenessProbe"].(map[string]any)
			livenessHTTP, _ := liveness["httpGet"].(map[string]any)
			readiness, _ := container["readinessProbe"].(map[string]any)
			readinessHTTP, _ := readiness["httpGet"].(map[string]any)
			if stringField(livenessHTTP, "path") != "/livez" || stringField(readinessHTTP, "path") != "/readyz" {
				t.Fatalf("worker %s does not separate liveness from readiness: liveness=%+v readiness=%+v", name, livenessHTTP, readinessHTTP)
			}
			checked++
		}
	}
	if checked != 2 {
		t.Fatalf("checked %d edge worker containers, want 2", checked)
	}
}

func TestEdgeWorkerTemplateAllowsActiveSlotHeartbeats(t *testing.T) {
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
		metadata, _ := item["metadata"].(map[string]any)
		name := stringField(metadata, "name")
		if name != group.Worker.Transition.EdgeGroupAB.WorkerAName && name != group.Worker.Transition.EdgeGroupAB.WorkerBName {
			continue
		}
		spec, _ := item["spec"].(map[string]any)
		template, _ := spec["template"].(map[string]any)
		templateMetadata, _ := template["metadata"].(map[string]any)
		annotations, _ := templateMetadata["annotations"].(map[string]any)
		if stringField(annotations, "fugue.io/edge-heartbeat-fenced") == "true" {
			t.Fatalf("worker %s permanently fences its active-slot heartbeat", name)
		}
		checked++
	}
	if checked != 2 {
		t.Fatalf("checked %d edge worker templates, want 2", checked)
	}
}

func TestEdgeWorkerTemplateBindsCurrentAndInactiveCandidateBundleSources(t *testing.T) {
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
	wantCurrent := "http://edge-control-gamma.fugue-system.svc:8092/v1/edge/routes"
	wantCandidate := "http://edge-control-gamma.fugue-system.svc:8092/v1/edge/candidate-routes"
	checked := 0
	for _, item := range set.Items {
		if item["kind"] != "DaemonSet" {
			continue
		}
		metadata, _ := item["metadata"].(map[string]any)
		name := fmt.Sprint(metadata["name"])
		if name != "edge-gamma-worker-a" && name != "edge-gamma-worker-b" {
			continue
		}
		spec, _ := item["spec"].(map[string]any)
		template, _ := spec["template"].(map[string]any)
		templateSpec, _ := template["spec"].(map[string]any)
		containers, _ := templateSpec["containers"].([]any)
		var container map[string]any
		for _, rawContainer := range containers {
			candidate, _ := rawContainer.(map[string]any)
			if candidate["name"] == "edge" {
				container = candidate
				break
			}
		}
		if container == nil {
			t.Fatalf("%s has no edge container", name)
		}
		environment, _ := container["env"].([]any)
		values := make(map[string]string, len(environment))
		for _, rawEnv := range environment {
			env, _ := rawEnv.(map[string]any)
			values[fmt.Sprint(env["name"])] = fmt.Sprint(env["value"])
		}
		if values["FUGUE_EDGE_ROUTE_BUNDLE_URL"] != wantCurrent {
			t.Fatalf("%s current route source=%q, want %q", name, values["FUGUE_EDGE_ROUTE_BUNDLE_URL"], wantCurrent)
		}
		if values["FUGUE_EDGE_CANDIDATE_ROUTE_BUNDLE_URL"] != wantCandidate {
			t.Fatalf("%s candidate route source=%q, want %q", name, values["FUGUE_EDGE_CANDIDATE_ROUTE_BUNDLE_URL"], wantCandidate)
		}
		checked++
	}
	if checked != 2 {
		t.Fatalf("checked %d worker slots, want 2", checked)
	}
}

func edgeGroupFixture(id, groupID string) EdgeGroup {
	client := Component{
		ID: "edge-client-" + id, Family: "edge-client",
		IntentPath: "deploy/releases/edge-client-" + id + "/intent.json", ManifestPath: "deploy/releases/edge-client-" + id + "/resources.json",
		SourceRoots:     []string{"Dockerfile.edge", "cmd/fugue-dns", "cmd/fugue-ssh-front", "internal/edgefront"},
		Artifact:        Artifact{Repository: "ghcr.io/example/fugue-edge", Dockerfile: "Dockerfile.edge", Context: ".", BuildPackage: "./cmd/fugue-ssh-front"},
		ArtifactTargets: []ArtifactTarget{{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-client-" + id + "-front", Container: "ssh-front", ContainerType: "container"}},
		Workload:        Workload{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-client-" + id + "-front", Container: "ssh-front", FieldManager: "fugue-edge-client-" + id + "-declarative", RolloutMode: "rolling"},
		Health:          []HealthProbe{{Type: "daemonset", Name: "edge-client-" + id + "-front"}},
		Concurrency:     "fugue-production-edge-client-" + id,
	}
	controlName := "edge-control-" + id
	frontName := "edge-" + id + "-front"
	workerAName := "edge-" + id + "-worker-a"
	workerBName := "edge-" + id + "-worker-b"
	control := Component{
		ID: "edge-control-" + id, Family: "edge",
		IntentPath: "deploy/releases/edge-control-" + id + "/intent.json", ManifestPath: "internal/edgecontrol/component/resources.authority.group.json",
		ManifestVariables: map[string]string{
			"API_ROUTE_CA_SECRET": "fugue-api-route-intent-ca-" + id, "CONTROL_NAME": controlName, "GROUP": id, "GROUP_ID": groupID,
			"INVENTORY_WRITER_SECRET": "fugue-edge-control-inventory-writer-" + id, "READER_SECRET": "fugue-edge-control-reader-" + id,
			"RECOVERY_SECRET": "fugue-edge-control-recovery-" + id, "ROUTE_INTENT_IDENTITY_SECRET": "fugue-edge-control-route-intent-identity-" + id,
			"SIGNING_SECRET": "fugue-edge-control-signing-" + id,
		},
		SourceRoots: []string{"Dockerfile.edge-control", "cmd/fugue-edge-control", "internal/edgecontrol"},
		Artifact:    Artifact{Repository: "ghcr.io/example/fugue-edge-control", Dockerfile: "Dockerfile.edge-control", Context: ".", BuildPackage: "./cmd/fugue-edge-control"},
		ArtifactTargets: []ArtifactTarget{
			{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: controlName, Container: "edge-control", ContainerType: "container"},
			{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: controlName, Container: "state-permissions", ContainerType: "init-container"},
		},
		Workload:    Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: controlName, Container: "edge-control", FieldManager: "fugue-edge-control-" + id + "-declarative", Replicas: 1, RolloutMode: "recreate"},
		Health:      []HealthProbe{{Type: "deployment", Name: controlName}},
		Concurrency: "fugue-production-edge-control-" + id,
	}
	worker := Component{
		ID: "edge-worker-" + id, Family: "edge",
		IntentPath: "deploy/releases/edge-worker-" + id + "/intent.json", ManifestPath: "internal/edge/component/resources.inventory-producer.group.json",
		ManifestVariables: map[string]string{
			"API_SECRET": "fugue-edge-worker-api-" + id, "CONTROL_NAME": controlName, "FRONT_NAME": frontName, "GROUP": id, "GROUP_ID": groupID,
			"FRONT_COMPONENT":             "edge-" + id + "-front",
			"INVENTORY_IDENTITY_A_SECRET": "fugue-edge-worker-inventory-identity-" + id + "-a", "INVENTORY_IDENTITY_B_SECRET": "fugue-edge-worker-inventory-identity-" + id + "-b",
			"READER_SECRET": "fugue-edge-worker-reader-" + id, "SERVICE_ACCOUNT": "edge-worker-" + id, "VERIFIER_SECRET": "fugue-edge-worker-verifier-" + id,
			"WORKER_A_COMPONENT": "edge-" + id + "-worker-a", "WORKER_A_NAME": workerAName,
			"WORKER_B_COMPONENT": "edge-" + id + "-worker-b", "WORKER_B_NAME": workerBName,
		},
		SourceRoots: []string{"Dockerfile.edge", "cmd/fugue-edge", "internal/edge"},
		Artifact:    Artifact{Repository: "ghcr.io/example/fugue-edge", Dockerfile: "Dockerfile.edge", Context: ".", BuildPackage: "./cmd/fugue-edge"},
		ArtifactTargets: []ArtifactTarget{
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: frontName, Container: "edge-front", ContainerType: "container"},
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: workerAName, Container: "edge", ContainerType: "container"},
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: workerBName, Container: "edge", ContainerType: "container"},
		},
		Workload:   Workload{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: frontName, Container: "edge-front", FieldManager: "fugue-edge-worker-" + id + "-declarative", RolloutMode: "on-delete"},
		Transition: &Transition{Type: "edge-group-ab", EdgeGroupAB: &EdgeGroupABTransition{GroupID: groupID, CandidateStageURL: "http://edge-control-" + id + ":8092/v1/authority/group-worker-candidates", CandidateKeyring: "/var/run/secrets/fugue-authority-recovery-" + id + "/keyring.json", FrontName: frontName, WorkerAName: workerAName, WorkerBName: workerBName, WorkerContainer: "edge", ActivationStatePath: "/var/lib/fugue-edge-front/activation.json", CASBinary: "/usr/local/bin/fugue-edge-front-cas", ExpectedNodes: 1, SoakSeconds: 180}},
		Health: []HealthProbe{
			{Type: "daemonset", Name: frontName},
			{Type: "edge-group-authority", Name: groupID},
			{Type: "public-route-http", Name: groupID, Address: "192.0.2.10:443", Host: "platform.example.test", Path: "/healthz", Expected: "ok"},
			{Type: "daemonset", Name: workerAName},
			{Type: "daemonset", Name: workerBName},
		},
		Concurrency: "fugue-production-edge-worker-" + id,
	}
	return EdgeGroup{ID: id, GroupID: groupID, FaultDomainID: "fault-domain-test-" + id, EdgePoolID: "edge-pool-test-" + id, Labels: map[string]string{"country": "test", "region": "test"}, Client: client, Control: control, Worker: worker}
}
