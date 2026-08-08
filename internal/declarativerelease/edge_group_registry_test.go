package declarativerelease

import (
	"fmt"
	"os"
	"path/filepath"
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
	worker := byID[virtual.Worker.ID]
	if control.Workload.Name != "edge-control-gamma" || control.Concurrency != "fugue-production-edge-control-gamma" ||
		worker.Concurrency != "fugue-production-edge-worker-gamma" || worker.BootstrapLKGPath != "deploy/releases/edge-worker-gamma/lkg.json" {
		t.Fatalf("third group did not get independent resources, Lease, and LKG: control=%+v worker=%+v", control, worker)
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

func TestEdgeGroupRegistryUpdateIsConfigurationOnlyThenSingleComponent(t *testing.T) {
	alpha := edgeGroupFixture("alpha", "edge-group-metro-alpha")
	alpha.Control.MigrationState = "pending"
	alpha.Worker.MigrationState = "pending"
	alpha.Control.AdoptionReceiptPath = ""
	alpha.Worker.AdoptionReceiptPath = ""
	initial := EdgeGroupRegistry{APIVersion: EdgeGroupRegistryAPIVersion, Kind: EdgeGroupRegistryKind, Groups: []EdgeGroup{alpha}}
	if err := ValidateEdgeGroupRegistryUpdate(nil, initial, Plan{APIVersion: IntentAPIVersion, Kind: "ProductionReleasePlan"}, []string{"deploy/releases/edge-groups.json"}); err != nil {
		t.Fatalf("pending data-only registry introduction: %v", err)
	}
	unsafeInitial := initial
	unsafeInitial.Groups = append([]EdgeGroup(nil), initial.Groups...)
	unsafeInitial.Groups[0].Control.MigrationState = "independent"
	unsafeInitial.Groups[0].Control.AdoptionReceiptPath = "deploy/releases/edge-control-alpha/adoption-receipt.json"
	if err := ValidateEdgeGroupRegistryUpdate(nil, unsafeInitial, Plan{}, []string{"deploy/releases/edge-groups.json"}); err == nil || !strings.Contains(err.Error(), "must begin pending") {
		t.Fatalf("active initial group was accepted: %v", err)
	}
	if err := ValidateEdgeGroupRegistryUpdate(nil, initial, Plan{}, []string{"deploy/releases/edge-groups.json", alpha.Control.IntentPath}); err == nil || !strings.Contains(err.Error(), "later production atom") {
		t.Fatalf("initial registry bundled a production intent: %v", err)
	}
	pendingTemplate := initial
	pendingTemplate.Groups = append([]EdgeGroup(nil), initial.Groups...)
	pendingTemplate.Groups[0].Control.ManifestPath = "internal/edgecontrol/component/resources.authority.group.json"
	pendingTemplate.Groups[0].Control.ManifestVariables = map[string]string{"CONTROL_NAME": "edge-control-alpha"}
	if err := ValidateEdgeGroupRegistryUpdate(&initial, pendingTemplate, Plan{}, []string{"deploy/releases/edge-groups.json", pendingTemplate.Groups[0].Control.ManifestPath}); err != nil {
		t.Fatalf("pending template configuration update: %v", err)
	}
	if err := ValidateEdgeGroupRegistryUpdate(&initial, pendingTemplate, Plan{}, []string{"deploy/releases/edge-groups.json", alpha.Control.IntentPath}); err == nil || !strings.Contains(err.Error(), "unselected component") {
		t.Fatalf("pending template update smuggled a production intent: %v", err)
	}

	next := initial
	next.Groups = append([]EdgeGroup(nil), initial.Groups...)
	next.Groups[0].Control.MigrationState = "adopting"
	controlPlan := Plan{Releases: []PlanRelease{{ComponentID: alpha.Control.ID}}}
	if err := ValidateEdgeGroupRegistryUpdate(&initial, next, controlPlan, []string{"deploy/releases/edge-groups.json", alpha.Control.IntentPath}); err != nil {
		t.Fatalf("single control adoption: %v", err)
	}
	independent := next
	independent.Groups = append([]EdgeGroup(nil), next.Groups...)
	independent.Groups[0].Control.MigrationState = "independent"
	independent.Groups[0].Control.AdoptionReceiptPath = "deploy/releases/edge-control-alpha/adoption-receipt.json"
	if err := ValidateEdgeGroupRegistryUpdate(&next, independent, Plan{}, []string{"deploy/releases/edge-groups.json", independent.Groups[0].Control.AdoptionReceiptPath}); err != nil {
		t.Fatalf("single control independence: %v", err)
	}
	receipt := adoptionReceiptFixture(t, independent.Groups[0].Control, independent.Groups[0].GroupID)
	if err := ValidateEdgeGroupAdoptionReceipts(&next, independent, func(path string) (OwnershipAdoptionReceipt, error) {
		if path != independent.Groups[0].Control.AdoptionReceiptPath {
			t.Fatalf("unexpected adoption receipt path %q", path)
		}
		return receipt, nil
	}); err != nil {
		t.Fatalf("verified control independence: %v", err)
	}
	skipped := initial
	skipped.Groups = append([]EdgeGroup(nil), initial.Groups...)
	skipped.Groups[0].Control.MigrationState = "independent"
	skipped.Groups[0].Control.AdoptionReceiptPath = "deploy/releases/edge-control-alpha/adoption-receipt.json"
	if err := ValidateEdgeGroupRegistryUpdate(&initial, skipped, controlPlan, []string{"deploy/releases/edge-groups.json"}); err == nil || !strings.Contains(err.Error(), "pending to adopting to independent") {
		t.Fatalf("skipped adoption state was accepted: %v", err)
	}
	crossGroup := next
	crossGroup.Groups = append([]EdgeGroup(nil), next.Groups...)
	crossGroup.Groups[0].Worker.MigrationState = "adopting"
	if err := ValidateEdgeGroupRegistryUpdate(&initial, crossGroup, controlPlan, []string{"deploy/releases/edge-groups.json", alpha.Control.IntentPath}); err == nil || !strings.Contains(err.Error(), "unselected component") {
		t.Fatalf("cross-component registry drift was accepted: %v", err)
	}
}

func TestEdgeGroupPublicationHealthMigratesForwardOnly(t *testing.T) {
	legacy := edgeGroupFixture("alpha", "edge-group-metro-alpha")
	legacy.Control.MigrationState = "pending"
	legacy.Worker.MigrationState = "pending"
	legacy.Control.AdoptionReceiptPath = ""
	legacy.Worker.AdoptionReceiptPath = ""
	legacy.Control.Health = append(legacy.Control.Health, HealthProbe{Type: "service-http", Name: legacy.Control.Workload.Name, Port: "http", Path: "/v1/authority/groups/" + legacy.GroupID + "/readyz"})
	legacy.Worker.Health = append(legacy.Worker.Health[:1], legacy.Worker.Health[2:]...)
	if err := legacy.validate(); err != nil {
		t.Fatalf("read legacy group health: %v", err)
	}
	staged := edgeGroupFixture("alpha", "edge-group-metro-alpha")
	staged.Control.MigrationState = "pending"
	staged.Worker.MigrationState = "pending"
	staged.Control.AdoptionReceiptPath = ""
	staged.Worker.AdoptionReceiptPath = ""
	previous := EdgeGroupRegistry{APIVersion: EdgeGroupRegistryAPIVersion, Kind: EdgeGroupRegistryKind, Groups: []EdgeGroup{legacy}}
	current := EdgeGroupRegistry{APIVersion: EdgeGroupRegistryAPIVersion, Kind: EdgeGroupRegistryKind, Groups: []EdgeGroup{staged}}
	if err := ValidateEdgeGroupRegistryUpdate(&previous, current, Plan{}, []string{"deploy/releases/edge-groups.json"}); err != nil {
		t.Fatalf("legacy to staged health migration: %v", err)
	}
	if err := ValidateEdgeGroupRegistryUpdate(&current, previous, Plan{}, []string{"deploy/releases/edge-groups.json"}); err == nil || !strings.Contains(err.Error(), "cannot regress") {
		t.Fatalf("staged health regressed to legacy: %v", err)
	}
	if len(staged.Control.Health) != 1 || staged.Control.Health[0].Type != "deployment" || staged.Control.Health[0].Name != staged.Control.Workload.Name {
		t.Fatalf("control readiness must be the workload readiness probe: %+v", staged.Control.Health)
	}
	for _, probe := range staged.Control.Health {
		if probe.Type == "service-http" {
			t.Fatalf("control atom must not bypass the group NetworkPolicy: %+v", probe)
		}
	}
	foundAuthority := false
	for _, probe := range staged.Worker.Health {
		foundAuthority = foundAuthority || (probe.Type == "edge-group-authority" && probe.Name == staged.GroupID)
	}
	if !foundAuthority {
		t.Fatalf("worker atom must verify group authority through worker runtime evidence: %+v", staged.Worker.Health)
	}
}

func TestPendingSharedEdgeManifestTemplateIsConfigurationOnly(t *testing.T) {
	base := Registry{APIVersion: RegistryAPIVersion, Kind: RegistryKind, Components: []Component{
		edgeGroupFixture("gamma", "edge-group-metro-gamma").Control,
	}}
	base.Components[0].MigrationState = "pending"
	base.Components[0].AdoptionReceiptPath = ""
	plan, err := BuildPlan(base, testSHA1, testSHA2, []string{base.Components[0].ManifestPath})
	if err != nil || len(plan.Releases) != 0 {
		t.Fatalf("pending shared template selected production: plan=%+v err=%v", plan, err)
	}
	if _, err := BuildPlan(base, testSHA1, testSHA2, []string{"internal/edgecontrol/authority_runtime.go"}); err == nil || !strings.Contains(err.Error(), "missing same-commit production intent") {
		t.Fatalf("pending product source changed without an intent: %v", err)
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

func edgeGroupFixture(id, groupID string) EdgeGroup {
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
		Concurrency: "fugue-production-edge-control-" + id, MigrationState: "independent",
		AdoptionReceiptPath: "deploy/releases/edge-control-" + id + "/adoption-receipt.json",
	}
	worker := Component{
		ID: "edge-worker-" + id, Family: "edge",
		IntentPath: "deploy/releases/edge-worker-" + id + "/intent.json", ManifestPath: "internal/edge/component/resources.inventory-producer.group.json", BootstrapLKGPath: "deploy/releases/edge-worker-" + id + "/lkg.json",
		ManifestVariables: map[string]string{
			"API_SECRET": "fugue-edge-worker-api-" + id, "CONTROL_NAME": controlName, "FRONT_NAME": frontName, "GROUP": id, "GROUP_ID": groupID,
			"INVENTORY_IDENTITY_A_SECRET": "fugue-edge-worker-inventory-identity-" + id + "-a", "INVENTORY_IDENTITY_B_SECRET": "fugue-edge-worker-inventory-identity-" + id + "-b",
			"READER_SECRET": "fugue-edge-worker-reader-" + id, "SERVICE_ACCOUNT": "edge-worker-" + id, "VERIFIER_SECRET": "fugue-edge-worker-verifier-" + id,
			"WORKER_A_NAME": workerAName, "WORKER_B_NAME": workerBName,
		},
		HeterogeneousBootstrapLKG: true,
		SourceRoots:               []string{"Dockerfile.edge", "cmd/fugue-edge", "internal/edge"},
		Artifact:                  Artifact{Repository: "ghcr.io/example/fugue-edge", Dockerfile: "Dockerfile.edge", Context: ".", BuildPackage: "./cmd/fugue-edge"},
		ArtifactTargets: []ArtifactTarget{
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: frontName, Container: "edge-front", ContainerType: "container"},
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: workerAName, Container: "edge", ContainerType: "container"},
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: workerBName, Container: "edge", ContainerType: "container"},
		},
		Workload:    Workload{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: frontName, Container: "edge-front", FieldManager: "fugue-edge-worker-" + id + "-declarative", RolloutMode: "on-delete"},
		Transition:  &Transition{Type: "edge-group-ab", EdgeGroupAB: &EdgeGroupABTransition{GroupID: groupID, FrontName: frontName, WorkerAName: workerAName, WorkerBName: workerBName, WorkerContainer: "edge", ActivationStatePath: "/var/lib/fugue-edge-front/activation.json", CASBinary: "/usr/local/bin/fugue-edge-front-cas", ExpectedNodes: 1, SoakSeconds: 180}},
		Health:      []HealthProbe{{Type: "daemonset", Name: frontName}, {Type: "edge-group-authority", Name: groupID}, {Type: "daemonset", Name: workerAName}, {Type: "daemonset", Name: workerBName}},
		Concurrency: "fugue-production-edge-worker-" + id, MigrationState: "independent",
		AdoptionReceiptPath: "deploy/releases/edge-worker-" + id + "/adoption-receipt.json",
	}
	return EdgeGroup{ID: id, GroupID: groupID, Control: control, Worker: worker}
}
