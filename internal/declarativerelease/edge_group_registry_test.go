package declarativerelease

import (
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
	for _, root := range []string{"../edge", "../edgecontrol", "../edgefront"} {
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

func TestEdgeGroupRegistryUpdateIsConfigurationOnlyThenSingleComponent(t *testing.T) {
	alpha := edgeGroupFixture("alpha", "edge-group-metro-alpha")
	alpha.Control.MigrationState = "pending"
	alpha.Worker.MigrationState = "pending"
	initial := EdgeGroupRegistry{APIVersion: EdgeGroupRegistryAPIVersion, Kind: EdgeGroupRegistryKind, Groups: []EdgeGroup{alpha}}
	if err := ValidateEdgeGroupRegistryUpdate(nil, initial, Plan{APIVersion: IntentAPIVersion, Kind: "ProductionReleasePlan"}, []string{"deploy/releases/edge-groups.json"}); err != nil {
		t.Fatalf("pending data-only registry introduction: %v", err)
	}
	unsafeInitial := initial
	unsafeInitial.Groups = append([]EdgeGroup(nil), initial.Groups...)
	unsafeInitial.Groups[0].Control.MigrationState = "independent"
	if err := ValidateEdgeGroupRegistryUpdate(nil, unsafeInitial, Plan{}, []string{"deploy/releases/edge-groups.json"}); err == nil || !strings.Contains(err.Error(), "must begin pending") {
		t.Fatalf("active initial group was accepted: %v", err)
	}
	if err := ValidateEdgeGroupRegistryUpdate(nil, initial, Plan{}, []string{"deploy/releases/edge-groups.json", alpha.Control.IntentPath}); err == nil || !strings.Contains(err.Error(), "later production atom") {
		t.Fatalf("initial registry bundled a production intent: %v", err)
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
	if err := ValidateEdgeGroupRegistryUpdate(&next, independent, controlPlan, []string{"deploy/releases/edge-groups.json"}); err != nil {
		t.Fatalf("single control independence: %v", err)
	}
	skipped := initial
	skipped.Groups = append([]EdgeGroup(nil), initial.Groups...)
	skipped.Groups[0].Control.MigrationState = "independent"
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

func edgeGroupFixture(id, groupID string) EdgeGroup {
	controlName := "edge-control-" + id
	frontName := "edge-" + id + "-front"
	workerAName := "edge-" + id + "-worker-a"
	workerBName := "edge-" + id + "-worker-b"
	control := Component{
		ID: "edge-control-" + id, Family: "edge",
		IntentPath: "deploy/releases/edge-control-" + id + "/intent.json", ManifestPath: "internal/edgecontrol/component/resources.authority." + id + ".json",
		SourceRoots: []string{"Dockerfile.edge-control", "cmd/fugue-edge-control", "internal/edgecontrol"},
		Artifact:    Artifact{Repository: "ghcr.io/example/fugue-edge-control", Dockerfile: "Dockerfile.edge-control", Context: ".", BuildPackage: "./cmd/fugue-edge-control"},
		Workload:    Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: controlName, Container: "edge-control", FieldManager: "fugue-edge-control-" + id + "-declarative", Replicas: 1, RolloutMode: "recreate"},
		Health:      []HealthProbe{{Type: "deployment", Name: controlName}, {Type: "service-http", Name: controlName, Port: "http", Path: "/v1/authority/groups/" + groupID + "/readyz"}},
		Concurrency: "fugue-production-edge-control-" + id, MigrationState: "independent",
	}
	worker := Component{
		ID: "edge-worker-" + id, Family: "edge",
		IntentPath: "deploy/releases/edge-worker-" + id + "/intent.json", ManifestPath: "internal/edge/component/resources.inventory-producer." + id + ".json", BootstrapLKGPath: "deploy/releases/edge-worker-" + id + "/lkg.json",
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
		Health:      []HealthProbe{{Type: "daemonset", Name: frontName}, {Type: "daemonset", Name: workerAName}, {Type: "daemonset", Name: workerBName}},
		Concurrency: "fugue-production-edge-worker-" + id, MigrationState: "independent",
	}
	return EdgeGroup{ID: id, GroupID: groupID, Control: control, Worker: worker}
}
