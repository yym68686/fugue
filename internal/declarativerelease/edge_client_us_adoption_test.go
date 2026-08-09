package declarativerelease

import (
	"bytes"
	"os"
	"reflect"
	"testing"
)

func TestEdgeClientUSAdoptionIsBoundToTheLiveBootstrap(t *testing.T) {
	registryFile, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	defer registryFile.Close()
	registry, err := DecodeRegistry(registryFile)
	if err != nil {
		t.Fatal(err)
	}
	var component *Component
	for index := range registry.Components {
		if registry.Components[index].ID == "edge-client-us" {
			component = &registry.Components[index]
			break
		}
	}
	if component == nil {
		t.Fatal("edge-client-us is absent from the production registry")
	}
	if component.MigrationState != "adopting" || component.BootstrapLKGPath != "deploy/releases/edge-client-us/lkg.json" ||
		component.AdoptionReceiptPath != "" || component.OwnershipAdoption == nil || component.BootstrapRuntime == nil {
		t.Fatalf("edge-client-us adoption metadata is incomplete: %+v", component)
	}
	if bootstrap := component.BootstrapRuntime; bootstrap.Container != "ssh-front" ||
		bootstrap.ImageDigest != "sha256:b835beb57193cc1b6129edd983edefcd47f30d9d37c33a3cfdfd56234371e81f" ||
		bootstrap.OCIRevision != "f5d6a6da6198c55371cd508dea5e9cb9f8726861" || bootstrap.Resource != (ResourceIdentity{
		APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "fugue-fugue-edge-ssh-front",
	}) {
		t.Fatalf("edge-client-us bootstrap runtime drifted: %+v", bootstrap)
	}
	wantManagers := []string{"helm", "kubectl-patch", "kubectl-rollout"}
	if component.OwnershipAdoption.LegacyFieldManager != "helm" || !reflect.DeepEqual(component.OwnershipAdoption.legacyManagers(), wantManagers) {
		t.Fatalf("edge-client-us legacy managers drifted: %+v", component.OwnershipAdoption)
	}
	wantScopes := []OwnershipAdoptionScope{
		{Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "fugue-fugue-dns"}, Fields: []string{
			"/spec/template/spec/containers[name=dns]/env[name=FUGUE_API_URL]/value",
			"/spec/template/spec/containers[name=dns]/image",
			"/spec/updateStrategy",
		}},
		{Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "fugue-fugue-edge-ssh-front"}, Fields: []string{
			"/spec/template/spec/containers[name=ssh-front]/image",
			"/spec/updateStrategy",
		}},
	}
	if !reflect.DeepEqual(component.OwnershipAdoption.Resources, wantScopes) {
		t.Fatalf("edge-client-us reviewed adoption scope drifted: %+v", component.OwnershipAdoption.Resources)
	}

	intentFile, err := os.Open("../../" + component.IntentPath)
	if err != nil {
		t.Fatal(err)
	}
	defer intentFile.Close()
	intent, err := DecodeIntent(intentFile)
	if err != nil {
		t.Fatal(err)
	}
	if intent.Generation != 4 || intent.SupersedesFailedConfigSHA != "" ||
		intent.ExpectedPreviousConfigSHA != "cdd6c08679ac78198e42c870b4ac1d5dfa2d78d0" ||
		intent.ExpectedPreviousManifestSHA != intent.ExpectedPreviousConfigSHA || intent.ExpectedPreviousOCIRevision != intent.ExpectedPreviousConfigSHA ||
		intent.ExpectedPreviousImageDigest != "sha256:9e75c56633641f6b9f4ebcdf519977180a6a7cf62e48f0aaa56bbbffa5d4fa30" {
		t.Fatalf("edge-client-us intent is not the exact bootstrap LKG: %+v", intent)
	}
	prior := intent
	prior.Generation = 3
	plan, err := BuildPlan(registry, "1111111111111111111111111111111111111111", "2222222222222222222222222222222222222222", []string{
		"deploy/releases/components.json", component.IntentPath, component.ManifestPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindIntents(registry, plan, map[string]Intent{component.ID: intent}, map[string]Intent{component.ID: prior},
		map[string]string{component.ID: intent.SupersedesFailedConfigSHA})
	if err != nil {
		t.Fatal(err)
	}
	if len(bound.Releases) != 1 || bound.Releases[0].ComponentID != component.ID || !bound.Releases[0].RetrySameLKG ||
		bound.Releases[0].SupersedesFailedConfigSHA != "" || bound.Releases[0].IntentGeneration != 4 ||
		bound.Releases[0].MigrationState != "adopting" || bound.Releases[0].OwnershipAdoption == nil || bound.Releases[0].BootstrapRuntime == nil {
		t.Fatalf("edge-client-us adoption plan is not exact: %+v", bound.Releases)
	}

	forwardRaw, err := os.ReadFile("../../" + component.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	forwardDNS, err := ResourceSetItem(forwardRaw, wantScopes[0].Identity)
	if err != nil {
		t.Fatal(err)
	}
	apiURL, err := adoptionPointerValue(forwardDNS, wantScopes[0].Fields[0])
	if err != nil || apiURL != "http://fugue-fugue:80?authority_service=edge-control-us" {
		t.Fatalf("edge-client-us DNS authority route drifted: value=%v err=%v", apiURL, err)
	}
	lkgRaw, err := os.ReadFile("../../" + component.BootstrapLKGPath)
	if err != nil {
		t.Fatal(err)
	}
	lkg, err := DecodeResourceSet(bytes.NewReader(lkgRaw))
	if err != nil {
		t.Fatal(err)
	}
	primary, err := lkg.Primary(bootstrapLKGWorkload(bound.Releases[0]))
	if err != nil {
		t.Fatal(err)
	}
	image, err := adoptionPointerValue(primary, wantScopes[1].Fields[0])
	if err != nil || image != component.Artifact.Repository+"@"+intent.ExpectedPreviousImageDigest {
		t.Fatalf("edge-client-us primary LKG image drifted: value=%v err=%v", image, err)
	}
}
