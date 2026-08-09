package declarativerelease

import (
	"os"
	"testing"
)

func TestEdgeClientUSIndependentTransitionReceipt(t *testing.T) {
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
	if component.MigrationState != "independent" || component.AdoptionReceiptPath != "deploy/releases/edge-client-us/adoption-receipt.json" ||
		component.OwnershipAdoption != nil || component.BootstrapRuntime != nil || component.BootstrapLKGPath != "" {
		t.Fatalf("edge-client-us did not retire its adopting-only metadata: %+v", component)
	}

	receiptFile, err := os.Open("../../" + component.AdoptionReceiptPath)
	if err != nil {
		t.Fatal(err)
	}
	defer receiptFile.Close()
	receipt, err := DecodeOwnershipAdoptionReceipt(receiptFile)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := receipt.digest()
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ReceiptDigest != wantDigest {
		t.Fatalf("edge-client-us receipt digest=%s, want %s", receipt.ReceiptDigest, wantDigest)
	}
	if err := receipt.Validate(*component, "edge-group-country-us"); err != nil {
		t.Fatal(err)
	}
	if receipt.RunID != 31296594575 || receipt.RunAttempt != 1 ||
		receipt.TerminalReceiptDigest != "sha256:cb76fc9cf2c39c1ead6a6ed756e37b1823dbd802aa817fed6d6de0f5bf44f475" ||
		receipt.Final.ConfigSHA != "4e88ecbdc40019d69f80cab0b3827e45928abc53" ||
		receipt.Final.ImageRef != "ghcr.io/yym68686/fugue-edge@sha256:2af80209adedb678e26adedb15e459c361a4d94cfa01bf898e654b5a450c9642" ||
		len(receipt.Final.Resources) != 2 {
		t.Fatalf("edge-client-us receipt is not bound to the verified takeover terminal: %+v", receipt)
	}
	for _, resource := range receipt.Final.Resources {
		if !resource.ReviewedOwnershipApplied || !resource.ReviewedOwnershipExclusive {
			t.Fatalf("edge-client-us receipt lacks exclusive reviewed ownership: %+v", resource)
		}
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
	if intent.Generation != 5 || intent.SupersedesFailedConfigSHA != "" || intent.Rollback != "previous-git-lkg" ||
		intent.ExpectedPreviousConfigSHA != receipt.Final.ConfigSHA ||
		intent.ExpectedPreviousManifestSHA != receipt.Final.ManifestSHA ||
		intent.ExpectedPreviousOCIRevision != receipt.Final.OCIRevision ||
		component.Artifact.Repository+"@"+intent.ExpectedPreviousImageDigest != receipt.Final.ImageRef {
		t.Fatalf("edge-client-us intent is not bound to the receipt predecessor: intent=%+v receipt=%+v", intent, receipt)
	}
	prior := intent
	prior.Generation = 4
	prior.ExpectedPreviousConfigSHA = "cdd6c08679ac78198e42c870b4ac1d5dfa2d78d0"
	prior.ExpectedPreviousManifestSHA = prior.ExpectedPreviousConfigSHA
	prior.ExpectedPreviousOCIRevision = prior.ExpectedPreviousConfigSHA
	prior.ExpectedPreviousImageDigest = "sha256:9e75c56633641f6b9f4ebcdf519977180a6a7cf62e48f0aaa56bbbffa5d4fa30"
	plan, err := BuildPlan(registry, "1111111111111111111111111111111111111111", "2222222222222222222222222222222222222222", []string{component.IntentPath})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindIntents(registry, plan, map[string]Intent{component.ID: intent}, map[string]Intent{component.ID: prior}, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if len(bound.Releases) != 1 || bound.Releases[0].ComponentID != component.ID || bound.Releases[0].RetrySameLKG ||
		bound.Releases[0].SupersedesFailedConfigSHA != "" || bound.Releases[0].IntentGeneration != 5 ||
		bound.Releases[0].MigrationState != "independent" || bound.Releases[0].OwnershipAdoption != nil ||
		bound.Releases[0].BootstrapRuntime != nil || bound.Releases[0].BootstrapLKGPath != "" ||
		bound.Releases[0].AdoptionReceiptPath != component.AdoptionReceiptPath {
		t.Fatalf("edge-client-us independent plan retained a force/bootstrap path: %+v", bound.Releases)
	}

	forwardRaw, err := os.ReadFile("../../" + component.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	dns, err := ResourceSetItem(forwardRaw, ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "fugue-fugue-dns"})
	if err != nil {
		t.Fatal(err)
	}
	apiURL, err := adoptionPointerValue(dns, "/spec/template/spec/containers[name=dns]/env[name=FUGUE_API_URL]/value")
	if err != nil || apiURL != "http://fugue-fugue:80?authority_service=edge-control-us" {
		t.Fatalf("edge-client-us DNS authority route drifted: value=%v err=%v", apiURL, err)
	}
}
