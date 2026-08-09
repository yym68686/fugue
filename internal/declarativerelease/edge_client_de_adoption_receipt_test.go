package declarativerelease

import (
	"os"
	"testing"
)

func TestEdgeClientDEIndependentTransitionReceipt(t *testing.T) {
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
		if registry.Components[index].ID == "edge-client-de" {
			component = &registry.Components[index]
			break
		}
	}
	if component == nil {
		t.Fatal("edge-client-de is absent from the production registry")
	}
	if component.MigrationState != "independent" || component.AdoptionReceiptPath != "deploy/releases/edge-client-de/adoption-receipt.json" ||
		component.OwnershipAdoption != nil || component.BootstrapRuntime != nil || component.BootstrapLKGPath != "" {
		t.Fatalf("edge-client-de did not retire its adopting-only metadata: %+v", component)
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
		t.Fatalf("edge-client-de receipt digest=%s, want %s", receipt.ReceiptDigest, wantDigest)
	}
	if err := receipt.Validate(*component, "edge-group-country-de"); err != nil {
		t.Fatal(err)
	}
	if receipt.RunID != 31284052228 || receipt.TerminalReceiptDigest != "sha256:6688b07d3b45b68390d8a314b99c54f71c18ebd77655a5e05e7d17351362d7f7" {
		t.Fatalf("edge-client-de receipt is not bound to the reviewed takeover terminal: %+v", receipt)
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
	if intent.Generation != 12 || intent.SupersedesFailedConfigSHA != "" || intent.Rollback != "previous-git-lkg" ||
		intent.ExpectedPreviousConfigSHA != "a1de385381254562e531afc98ff530dc73caeb3d" ||
		intent.ExpectedPreviousManifestSHA != intent.ExpectedPreviousConfigSHA ||
		intent.ExpectedPreviousOCIRevision != intent.ExpectedPreviousConfigSHA ||
		intent.ExpectedPreviousImageDigest != "sha256:73d128be4169567322555051e8af97126e7d4d242aef5be5f4bc71b68aaff8e5" {
		t.Fatalf("edge-client-de successor is not bound to the verified independent LKG: %+v", intent)
	}
	previous := intent
	previous.Generation = 11
	previous.ExpectedPreviousConfigSHA = "4743c1e20f0b048ec581f435ba46685a309b6cf4"
	previous.ExpectedPreviousManifestSHA = previous.ExpectedPreviousConfigSHA
	previous.ExpectedPreviousOCIRevision = previous.ExpectedPreviousConfigSHA
	previous.ExpectedPreviousImageDigest = "sha256:90d2682f2510bd1768fb3759bbb2c731471c97ffff0fe68bd80931f22017b5d2"
	previous.SupersedesFailedConfigSHA = ""
	plan, err := BuildPlan(registry,
		"1111111111111111111111111111111111111111",
		"2222222222222222222222222222222222222222",
		[]string{component.IntentPath},
	)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindIntents(registry, plan,
		map[string]Intent{component.ID: intent},
		map[string]Intent{component.ID: previous},
		map[string]string{component.ID: "a1de385381254562e531afc98ff530dc73caeb3d"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(bound.Releases) != 1 || bound.Releases[0].ComponentID != component.ID || bound.Releases[0].RetrySameLKG ||
		bound.Releases[0].SupersedesFailedConfigSHA != "" ||
		bound.Releases[0].MigrationState != "independent" || bound.Releases[0].OwnershipAdoption != nil ||
		bound.Releases[0].BootstrapRuntime != nil || bound.Releases[0].BootstrapLKGPath != "" {
		t.Fatalf("edge-client-de successor retained a force/bootstrap path: %+v", bound.Releases)
	}

	manifest, err := os.ReadFile("../../" + component.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"fugue-fugue-dns-country-de", "fugue-fugue-edge-country-de-ssh-front"} {
		item, err := ResourceSetItem(manifest, ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: name})
		if err != nil {
			t.Fatal(err)
		}
		metadata, err := objectField(item, "metadata")
		if err != nil {
			t.Fatal(err)
		}
		annotations, _ := metadata["annotations"].(map[string]any)
		if annotations["helm.sh/resource-policy"] != "keep" {
			t.Fatalf("%s is not protected from the final Helm ledger retirement: %#v", name, annotations)
		}
	}
}
