package declarativerelease

import (
	"os"
	"testing"
)

func TestImageCacheIndependentTransitionReceipt(t *testing.T) {
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
		if registry.Components[index].ID == "image-cache" {
			component = &registry.Components[index]
			break
		}
	}
	if component == nil {
		t.Fatal("image-cache is absent from the production registry")
	}
	if component.MigrationState != "independent" || component.AdoptionReceiptPath != "deploy/releases/image-cache/adoption-receipt.json" ||
		component.OwnershipAdoption != nil || component.BootstrapRuntime != nil || component.BootstrapLKGPath != "" {
		t.Fatalf("image-cache did not retire its adopting-only metadata: %+v", component)
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
		t.Fatalf("image-cache receipt digest=%s, want %s", receipt.ReceiptDigest, wantDigest)
	}
	if err := receipt.Validate(*component, "edge-group-image-cache"); err != nil {
		t.Fatal(err)
	}
	if receipt.RunID != 31286062061 || receipt.RunAttempt != 1 ||
		receipt.TerminalReceiptDigest != "sha256:ecb99dfc2beabbcd4c52dce4bb2eecccf971ab53cfcfcc3422cbd45323e3a670" ||
		receipt.Final.UID != "495d37c7-da7c-4d77-a37e-b80dd502c409" || receipt.Final.Generation != 107 ||
		receipt.Final.Desired != 8 || receipt.Final.Updated != 7 || receipt.Final.Ready != 7 || receipt.Final.Unavailable != 1 {
		t.Fatalf("image-cache receipt is not bound to the reviewed 7+1 adoption terminal: %+v", receipt)
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
	previous := intent
	previous.Generation--
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
		map[string]string{component.ID: "0deaad1fde11a8245d5b75eb18dd8ffc08d921c9"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(bound.Releases) != 1 || bound.Releases[0].ComponentID != component.ID || !bound.Releases[0].RetrySameLKG ||
		bound.Releases[0].SupersedesFailedConfigSHA != "" || bound.Releases[0].MigrationState != "independent" ||
		bound.Releases[0].OwnershipAdoption != nil || bound.Releases[0].BootstrapRuntime != nil || bound.Releases[0].BootstrapLKGPath != "" {
		t.Fatalf("image-cache independent retry retained a force/bootstrap path: %+v", bound.Releases)
	}
}
