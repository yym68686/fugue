package declarativerelease

import (
	"os"
	"testing"
)

func TestControllerIndependentTransitionReceipt(t *testing.T) {
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
		if registry.Components[index].ID == "controller" {
			component = &registry.Components[index]
			break
		}
	}
	if component == nil {
		t.Fatal("controller is absent from the production registry")
	}
	if component.MigrationState != "independent" || component.AdoptionReceiptPath != "deploy/releases/controller/adoption-receipt.json" ||
		component.OwnershipAdoption != nil || component.BootstrapRuntime != nil || component.BootstrapLKGPath != "" {
		t.Fatalf("controller did not retire adopting-only metadata: %+v", component)
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
	if err := receipt.Validate(*component, "edge-group-controller"); err != nil {
		t.Fatal(err)
	}
	if receipt.RunID != 31283246087 || receipt.TerminalReceiptDigest != "sha256:b07abab53353c356fc1ac6be0d3e731e0713938fb596072f350e21849a96c536" {
		t.Fatalf("controller receipt is not bound to the verified takeover terminal: %+v", receipt)
	}
}
