package declarativerelease

import (
	"bytes"
	"os"
	"strings"
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
	if intent.Generation != 12 || intent.SupersedesFailedConfigSHA != "" || intent.Rollback != "previous-git-lkg" ||
		intent.ExpectedPreviousConfigSHA != "b4777d453c3053ce3142a08a5092f22ee9a861a9" ||
		intent.ExpectedPreviousManifestSHA != intent.ExpectedPreviousConfigSHA ||
		intent.ExpectedPreviousOCIRevision != intent.ExpectedPreviousConfigSHA ||
		intent.ExpectedPreviousImageDigest != "sha256:cf0c5959727ae297c08916a5a6a2b628ad8245af2a7c6498e8c66d3f5e70d19b" {
		t.Fatalf("image-cache successor is not bound to the verified independent LKG: %+v", intent)
	}
	// Keep the historical receipt fallback matrix bound to the exact transition
	// for which it was created. Ordinary successors use the Git predecessor.
	intent.Generation = 11
	intent.ExpectedPreviousConfigSHA = receipt.Final.ConfigSHA
	intent.ExpectedPreviousManifestSHA = receipt.Final.ManifestSHA
	intent.ExpectedPreviousOCIRevision = receipt.Final.OCIRevision
	intent.ExpectedPreviousImageDigest = strings.TrimPrefix(receipt.Final.ImageRef, component.Artifact.Repository+"@")
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
		bound.Releases[0].AdoptionReceiptPath != component.AdoptionReceiptPath ||
		bound.Releases[0].OwnershipAdoption != nil || bound.Releases[0].BootstrapRuntime != nil || bound.Releases[0].BootstrapLKGPath != "" {
		t.Fatalf("image-cache independent retry retained a force/bootstrap path: %+v", bound.Releases)
	}
	drifted := bound
	drifted.Releases = append([]PlanRelease(nil), bound.Releases...)
	drifted.Releases[0].AdoptionReceiptPath = "deploy/releases/image-cache/../other/adoption-receipt.json"
	if err := drifted.ValidateBound(); err == nil {
		t.Fatal("release plan accepted an adoption receipt path outside its exact component directory")
	}

	verification := RegistryVerification{
		Image: component.Artifact.Repository + "@sha256:" + strings.Repeat("b", 64), IndexDigest: "sha256:" + strings.Repeat("b", 64),
		ManifestDigest: "sha256:" + strings.Repeat("c", 64), ConfigDigest: "sha256:" + strings.Repeat("d", 64),
		OCIRevision: strings.Repeat("2", 40), Platform: "linux/amd64", Verification: "registry_manifest_config_and_layer_get",
		BlobCount: 2, LayerProbeCount: 1, RequestCount: 5, TotalLayerBytes: 10,
	}
	artifact, err := MaterializeArtifactReceipt(bound, component.ID, verification)
	if err != nil {
		t.Fatal(err)
	}
	forwardRaw, err := os.ReadFile("../../" + component.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	item, err := ResourceSetItem(forwardRaw, ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "fugue-fugue-image-cache"})
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := objectField(item, "metadata")
	if err != nil {
		t.Fatal(err)
	}
	annotations, _ := metadata["annotations"].(map[string]any)
	if annotations["helm.sh/resource-policy"] != "keep" {
		t.Fatalf("image-cache is not protected from the final Helm ledger retirement: %#v", annotations)
	}
	lkgRaw, err := os.ReadFile("../../deploy/releases/image-cache/lkg.json")
	if err != nil {
		t.Fatal(err)
	}
	forwardSource, err := DecodeResourceSet(bytes.NewReader(forwardRaw))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := forwardSource.Primary(bound.Releases[0].Workload); err != nil {
		t.Fatalf("forward component strategy contract: %v", err)
	}
	if predecessorWorkload, allowed := receiptBoundHistoricalLKGWorkload(bound.Releases[0]); !allowed || predecessorWorkload.RolloutMode != "on-delete" {
		t.Fatalf("receipt-bound predecessor strategy contract: allowed=%v workload=%+v release=%+v", allowed, predecessorWorkload, bound.Releases[0])
	}
	rendered, err := RenderManifests(bound, component.ID, artifact, bytes.NewReader(forwardRaw), bytes.NewReader(lkgRaw))
	if err != nil {
		t.Fatal(err)
	}
	forwardSet, err := DecodeResourceSet(bytes.NewReader(rendered.Forward))
	if err != nil {
		t.Fatal(err)
	}
	lkgSet, err := DecodeResourceSet(bytes.NewReader(rendered.LKG))
	if err != nil {
		t.Fatal(err)
	}
	forwardPrimary, err := forwardSet.Primary(component.Workload)
	if err != nil {
		t.Fatal(err)
	}
	lkgWorkload, allowed := receiptBoundHistoricalLKGWorkload(bound.Releases[0])
	if !allowed {
		t.Fatal("receipt-bound LKG predecessor validator was not selected")
	}
	if _, err := lkgSet.Primary(lkgWorkload); err != nil {
		t.Fatal(err)
	}
	strategy := forwardPrimary["spec"].(map[string]any)["updateStrategy"].(map[string]any)
	rolling := strategy["rollingUpdate"].(map[string]any)
	if strategy["type"] != "RollingUpdate" {
		t.Fatalf("forward strategy=%v", strategy["type"])
	}
	if maxUnavailable, ok := integerField(rolling["maxUnavailable"]); !ok || maxUnavailable != 2 {
		t.Fatalf("forward maxUnavailable=%v", rolling["maxUnavailable"])
	}

	for name, candidate := range map[string][]byte{
		"receipt fallback RollingUpdate":    bytes.Replace(lkgRaw, []byte(`"type":"OnDelete"`), []byte(`"type":"RollingUpdate"`), 1),
		"receipt fallback unknown strategy": bytes.Replace(lkgRaw, []byte(`"type":"OnDelete"`), []byte(`"type":"BlueGreen"`), 1),
		"forward OnDelete":                  bytes.Replace(forwardRaw, []byte(`"type":"RollingUpdate"`), []byte(`"type":"OnDelete"`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			forward, lkg := forwardRaw, lkgRaw
			if name == "forward OnDelete" {
				forward = candidate
			} else {
				lkg = candidate
			}
			if _, err := RenderManifests(bound, component.ID, artifact, bytes.NewReader(forward), bytes.NewReader(lkg)); err == nil {
				t.Fatal("invalid receipt-bound strategy was accepted")
			}
		})
	}

	ordinaryRegistry := registry
	for index := range ordinaryRegistry.Components {
		if ordinaryRegistry.Components[index].ID == component.ID {
			ordinaryRegistry.Components[index].AdoptionReceiptPath = ""
		}
	}
	ordinary, err := BuildPlan(ordinaryRegistry,
		"1111111111111111111111111111111111111111",
		"2222222222222222222222222222222222222222",
		[]string{component.IntentPath},
	)
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err = BindIntents(ordinaryRegistry, ordinary,
		map[string]Intent{component.ID: intent}, map[string]Intent{component.ID: previous},
		map[string]string{component.ID: "0deaad1fde11a8245d5b75eb18dd8ffc08d921c9"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ordinaryArtifact, err := MaterializeArtifactReceipt(ordinary, component.ID, verification)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RenderManifests(ordinary, component.ID, ordinaryArtifact, bytes.NewReader(forwardRaw), bytes.NewReader(lkgRaw)); err == nil {
		t.Fatal("ordinary non-receipt LKG accepted a strategy mismatch")
	}
}
