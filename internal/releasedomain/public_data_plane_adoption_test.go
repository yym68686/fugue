package releasedomain

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type publicDataPlaneAdoptionFixture struct {
	input           PublicDataPlaneAdoptionInput
	ownership       []byte
	base            []byte
	target          []byte
	observed        []byte
	snapshot        []byte
	snapshotDoc     map[string]any
	secretHMACKey   []byte
	rawBase         []byte
	rawRenderedBase []byte
	rawTarget       []byte
	serverTarget    []byte
	lookupSnapshot  []byte
}

func TestPublicDataPlaneAdoptionStage1AndStage2Handoff(t *testing.T) {
	fixture := newPublicDataPlaneAdoptionFixture(t)
	plan, restore, err := BuildPublicDataPlaneAdoptionPlan(fixture.input)
	if err != nil {
		t.Fatalf("BuildPublicDataPlaneAdoptionPlan: %v", err)
	}
	if plan.ExpectedDomain != DomainAuthoritativeDNS || plan.BaseRevision != "806" || plan.TargetRevision != "807" {
		t.Fatalf("unexpected Stage1 plan: %#v", plan)
	}
	if len(plan.Intent.Patches) != 3 {
		t.Fatalf("patch count = %d, want 3", len(plan.Intent.Patches))
	}
	for _, patch := range plan.Intent.Patches {
		if !strings.HasSuffix(patch.WorkloadName, "worker-b") || patch.ImageRef != fixtureRef() {
			t.Fatalf("unexpected adoption patch: %#v", patch)
		}
	}
	envelope, err := NewPublicDataPlaneAdoptionTransaction(plan)
	if err != nil || VerifyPublicDataPlaneAdoptionTransaction(envelope) != nil {
		t.Fatalf("transaction verification failed: %v", err)
	}
	if err := VerifyPublicDataPlaneAdoptionRecoveryBase(envelope, "806", fixture.base); err != nil {
		t.Fatalf("exact recovery base failed: %v", err)
	}
	if err := VerifyPublicDataPlaneAdoptionRecoveryBase(envelope, "807", fixture.target); err == nil {
		t.Fatal("recovery accepted target Helm metadata as the restored base")
	}
	if err := VerifyPublicDataPlaneAdoptionPrewrite(plan, restore, fixture.input); err != nil {
		t.Fatalf("prewrite verification failed: %v", err)
	}
	patches, err := PublicDataPlaneAdoptionRestorePatches(restore)
	if err != nil || len(patches) != 3 {
		t.Fatalf("restore patches = %d, want only 3 adoption workers; err=%v", len(patches), err)
	}
	for _, patch := range patches {
		if len(patch.Patch) != 2 || patch.Patch[0].(map[string]any)["op"] != "test" ||
			patch.Patch[1].(map[string]any)["path"] != "/spec/template/spec/containers/0/image" {
			t.Fatalf("restore patch is not UID-tested exact edge image: %#v", patch)
		}
	}
	if err := VerifyPublicDataPlaneAdoptionRestore(restore, fixture.snapshot, "fugue", "fugue-system", "fugue-fugue"); err != nil {
		t.Fatalf("restore verification failed: %v", err)
	}

	finalSnapshot, finalObserved := fixture.finalEvidence(t)
	baseline, err := FinalizePublicDataPlaneAdoptionBaseline(
		plan, "807", fixture.target, finalSnapshot, finalObserved, fixture.ownership,
	)
	if err != nil {
		t.Fatalf("FinalizePublicDataPlaneAdoptionBaseline: %v", err)
	}
	if err := VerifyPublicDataPlaneStage2Baseline(baseline, "807", fixture.target); err != nil {
		t.Fatalf("Stage2 baseline failed: %v", err)
	}
	var trace PublicDataPlaneAdoptionExecutionTrace
	for _, phase := range []string{
		"prepared", "lease-acquired", "prewrite-verified", "fence-armed",
		"apply-started", "apply-succeeded", "baseline-finalized", "lease-released",
	} {
		trace, err = AppendPublicDataPlaneAdoptionTrace(trace, plan.Digest, phase, "2026-08-01T00:00:00Z")
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := VerifyPublicDataPlaneStage2Handoff(baseline, trace, "807", fixture.target); err != nil {
		t.Fatalf("completed Stage1 handoff failed: %v", err)
	}
	unfinished := trace
	unfinished.Events = append([]PublicDataPlaneAdoptionTraceEvent(nil), trace.Events[:len(trace.Events)-1]...)
	unfinished.Digest = publicDataPlaneAdoptionTraceDigest(unfinished)
	if err := VerifyPublicDataPlaneStage2Handoff(baseline, unfinished, "807", fixture.target); err == nil {
		t.Fatal("Stage2 accepted a Stage1 run that had not released its Lease")
	}
	swappedBaseline := baseline
	swappedBaseline.Stage1PlanDigest = "sha256:" + strings.Repeat("9", 64)
	swappedBaseline.Digest = publicDataPlaneAdoptionBaselineDigest(swappedBaseline)
	if err := VerifyPublicDataPlaneStage2Handoff(swappedBaseline, trace, "807", fixture.target); err == nil {
		t.Fatal("Stage2 accepted a baseline swapped from another Stage1 transaction")
	}
	if err := VerifyPublicDataPlaneStage2Baseline(baseline, "806", fixture.base); err == nil {
		t.Fatal("Stage2 accepted the old Helm806 plan/bundle")
	}
	if err := VerifyPublicDataPlaneStage2Handoff(baseline, trace, "806", fixture.base); err == nil {
		t.Fatal("Stage2 handoff accepted the old Helm806 plan/bundle")
	}
}

func TestPublicDataPlaneAdoptionTransactionPostRendererBindsActualHelmInputAndOutput(t *testing.T) {
	fixture := newPublicDataPlaneAdoptionFixture(t)
	plan, _, err := BuildPublicDataPlaneAdoptionPlan(fixture.input)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := NewPublicDataPlaneAdoptionTransaction(plan)
	if err != nil {
		t.Fatal(err)
	}
	target, err := RenderPublicDataPlaneAdoptionTransactionTarget(
		fixture.rawRenderedBase, fixture.ownership, "fugue-system", envelope, fixture.secretHMACKey,
	)
	if err != nil || !bytes.Equal(target, fixture.rawTarget) {
		t.Fatalf("transaction post-render failed: %v", err)
	}

	driftedInput := bytes.Replace(fixture.rawRenderedBase, []byte("caddy:2.10.2-alpine"), []byte("caddy:2.10.3-alpine"), 1)
	if _, err := RenderPublicDataPlaneAdoptionTransactionTarget(
		driftedInput, fixture.ownership, "fugue-system", envelope, fixture.secretHMACKey,
	); err == nil {
		t.Fatal("transaction post-render accepted apply-time base render drift")
	}

	wrongOutput := envelope
	wrongOutput.Plan.TargetManifestDigest = "sha256:" + strings.Repeat("e", 64)
	wrongOutput.Plan.RepeatedTargetDigest = wrongOutput.Plan.TargetManifestDigest
	wrongOutput.Plan.Stage2.RequiredBaseManifestDigest = wrongOutput.Plan.TargetManifestDigest
	resealPublicDataPlaneAdoptionEnvelope(&wrongOutput)
	if err := VerifyPublicDataPlaneAdoptionTransaction(wrongOutput); err != nil {
		t.Fatalf("test envelope should be structurally valid: %v", err)
	}
	if _, err := RenderPublicDataPlaneAdoptionTransactionTarget(
		fixture.rawRenderedBase, fixture.ownership, "fugue-system", wrongOutput, fixture.secretHMACKey,
	); err == nil {
		t.Fatal("transaction post-render accepted target output digest drift")
	}

	swappedIntent := envelope
	swappedIntent.Plan.Intent.TargetEdgeImageRef = "registry.example/edge@sha256:" + strings.Repeat("c", 64)
	for index := range swappedIntent.Plan.Intent.Patches {
		swappedIntent.Plan.Intent.Patches[index].ImageRef = swappedIntent.Plan.Intent.TargetEdgeImageRef
	}
	swappedIntent.Plan.Intent.Digest = publicDataPlaneAdoptionIntentDigest(swappedIntent.Plan.Intent)
	resealPublicDataPlaneAdoptionEnvelope(&swappedIntent)
	if err := VerifyPublicDataPlaneAdoptionTransaction(swappedIntent); err != nil {
		t.Fatalf("test swapped intent should be structurally valid: %v", err)
	}
	if _, err := RenderPublicDataPlaneAdoptionTransactionTarget(
		fixture.rawRenderedBase, fixture.ownership, "fugue-system", swappedIntent, fixture.secretHMACKey,
	); err == nil {
		t.Fatal("transaction post-render accepted an intent swapped out of the authorized output")
	}
}

func TestPublicDataPlaneAdoptionReconcilesOnlyExactHistoricalChecksums(t *testing.T) {
	fixture := newPublicDataPlaneAdoptionFixture(t)
	plan, _, err := BuildPublicDataPlaneAdoptionPlan(fixture.input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.ChecksumReconciliations) != 9 {
		t.Fatalf("checksum reconciliation count = %d, want 9", len(plan.ChecksumReconciliations))
	}
	if plan.ServerRenderedTargetDigest != digestBytesSHA256(fixture.serverTarget) ||
		plan.TargetManifestDigest != digestBytesSHA256(fixture.target) ||
		plan.ServerRenderedTargetDigest == plan.TargetManifestDigest {
		t.Fatalf("server/final target binding is incorrect: %#v", plan)
	}
	for _, reconciliation := range plan.ChecksumReconciliations {
		if publicDataPlaneRenderedChecksums()[reconciliation.WorkloadName] != reconciliation.RenderedValue {
			t.Fatalf("rendered checksum is not the captured live shape: %#v", reconciliation)
		}
	}

	envelope, err := NewPublicDataPlaneAdoptionTransaction(plan)
	if err != nil {
		t.Fatal(err)
	}
	output, err := RenderPublicDataPlaneAdoptionTransactionTarget(
		fixture.rawRenderedBase, fixture.ownership, "fugue-system", envelope, fixture.secretHMACKey,
	)
	if err != nil || !bytes.Equal(output, fixture.rawTarget) {
		t.Fatalf("transaction did not restore exact base checksums: %v", err)
	}

	tests := map[string]func(*testing.T, *publicDataPlaneAdoptionFixture){
		"missing front checksum": func(t *testing.T, fixture *publicDataPlaneAdoptionFixture) {
			fixture.input.TargetManifest = mutatePublicDataPlaneCanonicalObject(t, fixture, fixture.serverTarget, "fugue-fugue-edge-front", func(object map[string]any) {
				annotations := object["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)
				delete(annotations, publicDataPlaneFrontChecksumAnnotation)
			})
			fixture.input.RepeatedTarget = fixture.input.TargetManifest
		},
		"malformed worker checksum": func(t *testing.T, fixture *publicDataPlaneAdoptionFixture) {
			fixture.input.TargetManifest = mutatePublicDataPlaneCanonicalObject(t, fixture, fixture.serverTarget, "fugue-fugue-edge-worker-a", func(object map[string]any) {
				annotations := object["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)
				annotations[publicDataPlaneWorkerChecksumAnnotation] = "not-a-sha256"
			})
			fixture.input.RepeatedTarget = fixture.input.TargetManifest
		},
		"extra annotation": func(t *testing.T, fixture *publicDataPlaneAdoptionFixture) {
			fixture.input.TargetManifest = mutatePublicDataPlaneCanonicalObject(t, fixture, fixture.serverTarget, "fugue-fugue-edge-dynamic-worker-b", func(object map[string]any) {
				annotations := object["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)
				annotations["example.invalid/config-drift"] = strings.Repeat("d", 64)
			})
			fixture.input.RepeatedTarget = fixture.input.TargetManifest
		},
		"checksum plus real template drift": func(t *testing.T, fixture *publicDataPlaneAdoptionFixture) {
			fixture.input.TargetManifest = mutatePublicDataPlaneCanonicalObject(t, fixture, fixture.serverTarget, "fugue-fugue-edge-country-de-front", func(object map[string]any) {
				object["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["terminationGracePeriodSeconds"] = manifestNumber("31")
			})
			fixture.input.RepeatedTarget = fixture.input.TargetManifest
		},
		"unexpected checksum annotation": func(t *testing.T, fixture *publicDataPlaneAdoptionFixture) {
			fixture.input.TargetManifest = mutatePublicDataPlaneCanonicalObject(t, fixture, fixture.serverTarget, "fugue-fugue-edge-country-de-front", func(object map[string]any) {
				annotations := object["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)
				annotations[publicDataPlaneWorkerChecksumAnnotation] = strings.Repeat("e", 64)
			})
			fixture.input.RepeatedTarget = fixture.input.TargetManifest
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newPublicDataPlaneAdoptionFixture(t)
			mutate(t, fixture)
			if _, _, err := BuildPublicDataPlaneAdoptionPlan(fixture.input); err == nil {
				t.Fatal("checksum reconciliation authorized non-exact drift")
			}
		})
	}

	t.Run("apply-time checksum drift", func(t *testing.T) {
		drifted := mutatePublicDataPlaneCanonicalObject(t, fixture, fixture.rawRenderedBase, "fugue-fugue-edge-worker-b", func(object map[string]any) {
			annotations := object["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)
			annotations[publicDataPlaneWorkerChecksumAnnotation] = strings.Repeat("f", 64)
		})
		if _, err := RenderPublicDataPlaneAdoptionTransactionTarget(
			drifted, fixture.ownership, "fugue-system", envelope, fixture.secretHMACKey,
		); err == nil {
			t.Fatal("transaction accepted a checksum not bound by both server renders and prewrite")
		}
	})
}

func resealPublicDataPlaneAdoptionEnvelope(envelope *PublicDataPlaneAdoptionTransactionEnvelope) {
	envelope.Plan.Digest = publicDataPlaneAdoptionPlanDigest(envelope.Plan)
	envelope.Plan.Stage2.Stage1PlanDigest = envelope.Plan.Digest
	envelope.Plan.Digest = publicDataPlaneAdoptionPlanDigest(envelope.Plan)
	envelope.Plan.Stage2.Stage1PlanDigest = envelope.Plan.Digest
	envelope.PlanDigest = envelope.Plan.Digest
}

func TestPublicDataPlaneAdoptionPrewriteAcceptsPersistedTransactionRepresentation(t *testing.T) {
	fixture := newPublicDataPlaneAdoptionFixture(t)
	plan, restore, err := BuildPublicDataPlaneAdoptionPlan(fixture.input)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := NewPublicDataPlaneAdoptionTransaction(plan)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var decoded PublicDataPlaneAdoptionTransactionEnvelope
	decoder := json.NewDecoder(bytes.NewReader(persisted))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(decoded.Plan, plan) {
		t.Fatal("fixture did not exercise the empty-versus-nil persisted representation boundary")
	}
	if err := VerifyPublicDataPlaneAdoptionPrewrite(decoded.Plan, restore, fixture.input); err != nil {
		t.Fatalf("persisted transaction failed unchanged prewrite: %v", err)
	}

	drifted := fixture.input
	drifted.Values = []byte("edge:\n  enabled: false\n")
	if err := VerifyPublicDataPlaneAdoptionPrewrite(decoded.Plan, restore, drifted); err == nil {
		t.Fatal("persisted transaction accepted real Helm values drift")
	}
}

func TestPublicDataPlaneAdoptionPrewriteAndRestoreFailClosed(t *testing.T) {
	fixture := newPublicDataPlaneAdoptionFixture(t)
	plan, restore, err := BuildPublicDataPlaneAdoptionPlan(fixture.input)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("resource version drift", func(t *testing.T) {
		drifted := cloneJSONDocument(t, fixture.snapshotDoc)
		items := drifted["items"].([]any)
		metadata := items[0].(map[string]any)["metadata"].(map[string]any)
		metadata["resourceVersion"] = "rv-drift"
		input := fixture.input
		input.KubernetesSnapshot = mustJSON(t, drifted)
		if err := VerifyPublicDataPlaneAdoptionPrewrite(plan, restore, input); err == nil {
			t.Fatal("prewrite accepted resourceVersion drift")
		}
	})

	t.Run("record drift", func(t *testing.T) {
		drifted := cloneJSONDocument(t, fixture.snapshotDoc)
		record := findSnapshotObject(t, drifted, "ConfigMap", "fugue-fugue-public-data-plane-release")
		record["data"].(map[string]any)["active_slots"] = `{"fugue-fugue-edge-country-us":"a","fugue-fugue-edge-country-de":"b"}`
		input := fixture.input
		input.KubernetesSnapshot = mustJSON(t, drifted)
		if err := VerifyPublicDataPlaneAdoptionPrewrite(plan, restore, input); err == nil {
			t.Fatal("prewrite accepted active slot drift")
		}
	})

	t.Run("Helm revision drift", func(t *testing.T) {
		input := fixture.input
		input.BaseRevision, input.TargetRevision = "807", "808"
		if err := VerifyPublicDataPlaneAdoptionPrewrite(plan, restore, input); err == nil {
			t.Fatal("prewrite accepted Helm revision drift")
		}
	})

	t.Run("Helm manifest drift", func(t *testing.T) {
		input := fixture.input
		input.BaseManifest = bytes.Replace(input.BaseManifest, []byte("caddy:2.10.2-alpine"), []byte("caddy:2.10.3-alpine"), 1)
		if err := VerifyPublicDataPlaneAdoptionPrewrite(plan, restore, input); err == nil {
			t.Fatal("prewrite accepted Helm manifest drift")
		}
	})

	t.Run("Helm values drift", func(t *testing.T) {
		input := fixture.input
		input.Values = []byte("edge:\n  enabled: false\n")
		if err := VerifyPublicDataPlaneAdoptionPrewrite(plan, restore, input); err == nil {
			t.Fatal("prewrite accepted Helm values drift")
		}
	})

	t.Run("observed live drift", func(t *testing.T) {
		input := fixture.input
		input.ObservedLive = bytes.Replace(
			input.ObservedLive, []byte(fixtureRef()),
			[]byte("registry.example/edge@sha256:"+strings.Repeat("c", 64)), 1,
		)
		if err := VerifyPublicDataPlaneAdoptionPrewrite(plan, restore, input); err == nil {
			t.Fatal("prewrite accepted observed-live drift")
		}
	})

	t.Run("restore UID drift", func(t *testing.T) {
		drifted := cloneJSONDocument(t, fixture.snapshotDoc)
		member := findSnapshotObject(t, drifted, "DaemonSet", "fugue-fugue-edge-worker-b")
		member["metadata"].(map[string]any)["uid"] = "replacement-uid"
		if err := VerifyPublicDataPlaneAdoptionRestore(
			restore, mustJSON(t, drifted), "fugue", "fugue-system", "fugue-fugue",
		); err == nil {
			t.Fatal("restore accepted replacement UID")
		}
	})

	t.Run("restore spec drift", func(t *testing.T) {
		drifted := cloneJSONDocument(t, fixture.snapshotDoc)
		member := findSnapshotObject(t, drifted, "DaemonSet", "fugue-fugue-edge-worker-b")
		containers := member["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
		containers[1].(map[string]any)["image"] = "caddy:drift"
		if err := VerifyPublicDataPlaneAdoptionRestore(
			restore, mustJSON(t, drifted), "fugue", "fugue-system", "fugue-fugue",
		); err == nil {
			t.Fatal("restore accepted caddy drift")
		}
	})
}

func TestPublicDataPlaneAdoptionRejectsCrossDomainAndCohortDrift(t *testing.T) {
	tests := map[string]func(*testing.T, *publicDataPlaneAdoptionFixture){
		"caddy target drift": func(t *testing.T, fixture *publicDataPlaneAdoptionFixture) {
			fixture.input.TargetManifest = bytes.Replace(fixture.serverTarget, []byte("caddy:2.10.2-alpine"), []byte("caddy:2.10.3-alpine"), 1)
			fixture.input.RepeatedTarget = fixture.input.TargetManifest
		},
		"target non-image drift": func(t *testing.T, fixture *publicDataPlaneAdoptionFixture) {
			fixture.input.TargetManifest = bytes.Replace(fixture.serverTarget, []byte(`terminationGracePeriodSeconds: !!float "30"`), []byte(`terminationGracePeriodSeconds: !!float "31"`), 1)
			if bytes.Equal(fixture.input.TargetManifest, fixture.serverTarget) {
				t.Fatal("test mutation did not change target")
			}
			fixture.input.RepeatedTarget = fixture.input.TargetManifest
		},
		"mixed active digest": func(t *testing.T, fixture *publicDataPlaneAdoptionFixture) {
			doc := cloneJSONDocument(t, fixture.snapshotDoc)
			member := findSnapshotObject(t, doc, "DaemonSet", "fugue-fugue-edge-country-de-worker-b")
			containers := member["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
			containers[0].(map[string]any)["image"] = "registry.example/edge@sha256:" + strings.Repeat("c", 64)
			fixture.input.KubernetesSnapshot = mustJSON(t, doc)
		},
		"disabled worker becomes serving": func(t *testing.T, fixture *publicDataPlaneAdoptionFixture) {
			doc := cloneJSONDocument(t, fixture.snapshotDoc)
			member := findSnapshotObject(t, doc, "DaemonSet", "fugue-fugue-edge-dynamic-worker-b")
			status := member["status"].(map[string]any)
			for _, key := range []string{"desiredNumberScheduled", "currentNumberScheduled", "numberReady", "numberAvailable", "updatedNumberScheduled"} {
				status[key] = float64(1)
			}
			fixture.input.KubernetesSnapshot = mustJSON(t, doc)
		},
		"partial group": func(t *testing.T, fixture *publicDataPlaneAdoptionFixture) {
			doc := cloneJSONDocument(t, fixture.snapshotDoc)
			items := doc["items"].([]any)
			filtered := items[:0]
			for _, raw := range items {
				item := raw.(map[string]any)
				metadata, _ := item["metadata"].(map[string]any)
				if metadata["name"] != "fugue-fugue-edge-country-de-worker-a" {
					filtered = append(filtered, raw)
				}
			}
			doc["items"] = filtered
			fixture.input.KubernetesSnapshot = mustJSON(t, doc)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newPublicDataPlaneAdoptionFixture(t)
			mutate(t, fixture)
			if _, _, err := BuildPublicDataPlaneAdoptionPlan(fixture.input); err == nil {
				t.Fatal("drift was authorized")
			}
		})
	}
}

func (fixture *publicDataPlaneAdoptionFixture) finalEvidence(t *testing.T) ([]byte, []byte) {
	t.Helper()
	document := cloneJSONDocument(t, fixture.snapshotDoc)
	for _, name := range []string{
		"fugue-fugue-edge-country-de-worker-b",
		"fugue-fugue-edge-worker-b",
		"fugue-fugue-edge-dynamic-worker-b",
	} {
		member := findSnapshotObject(t, document, "DaemonSet", name)
		containers := member["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
		containers[0].(map[string]any)["image"] = fixtureRef()
	}
	snapshot := mustJSON(t, document)
	observedItems := cloneJSONDocument(t, document)["items"].([]any)
	filtered := make([]any, 0, 9)
	for _, raw := range observedItems {
		item := raw.(map[string]any)
		if item["kind"] != "DaemonSet" {
			continue
		}
		metadata := item["metadata"].(map[string]any)
		name := metadata["name"].(string)
		if strings.Contains(name, "-dynamic-worker-") {
			marker, candidate, err := CaptureDisabledPublicEdgeWorkerObservation(mustJSON(t, item), "fugue-system")
			if err != nil || !candidate {
				t.Fatalf("capture final disabled marker: candidate=%t err=%v", candidate, err)
			}
			metadata["annotations"] = map[string]any{DisabledPublicEdgeWorkerObservationAnnotation: marker}
		}
		delete(item, "status")
		delete(metadata, "uid")
		delete(metadata, "resourceVersion")
		delete(metadata, "generation")
		filtered = append(filtered, item)
	}
	observedRaw := mustJSON(t, map[string]any{"apiVersion": "v1", "kind": "List", "items": filtered})
	observed, err := MaterializeObservedLiveImageManifest(fixture.target, observedRaw, fixture.ownership, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, observed
}

func newPublicDataPlaneAdoptionFixture(t *testing.T) *publicDataPlaneAdoptionFixture {
	t.Helper()
	const releaseName, namespace, fullname = "fugue", "fugue-system", "fugue-fugue"
	groups := []string{
		fullname + "-edge-country-de",
		fullname + "-edge",
		fullname + "-edge-dynamic",
	}
	items := make([]any, 0, 10)
	for _, base := range groups {
		serving := !strings.HasSuffix(base, "-dynamic")
		frontCount := int64(0)
		workerCount := int64(0)
		if serving {
			frontCount, workerCount = 1, 1
		}
		items = append(items, publicDataPlaneDaemonSet(base+"-front", releaseName, "front", "", frontCount, "", ""))
		items = append(items, publicDataPlaneDaemonSet(base+"-worker-a", releaseName, "worker-a", "a", workerCount, "registry.example/edge:slot-a", "caddy:2.10.2-alpine"))
		bImage := "registry.example/edge:slot-b"
		if serving {
			bImage = fixtureRef()
		}
		items = append(items, publicDataPlaneDaemonSet(base+"-worker-b", releaseName, "worker-b", "b", workerCount, bImage, "caddy:2.10.2-alpine"))
	}
	recordSHA := strings.Repeat("a", 40)
	activeSlots := map[string]string{groups[0]: "b", groups[1]: "b"}
	activeJSON, _ := json.Marshal(activeSlots)
	items = append(items, map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{
			"name": fullname + "-public-data-plane-release", "namespace": namespace,
			"uid": "record-uid", "resourceVersion": "record-rv",
			"labels": map[string]any{
				"app.kubernetes.io/instance":  releaseName,
				"app.kubernetes.io/component": "public-data-plane-release",
				"fugue.io/rollout-subsystem":  "public-data-plane",
			},
		},
		"data": map[string]any{
			"release_id": "pdp-test-" + recordSHA, "mode": "node-local-blue-green",
			"active_slots": string(activeJSON), "daemonsets": strings.Join(groups, ","),
			"edge_resources": `{}`, "caddy_resources": `{}`, "git_sha": recordSHA,
			"recorded_at": "2026-08-01T00:00:00Z",
		},
	})
	snapshotDoc := map[string]any{"apiVersion": "v1", "kind": "List", "items": items}
	snapshot := mustJSON(t, snapshotDoc)
	ownership := publicDataPlaneAdoptionOwnership(groups)

	baseItems := make([]any, 0, 9)
	for _, raw := range items {
		item := raw.(map[string]any)
		if item["kind"] != "DaemonSet" {
			continue
		}
		copy := cloneJSONDocument(t, item)
		delete(copy, "status")
		metadata := copy["metadata"].(map[string]any)
		delete(metadata, "uid")
		delete(metadata, "resourceVersion")
		delete(metadata, "generation")
		name := metadata["name"].(string)
		if strings.HasSuffix(name, "worker-b") {
			containers := copy["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
			containers[0].(map[string]any)["image"] = "registry.example/edge:helm-slot-b"
		}
		baseItems = append(baseItems, copy)
	}
	secretPayloads := map[string]map[string]string{
		fullname + "-config": {
			"FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY": "workload-signing-secret",
			"FUGUE_BUNDLE_SIGNING_KEY":            "bundle-signing-secret",
			"FUGUE_EDGE_TLS_ASK_TOKEN":            "edge-tls-secret",
		},
		fullname + "-control-plane-postgres-app": {
			"username": "fugue", "password": "postgres-secret",
		},
		fullname + "-platform-component-identity": {
			"FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY":             "platform-signing-secret",
			"FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID":          "platform-key-id",
			"FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY":    "",
			"FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID": "",
			"FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS":         "",
		},
	}
	secretNames := make([]string, 0, len(secretPayloads))
	for name := range secretPayloads {
		secretNames = append(secretNames, name)
	}
	sort.Strings(secretNames)
	liveSecrets := make([]any, 0, len(secretNames))
	for _, name := range secretNames {
		typeName := "Opaque"
		if strings.HasSuffix(name, "control-plane-postgres-app") {
			typeName = "kubernetes.io/basic-auth"
		}
		stringData := map[string]any{}
		data := map[string]any{}
		for key, value := range secretPayloads[name] {
			stringData[key] = value
			data[key] = base64.StdEncoding.EncodeToString([]byte(value))
		}
		baseItems = append(baseItems, map[string]any{
			"apiVersion": "v1", "kind": "Secret", "type": typeName,
			"metadata": map[string]any{"name": name, "namespace": namespace,
				"labels": map[string]any{"app.kubernetes.io/instance": releaseName, "app.kubernetes.io/managed-by": "Helm"}},
			"stringData": stringData,
		})
		liveSecrets = append(liveSecrets, map[string]any{
			"apiVersion": "v1", "kind": "Secret", "type": typeName,
			"metadata": map[string]any{
				"name": name, "namespace": namespace, "uid": name + "-uid", "resourceVersion": name + "-rv",
				"labels":      map[string]any{"app.kubernetes.io/instance": releaseName, "app.kubernetes.io/managed-by": "Helm"},
				"annotations": map[string]any{"meta.helm.sh/release-name": releaseName, "meta.helm.sh/release-namespace": namespace},
			},
			"data": data,
		})
	}
	baseRaw := mustJSON(t, map[string]any{"apiVersion": "v1", "kind": "List", "items": baseItems})
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		t.Fatal(err)
	}
	secretHMACKey := bytes.Repeat([]byte{0x42}, 32)
	base, baseSecretWitness, err := CanonicalizePublicDataPlaneSecretFreeManifest(baseRaw, spec, namespace, secretHMACKey)
	if err != nil {
		t.Fatal(err)
	}
	rawBase, err := CanonicalizeRenderedManifest(baseRaw, spec, namespace)
	if err != nil {
		t.Fatal(err)
	}
	renderedDocument := cloneJSONDocument(t, map[string]any{"apiVersion": "v1", "kind": "List", "items": baseItems})
	renderedChecksums := publicDataPlaneRenderedChecksums()
	for _, raw := range renderedDocument["items"].([]any) {
		item := raw.(map[string]any)
		if item["kind"] != "DaemonSet" {
			continue
		}
		metadata := item["metadata"].(map[string]any)
		name := metadata["name"].(string)
		value, ok := renderedChecksums[name]
		if !ok {
			t.Fatalf("rendered checksum missing for %s", name)
		}
		annotations := item["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)
		if strings.HasSuffix(name, "-front") {
			annotations[publicDataPlaneFrontChecksumAnnotation] = value
		} else {
			annotations[publicDataPlaneWorkerChecksumAnnotation] = value
		}
	}
	rawRenderedBase, err := CanonicalizeRenderedManifest(mustJSON(t, renderedDocument), spec, namespace)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := BuildPublicDataPlaneAdoptionIntent(snapshot, recordSHA, releaseName, namespace, fullname)
	if err != nil {
		t.Fatal(err)
	}
	target, err := RenderPublicDataPlaneAdoptionTarget(base, ownership, namespace, intent)
	if err != nil {
		t.Fatal(err)
	}
	rawTarget, err := renderPublicDataPlaneAdoptionTargetCanonical(rawBase, spec, namespace, intent)
	if err != nil {
		t.Fatal(err)
	}
	rawServerTarget, err := renderPublicDataPlaneAdoptionTargetCanonical(rawRenderedBase, spec, namespace, intent)
	if err != nil {
		t.Fatal(err)
	}
	serverTarget, targetSecretWitness, err := CanonicalizePublicDataPlaneSecretFreeManifest(rawServerTarget, spec, namespace, secretHMACKey)
	if err != nil {
		t.Fatal(err)
	}
	lookupSnapshot := mustJSON(t, map[string]any{"apiVersion": "v1", "kind": "List", "items": liveSecrets})
	lookupWitness, err := BuildPublicDataPlaneSecretLookupWitness(
		lookupSnapshot,
		releaseName, namespace, PublicDataPlaneSecretLookupNames{
			Config: fullname + "-config", ControlPlaneDB: fullname + "-control-plane-postgres-app",
			PlatformIdentity: fullname + "-platform-component-identity",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	observedItems := cloneJSONDocument(t, snapshotDoc)["items"].([]any)
	filtered := make([]any, 0, 9)
	for _, raw := range observedItems {
		item := raw.(map[string]any)
		if item["kind"] != "DaemonSet" {
			continue
		}
		metadata := item["metadata"].(map[string]any)
		name := metadata["name"].(string)
		if strings.Contains(name, "-dynamic-worker-") {
			encoded := mustJSON(t, item)
			marker, candidate, markerErr := CaptureDisabledPublicEdgeWorkerObservation(encoded, namespace)
			if markerErr != nil || !candidate {
				t.Fatalf("capture disabled marker: candidate=%t err=%v", candidate, markerErr)
			}
			metadata["annotations"] = map[string]any{DisabledPublicEdgeWorkerObservationAnnotation: marker}
		}
		delete(item, "status")
		delete(metadata, "uid")
		delete(metadata, "resourceVersion")
		delete(metadata, "generation")
		filtered = append(filtered, item)
	}
	observedRaw := mustJSON(t, map[string]any{"apiVersion": "v1", "kind": "List", "items": filtered})
	observed, err := MaterializeObservedLiveImageManifest(base, observedRaw, ownership, namespace)
	if err != nil {
		t.Fatal(err)
	}

	input := PublicDataPlaneAdoptionInput{
		Ownership: ownership, Values: []byte("edge:\n  enabled: true\n"), BaseManifest: base, TargetManifest: serverTarget, RepeatedTarget: serverTarget,
		ObservedLive: observed, KubernetesSnapshot: snapshot, SourceCommit: recordSHA,
		ReleaseName: releaseName, ReleaseNamespace: namespace, ReleaseFullname: fullname,
		BaseRevision: "806", TargetRevision: "807", Bindings: map[string]string{},
		SecretLookupWitness: mustJSON(t, lookupWitness), BaseSecretRenderWitness: mustJSON(t, baseSecretWitness),
		TargetSecretRenderWitness: mustJSON(t, targetSecretWitness), RepeatedSecretRenderWitness: mustJSON(t, targetSecretWitness),
	}
	return &publicDataPlaneAdoptionFixture{
		input: input, ownership: ownership, base: base, target: target, observed: observed,
		snapshot: snapshot, snapshotDoc: snapshotDoc, secretHMACKey: secretHMACKey,
		rawBase: rawBase, rawRenderedBase: rawRenderedBase, rawTarget: rawTarget, serverTarget: serverTarget,
		lookupSnapshot: lookupSnapshot,
	}
}

func publicDataPlaneDaemonSet(name, release, role, slot string, count int64, edgeImage, caddyImage string) map[string]any {
	mode := "node-local-blue-green-front"
	labels := map[string]any{
		"app.kubernetes.io/instance": release, "app.kubernetes.io/managed-by": "Helm",
		"app.kubernetes.io/component": strings.TrimPrefix(name, "fugue-fugue-"),
		"helm.sh/chart":               "fugue-1.0.0", "fugue.io/rollout-subsystem": "public-data-plane",
		"fugue.io/downtime-class": "online-required",
	}
	containers := []any{map[string]any{"name": "front", "image": "registry.example/front:v1"}}
	annotations := map[string]any{}
	if role == "front" {
		annotations[publicDataPlaneFrontChecksumAnnotation] = "86e41d93a16440747f6c02fea2fd15fc7affc0258d5704c05521362a085c4d6b"
	}
	if strings.HasPrefix(role, "worker-") {
		mode = "node-local-blue-green-worker"
		labels["fugue.io/edge-slot"] = slot
		if slot == "a" {
			annotations[publicDataPlaneWorkerChecksumAnnotation] = "652d3af6567adec76c5c7ab1e40a22799813b78819e91447bdadbee35ec8dbd9"
		} else {
			annotations[publicDataPlaneWorkerChecksumAnnotation] = "70964b6dd144e0497ec9abfd937fd4484aeae5a6a6e8ef5ee2a79f20062fd67f"
		}
		containers = []any{
			map[string]any{"name": "edge", "image": edgeImage},
			map[string]any{"name": "caddy", "image": caddyImage},
		}
		if count > 0 && slot == "b" {
			annotations["fugue.io/public-data-plane-release-id"] = "pdp-test-" + strings.Repeat("a", 40)
			annotations["fugue.io/public-data-plane-release-mode"] = "node-local-blue-green-worker"
		}
	}
	labels["fugue.io/rollout-mode"] = mode
	return map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{
			"name": name, "namespace": "fugue-system", "uid": name + "-uid",
			"resourceVersion": name + "-rv", "generation": int64(7), "labels": labels,
		},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": name}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": name}, "annotations": annotations},
				"spec":     map[string]any{"terminationGracePeriodSeconds": int64(30), "containers": containers},
			},
		},
		"status": map[string]any{
			"observedGeneration": int64(7), "desiredNumberScheduled": count,
			"currentNumberScheduled": count, "numberReady": count, "numberAvailable": count,
			"updatedNumberScheduled": count, "numberUnavailable": int64(0), "numberMisscheduled": int64(0),
		},
	}
}

func publicDataPlaneAdoptionOwnership(groups []string) []byte {
	var rules strings.Builder
	for _, base := range groups {
		for _, role := range []string{"front", "worker-a", "worker-b"} {
			name := base + "-" + role
			mode := "node-local-blue-green-front"
			extra := ""
			if strings.HasPrefix(role, "worker-") {
				mode = "node-local-blue-green-worker"
				extra = "\n      fugue.io/edge-slot: " + strings.TrimPrefix(role, "worker-")
			}
			fmt.Fprintf(&rules, `  - id: %s
    domain: authoritative-dns
    apiGroup: apps
    version: v1
    kind: DaemonSet
    scope: Namespaced
    namespace: fugue-system
    name: %s
    requiredLabels:
      app.kubernetes.io/instance: fugue
      fugue.io/rollout-subsystem: public-data-plane
      fugue.io/rollout-mode: %s
      fugue.io/downtime-class: online-required%s
`, strings.ReplaceAll(name, "-", "_"), name, mode, extra)
		}
	}
	return []byte(`apiVersion: release-domain.fugue.dev/v1
kind: ReleaseDomainOwnership
domains: [node-local, authoritative-dns, control-plane, image-cache, backup]
requiredBindings: []
fileRules: []
valueRules: []
objectRules:
` + rules.String())
}

func fixtureRef() string { return "registry.example/edge@sha256:" + strings.Repeat("b", 64) }

func publicDataPlaneRenderedChecksums() map[string]string {
	return map[string]string{
		"fugue-fugue-edge-country-de-front":    "fd46c8c1bf7307b843c178e2afd49a8de0410146d4cd99ec2b946297915e4d23",
		"fugue-fugue-edge-country-de-worker-a": "aff21600cc90fa4f8cd40f5ff4890dc92b6e4fd828e7377ddf8832ba347ddf76",
		"fugue-fugue-edge-country-de-worker-b": "c49477014fa579158e406b72f2407410752f9ca954f41bebd47651eb112bf056",
		"fugue-fugue-edge-dynamic-front":       "a7c309cb633f0484400723858dd27f259d1f06cbd3c7158a29a0b3d4dacf5de3",
		"fugue-fugue-edge-dynamic-worker-a":    "17ef4b4f313fd47c8d5ff433bcbaa765700f9d582f638fb90415a9b29698eaaa",
		"fugue-fugue-edge-dynamic-worker-b":    "db75c3aff8a9c972270eaaa47dbc9506d0eb80477c1c1f39d14b1b31802b8280",
		"fugue-fugue-edge-front":               "f24a6144cfb15d725122858cb3327f4a23f3162d766da639b637b452aed36526",
		"fugue-fugue-edge-worker-a":            "d9fb982dca6864570707c8436895aba987fa8215c22a44046f133075a2398c1b",
		"fugue-fugue-edge-worker-b":            "4afb9006b89756012d1dcb2d5f587c5075079ff34d2d3294254f9e2dc05a1597",
	}
}

func mutatePublicDataPlaneCanonicalObject(
	t *testing.T,
	fixture *publicDataPlaneAdoptionFixture,
	manifest []byte,
	name string,
	mutate func(map[string]any),
) []byte {
	t.Helper()
	spec, err := LoadOwnership(bytes.NewReader(fixture.ownership))
	if err != nil {
		t.Fatal(err)
	}
	objects, unknown := decodeManifest(manifest, spec, "fugue-system", "test-mutation")
	if len(unknown) != 0 {
		t.Fatalf("decode test manifest: %#v", unknown)
	}
	indexed, duplicates := indexManifestObjects(objects, "test-mutation")
	if len(duplicates) != 0 {
		t.Fatalf("duplicate test manifest objects: %#v", duplicates)
	}
	key := identityKey(ObjectIdentity{
		APIGroup: "apps", Version: "v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: name,
	})
	object, ok := indexed[key]
	if !ok {
		t.Fatalf("test object %s is missing", name)
	}
	mutate(object.Object)
	indexed[key] = object
	result, err := encodePublicDataPlaneManifestObjects(indexed)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func cloneJSONDocument[T any](t *testing.T, input T) T {
	t.Helper()
	encoded := mustJSON(t, input)
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var output T
	if err := decoder.Decode(&output); err != nil {
		t.Fatal(err)
	}
	return output
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func findSnapshotObject(t *testing.T, document map[string]any, kind, name string) map[string]any {
	t.Helper()
	for _, raw := range document["items"].([]any) {
		item := raw.(map[string]any)
		metadata, _ := item["metadata"].(map[string]any)
		if item["kind"] == kind && metadata["name"] == name {
			return item
		}
	}
	t.Fatalf("object %s/%s not found", kind, name)
	return nil
}

func TestPublicDataPlaneAdoptionIntentDigestBindsEvidence(t *testing.T) {
	fixture := newPublicDataPlaneAdoptionFixture(t)
	intent, err := BuildPublicDataPlaneAdoptionIntent(fixture.snapshot, strings.Repeat("a", 40), "fugue", "fugue-system", "fugue-fugue")
	if err != nil {
		t.Fatal(err)
	}
	mutated := intent
	mutated.Groups = append([]PublicDataPlaneAdoptionGroupEvidence(nil), intent.Groups...)
	mutated.Groups[0].WorkerB.ResourceVersionDigest = strings.Repeat("0", 64)
	if reflect.DeepEqual(mutated, intent) || VerifyPublicDataPlaneAdoptionIntent(mutated) == nil {
		t.Fatal("intent accepted mutated evidence")
	}
}

func TestPublicDataPlaneAdoptionExecutionTraceIsExactlyOnce(t *testing.T) {
	planDigest := "sha256:" + strings.Repeat("d", 64)
	when := "2026-08-01T00:00:00.000000000Z"
	var trace PublicDataPlaneAdoptionExecutionTrace
	var err error
	for _, phase := range []string{
		"prepared", "lease-acquired", "prewrite-verified", "fence-armed",
		"apply-started", "apply-succeeded", "baseline-finalized", "lease-released",
	} {
		trace, err = AppendPublicDataPlaneAdoptionTrace(trace, planDigest, phase, when)
		if err != nil {
			t.Fatalf("append %s: %v", phase, err)
		}
	}
	if err := VerifyPublicDataPlaneAdoptionTrace(trace); err != nil {
		t.Fatal(err)
	}
	if _, err := AppendPublicDataPlaneAdoptionTrace(trace, planDigest, "apply-started", when); err == nil {
		t.Fatal("trace accepted a second apply")
	}

	trace = PublicDataPlaneAdoptionExecutionTrace{}
	for _, phase := range []string{
		"prepared", "lease-acquired", "prewrite-verified", "fence-armed",
		"apply-started", "apply-failed", "restore-started", "restore-succeeded", "recovery-fenced",
	} {
		trace, err = AppendPublicDataPlaneAdoptionTrace(trace, planDigest, phase, when)
		if err != nil {
			t.Fatalf("append failure path %s: %v", phase, err)
		}
	}
	if _, err := AppendPublicDataPlaneAdoptionTrace(trace, planDigest, "restore-started", when); err == nil {
		t.Fatal("trace accepted a second authoritative restore")
	}
}

func TestPublicDataPlaneAdoptionDurableWALCrossProcessRecoveryNeverReapplies(t *testing.T) {
	fixture := newPublicDataPlaneAdoptionFixture(t)
	plan, restore, err := BuildPublicDataPlaneAdoptionPlan(fixture.input)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := NewPublicDataPlaneAdoptionTransaction(plan)
	if err != nil {
		t.Fatal(err)
	}
	transaction := mustJSON(t, envelope)
	const owner = "release/123-1"
	const token = "0123456789abcdef0123456789abcdef"
	newWAL := func(t *testing.T) PublicDataPlaneAdoptionRecoveryWAL {
		wal, walErr := NewPublicDataPlaneAdoptionRecoveryWAL(
			envelope, transaction, restore, "fugue-system", "fugue-lock", owner, token,
			"2026-08-01T00:00:00Z", "123", 1,
		)
		if walErr != nil {
			t.Fatal(walErr)
		}
		return wal
	}
	restart := func(t *testing.T, wal PublicDataPlaneAdoptionRecoveryWAL) PublicDataPlaneAdoptionRecoveryWAL {
		encoded := mustJSON(t, wal)
		var recovered PublicDataPlaneAdoptionRecoveryWAL
		if err := json.Unmarshal(encoded, &recovered); err != nil {
			t.Fatal(err)
		}
		if err := VerifyPublicDataPlaneAdoptionRecoveryArtifacts(recovered, transaction, restore); err != nil {
			t.Fatal(err)
		}
		return recovered
	}
	advance := func(t *testing.T, wal PublicDataPlaneAdoptionRecoveryWAL, phase string) PublicDataPlaneAdoptionRecoveryWAL {
		updated, updateErr := AdvancePublicDataPlaneAdoptionRecoveryWAL(
			wal, owner, token, phase, "2026-08-01T00:00:01Z", "",
		)
		if updateErr != nil {
			t.Fatalf("advance %s: %v", phase, updateErr)
		}
		return updated
	}

	for _, test := range []struct {
		name   string
		phases []string
	}{
		{name: "death after fence before apply"},
		{name: "death mid apply", phases: []string{"apply-started"}},
		{name: "death after apply before finalize", phases: []string{"apply-started", "apply-succeeded"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			wal := newWAL(t)
			wal = advance(t, wal, "fence-armed")
			for _, phase := range test.phases {
				wal = advance(t, wal, phase)
			}
			wal = restart(t, wal)
			wal = advance(t, wal, "restore-started")
			if wal.RestoreAttempts != 1 || wal.ApplyAttempts > 1 {
				t.Fatalf("recovery counters drifted: %#v", wal)
			}
			if _, err := AdvancePublicDataPlaneAdoptionRecoveryWAL(
				wal, owner, token, "apply-started", "2026-08-01T00:00:02Z", "",
			); err == nil {
				t.Fatal("recovery WAL allowed a second Helm apply")
			}
			wal = advance(t, wal, "restore-succeeded")
			if wal.Phase != "restore-succeeded" {
				t.Fatal("recovery did not reach its terminal domain restore")
			}
		})
	}

	wal := newWAL(t)
	if _, err := AdvancePublicDataPlaneAdoptionRecoveryWAL(wal, owner, "wrong-token", "apply-started", "2026-08-01T00:00:01Z", ""); err == nil {
		t.Fatal("WAL accepted the wrong Lease fencing token")
	}
	tampered := append([]byte(nil), restore...)
	tampered[len(tampered)-1] ^= 1
	if err := VerifyPublicDataPlaneAdoptionRecoveryArtifacts(wal, transaction, tampered); err == nil {
		t.Fatal("WAL accepted a tampered restore witness")
	}
	wal = advance(t, wal, "fence-armed")
	wal = advance(t, wal, "apply-started")
	wal = advance(t, wal, "apply-failed")
	wal = advance(t, wal, "restore-started")
	awaiting, err := AdvancePublicDataPlaneAdoptionRecoveryWAL(
		wal, owner, token, "restore-succeeded-awaiting-helm-compensation", "2026-08-01T00:00:02Z", "",
	)
	if err != nil {
		t.Fatal(err)
	}
	if awaiting.Phase != "restore-succeeded-awaiting-helm-compensation" {
		t.Fatal("failed/pending target revision was not durably fenced for typed Helm compensation")
	}
	if _, err := AdvancePublicDataPlaneAdoptionRecoveryWAL(
		awaiting, owner, token, "baseline-finalized", "2026-08-01T00:00:03Z", "sha256:"+strings.Repeat("1", 64),
	); err == nil {
		t.Fatal("awaiting-compensation WAL incorrectly allowed Stage2 finalization")
	}
}

func TestPublicDataPlaneAdoptionRecoveryCandidateBindsUIDImageAndNonImageSpec(t *testing.T) {
	fixture := newPublicDataPlaneAdoptionFixture(t)
	_, restore, err := BuildPublicDataPlaneAdoptionPlan(fixture.input)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyPublicDataPlaneAdoptionRecoveryCandidate(
		restore, fixture.snapshot, "fugue-system", fixtureRef(),
	); err != nil {
		t.Fatalf("stage-before recovery candidate failed: %v", err)
	}
	targetSnapshot, _ := fixture.finalEvidence(t)
	if err := VerifyPublicDataPlaneAdoptionRecoveryCandidate(
		restore, targetSnapshot, "fugue-system", fixtureRef(),
	); err != nil {
		t.Fatalf("target recovery candidate failed: %v", err)
	}

	uidDrift := cloneJSONDocument(t, fixture.snapshotDoc)
	findSnapshotObject(t, uidDrift, "DaemonSet", "fugue-fugue-edge-dynamic-worker-b")["metadata"].(map[string]any)["uid"] = "replacement"
	if err := VerifyPublicDataPlaneAdoptionRecoveryCandidate(restore, mustJSON(t, uidDrift), "fugue-system", fixtureRef()); err == nil {
		t.Fatal("recovery candidate accepted UID drift")
	}
	caddyDrift := cloneJSONDocument(t, fixture.snapshotDoc)
	member := findSnapshotObject(t, caddyDrift, "DaemonSet", "fugue-fugue-edge-dynamic-worker-b")
	containers := member["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
	containers[1].(map[string]any)["image"] = "caddy:drift"
	if err := VerifyPublicDataPlaneAdoptionRecoveryCandidate(restore, mustJSON(t, caddyDrift), "fugue-system", fixtureRef()); err == nil {
		t.Fatal("recovery candidate accepted caddy/non-image drift")
	}
}
