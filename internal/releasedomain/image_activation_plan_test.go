package releasedomain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestBuildImageActivationPlanSeparatesBuildsFromActualWorkloadChanges(t *testing.T) {
	controllerDigest := md0Digest("c")
	input := md1ActivationFixture(
		t,
		md1Deployment("fugue-api", "api", "registry.test/api@"+md0Digest("a")),
		md1Deployment("fugue-api", "api", "registry.test/api@"+controllerDigest),
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		[]BuildArtifact{
			{Name: "api", SourceBaseCommit: md0BaseCommit, ArtifactDigest: controllerDigest, ProvenanceDigest: md0Digest("f")},
			{Name: "edge", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("e"), ProvenanceDigest: md0Digest("f")},
		},
	)

	activation, err := BuildImageActivationPlanFromManifests(input)
	if err != nil {
		t.Fatalf("derive image activation plan: %v", err)
	}
	if len(activation.Activations) != 1 {
		t.Fatalf("activation count = %d, want 1: %#v", len(activation.Activations), activation.Activations)
	}
	got := activation.Activations[0]
	if got.ArtifactName != "api" || got.Domain != DomainControlPlane ||
		got.Adapter != "control_plane_release_adapter_control_plane" ||
		got.Workload.Name != "fugue-api" || got.Workload.Container != "api" ||
		got.TargetImageRef != "registry.test/api@"+controllerDigest {
		t.Fatalf("activation binding drifted: %#v", got)
	}
	if strings.Contains(got.ID, "edge") {
		t.Fatalf("built-only edge artifact entered activation plan: %#v", got)
	}
	if activation.BuildArtifactPlanDigest != input.BuildPlan.Digest || activation.LiveStateDigest != input.ReleasePlan.Digests.BaseManifest {
		t.Fatalf("activation plan digest binding drifted: %#v", activation)
	}
}

func TestBuildImageActivationPlanAssignsSharedImagePerRenderedWorkloadDomain(t *testing.T) {
	sharedDigest := md0Digest("c")
	base := strings.Join([]string{
		md1Deployment("fugue-api", "service", "registry.test/shared@"+md0Digest("a")),
		md1Deployment("fugue-dns", "service", "registry.test/shared@"+md0Digest("b")),
	}, "\n---\n")
	target := strings.Join([]string{
		md1Deployment("fugue-api", "service", "registry.test/shared@"+sharedDigest),
		md1Deployment("fugue-dns", "service", "registry.test/shared@"+sharedDigest),
	}, "\n---\n")
	input := md1ActivationFixture(
		t,
		base,
		target,
		[]md1OwnershipRule{
			{name: "fugue-api", domain: DomainControlPlane},
			{name: "fugue-dns", domain: DomainAuthoritativeDNS},
		},
		[]BuildArtifact{{Name: "shared", SourceBaseCommit: md0BaseCommit, ArtifactDigest: sharedDigest, ProvenanceDigest: md0Digest("f")}},
	)

	activation, err := BuildImageActivationPlanFromManifests(input)
	if err != nil {
		t.Fatalf("derive shared activation plan: %v", err)
	}
	if len(activation.Activations) != 2 {
		t.Fatalf("activation count = %d, want 2", len(activation.Activations))
	}
	domains := map[Domain]string{}
	for _, item := range activation.Activations {
		domains[item.Domain] = item.Adapter
	}
	if domains[DomainControlPlane] != "control_plane_release_adapter_control_plane" ||
		domains[DomainAuthoritativeDNS] != "control_plane_release_adapter_authoritative_dns" {
		t.Fatalf("shared image was not assigned by rendered workload ownership: %#v", domains)
	}
}

func TestBuildImageActivationReportKeepsOwnershipGapExplicit(t *testing.T) {
	telemetryDigest := md0Digest("d")
	input := md1ActivationFixture(
		t,
		md1Deployment("fugue-telemetry-agent", "telemetry-agent", "registry.test/telemetry@"+md0Digest("c")),
		md1Deployment("fugue-telemetry-agent", "telemetry-agent", "registry.test/telemetry@"+telemetryDigest),
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		[]BuildArtifact{{Name: "telemetry_agent", SourceBaseCommit: md0BaseCommit, ArtifactDigest: telemetryDigest, ProvenanceDigest: md0Digest("f")}},
	)

	activation, evidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatalf("derive report-only activation evidence: %v", err)
	}
	if len(activation.Activations) != 0 || evidence.Complete || len(evidence.Unresolved) != 1 {
		t.Fatalf("ownership gap was hidden or promoted: plan=%#v evidence=%#v", activation, evidence)
	}
	gap := evidence.Unresolved[0]
	if gap.Reason != ImageActivationGapOwnershipMissing ||
		!reflect.DeepEqual(gap.MatchingBuildArtifacts, []string{"telemetry_agent"}) ||
		len(gap.OwnershipDomains) != 0 || len(evidence.BuiltOnlyArtifacts) != 0 ||
		evidence.ResolvedImageActivationPlanDigest != activation.Digest {
		t.Fatalf("ownership gap evidence drifted: %#v", evidence)
	}
}

func TestCanonicalOwnershipResolvesTelemetryAgentThroughControlPlaneAdapter(t *testing.T) {
	ownership, err := os.ReadFile("../../deploy/release-domains/ownership-v1.yaml")
	if err != nil {
		t.Fatalf("read canonical ownership: %v", err)
	}
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		t.Fatalf("load canonical ownership: %v", err)
	}
	bindings := testBindings()
	context, err := NewClassificationContextEvidence("fugue-system", bindings, false)
	if err != nil {
		t.Fatalf("classification context: %v", err)
	}
	telemetryDigest := md0Digest("d")
	baseRaw := md1LabeledDeployment(
		bindings["telemetryAgentName"], "telemetry-agent", "telemetry-agent",
		"registry.test/telemetry@"+md0Digest("c"),
	)
	targetRaw := md1LabeledDeployment(
		bindings["telemetryAgentName"], "telemetry-agent", "telemetry-agent",
		"registry.test/telemetry@"+telemetryDigest,
	)
	base, err := CanonicalizeRenderedManifest([]byte(baseRaw), spec, context.DefaultNamespace)
	if err != nil {
		t.Fatalf("canonicalize base: %v", err)
	}
	target, err := CanonicalizeRenderedManifest([]byte(targetRaw), spec, context.DefaultNamespace)
	if err != nil {
		t.Fatalf("canonicalize target: %v", err)
	}
	changedDigest := md0Digest("f")
	build, err := NewBuildArtifactPlan(md0BaseCommit, md0TargetCommit, changedDigest, []BuildArtifact{{
		Name: "telemetry_agent", SourceBaseCommit: md0BaseCommit,
		ArtifactDigest: telemetryDigest, ProvenanceDigest: md0Digest("e"),
	}})
	if err != nil {
		t.Fatalf("build artifact plan: %v", err)
	}
	rendered := ClassifyRendered(base, target, spec, RenderedOptions{
		DefaultNamespace: context.DefaultNamespace,
		Bindings:         context.BindingMap(),
	})
	plan := BuildPlan(PlanInput{
		Files:    FileClassification{Domains: []Domain{}, Evidence: []Evidence{}},
		Rendered: rendered,
		Digests: DigestEvidence{
			Base: md0Digest("1"), Target: md0Digest("2"), Live: md0Digest("1"),
			BaseManifest: digestBytesSHA256(base), TargetManifest: digestBytesSHA256(target),
			RepeatedTargetManifest: digestBytesSHA256(target), Ownership: digestBytesSHA256(ownership),
			ChangedFiles: changedDigest, ClassificationContext: context,
		},
	})
	activation, evidence, err := BuildImageActivationReportFromManifests(ImageActivationPlanInput{
		BuildPlan: build, ReleasePlan: plan, Ownership: ownership,
		BaseManifest: base, TargetManifest: target,
	})
	if err != nil {
		t.Fatalf("derive canonical telemetry activation: %v", err)
	}
	if !evidence.Complete || len(evidence.Unresolved) != 0 || len(activation.Activations) != 1 {
		t.Fatalf("canonical telemetry ownership remained incomplete: plan=%#v evidence=%#v", activation, evidence)
	}
	got := activation.Activations[0]
	if got.ArtifactName != "telemetry_agent" || got.Domain != DomainControlPlane ||
		got.Adapter != "control_plane_release_adapter_control_plane" ||
		got.Workload.Name != bindings["telemetryAgentName"] || got.Workload.Container != "telemetry-agent" {
		t.Fatalf("canonical telemetry activation binding drifted: %#v", got)
	}
}

func TestBuildImageActivationReportAcceptsOnlyDeterministicImmutableTarget(t *testing.T) {
	targetDigest := md0Digest("d")
	input := md1ActivationFixture(
		t,
		md1Deployment("fugue-api", "api", "registry.test/api@"+md0Digest("a")),
		md1Deployment("fugue-api", "api", "registry.test/api:"+md0TargetCommit),
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		[]BuildArtifact{{
			Name: "api", SourceBaseCommit: md0BaseCommit,
			ArtifactDigest: targetDigest, ProvenanceDigest: md0Digest("f"),
			PublishedImageRef: "registry.test/api@" + targetDigest,
		}},
	)
	context := input.ReleasePlan.Digests.ClassificationContext
	immutableTarget, err := MaterializeTargetPublishedImageRefs(
		input.TargetManifest, input.Ownership, context.DefaultNamespace,
		input.BuildPlan.TargetCommit, input.BuildPlan,
	)
	if err != nil {
		t.Fatalf("materialize immutable target: %v", err)
	}
	input.ImmutableTargetManifest = immutableTarget

	activation, evidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatalf("derive report from deterministic immutable target: %v", err)
	}
	if len(activation.Activations) != 1 || !evidence.Complete || len(evidence.Unresolved) != 0 ||
		activation.Activations[0].TargetImageRef != "registry.test/api@"+targetDigest {
		t.Fatalf("immutable target resolution drifted: plan=%#v evidence=%#v", activation, evidence)
	}

	tampered := append([]byte(nil), immutableTarget...)
	tampered[len(tampered)-2] ^= 1
	input.ImmutableTargetManifest = tampered
	if _, _, err := BuildImageActivationReportFromManifests(input); err == nil ||
		!strings.Contains(err.Error(), "immutable target manifest binding mismatch") {
		t.Fatalf("tampered immutable target was not rejected: %v", err)
	}
}

func TestBuildImageActivationPlanFailsClosedOnIncompleteEvidence(t *testing.T) {
	targetDigest := md0Digest("d")
	valid := md1ActivationFixture(
		t,
		md1Deployment("fugue-api", "api", "registry.test/api@"+md0Digest("a")),
		md1Deployment("fugue-api", "api", "registry.test/api@"+targetDigest),
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		[]BuildArtifact{{Name: "api", SourceBaseCommit: md0BaseCommit, ArtifactDigest: targetDigest, ProvenanceDigest: md0Digest("f")}},
	)

	tests := []struct {
		name   string
		mutate func(ImageActivationPlanInput) ImageActivationPlanInput
	}{
		{name: "manifest digest drift", mutate: func(input ImageActivationPlanInput) ImageActivationPlanInput {
			input.TargetManifest = append(append([]byte(nil), input.TargetManifest...), '\n')
			return input
		}},
		{name: "ambiguous artifact digest", mutate: func(input ImageActivationPlanInput) ImageActivationPlanInput {
			input.BuildPlan.Artifacts = append(input.BuildPlan.Artifacts, BuildArtifact{
				Name: "api-copy", SourceBaseCommit: md0BaseCommit,
				ArtifactDigest: targetDigest, ProvenanceDigest: md0Digest("e"),
			})
			input.BuildPlan.Artifacts = canonicalBuildArtifacts(input.BuildPlan.Artifacts)
			input.BuildPlan.Digest = buildArtifactPlanDigest(input.BuildPlan)
			return input
		}},
		{name: "unverified target image", mutate: func(input ImageActivationPlanInput) ImageActivationPlanInput {
			input.BuildPlan.Artifacts = []BuildArtifact{{
				Name: "edge", SourceBaseCommit: md0BaseCommit,
				ArtifactDigest: md0Digest("e"), ProvenanceDigest: md0Digest("f"),
			}}
			input.BuildPlan.Digest = buildArtifactPlanDigest(input.BuildPlan)
			return input
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := BuildImageActivationPlanFromManifests(test.mutate(valid)); err == nil {
				t.Fatal("incomplete evidence unexpectedly produced an activation plan")
			}
		})
	}
}

func TestBuildImageActivationPlanKeepsAbsentCreateOutOfImageReplacement(t *testing.T) {
	targetDigest := md0Digest("d")
	input := md1ActivationFixture(
		t,
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: preserved\n  namespace: fugue-system\ndata:\n  value: stable\n",
		md1Deployment("fugue-api", "api", "registry.test/api@"+targetDigest),
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		[]BuildArtifact{{Name: "api", SourceBaseCommit: md0BaseCommit, ArtifactDigest: targetDigest, ProvenanceDigest: md0Digest("f")}},
	)
	if _, err := BuildImageActivationPlanFromManifests(input); err == nil || !strings.Contains(err.Error(), "absent-create") {
		t.Fatalf("absent-create boundary was not enforced: %v", err)
	}
}

func TestDisabledDynamicPublicEdgeWorkerImageOnlyTargetStaysBuiltOnly(t *testing.T) {
	input := disabledDynamicWorkerActivationInput(t, "dynamic", DomainAuthoritativeDNS)
	observed := input.ObservedLiveManifest
	input.ObservedLiveManifest = nil

	withoutObserved, withoutEvidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(withoutObserved.Activations) != 0 || withoutEvidence.Complete ||
		len(withoutEvidence.Unresolved) != 1 ||
		withoutEvidence.Unresolved[0].Workload.Container != "edge" ||
		withoutEvidence.Unresolved[0].Reason != ImageActivationGapArtifactNotBuilt {
		t.Fatalf("production regression was not reproduced: plan=%#v evidence=%#v", withoutObserved, withoutEvidence)
	}

	if !bytes.Contains(observed, []byte(DisabledPublicEdgeWorkerObservationAnnotation)) {
		t.Fatalf("observed witness is missing disabled-worker reverse evidence:\n%s", observed)
	}
	input.ObservedLiveManifest = observed
	plan, evidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Activations) != 0 || !evidence.Complete || len(evidence.Unresolved) != 0 ||
		!reflect.DeepEqual(evidence.BuiltOnlyArtifacts, []string{"edge"}) {
		t.Fatalf("disabled worker image drift was not kept built-only: plan=%#v evidence=%#v", plan, evidence)
	}
}

func TestDisabledDynamicPublicEdgeWorkerRejectsSplitEdgeImagePointers(t *testing.T) {
	name := "fugue-fugue-edge-dynamic-worker-b"
	target := disabledDynamicWorkerDaemonSet(name, "registry.example/edge@"+md0Digest("9"), "dynamic")
	target = strings.Replace(target, "registry.example/edge@"+md0Digest("9"), "registry.example/edge@"+md0Digest("8"), 1)
	input := disabledDynamicWorkerActivationInputForTarget(t, target, DomainAuthoritativeDNS)
	if _, _, err := BuildImageActivationReportFromManifests(input); err == nil ||
		!strings.Contains(err.Error(), "main and identity init image pointers are not bound") {
		t.Fatalf("split main/init Edge image pointers were accepted: %v", err)
	}
}

func TestPublicEdgeImageActivationBindsMainAndIdentityInitToOneArtifact(t *testing.T) {
	name := "fugue-fugue-edge-worker-b"
	baseDigest := md0Digest("8")
	targetDigest := md0Digest("9")
	input := md1ActivationFixture(
		t,
		disabledDynamicWorkerDaemonSet(name, "registry.example/edge@"+baseDigest, "dynamic"),
		disabledDynamicWorkerDaemonSet(name, "registry.example/edge@"+targetDigest, "dynamic"),
		[]md1OwnershipRule{{name: name, domain: DomainAuthoritativeDNS, kind: "DaemonSet"}},
		[]BuildArtifact{{
			Name: "edge", SourceBaseCommit: md0BaseCommit, ArtifactDigest: targetDigest,
			ProvenanceDigest: md0Digest("1"), PublishedImageRef: "registry.example/edge@" + targetDigest,
		}},
	)
	plan, err := BuildImageActivationPlanFromManifests(input)
	if err != nil {
		t.Fatalf("build public Edge activation plan: %v", err)
	}
	if len(plan.Activations) != 1 {
		t.Fatalf("public Edge activation count = %d, want one logical artifact: %#v", len(plan.Activations), plan.Activations)
	}
	activation := plan.Activations[0]
	if activation.ArtifactName != "edge" || activation.ArtifactDigest != targetDigest ||
		activation.TargetImageRef != "registry.example/edge@"+targetDigest || activation.Workload.Container != "edge" {
		t.Fatalf("public Edge activation is not one immutable logical artifact: %#v", activation)
	}
}

func TestPublicEdgeIdentityInitMigrationDoesNotCreateImageActivation(t *testing.T) {
	name := "fugue-fugue-edge-worker-b"
	image := "registry.example/edge@" + md0Digest("8")
	target := disabledDynamicWorkerDaemonSet(name, image, "dynamic")
	base := strings.Replace(target, fmt.Sprintf(`      initContainers:
        - name: edge-workload-identity
          image: %s
`, image), "", 1)
	if base == target {
		t.Fatal("old Edge fixture still contains the identity init")
	}
	input := md1ActivationFixture(
		t, base, target,
		[]md1OwnershipRule{{name: name, domain: DomainAuthoritativeDNS, kind: "DaemonSet"}},
		nil,
	)
	plan, evidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatalf("plan old-to-new Edge identity migration: %v", err)
	}
	if len(plan.Activations) != 0 || !evidence.Complete || len(evidence.Unresolved) != 0 || len(evidence.BuiltOnlyArtifacts) != 0 {
		t.Fatalf("identity-only Edge migration became an image activation: plan=%#v evidence=%#v", plan, evidence)
	}
}

func disabledDynamicWorkerActivationInput(t *testing.T, targetNodeClass string, domain Domain) ImageActivationPlanInput {
	t.Helper()
	name := "fugue-fugue-edge-dynamic-worker-b"
	return disabledDynamicWorkerActivationInputForTarget(
		t, disabledDynamicWorkerDaemonSet(name, "registry.example/edge@"+md0Digest("9"), targetNodeClass), domain,
	)
}

func disabledDynamicWorkerActivationInputForTarget(t *testing.T, target string, domain Domain) ImageActivationPlanInput {
	t.Helper()
	name := "fugue-fugue-edge-dynamic-worker-b"
	builtDigest := md0Digest("b")
	input := md1ActivationFixture(
		t, disabledDynamicWorkerDaemonSet(name, "registry.example/edge:live", "dynamic"), target,
		[]md1OwnershipRule{{name: name, domain: domain, kind: "DaemonSet"}},
		[]BuildArtifact{{
			Name: "edge", SourceBaseCommit: md0BaseCommit, ArtifactDigest: builtDigest,
			ProvenanceDigest: md0Digest("1"), PublishedImageRef: "registry.example/edge@" + builtDigest,
		}},
	)
	liveWitness := disabledDynamicWorkerLiveWitness(t, name, "registry.example/edge:live")
	var err error
	input.ObservedLiveManifest, err = MaterializeObservedLiveImageManifest(
		input.BaseManifest, liveWitness, input.Ownership, "fugue-system",
	)
	if err != nil {
		t.Fatal(err)
	}
	return input
}

func disabledDynamicWorkerLiveWitness(t *testing.T, name, image string) []byte {
	t.Helper()
	liveObject := disabledDynamicWorkerKubernetesObject(t, name, image)
	liveJSON, err := json.Marshal(liveObject)
	if err != nil {
		t.Fatal(err)
	}
	marker, candidate, err := CaptureDisabledPublicEdgeWorkerObservation(liveJSON, "fugue-system")
	if err != nil || !candidate {
		t.Fatalf("capture disabled worker observation: candidate=%v err=%v", candidate, err)
	}
	delete(liveObject, "status")
	metadata := liveObject["metadata"].(map[string]any)
	delete(metadata, "uid")
	delete(metadata, "resourceVersion")
	delete(metadata, "generation")
	metadata["annotations"] = map[string]any{DisabledPublicEdgeWorkerObservationAnnotation: marker}
	liveWitness, err := json.Marshal(liveObject)
	if err != nil {
		t.Fatal(err)
	}
	return liveWitness
}

func disabledDynamicWorkerDaemonSet(name, image, nodeClass string) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: %s
  namespace: fugue-system
  labels:
    app.kubernetes.io/instance: fugue
    app.kubernetes.io/component: edge-dynamic-worker-b
    fugue.io/rollout-subsystem: public-data-plane
    fugue.io/rollout-mode: node-local-blue-green-worker
    fugue.io/downtime-class: online-required
    fugue.io/edge-slot: b
spec:
  revisionHistoryLimit: 2
  updateStrategy:
    type: OnDelete
  selector:
    matchLabels:
      app: edge-dynamic-worker-b
  template:
    metadata:
      labels:
        app: edge-dynamic-worker-b
    spec:
      nodeSelector:
        fugue.io/edge-workload: %s
      initContainers:
        - name: edge-workload-identity
          image: %s
      containers:
        - name: edge
          image: %s
          env:
            - name: FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID
              value: ""
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
        - name: caddy
          image: caddy:2.10.2-alpine
`, name, nodeClass, image, image)
}

func disabledDynamicWorkerKubernetesObject(t *testing.T, name, image string) map[string]any {
	t.Helper()
	return map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{
			"name": name, "namespace": "fugue-system",
			"uid": "7a8f8d52-1b9f-4dfa-bac0-8a9f1e043073", "resourceVersion": "4815162342",
			"generation": float64(17),
			"labels": map[string]any{
				"app.kubernetes.io/instance": "fugue", "app.kubernetes.io/component": "edge-dynamic-worker-b",
				"fugue.io/rollout-subsystem": "public-data-plane", "fugue.io/rollout-mode": "node-local-blue-green-worker",
				"fugue.io/downtime-class": "online-required", "fugue.io/edge-slot": "b",
			},
		},
		"spec": map[string]any{
			"revisionHistoryLimit": float64(2), "updateStrategy": map[string]any{"type": "OnDelete"},
			"selector": map[string]any{"matchLabels": map[string]any{"app": "edge-dynamic-worker-b"}},
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{"app": "edge-dynamic-worker-b"}, "creationTimestamp": nil,
				},
				"spec": map[string]any{
					"nodeSelector": map[string]any{"fugue.io/edge-workload": "dynamic"},
					"dnsPolicy":    "ClusterFirst", "restartPolicy": "Always", "schedulerName": "default-scheduler",
					"securityContext": map[string]any{}, "terminationGracePeriodSeconds": float64(30),
					"enableServiceLinks": true, "serviceAccountName": "default",
					"initContainers": []any{map[string]any{
						"name": publicDataPlaneEdgeIdentityContainer, "image": image,
						"terminationMessagePath": "/dev/termination-log", "terminationMessagePolicy": "File",
						"resources": map[string]any{},
					}},
					"containers": []any{map[string]any{
						"name": "edge", "image": image, "terminationMessagePath": "/dev/termination-log",
						"terminationMessagePolicy": "File", "resources": map[string]any{},
						"env": []any{
							map[string]any{"name": "FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID"},
							map[string]any{"name": "POD_NAME", "valueFrom": map[string]any{
								"fieldRef": map[string]any{"apiVersion": "v1", "fieldPath": "metadata.name"},
							}},
						},
					}, map[string]any{
						"name": "caddy", "image": "caddy:2.10.2-alpine", "terminationMessagePath": "/dev/termination-log",
						"terminationMessagePolicy": "File", "resources": map[string]any{},
					}},
				},
			},
		},
		"status": map[string]any{
			"observedGeneration": float64(17),
			// Kubernetes omits zero-valued DaemonSet counters from JSON.
		},
	}
}

func TestBuildImageActivationReportPreservesAbsentNonImmutableTarget(t *testing.T) {
	input := md1ActivationFixture(
		t,
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: preserved\n  namespace: fugue-system\ndata:\n  value: stable\n",
		md1Deployment("fugue-api", "api", "registry.test/api:unreleased"),
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		[]BuildArtifact{{Name: "api", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("d"), ProvenanceDigest: md0Digest("f")}},
	)

	activation, evidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatalf("derive report for absent non-immutable target: %v", err)
	}
	if len(activation.Activations) != 0 || evidence.Complete || len(evidence.Unresolved) != 1 {
		t.Fatalf("absent non-immutable target was hidden or promoted: plan=%#v evidence=%#v", activation, evidence)
	}
	gap := evidence.Unresolved[0]
	if gap.Reason != ImageActivationGapTargetNotImmutable || gap.LiveImageRef != "" ||
		gap.ReverseRenderedDigest != "" || gap.ArtifactDigest != "" ||
		len(gap.MatchingBuildArtifacts) != 0 {
		t.Fatalf("absent non-immutable target evidence drifted: %#v", gap)
	}
}

type md1OwnershipRule struct {
	name   string
	domain Domain
	kind   string
}

func md1ActivationFixture(t *testing.T, baseRaw, targetRaw string, rules []md1OwnershipRule, artifacts []BuildArtifact) ImageActivationPlanInput {
	t.Helper()
	ownership := md1Ownership(rules)
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		t.Fatalf("load ownership: %v", err)
	}
	base, err := CanonicalizeRenderedManifest([]byte(baseRaw), spec, "fugue-system")
	if err != nil {
		t.Fatalf("canonicalize base: %v", err)
	}
	target, err := CanonicalizeRenderedManifest([]byte(targetRaw), spec, "fugue-system")
	if err != nil {
		t.Fatalf("canonicalize target: %v", err)
	}
	context, err := NewClassificationContextEvidence(
		"fugue-system",
		map[string]string{"releaseNamespace": "fugue-system"},
		false,
	)
	if err != nil {
		t.Fatalf("classification context: %v", err)
	}
	changedDigest := md0Digest("f")
	build, err := NewBuildArtifactPlan(md0BaseCommit, md0TargetCommit, changedDigest, artifacts)
	if err != nil {
		t.Fatalf("build artifact plan: %v", err)
	}
	rendered := ClassifyRendered(base, target, spec, RenderedOptions{
		DefaultNamespace: "fugue-system",
		Bindings:         context.BindingMap(),
	})
	plan := BuildPlan(PlanInput{
		Files:    FileClassification{Domains: []Domain{}, Evidence: []Evidence{}},
		Rendered: rendered,
		Digests: DigestEvidence{
			Base: md0Digest("1"), Target: md0Digest("2"), Live: md0Digest("1"),
			BaseManifest: digestBytesSHA256(base), TargetManifest: digestBytesSHA256(target),
			RepeatedTargetManifest: digestBytesSHA256(target), Ownership: digestBytesSHA256(ownership),
			ChangedFiles: changedDigest, ClassificationContext: context,
		},
	})
	return ImageActivationPlanInput{
		BuildPlan: build, ReleasePlan: plan, Ownership: ownership,
		BaseManifest: base, TargetManifest: target,
	}
}

func md1Ownership(rules []md1OwnershipRule) []byte {
	var result strings.Builder
	result.WriteString("apiVersion: release-domain.fugue.dev/v1\nkind: ReleaseDomainOwnership\ndomains:\n")
	for _, domain := range KnownDomains() {
		fmt.Fprintf(&result, "  - %s\n", domain)
	}
	result.WriteString("requiredBindings:\n  - releaseNamespace\nfileRules: []\nvalueRules: []\nobjectRules:\n")
	for index, rule := range rules {
		kind := rule.kind
		if kind == "" {
			kind = "Deployment"
		}
		fmt.Fprintf(&result, "  - id: workload-%d\n    domain: %s\n    apiGroup: apps\n    version: v1\n    kind: %s\n    scope: Namespaced\n    namespace: ${releaseNamespace}\n    name: %s\n", index, rule.domain, kind, rule.name)
	}
	return []byte(result.String())
}

func md1Deployment(name, container, image string) string {
	return fmt.Sprintf("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: %s\n  namespace: fugue-system\nspec:\n  selector:\n    matchLabels:\n      app: %s\n  template:\n    metadata:\n      labels:\n        app: %s\n    spec:\n      containers:\n        - name: %s\n          image: %s\n", name, name, name, container, image)
}

func md1DaemonSet(name, container, image string) string {
	return fmt.Sprintf("apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: %s\n  namespace: fugue-system\nspec:\n  selector:\n    matchLabels:\n      app: %s\n  template:\n    metadata:\n      labels:\n        app: %s\n    spec:\n      containers:\n        - name: %s\n          image: %s\n", name, name, name, container, image)
}

func md1LabeledDeployment(name, container, component, image string) string {
	return fmt.Sprintf("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: %s\n  namespace: fugue-system\n  labels:\n    app.kubernetes.io/component: %s\nspec:\n  selector:\n    matchLabels:\n      app: %s\n  template:\n    metadata:\n      labels:\n        app: %s\n    spec:\n      containers:\n        - name: %s\n          image: %s\n", name, component, name, name, container, image)
}
