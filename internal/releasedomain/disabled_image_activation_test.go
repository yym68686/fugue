package releasedomain

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestCaptureDisabledPublicEdgeWorkerObservationRequiresConvergedZeroState(t *testing.T) {
	valid := disabledDynamicWorkerKubernetesObject(
		t, "fugue-fugue-edge-dynamic-worker-b", "registry.example/edge:live",
	)
	encoded := encodeDisabledWorkerFixture(t, valid)
	marker, candidate, err := CaptureDisabledPublicEdgeWorkerObservation(encoded, "fugue-system")
	if err != nil || !candidate || marker == "" {
		t.Fatalf("valid zero-state observation: marker=%q candidate=%v err=%v", marker, candidate, err)
	}

	for _, field := range []string{
		"desiredNumberScheduled", "currentNumberScheduled", "numberReady",
		"numberAvailable", "updatedNumberScheduled", "numberUnavailable", "numberMisscheduled",
	} {
		t.Run(field+" nonzero", func(t *testing.T) {
			object := cloneDisabledWorkerFixture(t, valid)
			object["status"].(map[string]any)[field] = float64(1)
			if _, candidate, err := CaptureDisabledPublicEdgeWorkerObservation(
				encodeDisabledWorkerFixture(t, object), "fugue-system",
			); err == nil || !candidate {
				t.Fatalf("nonzero %s did not fail closed: candidate=%v err=%v", field, candidate, err)
			}
		})
	}

	tests := map[string]func(map[string]any){
		"status missing": func(object map[string]any) { delete(object, "status") },
		"observed generation missing": func(object map[string]any) {
			delete(object["status"].(map[string]any), "observedGeneration")
		},
		"generation mismatch": func(object map[string]any) {
			object["status"].(map[string]any)["observedGeneration"] = float64(16)
		},
		"deleting": func(object map[string]any) {
			object["metadata"].(map[string]any)["deletionTimestamp"] = "2026-08-01T00:00:00Z"
		},
		"negative zero field": func(object map[string]any) {
			object["status"].(map[string]any)["numberReady"] = float64(-1)
		},
		"fractional zero field": func(object map[string]any) {
			object["status"].(map[string]any)["numberReady"] = 0.5
		},
		"server annotation drift": func(object map[string]any) {
			object["metadata"].(map[string]any)["annotations"] = map[string]any{
				"meta.helm.sh/release-name": "other",
			}
		},
		"runtime annotation pair missing": func(object map[string]any) {
			template := object["spec"].(map[string]any)["template"].(map[string]any)
			template["metadata"].(map[string]any)["annotations"] = map[string]any{
				"fugue.io/public-data-plane-release-id": "pdp-test",
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			object := cloneDisabledWorkerFixture(t, valid)
			mutate(object)
			if _, candidate, err := CaptureDisabledPublicEdgeWorkerObservation(
				encodeDisabledWorkerFixture(t, object), "fugue-system",
			); err == nil || !candidate {
				t.Fatalf("unproven zero state did not fail closed: candidate=%v err=%v", candidate, err)
			}
		})
	}

	wrongIdentity := cloneDisabledWorkerFixture(t, valid)
	wrongIdentity["metadata"].(map[string]any)["labels"].(map[string]any)["fugue.io/edge-slot"] = "a"
	if marker, candidate, err := CaptureDisabledPublicEdgeWorkerObservation(
		encodeDisabledWorkerFixture(t, wrongIdentity), "fugue-system",
	); err != nil || candidate || marker != "" {
		t.Fatalf("wrong identity was treated as a disabled candidate: marker=%q candidate=%v err=%v", marker, candidate, err)
	}
	ordinaryZeroDaemonSet := cloneDisabledWorkerFixture(t, valid)
	ordinaryMetadata := ordinaryZeroDaemonSet["metadata"].(map[string]any)
	ordinaryMetadata["name"] = "fugue-fugue-edge-worker-b"
	ordinaryMetadata["labels"].(map[string]any)["app.kubernetes.io/component"] = "edge-worker-b"
	if marker, candidate, err := CaptureDisabledPublicEdgeWorkerObservation(
		encodeDisabledWorkerFixture(t, ordinaryZeroDaemonSet), "fugue-system",
	); err != nil || candidate || marker != "" {
		t.Fatalf("ordinary zero DaemonSet was generalized into the exception: marker=%q candidate=%v err=%v", marker, candidate, err)
	}
}

func TestDisabledPublicEdgeWorkerActivationSkipFailsClosed(t *testing.T) {
	name := "fugue-fugue-edge-dynamic-worker-b"
	tests := []struct {
		name   string
		input  func(*testing.T) ImageActivationPlanInput
		reason string
	}{
		{
			name: "target changes scheduling",
			input: func(t *testing.T) ImageActivationPlanInput {
				return disabledDynamicWorkerActivationInput(t, "enabled-class", DomainAuthoritativeDNS)
			},
			reason: ImageActivationGapArtifactNotBuilt,
		},
		{
			name: "ownership is not authoritative dns",
			input: func(t *testing.T) ImageActivationPlanInput {
				return disabledDynamicWorkerActivationInput(t, "dynamic", DomainControlPlane)
			},
			reason: ImageActivationGapArtifactNotBuilt,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := test.input(t)
			plan, evidence, err := BuildImageActivationReportFromManifests(input)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Activations) != 0 || evidence.Complete || len(evidence.Unresolved) != 1 ||
				evidence.Unresolved[0].Reason != test.reason {
				t.Fatalf("unsafe disabled-worker skip: plan=%#v evidence=%#v", plan, evidence)
			}
		})
	}

	t.Run("caddy image drifts", func(t *testing.T) {
		target := disabledDynamicWorkerDaemonSet(name, "registry.example/edge@"+md0Digest("9"), "dynamic")
		target = strings.Replace(target, "caddy:2.10.2-alpine", "caddy:2.10.3-alpine", 1)
		input := disabledDynamicWorkerActivationInputForTarget(t, target, DomainAuthoritativeDNS)
		plan, evidence, err := BuildImageActivationReportFromManifests(input)
		if err != nil {
			t.Fatal(err)
		}
		gaps := map[string]string{}
		for _, gap := range evidence.Unresolved {
			gaps[gap.Workload.Container] = gap.Reason
		}
		if len(plan.Activations) != 0 || evidence.Complete || len(evidence.Unresolved) != 2 ||
			gaps["edge"] != ImageActivationGapArtifactNotBuilt || gaps["caddy"] != ImageActivationGapTargetNotImmutable {
			t.Fatalf("caddy image drift was hidden: plan=%#v evidence=%#v", plan, evidence)
		}
	})

	t.Run("additional container pointer", func(t *testing.T) {
		target := disabledDynamicWorkerDaemonSet(name, "registry.example/edge@"+md0Digest("9"), "dynamic")
		target = strings.Replace(target, "        - name: caddy\n", "        - name: metrics\n          image: metrics:1\n        - name: caddy\n", 1)
		input := disabledDynamicWorkerActivationInputForTarget(t, target, DomainAuthoritativeDNS)
		if _, _, err := BuildImageActivationReportFromManifests(input); err == nil ||
			!strings.Contains(err.Error(), "exactly the edge and caddy containers") {
			t.Fatalf("additional owned image pointer did not fail closed: %v", err)
		}
	})
}

func TestDisabledPublicEdgeWorkerWitnessRejectsLiveOrMarkerDrift(t *testing.T) {
	input := disabledDynamicWorkerActivationInput(t, "dynamic", DomainAuthoritativeDNS)

	driftedLive := disabledDynamicWorkerKubernetesObject(
		t, "fugue-fugue-edge-dynamic-worker-b", "registry.example/edge:live",
	)
	liveJSON := encodeDisabledWorkerFixture(t, driftedLive)
	marker, candidate, err := CaptureDisabledPublicEdgeWorkerObservation(liveJSON, "fugue-system")
	if err != nil || !candidate {
		t.Fatalf("capture fixture: candidate=%v err=%v", candidate, err)
	}
	delete(driftedLive, "status")
	metadata := driftedLive["metadata"].(map[string]any)
	delete(metadata, "uid")
	delete(metadata, "resourceVersion")
	delete(metadata, "generation")
	metadata["annotations"] = map[string]any{DisabledPublicEdgeWorkerObservationAnnotation: marker}
	driftedLive["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["nodeSelector"] =
		map[string]any{"fugue.io/edge-workload": "other"}
	if _, err := MaterializeObservedLiveImageManifest(
		input.BaseManifest, encodeDisabledWorkerFixture(t, driftedLive), input.Ownership, "fugue-system",
	); err == nil || !strings.Contains(err.Error(), "differs from the Helm base") {
		t.Fatalf("live scheduling drift was accepted: %v", err)
	}
	var caddyDrift map[string]any
	if err := json.Unmarshal(disabledDynamicWorkerLiveWitness(
		t, "fugue-fugue-edge-dynamic-worker-b", "registry.example/edge:live",
	), &caddyDrift); err != nil {
		t.Fatal(err)
	}
	caddyContainers := caddyDrift["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
	caddyContainers[1].(map[string]any)["image"] = "caddy:2.10.3-alpine"
	if _, err := MaterializeObservedLiveImageManifest(
		input.BaseManifest, encodeDisabledWorkerFixture(t, caddyDrift), input.Ownership, "fugue-system",
	); err == nil || !strings.Contains(err.Error(), "/spec/template/spec/containers/1/image") {
		t.Fatalf("live caddy image drift was accepted: %v", err)
	}
	fractionalDefault := cloneDisabledWorkerFixture(t, driftedLive)
	fractionalSpec := fractionalDefault["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	fractionalSpec["nodeSelector"] = map[string]any{"fugue.io/edge-workload": "dynamic"}
	fractionalSpec["terminationGracePeriodSeconds"] = 30.5
	if _, err := MaterializeObservedLiveImageManifest(
		input.BaseManifest, encodeDisabledWorkerFixture(t, fractionalDefault), input.Ownership, "fugue-system",
	); err == nil || !strings.Contains(err.Error(), "terminationGracePeriodSeconds") {
		t.Fatalf("fractional API default was accepted: %v", err)
	}

	var documents []byte
	documents = append(documents, input.ObservedLiveManifest...)
	documents = bytes.Replace(documents, []byte(`"numberReady":0`), []byte(`"numberReady":1`), 1)
	if err := VerifyObservedLiveImageManifest(
		input.BaseManifest, documents, input.Ownership, "fugue-system",
	); err == nil {
		t.Fatal("tampered disabled-worker marker was accepted")
	}
}

func encodeDisabledWorkerFixture(t *testing.T, object map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func cloneDisabledWorkerFixture(t *testing.T, object map[string]any) map[string]any {
	t.Helper()
	var clone map[string]any
	if err := json.Unmarshal(encodeDisabledWorkerFixture(t, object), &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}
