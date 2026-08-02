package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"fugue/internal/releasedomain"
)

type fixedHotfixRuntime struct {
	observations []releasedomain.ControlPlaneHotfixObservation
	observed     int
	acquired     int
	forwarded    int
	released     int
}

func (runtime *fixedHotfixRuntime) Observe(context.Context) (releasedomain.ControlPlaneHotfixObservation, error) {
	if runtime.observed >= len(runtime.observations) {
		return releasedomain.ControlPlaneHotfixObservation{}, errors.New("unexpected observation")
	}
	value := runtime.observations[runtime.observed]
	runtime.observed++
	return value, nil
}

func (runtime *fixedHotfixRuntime) AcquireLease(context.Context, releasedomain.ControlPlaneHotfixAdoptionPlan) error {
	runtime.acquired++
	return nil
}

func (*fixedHotfixRuntime) VerifyLease(context.Context, releasedomain.ControlPlaneHotfixAdoptionPlan) error {
	return nil
}

func (*fixedHotfixRuntime) PersistWAL(context.Context, releasedomain.ControlPlaneHotfixAdoptionWAL) error {
	return nil
}

func (runtime *fixedHotfixRuntime) Forward(context.Context, releasedomain.ControlPlaneHotfixAdoptionPlan) (releasedomain.ControlPlaneHotfixCommitResult, error) {
	runtime.forwarded++
	return releasedomain.ControlPlaneHotfixCommitAcknowledged, nil
}

func (*fixedHotfixRuntime) Compensate(context.Context, releasedomain.ControlPlaneHotfixAdoptionPlan) (releasedomain.ControlPlaneHotfixCommitResult, error) {
	return releasedomain.ControlPlaneHotfixCommitRejected, errors.New("unexpected compensation")
}

func (runtime *fixedHotfixRuntime) ReleaseLease(context.Context, releasedomain.ControlPlaneHotfixAdoptionPlan) error {
	runtime.released++
	return nil
}

func TestRunReadsOnlyCanonicalPlanAndUsesTheInjectedRuntime(t *testing.T) {
	t.Parallel()

	plan := commandTestPlan()
	base := commandObservation(plan, "base")
	target := commandObservation(plan, "target")
	runtime := &fixedHotfixRuntime{observations: []releasedomain.ControlPlaneHotfixObservation{base, base, target}}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	factoryCalls := 0
	exitCode := run(nil, bytes.NewReader(raw), &stdout, &stderr, "false", func(got releasedomain.ControlPlaneHotfixAdoptionPlan) (releasedomain.ControlPlaneHotfixRuntime, error) {
		factoryCalls++
		if got.Digest != plan.Digest {
			t.Fatalf("runtime received the wrong plan: %+v", got)
		}
		return runtime, nil
	})
	if exitCode != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"status":"verified"`) {
		t.Fatalf("run failed: exit=%d stdout=%q stderr=%q", exitCode, stdout.String(), stderr.String())
	}
	if factoryCalls != 1 || runtime.acquired != 1 || runtime.forwarded != 1 || runtime.released != 1 {
		t.Fatalf("fixed runtime contract was not followed: factory=%d runtime=%+v", factoryCalls, runtime)
	}
}

func TestRunRejectsArgumentsNonCanonicalJSONAndMissingRuntime(t *testing.T) {
	t.Parallel()

	plan := commandTestPlan()
	canonical, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	pretty, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		args    []string
		input   []byte
		factory runtimeFactory
	}{
		{name: "argv", args: []string{"--plan", "arbitrary"}, input: canonical, factory: func(releasedomain.ControlPlaneHotfixAdoptionPlan) (releasedomain.ControlPlaneHotfixRuntime, error) {
			t.Fatal("factory called")
			return nil, nil
		}},
		{name: "noncanonical", input: pretty, factory: func(releasedomain.ControlPlaneHotfixAdoptionPlan) (releasedomain.ControlPlaneHotfixRuntime, error) {
			t.Fatal("factory called")
			return nil, nil
		}},
		{name: "missing runtime", input: canonical, factory: func(releasedomain.ControlPlaneHotfixAdoptionPlan) (releasedomain.ControlPlaneHotfixRuntime, error) {
			return nil, errors.New("not injected")
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			if exitCode := run(test.args, bytes.NewReader(test.input), &stdout, &stderr, "false", test.factory); exitCode == 0 || stderr.Len() == 0 {
				t.Fatalf("unsafe invocation succeeded: stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
}

func commandTestPlan() releasedomain.ControlPlaneHotfixAdoptionPlan {
	digest := "sha256:" + strings.Repeat("a", 64)
	plan := releasedomain.ControlPlaneHotfixAdoptionPlan{
		APIVersion:  releasedomain.ControlPlaneHotfixAdoptionAPIVersion,
		Kind:        releasedomain.ControlPlaneHotfixAdoptionPlanKind,
		Policy:      releasedomain.ControlPlaneHotfixAdoptionPolicy,
		ExpectedSHA: strings.Repeat("1", 40), RunID: "30733955954", RunAttempt: 1,
		Namespace: "fugue-system", ReleaseName: "fugue", ReleaseFullname: "fugue-fugue",
		BaseRevision: 806, TargetRevision: 807, BaseStatus: "deployed",
		HelmRecordDigest: digest, BaseValuesDigest: digest,
		TargetValuesDigest: "sha256:" + strings.Repeat("e", 64), ChartTreeDigest: digest,
		BaseManifestDigest:      "sha256:" + strings.Repeat("b", 64),
		TargetManifestDigest:    "sha256:" + strings.Repeat("c", 64),
		HybridManifestDigest:    "sha256:" + strings.Repeat("d", 64),
		TargetAPITemplateDigest: "sha256:" + strings.Repeat("6", 64),
		HybridAPITemplateDigest: "sha256:" + strings.Repeat("7", 64),
		CurrentSource:           strings.Repeat("2", 40), AdoptedSource: strings.Repeat("3", 40),
		LiveImageRef: "ghcr.io/example/fugue-api@" + digest,
		Fence:        "fence-token-1234567890", Nonce: "nonce-token-1234567890",
		Provenance: releasedomain.ControlPlaneHotfixProvenance{
			BuildRunID: "30733955954", BuildRunAttempt: 1,
			ArtifactName: "fugue-api-artifact", ArtifactDigest: digest,
			Repository: "ghcr.io/example/fugue-api", IndexDigest: digest,
			PlatformManifestDigest: digest, ConfigDigest: digest,
			OCIRevision: strings.Repeat("3", 40), Verified: true,
		},
		Kubernetes: releasedomain.ControlPlaneHotfixKubernetesEvidence{
			APIName: "fugue-fugue-api", APIUID: "api-uid", APIResourceVersion: "100",
			APIGeneration: 9, APIObservedGeneration: 9, APITemplateDigest: digest,
			APIImageRef: "ghcr.io/example/fugue-api@" + digest,
			APIImageID:  "containerd://" + strings.Repeat("f", 64), APIHealthDigest: digest,
			APIReplicas: 2, APIReady: 2, APIUpdated: 2, APIAvailable: 2,
			ServiceName: "fugue-fugue", ServiceUID: "service-uid", ServiceResourceVersion: "200", ServiceSelectorDigest: digest,
			EndpointSliceName: "fugue-fugue-abc", EndpointSliceUID: "slice-uid", EndpointSliceResourceVersion: "300",
			EndpointServiceName: "fugue-fugue", EndpointBindingDigest: digest, ReadyServingEndpoints: 2,
		},
		Lease: releasedomain.ControlPlaneHotfixLeaseEvidence{
			Namespace: "fugue-system", Name: "fugue-fugue-control-plane-db-backup", UID: "lease-uid", ResourceVersion: "400",
		},
	}
	plan.Digest = commandPlanDigest(plan)
	return plan
}

func commandObservation(plan releasedomain.ControlPlaneHotfixAdoptionPlan, phase string) releasedomain.ControlPlaneHotfixObservation {
	observation := releasedomain.ControlPlaneHotfixObservation{
		HelmRevision: plan.BaseRevision, HelmStatus: plan.BaseStatus,
		HelmRecordDigest: plan.HelmRecordDigest,
		ManifestDigest:   plan.BaseManifestDigest, ValuesDigest: plan.BaseValuesDigest,
		ChartTreeDigest: plan.ChartTreeDigest, Source: plan.CurrentSource,
		LiveImageRef: plan.LiveImageRef, APIImageID: plan.Kubernetes.APIImageID,
		Kubernetes: plan.Kubernetes, APIHealthStatus: 200, APIHealthDigest: plan.Kubernetes.APIHealthDigest,
	}
	if phase == "target" {
		observation.HelmRevision = plan.TargetRevision
		observation.ManifestDigest = plan.TargetManifestDigest
		observation.ValuesDigest = plan.TargetValuesDigest
		observation.Source = plan.AdoptedSource
		observation.Kubernetes.APIGeneration++
		observation.Kubernetes.APIObservedGeneration++
		observation.Kubernetes.APITemplateDigest = plan.TargetAPITemplateDigest
		observation.Kubernetes.APIResourceVersion = "101"
		observation.Kubernetes.EndpointSliceResourceVersion = "301"
	}
	return observation
}

func commandPlanDigest(plan releasedomain.ControlPlaneHotfixAdoptionPlan) string {
	plan.Digest = ""
	data, _ := json.Marshal(plan)
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}
