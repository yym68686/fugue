package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

func TestConfigValidationAcceptsOnlyTheExactAPIProjection(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	const source = "d1e7ed9cdedbaa09db9bd78b4e433b94c7357510"
	const image = "ghcr.io/example/fugue-api@sha256:410a1c75efe1fe9dd51dd83e32d535d548ab4471281223be7a8bc6b7297ae9d8"
	const secret = "fugue-fugue-edge-activation-signing-v1"
	base := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: fugue-fugue-api
  namespace: fugue-system
spec:
  template:
    metadata:
      annotations:
        fugue.pro/source-commit: ` + source + `
    spec:
      containers:
        - name: api
          image: ` + image + `
          env:
            - name: EXISTING
              value: preserved
          volumeMounts:
            - name: data
              mountPath: /var/lib/fugue
      volumes:
        - name: data
          emptyDir: {}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: preserved
  namespace: fugue-system
data:
  value: exact
`
	target := strings.Replace(base, `          env:
            - name: EXISTING
              value: preserved
          volumeMounts:
            - name: data
              mountPath: /var/lib/fugue
      volumes:
        - name: data
          emptyDir: {}
`, `          env:
            - name: EXISTING
              value: preserved
            - name: FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_PROJECTION_DIR
              value: /var/run/secrets/fugue-edge-activation
          volumeMounts:
            - name: data
              mountPath: /var/lib/fugue
            - name: edge-activation-plan-signing-key
              mountPath: /var/run/secrets/fugue-edge-activation
              readOnly: true
      volumes:
        - name: data
          emptyDir: {}
        - name: edge-activation-plan-signing-key
          secret:
            secretName: `+secret+`
            defaultMode: 0400
            items:
              - key: FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY
                path: plan-signing-key
              - key: FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY_ID
                path: key-id
              - key: FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY_GENERATION
                path: key-generation
`, 1)
	write := func(name, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(directory, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("base.yaml", base)
	write("target.yaml", target)
	write("repeated-target.yaml", target)
	write("hybrid.yaml", base)
	baseValues := `{"api":{"replicas":2},"edgeActivation":{"enabled":false,"signingSecretName":""}}`
	targetEnvelope := `{"config":{"api":{"replicas":2},"edgeActivation":{"enabled":true,"signingSecretName":"` + secret + `"}}}`
	hybridEnvelope := `{"config":` + baseValues + `}`
	write("base-values.json", baseValues)
	write("target.yaml.json", targetEnvelope)
	write("repeated-target.yaml.json", targetEnvelope)
	write("hybrid.yaml.json", hybridEnvelope)
	var stderr bytes.Buffer
	if exitCode := runConfigValidation(nil, &stderr, directory, secret, source, image); exitCode != 0 {
		t.Fatalf("exact projection was rejected: %s", stderr.String())
	}
	write("target.yaml", strings.Replace(target, "value: exact", "value: drifted", 1))
	stderr.Reset()
	if exitCode := runConfigValidation(nil, &stderr, directory, secret, source, image); exitCode == 0 || stderr.Len() == 0 {
		t.Fatalf("non-API drift was accepted: %s", stderr.String())
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
