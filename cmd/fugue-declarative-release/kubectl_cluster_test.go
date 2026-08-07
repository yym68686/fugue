package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestVerifyTargetUsesFixedMetadataOnlyRegistryEvidence(t *testing.T) {
	image := "ghcr.io/example/fugue-telemetry-agent@sha256:" + strings.Repeat("b", 64)
	revision := strings.Repeat("a", 40)
	script := filepath.Join(t.TempDir(), "verify.py")
	program := `import json, sys
image = "ghcr.io/example/fugue-telemetry-agent@sha256:" + "b" * 64
revision = "a" * 40
expected = ["--image", image, "--platform", "linux/amd64", "--expected-revision", revision, "--metadata-only", "--timeout-seconds", "18", "--request-timeout-seconds", "5", "--max-attempts", "2", "--retry-delay-seconds", "0.1"]
if sys.argv[1:] != expected:
    raise SystemExit(2)
print(json.dumps({"image": image, "index_digest": "sha256:" + "b" * 64, "manifest_digest": "sha256:" + "c" * 64, "config_digest": "sha256:" + "d" * 64, "oci_revision": revision, "platform": "linux/amd64", "verification": "registry_manifest_config_get", "blob_count": 0, "layer_get_probe_count": 0, "request_count": 3, "total_layer_bytes": 0}, separators=(",", ":")))
`
	if err := os.WriteFile(script, []byte(program), 0o600); err != nil {
		t.Fatal(err)
	}
	cluster := &kubectlCluster{verifier: script, timeout: time.Second}
	if err := cluster.VerifyTarget(context.Background(), declarativerelease.TargetIdentity{Present: true, ImageRef: image, OCIRevision: revision}); err != nil {
		t.Fatalf("verify exact immutable predecessor: %v", err)
	}
}

func TestVerifyBootstrapTargetAllowsMissingRevisionOnlyForExactAdoptionLKG(t *testing.T) {
	image := "ghcr.io/example/fugue-edge@sha256:" + strings.Repeat("b", 64)
	revision := strings.Repeat("a", 40)
	script := filepath.Join(t.TempDir(), "verify.py")
	program := `import json, sys
image = "ghcr.io/example/fugue-edge@sha256:" + "b" * 64
expected = ["--image", image, "--platform", "linux/amd64", "--metadata-only", "--timeout-seconds", "18", "--request-timeout-seconds", "5", "--max-attempts", "2", "--retry-delay-seconds", "0.1"]
if sys.argv[1:] != expected:
    raise SystemExit(2)
print(json.dumps({"image": image, "index_digest": "sha256:" + "b" * 64, "manifest_digest": "sha256:" + "c" * 64, "config_digest": "sha256:" + "d" * 64, "oci_revision": "", "platform": "linux/amd64", "verification": "registry_manifest_config_get", "blob_count": 0, "layer_get_probe_count": 0, "request_count": 3, "total_layer_bytes": 0}, separators=(",", ":")))
`
	if err := os.WriteFile(script, []byte(program), 0o600); err != nil {
		t.Fatal(err)
	}
	release := declarativerelease.PlanRelease{
		MigrationState: "adopting", RetrySameLKG: true, HeterogeneousBootstrapLKG: true,
		BootstrapLKGPath: "deploy/releases/edge-worker-de/lkg.json", ExpectedPreviousPresent: true,
		ExpectedPreviousConfigSHA: revision, ExpectedPreviousManifestSHA: revision, ExpectedPreviousOCIRevision: revision,
		ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("b", 64), OwnershipAdoption: &declarativerelease.OwnershipAdoption{LegacyFieldManager: "helm"},
	}
	target := declarativerelease.TargetIdentity{Present: true, ImageRef: image, ConfigSHA: revision, ManifestSHA: revision, OCIRevision: revision}
	cluster := &kubectlCluster{verifier: script, timeout: time.Second}
	if err := cluster.VerifyBootstrapTarget(context.Background(), release, target); err != nil {
		t.Fatalf("verify legacy immutable bootstrap: %v", err)
	}
	release.MigrationState = "independent"
	if err := cluster.VerifyBootstrapTarget(context.Background(), release, target); err == nil {
		t.Fatal("independent release accepted bootstrap registry compatibility")
	}
}

func TestNormalizeAdoptionBootstrapDegradedIdentityBindsLegacyShape(t *testing.T) {
	revision := strings.Repeat("a", 40)
	release := declarativerelease.PlanRelease{
		MigrationState: "adopting", RetrySameLKG: true, HeterogeneousBootstrapLKG: true,
		BootstrapLKGPath: "deploy/releases/edge-worker-de/lkg.json", ExpectedPreviousPresent: true,
		ExpectedPreviousConfigSHA: revision, ExpectedPreviousManifestSHA: revision,
		ExpectedPreviousOCIRevision: revision, ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("b", 64),
		Artifact:          declarativerelease.Artifact{Repository: "ghcr.io/example/fugue-edge"},
		Workload:          declarativerelease.Workload{FieldManager: "fugue-edge-worker-de-declarative"},
		OwnershipAdoption: &declarativerelease.OwnershipAdoption{LegacyFieldManager: "helm"},
	}
	observation := declarativerelease.Observation{
		Present: true, UID: "uid", ResourceVersion: "42", Generation: 7,
		ImageRef: "ghcr.io/example/fugue-edge:" + revision, ManifestSHA: revision, OCIRevision: revision,
		TemplateDigest: "sha256:" + strings.Repeat("c", 64),
		FieldManagers:  []string{"fugue-edge-worker-de-declarative", "helm"},
		Primary:        declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-front"},
	}
	observation.Resources = []declarativerelease.ResourceObservation{{
		Identity: observation.Primary, Present: true, UID: "uid", ResourceVersion: "42", Generation: 7,
		ObjectDigest: "sha256:" + strings.Repeat("d", 64), FieldManagers: []string{"fugue-edge-worker-de-declarative", "helm"},
	}}
	if err := normalizeAdoptionBootstrapDegradedIdentity(&observation, release); err != nil {
		t.Fatalf("normalize legacy bootstrap identity: %v", err)
	}
	if observation.ImageRef != release.Artifact.Repository+"@"+release.ExpectedPreviousImageDigest || observation.ConfigSHA != revision {
		t.Fatalf("unexpected normalized identity: %+v", observation)
	}
	if err := observation.ValidateDegradedPredecessor(release); err != nil {
		t.Fatalf("normalized observation is not valid: %v", err)
	}
	if !allowsLegacyBootstrapRegistryRevision(release, observation) {
		t.Fatal("exact adoption bootstrap did not allow a missing legacy registry revision")
	}

	independent := release
	independent.MigrationState = "independent"
	untouched := observation
	untouched.ImageRef = "ghcr.io/example/fugue-edge:" + revision
	untouched.ConfigSHA = ""
	if err := normalizeAdoptionBootstrapDegradedIdentity(&untouched, independent); err != nil {
		t.Fatalf("independent observation should not invoke adoption fallback: %v", err)
	}
	if untouched.ImageRef != "ghcr.io/example/fugue-edge:"+revision || untouched.ConfigSHA != "" {
		t.Fatalf("independent path changed legacy identity: %+v", untouched)
	}
	if allowsLegacyBootstrapRegistryRevision(independent, observation) {
		t.Fatal("independent release allowed a missing registry revision")
	}
}

func TestNormalizeAdoptionBootstrapDegradedIdentityRejectsWrongSourceTag(t *testing.T) {
	revision := strings.Repeat("a", 40)
	release := declarativerelease.PlanRelease{
		MigrationState: "adopting", RetrySameLKG: true, HeterogeneousBootstrapLKG: true,
		BootstrapLKGPath: "deploy/releases/edge-worker-de/lkg.json", ExpectedPreviousPresent: true,
		ExpectedPreviousConfigSHA: revision, ExpectedPreviousManifestSHA: revision,
		ExpectedPreviousOCIRevision: revision, ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("b", 64),
		Artifact:          declarativerelease.Artifact{Repository: "ghcr.io/example/fugue-edge"},
		Workload:          declarativerelease.Workload{FieldManager: "fugue-edge-worker-de-declarative"},
		OwnershipAdoption: &declarativerelease.OwnershipAdoption{LegacyFieldManager: "helm"},
	}
	observation := declarativerelease.Observation{Present: true, ImageRef: "ghcr.io/example/fugue-edge:" + strings.Repeat("c", 40), ManifestSHA: revision, OCIRevision: revision}
	if err := normalizeAdoptionBootstrapDegradedIdentity(&observation, release); err == nil {
		t.Fatal("wrong source tag was accepted")
	}
}

func TestSanitizeObservedResourceDropsDaemonSetControllerGenerationAnnotation(t *testing.T) {
	resource := map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{
			"deprecated.daemonset.template.generation": "109",
			"fugue.pro/source-commit":                  strings.Repeat("a", 40),
		}},
		"spec":   map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "edge", "image": "ghcr.io/example/edge@sha256:" + strings.Repeat("b", 64)}}}}},
		"status": map[string]any{"numberReady": 1},
	}
	clean := sanitizeObservedResource(resource)
	annotations := mapField(clean["metadata"].(map[string]any), "annotations")
	if _, ok := annotations["deprecated.daemonset.template.generation"]; ok {
		t.Fatal("controller-owned generation annotation remained in CAS digest")
	}
	if annotations["fugue.pro/source-commit"] != strings.Repeat("a", 40) {
		t.Fatal("declarative source identity was removed")
	}
	if _, ok := clean["status"]; ok {
		t.Fatal("status remained in CAS digest")
	}
}

func TestParseObservationRequiresOneStableImmutableCohort(t *testing.T) {
	release := declarativerelease.PlanRelease{
		ComponentID: "api",
		Workload:    declarativerelease.Workload{Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api", FieldManager: "fugue-api-declarative", Replicas: 2},
	}
	workload := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": "fugue-fugue-api", "namespace": "fugue-system", "uid": "api-uid", "resourceVersion": "42", "generation": 7,
			"annotations":   map[string]any{"fugue.pro/production-config-sha": strings.Repeat("1", 40)},
			"managedFields": []any{map[string]any{"manager": "fugue-api-declarative"}},
		},
		"spec": map[string]any{
			"replicas": 2,
			"selector": map[string]any{"matchLabels": map[string]any{"app": "api"}},
			"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": strings.Repeat("1", 40)}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "api", "image": "ghcr.io/example/fugue-api@sha256:" + strings.Repeat("a", 64)}}},
			},
		},
		"status": map[string]any{"observedGeneration": 7, "updatedReplicas": 2, "readyReplicas": 2, "availableReplicas": 2, "unavailableReplicas": 0},
	}
	pods := map[string]any{"items": []any{
		podFixture("api-1", "uid-1", strings.Repeat("1", 40), strings.Repeat("b", 64)),
		podFixture("api-2", "uid-2", strings.Repeat("1", 40), strings.Repeat("b", 64)),
	}}
	observation, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, false, false)
	if err != nil {
		t.Fatalf("parse observation: %v", err)
	}
	if observation.UID != "api-uid" || observation.ResourceVersion != "42" || observation.Desired != 2 || observation.Ready != 2 ||
		observation.ImageID != "sha256:"+strings.Repeat("b", 64) || observation.ConfigSHA != strings.Repeat("1", 40) ||
		len(observation.FieldManagers) != 1 || observation.FieldManagers[0] != "fugue-api-declarative" {
		t.Fatalf("unexpected observation: %+v", observation)
	}
	pods["items"].([]any)[1] = podFixture("api-2", "uid-2", strings.Repeat("1", 40), strings.Repeat("c", 64))
	if _, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, false, false); err == nil || !strings.Contains(err.Error(), "mixed image IDs") {
		t.Fatalf("mixed cohort was accepted: %v", err)
	}
}

func TestParseDegradedObservationKeepsOwnedIdentityWithoutPodHealth(t *testing.T) {
	revision := strings.Repeat("9", 40)
	release := declarativerelease.PlanRelease{
		ComponentID: "telemetry",
		Workload: declarativerelease.Workload{
			APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "telemetry",
			Container: "telemetry-agent", FieldManager: "fugue-telemetry-declarative", Replicas: 1,
		},
	}
	workload := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": "telemetry", "namespace": "fugue-system", "uid": "telemetry-uid", "resourceVersion": "42", "generation": 7,
			"annotations":   map[string]any{"fugue.pro/production-config-sha": revision},
			"managedFields": []any{map[string]any{"manager": "fugue-telemetry-declarative"}},
		},
		"spec": map[string]any{
			"replicas": 1,
			"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": revision, "fugue.pro/oci-revision": revision}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "telemetry-agent", "image": "ghcr.io/example/telemetry@sha256:" + strings.Repeat("a", 64)}}},
			},
		},
		"status": map[string]any{"observedGeneration": 7, "updatedReplicas": 1, "readyReplicas": 0, "availableReplicas": 0, "unavailableReplicas": 1},
	}
	observation, err := parseDegradedObservation(mustJSON(t, workload), release)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ConfigSHA != revision || observation.ManifestSHA != revision || observation.OCIRevision != revision ||
		observation.Ready != 0 || observation.HealthDigest != "" || len(observation.FieldManagers) != 1 {
		t.Fatalf("unexpected degraded observation: %+v", observation)
	}
}

func TestParseObservationAllowsOnlyStableHistoricalLKGRestarts(t *testing.T) {
	release := declarativerelease.PlanRelease{
		ComponentID: "telemetry",
		Workload: declarativerelease.Workload{
			Kind: "Deployment", Namespace: "fugue-system", Name: "telemetry",
			Container: "api", FieldManager: "fugue-telemetry-declarative", Replicas: 1,
		},
	}
	source := strings.Repeat("1", 40)
	workload := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": "telemetry", "namespace": "fugue-system", "uid": "telemetry-uid",
			"resourceVersion": "42", "generation": 7,
			"annotations":   map[string]any{"fugue.pro/production-config-sha": source},
			"managedFields": []any{map[string]any{"manager": "fugue-telemetry-declarative"}},
		},
		"spec": map[string]any{
			"replicas": 1,
			"selector": map[string]any{"matchLabels": map[string]any{"app": "telemetry"}},
			"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": source}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "api", "image": "ghcr.io/example/telemetry@sha256:" + strings.Repeat("a", 64)}}},
			},
		},
		"status": map[string]any{"observedGeneration": 7, "updatedReplicas": 1, "readyReplicas": 1, "availableReplicas": 1},
	}
	pod := podFixture("telemetry-1", "uid-1", source, strings.Repeat("b", 64))
	pod["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["restartCount"] = 3
	pods := map[string]any{"items": []any{pod}}
	if _, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, false, false); err == nil || !strings.Contains(err.Error(), "restarted") {
		t.Fatalf("ordinary target accepted a restarted pod: %v", err)
	}
	first, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, true, false)
	if err != nil {
		t.Fatalf("historical LKG restart was rejected: %v", err)
	}
	pod["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["restartCount"] = 4
	second, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, true, false)
	if err != nil {
		t.Fatalf("read historical LKG restart: %v", err)
	}
	if first.HealthDigest == second.HealthDigest {
		t.Fatal("historical restart count did not enter the health witness")
	}
	delete(pod["metadata"].(map[string]any)["annotations"].(map[string]any), "fugue.pro/source-commit")
	pod["spec"] = map[string]any{"containers": []any{map[string]any{"name": "api", "image": "ghcr.io/example/telemetry:" + source}}}
	if _, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, true, false); err != nil {
		t.Fatalf("explicit adoption did not recover the exact legacy pod source tag: %v", err)
	}
	if _, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, false, false); err == nil {
		t.Fatal("ordinary target recovered a missing pod source from a legacy tag")
	}
	pod["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] = "ghcr.io/example/telemetry:" + strings.Repeat("2", 40)
	if _, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, true, false); err == nil {
		t.Fatal("adoption accepted a legacy pod tag for another source")
	}
	pod["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] = "ghcr.io/example/telemetry@sha256:" + strings.Repeat("b", 64)
	if _, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, true, false); err == nil {
		t.Fatal("adoption inferred source from an immutable pod before artifact proof")
	}
	if _, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, true, true); err != nil {
		t.Fatalf("adoption rejected an exact declared immutable pod after artifact proof: %v", err)
	}
}

func TestObservedVerificationImageBindsLegacySourceTagToPodDigestOnlyDuringAdoption(t *testing.T) {
	source := strings.Repeat("1", 40)
	digest := "sha256:" + strings.Repeat("a", 64)
	legacy := "ghcr.io/example/fugue-edge:" + source
	immutable := "ghcr.io/example/fugue-edge@" + digest
	if got, err := observedVerificationImage(legacy, digest, source, true); err != nil || got != immutable {
		t.Fatalf("bind exact legacy source to immutable pod identity: got=%q err=%v", got, err)
	}
	if _, err := observedVerificationImage(legacy, digest, source, false); err == nil {
		t.Fatal("independent observation accepted a legacy source tag")
	}
	if _, err := observedVerificationImage(legacy, digest, strings.Repeat("2", 40), true); err == nil {
		t.Fatal("adoption accepted a legacy tag for another source")
	}
	if got, err := observedVerificationImage(immutable, digest, source, false); err != nil || got != immutable {
		t.Fatalf("ordinary immutable observation changed: got=%q err=%v", got, err)
	}
	oci := strings.Repeat("2", 40)
	verification := declarativerelease.RegistryVerification{Image: immutable, OCIRevision: oci}
	if !observedRegistryIdentityMatches(verification, immutable, oci, true) {
		t.Fatal("adoption conflated deployment source with exact OCI revision")
	}
	verification.OCIRevision = source
	if observedRegistryIdentityMatches(verification, immutable, oci, true) {
		t.Fatal("adoption accepted a registry revision copied from the deployment tag")
	}
}

func TestParseObservationRecoversExactLegacyOnDeleteUpdatedStatusOnlyDuringAdoption(t *testing.T) {
	source := strings.Repeat("1", 40)
	digest := strings.Repeat("a", 64)
	release := declarativerelease.PlanRelease{
		ComponentID: "edge-worker-gamma",
		Workload: declarativerelease.Workload{Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-gamma-front",
			Container: "edge-front", FieldManager: "fugue-edge-worker-gamma-declarative", RolloutMode: "on-delete"},
	}
	workload := map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{"name": "edge-gamma-front", "namespace": "fugue-system", "uid": "front-uid", "resourceVersion": "42", "generation": 79,
			"annotations": map[string]any{"fugue.pro/production-config-sha": source}, "managedFields": []any{map[string]any{"manager": "helm"}}},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": "edge-front"}},
			"template": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": source}},
				"spec": map[string]any{"containers": []any{map[string]any{"name": "edge-front", "image": "ghcr.io/example/fugue-edge:" + source}}}},
		},
		"status": map[string]any{"observedGeneration": 79, "desiredNumberScheduled": 1, "currentNumberScheduled": 1,
			"numberReady": 1, "numberAvailable": 1, "numberMisscheduled": 0},
	}
	pod := podFixture("edge-front-1", "pod-uid", source, digest)
	pod["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["name"] = "edge-front"
	pods := map[string]any{"items": []any{pod}}
	adoption, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, true, false)
	if err != nil || adoption.Updated != 1 {
		t.Fatalf("exact healthy legacy OnDelete cohort was not recovered: observation=%+v err=%v", adoption, err)
	}
	workload["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] = "ghcr.io/example/fugue-edge@sha256:" + digest
	independent, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, false, false)
	if err != nil {
		t.Fatalf("read immutable independent cohort: %v", err)
	}
	if independent.Updated != 0 {
		t.Fatal("independent observation inherited legacy OnDelete updated compatibility")
	}
}

func TestBootstrapObservationWorkloadKeepsDeclaredLKGIdentityAndLiveCAS(t *testing.T) {
	source := strings.Repeat("1", 40)
	forward := strings.Repeat("2", 40)
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-front"}
	live := map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{
			"name": identity.Name, "namespace": identity.Namespace, "uid": "live-uid", "resourceVersion": "42", "generation": 9,
			"annotations":   map[string]any{"fugue.pro/production-config-sha": forward},
			"managedFields": []any{map[string]any{"manager": "edge-worker-de-declarative"}},
		},
		"spec":   map[string]any{"template": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": forward}}}},
		"status": map[string]any{"observedGeneration": 9, "desiredNumberScheduled": 1, "numberReady": 1, "numberAvailable": 1},
	}
	lkg := declarativerelease.ResourceSet{APIVersion: "release.fugue.dev/v2", Kind: "ComponentResourceSet", Items: []map[string]any{{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{"name": identity.Name, "namespace": identity.Namespace, "annotations": map[string]any{"fugue.pro/production-config-sha": source}},
		"spec":     map[string]any{"template": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": source}}}},
	}}}
	manifest, err := declarativerelease.CanonicalJSON(lkg)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := bootstrapObservationWorkload(mustJSON(t, live), manifest, identity)
	if err != nil {
		t.Fatal(err)
	}
	value, err := decodeJSONObject(projected)
	if err != nil {
		t.Fatal(err)
	}
	metadata := mapField(value, "metadata")
	annotations := mapStringField(metadata, "annotations")
	templateAnnotations := mapStringField(mapField(mapField(mapField(value, "spec"), "template"), "metadata"), "annotations")
	if annotations["fugue.pro/production-config-sha"] != source || templateAnnotations["fugue.pro/source-commit"] != source ||
		stringValue(metadata["uid"]) != "live-uid" || stringValue(metadata["resourceVersion"]) != "42" || int64Value(metadata["generation"]) != 9 ||
		len(managedFieldManagers(metadata)) != 1 || int64Value(mapField(value, "status")["observedGeneration"]) != 9 {
		t.Fatalf("bootstrap projection mixed desired and live identity: %s", projected)
	}
	liveMetadata := mapField(live, "metadata")
	delete(liveMetadata, "resourceVersion")
	if _, err := bootstrapObservationWorkload(mustJSON(t, live), manifest, identity); err == nil {
		t.Fatal("bootstrap projection accepted missing live CAS")
	}
}

func TestHistoricalLKGAllowsLegacyManagerOnlyDuringAdoption(t *testing.T) {
	release := declarativerelease.PlanRelease{
		ExpectedPreviousPresent:     true,
		ExpectedPreviousConfigSHA:   strings.Repeat("1", 40),
		ExpectedPreviousManifestSHA: strings.Repeat("2", 40),
		ExpectedPreviousOCIRevision: strings.Repeat("3", 40),
		IntentGeneration:            6,
		MigrationState:              "adopting",
		OwnershipAdoption: &declarativerelease.OwnershipAdoption{
			LegacyFieldManager: "helm",
			Resources: []declarativerelease.OwnershipAdoptionScope{{
				Identity: declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api"},
				Fields:   []string{"/spec/template"},
			}},
		},
	}
	lkg := declarativerelease.TargetIdentity{
		Present: true, ConfigSHA: release.ExpectedPreviousConfigSHA,
		ManifestSHA: release.ExpectedPreviousManifestSHA, OCIRevision: release.ExpectedPreviousOCIRevision,
	}
	if !allowsHistoricalRestarts(release, lkg) {
		t.Fatal("declared historical LKG lost its restart and legacy-manager allowance")
	}
	forward := lkg
	forward.ConfigSHA = strings.Repeat("4", 40)
	if allowsHistoricalRestarts(release, forward) {
		t.Fatal("forward target inherited the historical LKG allowance")
	}
	release.MigrationState = "independent"
	release.OwnershipAdoption = nil
	if allowsHistoricalRestarts(release, lkg) {
		t.Fatal("independent release retained historical restart or legacy-manager allowance")
	}
}

func TestManagedFieldsOwnershipRequiresEveryReviewedPointer(t *testing.T) {
	metadata := map[string]any{"managedFields": []any{map[string]any{
		"manager": "edge-worker-declarative", "operation": "Apply", "fieldsV1": map[string]any{
			"f:metadata": map[string]any{"f:labels": map[string]any{"f:fugue.io/edge-group-id": map[string]any{}}},
			"f:spec":     map[string]any{"f:selector": map[string]any{}, "f:template": map[string]any{}},
		},
	}}}
	if !managedFieldsOwnPointers(metadata, "edge-worker-declarative", []string{"/metadata/labels", "/spec/selector", "/spec/template"}) {
		t.Fatal("reviewed ownership pointers were not recognized")
	}
	if managedFieldsOwnPointers(metadata, "edge-worker-declarative", []string{"/spec/updateStrategy"}) {
		t.Fatal("an unowned pointer was accepted")
	}
	if managedFieldsOwnPointers(metadata, "helm", []string{"/spec/template"}) {
		t.Fatal("the legacy manager was accepted as the declarative owner")
	}
}

func TestApplyArgumentsNeverImplicitlyForceOwnershipHandoff(t *testing.T) {
	release := declarativerelease.PlanRelease{IntentGeneration: 1, Workload: declarativerelease.Workload{FieldManager: "fugue-api-declarative"}}
	first := strings.Join(applyArguments(release, true), " ")
	if strings.Contains(first, "--force-conflicts") || !strings.Contains(first, "--dry-run=server") {
		t.Fatalf("ordinary first-generation apply gained ownership handoff privileges: %s", first)
	}
	release.IntentGeneration = 2
	release.RetrySameLKG = true
	next := strings.Join(applyArguments(release, false), " ")
	if strings.Contains(next, "--force-conflicts") || strings.Contains(next, "--dry-run") {
		t.Fatalf("same-LKG retry gained ownership handoff privileges: %s", next)
	}
	if _, err := adoptionApplyArguments(release, true); err == nil {
		t.Fatal("unbound release obtained ownership adoption arguments")
	}
	release.MigrationState = "adopting"
	release.OwnershipAdoption = &declarativerelease.OwnershipAdoption{
		LegacyFieldManager: "helm",
		Resources: []declarativerelease.OwnershipAdoptionScope{{
			Identity: declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api"},
			Fields:   []string{"/spec/template/spec/containers/name=api/image"},
		}},
	}
	adoption, err := adoptionApplyArguments(release, true)
	if err != nil {
		t.Fatal(err)
	}
	if args := strings.Join(adoption, " "); strings.Contains(args, "--force-conflicts") || !strings.Contains(args, "--dry-run=server") {
		t.Fatalf("explicit adoption args are incomplete: %s", args)
	}
	force, err := adoptionForceApplyArguments(release, true)
	if err != nil || !strings.Contains(strings.Join(force, " "), "--force-conflicts") {
		t.Fatalf("reviewed adoption conflict did not produce force arguments: %v %v", force, err)
	}
}

func TestApplyResourceSetConsumesDependentCASBeforeStartingTheWorkload(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "order.log")
	kubectl := filepath.Join(directory, "kubectl")
	program := `#!/bin/sh
set -eu
python3 -c 'import json,os,sys; value=json.load(sys.stdin); open(os.environ["ORDER_LOG"], "a").write(value["kind"]+"/"+value["metadata"]["name"]+"\n")'
printf '{}\n'
`
	if err := os.WriteFile(kubectl, []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ORDER_LOG", logPath)
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{
		APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de",
		FieldManager: "fugue-edge-control-de-declarative",
	}}
	manifest := mustJSON(t, map[string]any{
		"apiVersion": "release.fugue.dev/v2", "kind": "ComponentResourceSet", "items": []any{
			map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "edge-control-de", "namespace": "fugue-system"}},
			map[string]any{"apiVersion": "policy/v1", "kind": "PodDisruptionBudget", "metadata": map[string]any{"name": "edge-control-de", "namespace": "fugue-system"}},
			map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "edge-control-de", "namespace": "fugue-system"}},
		},
	})
	cluster := &kubectlCluster{kubectl: kubectl, timeout: time.Second}
	if err := cluster.applyResourceSet(context.Background(), release, manifest, false); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(raw), "PodDisruptionBudget/edge-control-de\nService/edge-control-de\nDeployment/edge-control-de\n"; got != want {
		t.Fatalf("apply order=%q want=%q", got, want)
	}
}

func TestRefreshOwnershipTakeoverCASUpdatesOnlyResourceVersions(t *testing.T) {
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge"}
	adoption := declarativerelease.OwnershipAdoptionPlan{
		UID: "primary", ResourceVersion: "10", Generation: 7,
		Resources: []declarativerelease.OwnershipAdoptionResourcePlan{{Identity: identity, UID: "resource", ResourceVersion: "10", Generation: 4, Fields: []string{"/spec/template"}}},
	}
	current := declarativerelease.Observation{
		Present: true, UID: "primary", ResourceVersion: "22", Generation: 7,
		Resources: []declarativerelease.ResourceObservation{{Identity: identity, Present: true, UID: "resource", ResourceVersion: "23", Generation: 4}},
	}
	if err := refreshOwnershipTakeoverCAS(&adoption, current); err != nil {
		t.Fatalf("refresh rejected status-only RV movement: %v", err)
	}
	if adoption.ResourceVersion != "22" || adoption.Resources[0].ResourceVersion != "23" || adoption.Generation != 7 {
		t.Fatalf("refresh changed non-RV CAS identity: %+v", adoption)
	}
	current.Resources[0].Generation = 5
	if err := refreshOwnershipTakeoverCAS(&adoption, current); err == nil {
		t.Fatal("refresh accepted generation drift")
	}
}

func TestRefreshOwnershipTakeoverPostPatchCASAcceptsOnlyReviewedGenerationMovement(t *testing.T) {
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge"}
	adoption := declarativerelease.OwnershipAdoptionPlan{
		UID: "primary", ResourceVersion: "10", Generation: 7,
		Resources: []declarativerelease.OwnershipAdoptionResourcePlan{{Identity: identity, UID: "resource", ResourceVersion: "10", Generation: 4, Fields: []string{"/spec/template"}}},
	}
	current := declarativerelease.Observation{
		Present: true, UID: "primary", ResourceVersion: "22", Generation: 8,
		Resources: []declarativerelease.ResourceObservation{{Identity: identity, Present: true, UID: "resource", ResourceVersion: "23", Generation: 5}},
	}
	if err := refreshOwnershipTakeoverPostPatchCAS(&adoption, current); err != nil {
		t.Fatalf("post-patch refresh rejected reviewed generation movement: %v", err)
	}
	if adoption.ResourceVersion != "22" || adoption.Generation != 8 || adoption.Resources[0].ResourceVersion != "23" || adoption.Resources[0].Generation != 5 {
		t.Fatalf("post-patch refresh did not bind the newest CAS: %+v", adoption)
	}
	current.Resources[0].UID = "changed"
	if err := refreshOwnershipTakeoverPostPatchCAS(&adoption, current); err == nil {
		t.Fatal("post-patch refresh accepted UID drift")
	}
}

func TestOwnershipTakeoverForwardOnlyCompensationPathsAreExplicit(t *testing.T) {
	forward := map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
		"serviceAccountName": "edge-worker-de",
		"initContainers":     []any{map[string]any{"name": "edge-workload-identity"}},
	}}}}
	lkg := map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{}}}}
	paths, err := ownershipTakeoverForwardOnlyPaths(forward, lkg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(paths, ",") != "/spec/template/spec/serviceAccount,/spec/template/spec/serviceAccountName,/spec/template/spec/initContainers" {
		t.Fatalf("unexpected explicit compensation paths: %v", paths)
	}
	lkg["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["serviceAccountName"] = "edge-worker-de"
	paths, err = ownershipTakeoverForwardOnlyPaths(forward, lkg)
	if err != nil || len(paths) != 1 || paths[0] != "/spec/template/spec/initContainers" {
		t.Fatalf("LKG-owned field was not preserved: %v %v", paths, err)
	}
	spec := lkg["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	delete(spec, "serviceAccountName")
	spec["serviceAccount"] = "edge-worker-de"
	paths, err = ownershipTakeoverForwardOnlyPaths(forward, lkg)
	if err != nil || len(paths) != 2 || paths[0] != "/spec/template/spec/serviceAccountName" || paths[1] != "/spec/template/spec/initContainers" {
		t.Fatalf("legacy LKG serviceAccount alias was not preserved: %v %v", paths, err)
	}
}

func TestEdgeOwnershipTakeoverTemplatePatchReplacesLegacyAssociativeListMembers(t *testing.T) {
	targetTemplate := map[string]any{
		"metadata": map[string]any{"labels": map[string]any{"fugue.io/edge-group-id": "edge-group-gamma"}},
		"spec": map[string]any{
			"serviceAccountName": "edge-worker-gamma",
			"containers": []any{map[string]any{
				"name": "edge", "image": "example.invalid/edge@sha256:" + strings.Repeat("a", 64),
				"env": []any{map[string]any{"name": "FUGUE_EDGE_CONTROL_URL", "value": "https://edge-control-gamma"}},
			}},
			"volumes": []any{map[string]any{"name": "edge-control-reader", "secret": map[string]any{"secretName": "reader-gamma"}}},
		},
	}
	target := map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{"name": "edge-gamma-worker-a", "namespace": "fugue-system"},
		"spec":     map[string]any{"template": targetTemplate},
	}
	live := map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{"name": "edge-gamma-worker-a", "namespace": "fugue-system", "uid": "worker-uid", "resourceVersion": "84", "generation": json.Number("12")},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "edge", "env": []any{
				map[string]any{"name": "FUGUE_BUNDLE_SIGNING_KEY", "value": "legacy"},
				map[string]any{"name": "FUGUE_EDGE_CONTROL_URL", "value": "https://edge-control-gamma"},
			}}},
			"volumes": []any{
				map[string]any{"name": "legacy-signing", "secret": map[string]any{"secretName": "legacy"}},
				map[string]any{"name": "edge-control-reader", "secret": map[string]any{"secretName": "reader-gamma"}},
			},
		}}},
	}
	scope := declarativerelease.OwnershipAdoptionResourcePlan{
		Identity: declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-gamma-worker-a"},
		UID:      "worker-uid", ResourceVersion: "42", Generation: 11, Fields: []string{"/spec/template"},
	}
	patch, err := edgeOwnershipTakeoverTemplatePatch(target, live, scope)
	if err != nil {
		t.Fatalf("build exact Edge takeover patch: %v", err)
	}
	var operations []map[string]any
	decoder := json.NewDecoder(bytes.NewReader(patch))
	decoder.UseNumber()
	if err := decoder.Decode(&operations); err != nil {
		t.Fatal(err)
	}
	if len(operations) != 4 || operations[0]["path"] != "/metadata/uid" || operations[0]["value"] != "worker-uid" ||
		operations[1]["path"] != "/metadata/resourceVersion" || operations[1]["value"] != "84" ||
		operations[2]["path"] != "/metadata/generation" || fmt.Sprint(operations[2]["value"]) != "12" ||
		operations[3]["op"] != "replace" || operations[3]["path"] != "/spec/template" {
		t.Fatalf("unexpected bounded takeover patch: %s", patch)
	}
	replacement, ok := operations[3]["value"].(map[string]any)
	if !ok {
		t.Fatalf("replacement template is not an object: %s", patch)
	}
	replacementRaw, _ := json.Marshal(replacement)
	if bytes.Contains(replacementRaw, []byte("FUGUE_BUNDLE_SIGNING_KEY")) || bytes.Contains(replacementRaw, []byte("legacy-signing")) ||
		!bytes.Contains(replacementRaw, []byte("FUGUE_EDGE_CONTROL_URL")) || !bytes.Contains(replacementRaw, []byte("edge-control-reader")) {
		t.Fatalf("replacement did not remove only legacy list members: %s", replacementRaw)
	}

	scope.Fields = []string{"/metadata/labels"}
	if _, err := edgeOwnershipTakeoverTemplatePatch(target, live, scope); err == nil {
		t.Fatal("takeover patch escaped the reviewed Pod template scope")
	}
	scope.Fields = []string{"/spec/template"}
	live["metadata"].(map[string]any)["uid"] = "changed-uid"
	if _, err := edgeOwnershipTakeoverTemplatePatch(target, live, scope); err == nil {
		t.Fatal("takeover patch accepted UID drift")
	}
}

func TestAdoptionConflictProofRequiresExactManagerAndFieldScope(t *testing.T) {
	allowed := errors.New(`command failed: exit status 1: error: Apply failed with 1 conflict: conflict with "kubectl-patch" using apps/v1: .spec.template.spec.containers[name="edge-front"].image`)
	managers := []string{"helm", "kubectl-patch"}
	if err := validateAdoptionConflicts(allowed, managers, []string{"/spec/template"}); err != nil {
		t.Fatalf("reviewed conflict was rejected: %v", err)
	}
	if err := validateAdoptionConflicts(allowed, []string{"helm"}, []string{"/spec/template"}); err == nil {
		t.Fatal("unreviewed field manager was accepted")
	}
	if err := validateAdoptionConflicts(allowed, managers, []string{"/metadata/annotations"}); err == nil {
		t.Fatal("out-of-scope conflict field was accepted")
	}
	inconsistent := errors.New(`error: Apply failed with 2 conflicts: conflict with "kubectl-patch" using apps/v1: .spec.template.spec.containers[name="edge-front"].image`)
	if err := validateAdoptionConflicts(inconsistent, managers, []string{"/spec/template"}); err == nil {
		t.Fatal("inconsistent conflict count was accepted")
	}
	grouped := errors.New("command failed: error: Apply failed with 2 conflicts: conflicts with \"helm\" using apps/v1:\n- .spec.template.spec.containers[name=\"caddy\"].image\n- .spec.template.spec.containers[name=\"edge\"].image")
	if err := validateAdoptionConflicts(grouped, managers, []string{"/spec/template"}); err != nil {
		t.Fatalf("reviewed grouped conflicts were rejected: %v", err)
	}
}

func TestParseLeaderLeaseRequiresTypedRenewTime(t *testing.T) {
	raw := []byte(`{"spec":{"holderIdentity":"controller-1","renewTime":"2026-08-05T01:02:03.123456Z"}}`)
	holder, renew, err := parseLeaderLease(raw)
	if err != nil || holder != "controller-1" || renew.IsZero() {
		t.Fatalf("parse lease: holder=%q renew=%s err=%v", holder, renew, err)
	}
	if _, _, err := parseLeaderLease([]byte(`{"spec":{"holderIdentity":"","renewTime":"bad"}}`)); err == nil {
		t.Fatal("invalid leader lease was accepted")
	}
}

func TestLegacySourceTagIsOnlyDerivedFromCanonicalImmutableCommitTag(t *testing.T) {
	valid := "ghcr.io/example/fugue-image-cache:" + strings.Repeat("a", 40)
	if got := legacySourceTag(valid); got != strings.Repeat("a", 40) {
		t.Fatalf("canonical legacy tag was not derived: %q", got)
	}
	for _, value := range []string{
		"ghcr.io/example/fugue-image-cache:latest",
		"ghcr.io/example/fugue-image-cache:" + strings.Repeat("A", 40),
		"registry.example:5000/fugue-image-cache@sha256:" + strings.Repeat("a", 64),
	} {
		if got := legacySourceTag(value); got != "" {
			t.Fatalf("non-canonical legacy image derived source %q from %q", got, value)
		}
	}
}

func TestResourceDeletionAllowlistContainsOnlyOwnedComponentKinds(t *testing.T) {
	for _, identity := range []declarativerelease.ResourceIdentity{
		{APIVersion: "apps/v1", Kind: "Deployment"},
		{APIVersion: "apps/v1", Kind: "DaemonSet"},
		{APIVersion: "batch/v1", Kind: "Job"},
		{APIVersion: "v1", Kind: "Service"},
		{APIVersion: "v1", Kind: "ServiceAccount"},
		{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		{APIVersion: "policy/v1", Kind: "PodDisruptionBudget"},
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"},
	} {
		if _, err := resourceGVR(identity); err != nil {
			t.Fatalf("owned kind is missing from deletion allowlist: %+v: %v", identity, err)
		}
	}
	if _, err := resourceGVR(declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "Secret"}); err == nil {
		t.Fatal("Secret unexpectedly entered release-owned deletion allowlist")
	}
}

func TestCreatedResourceDeletionsSelectOnlyAbsentToPresentNonRetainedObjects(t *testing.T) {
	deployment := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "api"}
	service := declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "Service", Namespace: "fugue-system", Name: "api-tls"}
	pvc := declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: "fugue-system", Name: "state"}
	identities := []declarativerelease.ResourceIdentity{deployment, pvc, service}
	before := declarativerelease.Observation{Resources: []declarativerelease.ResourceObservation{{Identity: deployment, Present: true}, {Identity: pvc}, {Identity: service}}}
	after := declarativerelease.Observation{Resources: []declarativerelease.ResourceObservation{
		{Identity: deployment, Present: true, UID: "api-uid", ResourceVersion: "10"},
		{Identity: pvc, Present: true, RetainOnRollback: true, UID: "pvc-uid", ResourceVersion: "11"},
		{Identity: service, Present: true, UID: "service-uid", ResourceVersion: "12"},
	}}
	deletions, err := createdResourceDeletions(identities, before, after)
	if err != nil || len(deletions) != 1 || deletions[0].Identity != service || deletions[0].UID != "service-uid" {
		t.Fatalf("created-resource deletions=%+v err=%v", deletions, err)
	}
	after.Resources = after.Resources[:2]
	if _, err := createdResourceDeletions(identities, before, after); err == nil {
		t.Fatal("missing rollback identity was accepted")
	}
}

func TestOwnershipTakeoverCreatedDeletionsPreserveForwardServiceAccounts(t *testing.T) {
	serviceAccount := declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "fugue-system", Name: "edge-worker-de"}
	service := declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "Service", Namespace: "fugue-system", Name: "edge-service"}
	identities := []declarativerelease.ResourceIdentity{serviceAccount, service}
	before := declarativerelease.Observation{Resources: []declarativerelease.ResourceObservation{{Identity: serviceAccount}, {Identity: service}}}
	after := declarativerelease.Observation{Resources: []declarativerelease.ResourceObservation{
		{Identity: serviceAccount, Present: true, UID: "sa-uid", ResourceVersion: "10"},
		{Identity: service, Present: true, UID: "service-uid", ResourceVersion: "11"},
	}}
	deletions, err := createdResourceDeletions(identities, before, after)
	if err != nil || len(deletions) != 2 {
		t.Fatalf("unexpected created resources: %+v err=%v", deletions, err)
	}
	preserve := map[declarativerelease.ResourceIdentity]struct{}{serviceAccount: {}}
	filtered := deletions[:0]
	for _, deletion := range deletions {
		if _, keep := preserve[deletion.Identity]; !keep {
			filtered = append(filtered, deletion)
		}
	}
	if len(filtered) != 1 || filtered[0].Identity != service {
		t.Fatalf("service account was not excluded from takeover compensation: %+v", filtered)
	}
}

func TestOwnershipTakeoverCompensationAllowsHistoricalLKGToOmitServiceAccount(t *testing.T) {
	if !canOmitLKGCompensationResource(declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "ServiceAccount"}) {
		t.Fatal("forward ServiceAccount should be retained when absent from historical LKG")
	}
	if canOmitLKGCompensationResource(declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "Service"}) {
		t.Fatal("non-dependency resource must not be omitted from historical LKG compensation")
	}
	if canOmitLKGCompensationResource(declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet"}) {
		t.Fatal("workload resource must not be omitted from historical LKG compensation")
	}
}

func TestFreshDeletionPreconditionsRefreshStatusOnlyResourceVersion(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
		"metadata": map[string]any{"name": "edge-control-de", "namespace": "fugue-system", "uid": "pdb-uid", "resourceVersion": "22", "generation": int64(1)},
	}}
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	resource := client.Resource(gvr).Namespace("fugue-system")
	expected := declarativerelease.ResourceObservation{
		Identity: declarativerelease.ResourceIdentity{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Namespace: "fugue-system", Name: "edge-control-de"},
		Present:  true, UID: "pdb-uid", ResourceVersion: "10", Generation: 1,
	}
	uid, rv, present, err := freshDeletionPreconditions(context.Background(), resource, expected)
	if err != nil || !present || uid != types.UID("pdb-uid") || rv != "22" {
		t.Fatalf("fresh delete preconditions: uid=%q rv=%q present=%t err=%v", uid, rv, present, err)
	}
	expected.UID = "replacement-uid"
	if _, _, _, err := freshDeletionPreconditions(context.Background(), resource, expected); err == nil {
		t.Fatal("replacement object entered created-resource rollback")
	}
	if err := client.Resource(gvr).Namespace("fugue-system").Delete(context.Background(), "edge-control-de", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	expected.UID = "pdb-uid"
	if _, _, present, err := freshDeletionPreconditions(context.Background(), resource, expected); err != nil || present {
		t.Fatalf("already absent object did not reconcile: present=%t err=%v", present, err)
	}
}

func TestParseSucceededJobPodRequiresOneZeroExitImmutableExecution(t *testing.T) {
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{Container: "schema-migrate"}}
	source := strings.Repeat("a", 40)
	digest := strings.Repeat("b", 64)
	pods := map[string]any{"items": []any{map[string]any{
		"metadata": map[string]any{"uid": "job-pod", "annotations": map[string]any{"fugue.pro/source-commit": source}},
		"status": map[string]any{"phase": "Succeeded", "containerStatuses": []any{map[string]any{
			"name": "schema-migrate", "restartCount": 0, "imageID": "docker-pullable://ghcr.io/example/schema@sha256:" + digest,
			"state": map[string]any{"terminated": map[string]any{"exitCode": 0, "reason": "Completed"}},
		}}},
	}}}
	imageID, health, err := parseSucceededJobPod(mustJSON(t, pods), release, source)
	if err != nil || imageID != "sha256:"+digest || !strings.HasPrefix(health, "sha256:") {
		t.Fatalf("parse succeeded Job pod: image=%q health=%q err=%v", imageID, health, err)
	}
	pods["items"].([]any)[0].(map[string]any)["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["state"].(map[string]any)["terminated"].(map[string]any)["exitCode"] = 1
	if _, _, err := parseSucceededJobPod(mustJSON(t, pods), release, source); err == nil {
		t.Fatal("failed migration Job was accepted")
	}
}

func TestWorkloadFromDeclaredResourceDerivesOnlyTypedRolloutShapes(t *testing.T) {
	identity := declarativerelease.ResourceIdentity{
		APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-worker-de-a",
	}
	desired := map[string]any{"spec": map[string]any{"updateStrategy": map[string]any{"type": "OnDelete"}}}
	workload, err := workloadFromDeclaredResource(desired, identity, "edge", "fugue-edge-worker-de-declarative")
	if err != nil {
		t.Fatal(err)
	}
	if workload.Kind != "DaemonSet" || workload.RolloutMode != "on-delete" || workload.Container != "edge" || workload.Replicas != 0 {
		t.Fatalf("unexpected derived workload: %+v", workload)
	}

	desired["spec"].(map[string]any)["updateStrategy"] = map[string]any{"type": "Unknown"}
	if _, err := workloadFromDeclaredResource(desired, identity, "edge", "fugue-edge-worker-de-declarative"); err == nil {
		t.Fatal("unsupported auxiliary rollout strategy was accepted")
	}
}

func TestHealthSoakTrackerRequiresOneContinuousWindow(t *testing.T) {
	start := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)
	tracker := healthSoakTracker{required: 180 * time.Second}
	if tracker.observe(start, true) || tracker.observe(start.Add(179*time.Second), true) {
		t.Fatal("health soak completed before its continuous window")
	}
	if tracker.observe(start.Add(179*time.Second), false) {
		t.Fatal("unhealthy observation completed health soak")
	}
	if tracker.observe(start.Add(200*time.Second), true) || !tracker.observe(start.Add(380*time.Second), true) {
		t.Fatal("health soak did not reset and complete after a new continuous window")
	}
}

func TestBootstrapHealthKeepsReadinessButNotForwardAuthorityOrSoak(t *testing.T) {
	release := declarativerelease.PlanRelease{Transition: &declarativerelease.Transition{EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{SoakSeconds: 180}}}
	probe := declarativerelease.HealthProbe{Type: "pod-http", Expected: `"route_authority":"edge-control"`}
	if got := healthSoakDuration(release, true); got != 0 {
		t.Fatalf("bootstrap inherited forward soak: %s", got)
	}
	if got := probeExpectedBody(probe, true); got != "" {
		t.Fatalf("bootstrap required future authority body: %q", got)
	}
	if got := healthSoakDuration(release, false); got != 180*time.Second {
		t.Fatalf("forward soak changed: %s", got)
	}
	if got := probeExpectedBody(probe, false); got != probe.Expected {
		t.Fatalf("forward authority predicate changed: %q", got)
	}
	serviceProbe := declarativerelease.HealthProbe{Type: "service-http", Expected: "ok"}
	if got := probeExpectedBody(serviceProbe, true); got != "ok" {
		t.Fatalf("bootstrap weakened unrelated probe: %q", got)
	}
}

func TestPodHTTPUsesBoundReadyPodIPAndNamedPort(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/readyz" {
			t.Fatalf("unexpected health path: %s", request.URL.Path)
		}
		_, _ = response.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()
	host, portRaw, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil {
		t.Fatal(err)
	}
	pod := podFixture("edge-front-1", "pod-uid", strings.Repeat("1", 40), strings.Repeat("a", 64))
	pod["spec"] = map[string]any{"containers": []any{map[string]any{"name": "edge-front", "ports": []any{map[string]any{"name": "health", "containerPort": port}}}}}
	pod["status"].(map[string]any)["podIP"] = host
	endpoints, err := podHTTPEndpointsFromJSON(mustJSON(t, map[string]any{"items": []any{pod}}), "edge-front", "health")
	if err != nil || len(endpoints) != 1 || endpoints[0].Name != "edge-front-1" || endpoints[0].IP != host || endpoints[0].Port != port {
		t.Fatalf("parse bound pod endpoint: endpoints=%+v err=%v", endpoints, err)
	}
	body, err := readPodHTTP(context.Background(), endpoints[0], "/readyz")
	if err != nil || string(body) != `{"status":"ok"}` {
		t.Fatalf("direct pod health: body=%q err=%v", body, err)
	}
	pod["status"].(map[string]any)["podIP"] = "not-an-ip"
	if _, err := podHTTPEndpointsFromJSON(mustJSON(t, map[string]any{"items": []any{pod}}), "edge-front", "health"); err == nil {
		t.Fatal("invalid pod IP was accepted")
	}
}

func TestEdgeWorkerHealthUsesCanonicalNamedPort(t *testing.T) {
	pod := podFixture("edge-worker-a-1", "pod-uid", strings.Repeat("1", 40), strings.Repeat("a", 64))
	pod["status"].(map[string]any)["podIP"] = "10.42.0.17"
	pod["spec"] = map[string]any{"containers": []any{map[string]any{
		"name":  "edge",
		"ports": []any{map[string]any{"name": "health", "containerPort": 7832}},
	}}}
	raw := mustJSON(t, map[string]any{"items": []any{pod}})
	if endpoints, err := podHTTPEndpointsFromJSON(raw, "edge", "health"); err != nil || len(endpoints) != 1 || endpoints[0].Port != 7832 {
		t.Fatalf("canonical worker health port was not accepted: endpoints=%+v err=%v", endpoints, err)
	}
	if _, err := podHTTPEndpointsFromJSON(raw, "edge", "http"); err == nil {
		t.Fatal("non-canonical worker health port was accepted")
	}
}

func TestEdgeHealthPortNamesKeepLegacyFallbackAdoptionScoped(t *testing.T) {
	if got := edgeHealthPortNames(false); !reflect.DeepEqual(got, []string{"health"}) {
		t.Fatalf("independent path accepted legacy health ports: %v", got)
	}
	if got := edgeHealthPortNames(true); !reflect.DeepEqual(got, []string{"health", "http"}) {
		t.Fatalf("adoption path did not preserve exact legacy port fallback: %v", got)
	}
}

func TestHealthWorkloadContainerUsesTransitionBoundEdgeWorker(t *testing.T) {
	release := declarativerelease.PlanRelease{
		Workload: declarativerelease.Workload{Namespace: "fugue-system"},
		Transition: &declarativerelease.Transition{EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{
			WorkerAName: "edge-gamma-worker-a", WorkerBName: "edge-gamma-worker-b", WorkerContainer: "edge",
		}},
		ArtifactTargets: []declarativerelease.ArtifactTarget{
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-gamma-worker-a", ContainerType: "container", Container: "edge"},
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-gamma-worker-a", ContainerType: "container", Container: "caddy"},
		},
	}
	container, err := healthWorkloadContainer(release, "apps/v1", "DaemonSet", "edge-gamma-worker-a")
	if err != nil || container != "edge" {
		t.Fatalf("select transition-bound worker: container=%q err=%v", container, err)
	}
	release.Transition.EdgeGroupAB.WorkerContainer = "missing"
	if _, err := healthWorkloadContainer(release, "apps/v1", "DaemonSet", "edge-gamma-worker-a"); err == nil {
		t.Fatal("missing transition-bound worker container was accepted")
	}
	release.Transition = nil
	if _, err := healthWorkloadContainer(release, "apps/v1", "DaemonSet", "edge-gamma-worker-a"); err == nil {
		t.Fatal("unbound multi-container health workload was accepted")
	}
}

func TestBootstrapAuxiliaryIdentityAndEveryArtifactImageAreExact(t *testing.T) {
	source := strings.Repeat("a", 40)
	edgeDigest := strings.Repeat("b", 64)
	caddyDigest := strings.Repeat("c", 64)
	desired := map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{"name": "edge-gamma-worker-a", "namespace": "fugue-system"},
		"spec": map[string]any{
			"selector":       map[string]any{"matchLabels": map[string]any{"app": "worker"}},
			"updateStrategy": map[string]any{"type": "OnDelete"},
			"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": source, "fugue.pro/oci-revision": source}},
				"spec": map[string]any{
					"containers": []any{
						map[string]any{"name": "edge", "image": "ghcr.io/example/fugue-edge@sha256:" + edgeDigest},
						map[string]any{"name": "caddy", "image": "docker.io/library/caddy@sha256:" + caddyDigest},
					},
				},
			},
		},
	}
	workload, err := workloadFromDeclaredResource(desired, declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-gamma-worker-a"}, "edge", "fugue-edge-worker-gamma-declarative")
	if err != nil {
		t.Fatal(err)
	}
	target, err := targetIdentityFromDeclaredWorkload(desired, workload)
	if err != nil || target.ImageRef != "ghcr.io/example/fugue-edge@sha256:"+edgeDigest || target.ConfigSHA != source || target.ManifestSHA != source || target.OCIRevision != source {
		t.Fatalf("bootstrap auxiliary target=%+v err=%v", target, err)
	}
	manifest := mustJSON(t, map[string]any{"apiVersion": "release.fugue.dev/v2", "kind": "ComponentResourceSet", "items": []any{desired}})
	pods := mustJSON(t, map[string]any{"items": []any{map[string]any{"status": map[string]any{"containerStatuses": []any{
		map[string]any{"name": "edge", "imageID": "ghcr.io/example/fugue-edge@sha256:" + edgeDigest},
		map[string]any{"name": "caddy", "imageID": "docker.io/library/caddy@sha256:" + caddyDigest},
	}}}}})
	release := declarativerelease.PlanRelease{
		Workload: workload,
		ArtifactTargets: []declarativerelease.ArtifactTarget{
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-gamma-worker-a", Container: "caddy", ContainerType: "container"},
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-gamma-worker-a", Container: "edge", ContainerType: "container"},
		},
	}
	if err := verifyDeclaredArtifactImageIDs(pods, manifest, release); err != nil {
		t.Fatal(err)
	}
	tampered := bytes.Replace(pods, []byte(caddyDigest), []byte(strings.Repeat("d", 64)), 1)
	if err := verifyDeclaredArtifactImageIDs(tampered, manifest, release); err == nil {
		t.Fatal("cross-container LKG image drift was accepted")
	}
}

func podFixture(name, uid, source, imageDigest string) map[string]any {
	return map[string]any{
		"metadata": map[string]any{"name": name, "uid": uid, "annotations": map[string]any{"fugue.pro/source-commit": source}},
		"status": map[string]any{
			"conditions":        []any{map[string]any{"type": "Ready", "status": "True"}},
			"containerStatuses": []any{map[string]any{"name": "api", "imageID": "docker-pullable://ghcr.io/example/fugue-api@sha256:" + imageDigest, "restartCount": 0}},
		},
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
