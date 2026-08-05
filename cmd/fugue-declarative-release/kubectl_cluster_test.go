package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
	observation, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, false)
	if err != nil {
		t.Fatalf("parse observation: %v", err)
	}
	if observation.UID != "api-uid" || observation.ResourceVersion != "42" || observation.Desired != 2 || observation.Ready != 2 ||
		observation.ImageID != "sha256:"+strings.Repeat("b", 64) || observation.ConfigSHA != strings.Repeat("1", 40) ||
		len(observation.FieldManagers) != 1 || observation.FieldManagers[0] != "fugue-api-declarative" {
		t.Fatalf("unexpected observation: %+v", observation)
	}
	pods["items"].([]any)[1] = podFixture("api-2", "uid-2", strings.Repeat("1", 40), strings.Repeat("c", 64))
	if _, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, false); err == nil || !strings.Contains(err.Error(), "mixed image IDs") {
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
	if _, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, false); err == nil || !strings.Contains(err.Error(), "restarted") {
		t.Fatalf("ordinary target accepted a restarted pod: %v", err)
	}
	first, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, true)
	if err != nil {
		t.Fatalf("historical LKG restart was rejected: %v", err)
	}
	pod["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["restartCount"] = 4
	second, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release, true)
	if err != nil {
		t.Fatalf("read historical LKG restart: %v", err)
	}
	if first.HealthDigest == second.HealthDigest {
		t.Fatal("historical restart count did not enter the health witness")
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
