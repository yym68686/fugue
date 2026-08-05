package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
)

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
	observation, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release)
	if err != nil {
		t.Fatalf("parse observation: %v", err)
	}
	if observation.UID != "api-uid" || observation.ResourceVersion != "42" || observation.Desired != 2 || observation.Ready != 2 ||
		observation.ImageID != "sha256:"+strings.Repeat("b", 64) || observation.ConfigSHA != strings.Repeat("1", 40) ||
		len(observation.FieldManagers) != 1 || observation.FieldManagers[0] != "fugue-api-declarative" {
		t.Fatalf("unexpected observation: %+v", observation)
	}
	pods["items"].([]any)[1] = podFixture("api-2", "uid-2", strings.Repeat("1", 40), strings.Repeat("c", 64))
	if _, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release); err == nil || !strings.Contains(err.Error(), "mixed image IDs") {
		t.Fatalf("mixed cohort was accepted: %v", err)
	}
}

func TestApplyArgumentsForceOnlyFirstOwnershipHandoff(t *testing.T) {
	release := declarativerelease.PlanRelease{IntentGeneration: 1, Workload: declarativerelease.Workload{FieldManager: "fugue-api-declarative"}}
	first := strings.Join(applyArguments(release, true), " ")
	if !strings.Contains(first, "--force-conflicts") || !strings.Contains(first, "--dry-run=server") {
		t.Fatalf("first ownership handoff args are incomplete: %s", first)
	}
	release.IntentGeneration = 2
	next := strings.Join(applyArguments(release, false), " ")
	if strings.Contains(next, "--force-conflicts") || strings.Contains(next, "--dry-run") {
		t.Fatalf("ordinary component apply retained handoff privileges: %s", next)
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
