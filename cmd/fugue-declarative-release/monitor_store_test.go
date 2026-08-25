package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestAPIGuardianHandoffBindsProductionLKG(t *testing.T) {
	t.Chdir("../..")
	registry, err := loadProductionRegistry("deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	var api declarativerelease.Component
	for _, component := range registry.Components {
		if component.ID == "api" {
			api = component
			break
		}
	}
	if api.ID == "" {
		t.Fatal("API component is absent from the production registry")
	}
	if api.Delivery == nil || api.Delivery.Writer != "guardian" || api.Delivery.Group != "global" || api.Delivery.DependencyService != "fugue-fugue" {
		t.Fatalf("API Guardian delivery is not exact: %+v", api.Delivery)
	}
	intentFile, err := os.Open(api.IntentPath)
	if err != nil {
		t.Fatal(err)
	}
	intent, decodeErr := declarativerelease.DecodeIntent(intentFile)
	closeErr := intentFile.Close()
	if decodeErr != nil || closeErr != nil {
		t.Fatalf("decode API intent: %v close: %v", decodeErr, closeErr)
	}
	const lkgSHA = "a0db3ce07ec9053042ffa9f64b632472988a3821"
	const lkgImage = "sha256:81a2523455e2a3ee198cc8281f5de5b8288ecfc7fa5a1928528a5551a0f4830b"
	if intent.Generation != 54 || intent.ExpectedPreviousConfigSHA != lkgSHA || intent.ExpectedPreviousManifestSHA != lkgSHA ||
		intent.ExpectedPreviousOCIRevision != lkgSHA || intent.ExpectedPreviousImageDigest != lkgImage ||
		intent.SupersedesFailedConfigSHA != "3aec6376a36e3f14367c8d35efc7d510c8176cd2" {
		t.Fatalf("API Guardian intent is not bound to the production LKG: %+v", intent)
	}
}

func TestReleaseGuardianIntentBindsCurrentProductionLKG(t *testing.T) {
	t.Chdir("../..")
	file, err := os.Open("deploy/releases/guardian/intent.json")
	if err != nil {
		t.Fatal(err)
	}
	intent, decodeErr := declarativerelease.DecodeIntent(file)
	closeErr := file.Close()
	if decodeErr != nil || closeErr != nil {
		t.Fatalf("decode release Guardian intent: %v close: %v", decodeErr, closeErr)
	}
	const lkgSHA = "5d9322c5c74c31efba7af30ad4295db2cc11bd3d"
	const lkgImage = "sha256:42ecfb57c248a1371b7422d4452de3f22afd53e5f255a30c22f749f59c79e72f"
	if intent.Generation != 198 || intent.ExpectedPreviousConfigSHA != lkgSHA || intent.ExpectedPreviousManifestSHA != lkgSHA ||
		intent.ExpectedPreviousOCIRevision != lkgSHA || intent.ExpectedPreviousImageDigest != lkgImage || intent.SupersedesFailedConfigSHA != "" {
		t.Fatalf("release Guardian intent is not bound to the production LKG: %+v", intent)
	}
}

func TestMonitorStorePersistsImmutableRecordAndCASState(t *testing.T) {
	files, terminal, release := monitorBundleFixture(t)
	client := kubernetesfake.NewSimpleClientset()
	store := &monitorStore{client: client.CoreV1(), now: func() time.Time { return time.Date(2026, 8, 10, 19, 0, 0, 0, time.UTC) }}
	snapshot, err := store.persistVerified(context.Background(), release, files, terminal)
	if err != nil {
		t.Fatal(err)
	}
	record, err := client.CoreV1().ConfigMaps(release.Workload.Namespace).Get(context.Background(), snapshot.RecordName, metav1.GetOptions{})
	if err != nil || record.Immutable == nil || !*record.Immutable || record.Data["record.json"] == "" || record.Data["lkg.json"] == "" {
		t.Fatalf("immutable monitor record is incomplete: record=%+v err=%v", record, err)
	}
	stateMap, err := client.CoreV1().ConfigMaps(release.Workload.Namespace).Get(context.Background(), snapshot.StateName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stateMap.SetUID("monitor-state-uid")
	stateMap.SetResourceVersion("1")
	if _, err := client.CoreV1().ConfigMaps(release.Workload.Namespace).Update(context.Background(), stateMap, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.load(context.Background(), release.Workload.Namespace, release.ComponentID)
	if err != nil || loaded.Bundle.Record.RecordDigest != snapshot.Bundle.Record.RecordDigest || loaded.State.ConsecutiveFailures != 0 {
		t.Fatalf("load persisted monitor record: snapshot=%+v err=%v", loaded, err)
	}
	next, rollback, err := declarativerelease.NewMonitorState(loaded.Bundle.Record, loaded.State, false, "route failed", time.Date(2026, 8, 10, 19, 5, 0, 0, time.UTC))
	if err != nil || rollback {
		t.Fatalf("first monitor failure: state=%+v rollback=%t err=%v", next, rollback, err)
	}
	updated, err := store.updateState(context.Background(), loaded, next)
	if err != nil || updated.State.ConsecutiveFailures != 1 {
		t.Fatalf("CAS monitor state update: state=%+v err=%v", updated, err)
	}
	changed, err := client.CoreV1().ConfigMaps(release.Workload.Namespace).Get(context.Background(), loaded.StateName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	changed.SetResourceVersion("2")
	if _, err := client.CoreV1().ConfigMaps(release.Workload.Namespace).Update(context.Background(), changed, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.updateState(context.Background(), loaded, next); err == nil || !strings.Contains(err.Error(), "changed before") {
		t.Fatalf("stale monitor state resourceVersion was accepted: %v", err)
	}
}

func TestMonitorStoreRejectsTamperedVerifiedFiles(t *testing.T) {
	files, terminal, release := monitorBundleFixture(t)
	files["lkg.json"] = bytes.Replace(files["lkg.json"], []byte(strings.Repeat("a", 40)), []byte(strings.Repeat("c", 40)), 1)
	client := kubernetesfake.NewSimpleClientset()
	store := &monitorStore{client: client.CoreV1(), now: time.Now}
	if _, err := store.persistVerified(context.Background(), release, files, terminal); err == nil {
		t.Fatal("tampered monitor LKG bytes were persisted")
	}
}

func monitorBundleFixture(t *testing.T) (map[string][]byte, declarativerelease.ExecutionResult, declarativerelease.PlanRelease) {
	t.Helper()
	sha1 := strings.Repeat("a", 40)
	sha2 := strings.Repeat("b", 40)
	digest1 := "sha256:" + strings.Repeat("1", 64)
	digest2 := "sha256:" + strings.Repeat("2", 64)
	release := declarativerelease.PlanRelease{
		ComponentID: "api", ChangedPaths: []string{"cmd/fugue-api/main.go", "deploy/releases/api/intent.json"},
		IntentPath: "deploy/releases/api/intent.json", IntentDigest: "sha256:" + strings.Repeat("3", 64), IntentGeneration: 2,
		ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: sha1, ExpectedPreviousManifestSHA: sha1,
		ExpectedPreviousOCIRevision: sha1, ExpectedPreviousImageDigest: digest1,
		ManifestPath: "deploy/releases/api/deployment.json",
		Artifact:     declarativerelease.Artifact{Repository: "ghcr.io/example/fugue-api", Dockerfile: "Dockerfile.api", Context: ".", BuildPackage: "./cmd/fugue-api"},
		Workload:     declarativerelease.Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api", FieldManager: "fugue-api-declarative", Replicas: 2, RolloutMode: "rolling"},
		Health:       []declarativerelease.HealthProbe{{Type: "deployment", Name: "fugue-fugue-api"}}, Concurrency: "fugue-production-api",
	}
	plan := declarativerelease.Plan{APIVersion: declarativerelease.IntentAPIVersion, Kind: "ProductionReleasePlan", BaseSHA: sha1, HeadSHA: sha2, Releases: []declarativerelease.PlanRelease{release}}
	planRaw, err := declarativerelease.CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestBytesForMonitorTest(planRaw)
	verification := declarativerelease.RegistryVerification{
		Image: "ghcr.io/example/fugue-api@" + digest2, IndexDigest: digest2,
		ManifestDigest: "sha256:" + strings.Repeat("4", 64), ConfigDigest: "sha256:" + strings.Repeat("5", 64),
		OCIRevision: sha2, Platform: "linux/amd64", Verification: "registry_manifest_config_and_layer_get",
		BlobCount: 2, LayerProbeCount: 1, RequestCount: 4, TotalLayerBytes: 10,
	}
	artifact, err := declarativerelease.MaterializeArtifactReceipt(plan, "api", verification)
	if err != nil {
		t.Fatal(err)
	}
	manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2,"strategy":{"type":"RollingUpdate"},"template":{"metadata":{},"spec":{"containers":[{"image":"ghcr.io/example/fugue-api:old","name":"api"}]}}}}],"kind":"ComponentResourceSet"}`)
	rendered, err := declarativerelease.RenderManifests(plan, "api", artifact, bytes.NewReader(manifest), bytes.NewReader(manifest))
	if err != nil {
		t.Fatal(err)
	}
	lkg := monitorObservation(sha1, "ghcr.io/example/fugue-api@"+digest1, "10")
	forward := monitorObservation(sha2, artifact.ImmutableRef, "11")
	prepared := declarativerelease.ExecutionPlan{
		APIVersion: declarativerelease.ExecutionPlanAPIVersion, Kind: declarativerelease.ExecutionPlanKind,
		Component: "api", ConfigSHA: sha2, ReleasePlanDigest: plan.PlanDigest, IntentDigest: release.IntentDigest,
		ArtifactDigest: artifact.ReceiptDigest,
		Forward:        declarativerelease.TargetIdentity{Present: true, ImageRef: artifact.ImmutableRef, ConfigSHA: sha2, ManifestSHA: sha2, OCIRevision: sha2, ManifestDigest: rendered.ForwardDigest},
		LKG:            declarativerelease.TargetIdentity{Present: true, ImageRef: "ghcr.io/example/fugue-api@" + digest1, ConfigSHA: sha1, ManifestSHA: sha1, OCIRevision: sha1, ManifestDigest: rendered.LKGDigest},
		Prewrite:       lkg, PreparedAt: time.Date(2026, 8, 10, 18, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}
	preparedRaw, err := declarativerelease.CanonicalJSON(prepared)
	if err != nil {
		t.Fatal(err)
	}
	prepared.PlanDigest = digestBytesForMonitorTest(preparedRaw)
	terminal := declarativerelease.ExecutionResult{
		APIVersion: declarativerelease.ExecutionPlanAPIVersion, Kind: declarativerelease.ExecutionResultKind,
		Component: "api", ConfigSHA: sha2, ExecutionPlanDigest: prepared.PlanDigest,
		Status: "verified", Reason: "forward-verified", ForwardApplyCount: 1, Final: forward,
	}
	terminalRaw, err := declarativerelease.CanonicalJSON(terminal)
	if err != nil {
		t.Fatal(err)
	}
	terminal.ReceiptDigest = digestBytesForMonitorTest(terminalRaw)
	files := map[string][]byte{}
	for name, value := range map[string]any{"release-plan.json": plan, "artifact-receipt.json": artifact, "execution-plan.json": prepared} {
		raw, encodeErr := declarativerelease.CanonicalJSON(value)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		files[name] = raw
	}
	files["forward.json"] = rendered.Forward
	files["lkg.json"] = rendered.LKG
	return files, terminal, release
}

func monitorObservation(revision, image, rv string) declarativerelease.Observation {
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api"}
	return declarativerelease.Observation{
		Present: true, Primary: identity, UID: "uid-api", ResourceVersion: rv, Generation: 4, ObservedGeneration: 4,
		Desired: 2, Updated: 2, Ready: 2, Available: 2,
		ImageRef: image, ImageID: "sha256:" + strings.Repeat("6", 64), ConfigSHA: revision, ManifestSHA: revision, OCIRevision: revision,
		TemplateDigest: "sha256:" + strings.Repeat("7", 64), HealthDigest: "sha256:" + strings.Repeat("8", 64), FieldManagers: []string{"fugue-api-declarative"},
		Resources: []declarativerelease.ResourceObservation{{Identity: identity, Present: true, UID: "uid-api", ResourceVersion: rv, Generation: 4, ObjectDigest: "sha256:" + strings.Repeat("9", 64), FieldManagers: []string{"fugue-api-declarative"}}},
	}
}

func digestBytesForMonitorTest(raw []byte) string {
	return fmt.Sprintf("sha256:%x", sha256Sum(raw))
}

func sha256Sum(raw []byte) [32]byte {
	return sha256.Sum256(raw)
}
