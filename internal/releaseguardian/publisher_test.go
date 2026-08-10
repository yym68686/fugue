package releaseguardian

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestPublishDesiredCreatesImmutableRecordAndRejectsUnsettledSuccessor(t *testing.T) {
	now := time.Date(2026, 8, 10, 20, 0, 0, 0, time.UTC)
	key := Key{Component: "edge-control-de", Group: "de"}
	stableData, stableRecord, stableArtifact, stableTarget := guardianStableFixture(t, key, now)
	recordName := monitorRecordNameFromDigest(key.Component, stableRecord.RecordDigest)
	immutable := true
	state := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "fugue-release-monitor-edge-control-de", Namespace: "fugue-system", UID: types.UID("state-uid"), ResourceVersion: "10"},
		Data:       map[string]string{"recordName": recordName, "state.json": `{}`},
	}
	recordMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: recordName, Namespace: "fugue-system", Labels: map[string]string{"fugue.pro/component": key.Component}},
		Immutable:  &immutable, Data: stableData,
	}
	replicas := int32(1)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "edge-control-de", Namespace: "fugue-system", Annotations: map[string]string{"fugue.pro/production-config-sha": stableTarget.ConfigSHA}, Generation: 4},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "edge-control-de"}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"fugue.pro/oci-revision": stableTarget.OCIRevision}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "edge-control", Image: stableTarget.ImageRef}}}},
		},
		Status: appsv1.DeploymentStatus{ObservedGeneration: 4, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "edge-control-de-1", Namespace: "fugue-system", Labels: map[string]string{"app": "edge-control-de"}},
		Status: corev1.PodStatus{
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "edge-control", ImageID: stableTarget.ImageRef, RestartCount: 0}},
		},
	}
	ready := true
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "fugue-system", Labels: map[string]string{discoveryv1.LabelServiceName: "fugue-fugue"}},
		Endpoints:  []discoveryv1.Endpoint{{Conditions: discoveryv1.EndpointConditions{Ready: &ready}}},
	}
	client := kubernetesfake.NewSimpleClientset(state, recordMap, deployment, pod, slice)
	store, err := NewKubeStore(client, []TargetConfig{{Key: key, Namespace: "fugue-system", MonitorComponent: key.Component, DependencyService: "fugue-fugue"}})
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	healthRaw, _ := declarativerelease.CanonicalJSON(stableDataHealth(t, stableData))
	stableGuardian, err := NewReleaseRecord(key, stableTarget.ConfigSHA, stableArtifact.TopDigest, stableRecord.ForwardManifestDigest, stableRecord.RecordDigest, digest(healthRaw))
	if err != nil {
		t.Fatal(err)
	}
	canary, err := NewCanaryResult(stableGuardian, HealthHealthy, testDigest, now, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	canaryRaw, _ := declarativerelease.CanonicalJSON(canary)
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Create(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: canaryName(key), Namespace: "fugue-system"}, Data: map[string]string{"result.json": string(canaryRaw)}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	candidate := guardianCandidateFixture(t, key, stableTarget, stableArtifact.TopDigest, []byte(stableData["forward.json"]), now)
	record, desired, err := store.PublishDesired(context.Background(), key, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if desired.RecordDigest != record.RecordDigest || desired.Generation != 2 || record.LKGRecordDigest != stableGuardian.RecordDigest {
		t.Fatalf("record=%+v desired=%+v", record, desired)
	}
	created, err := client.CoreV1().ConfigMaps("fugue-system").Get(context.Background(), releaseRecordName(key, record.RecordDigest), metav1.GetOptions{})
	if err != nil || created.Immutable == nil || !*created.Immutable || created.Data["lkg-monitor-record-digest"] != stableRecord.RecordDigest {
		t.Fatalf("immutable candidate=%+v err=%v", created, err)
	}
	if _, _, err := store.PublishDesired(context.Background(), key, candidate); err == nil || !strings.Contains(err.Error(), "not a healthy settled release") {
		t.Fatalf("unsettled successor was accepted: %v", err)
	}
	desiredMap, err := client.CoreV1().ConfigMaps("fugue-system").Get(context.Background(), desiredName(key), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	desiredMap.ResourceVersion = "20"
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Update(context.Background(), desiredMap, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Load(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Record.RecordDigest != record.RecordDigest || snapshot.LKGMonitorRecordDigest != stableRecord.RecordDigest {
		t.Fatalf("candidate snapshot=%+v", snapshot)
	}
	if err := store.SetDesiredToLKG(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	rolledBack, err := client.CoreV1().ConfigMaps("fugue-system").Get(context.Background(), desiredName(key), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var restored DesiredRelease
	if decodeStrict([]byte(rolledBack.Data["desired.json"]), &restored) != nil || restored.RecordDigest != stableGuardian.RecordDigest || restored.Generation != 3 {
		t.Fatalf("restored DesiredRelease=%+v", restored)
	}
	if err := store.SetDesiredToLKG(context.Background(), snapshot); err == nil {
		t.Fatalf("stale LKG CAS was accepted: %v", err)
	}
}

func TestWaitForTerminalRecognizesSuccessAndExactLKGCompensation(t *testing.T) {
	now := time.Date(2026, 8, 10, 21, 0, 0, 0, time.UTC)
	key := Key{Component: "edge-control-de", Group: "de"}
	candidate := DesiredRelease{APIVersion: APIVersion, Kind: DesiredReleaseKind, Component: key.Component, Group: key.Group, RecordDigest: testDigest, Generation: 2}
	health := HealthSnapshot{
		Local:      LayerHealth{State: HealthHealthy, EvidenceDigest: testDigest, ObservedAt: now.Format(time.RFC3339Nano)},
		Dependency: LayerHealth{State: HealthHealthy, EvidenceDigest: testDigest, ObservedAt: now.Format(time.RFC3339Nano)},
		Route:      LayerHealth{State: HealthHealthy, EvidenceDigest: testDigest, ObservedAt: now.Format(time.RFC3339Nano)},
	}
	for _, test := range []struct {
		name       string
		status     ReleaseStatus
		desired    DesiredRelease
		wantErr    bool
		wantReason string
	}{
		{
			name: "stable target",
			status: ReleaseStatus{Component: key.Component, Group: key.Group, State: StateStable, CurrentRecordDigest: testDigest,
				TargetRecordDigest: testDigest, LastSuccessfulLKG: otherDigest, Health: health, Reason: "forward verified", ObservedAt: now.Format(time.RFC3339Nano)},
			desired: candidate,
		},
		{
			name: "known LKG compensation",
			status: ReleaseStatus{Component: key.Component, Group: key.Group, State: StateLKGStable, CurrentRecordDigest: otherDigest,
				TargetRecordDigest: otherDigest, LastSuccessfulLKG: otherDigest, Health: health, Reason: "forward compensated", RolloutReceiptDigest: testDigest,
				ObservedAt: now.Format(time.RFC3339Nano)},
			desired: DesiredRelease{APIVersion: APIVersion, Kind: DesiredReleaseKind, Component: key.Component, Group: key.Group, RecordDigest: otherDigest, Generation: 3},
			wantErr: true, wantReason: "restored its LKG",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sealed, err := test.status.Seal()
			if err != nil {
				t.Fatal(err)
			}
			statusRaw, _ := declarativerelease.CanonicalJSON(sealed)
			desiredRaw, _ := declarativerelease.CanonicalJSON(test.desired)
			client := kubernetesfake.NewSimpleClientset(
				&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: statusName(key), Namespace: "fugue-system"}, Data: map[string]string{"status.json": string(statusRaw)}},
				&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: desiredName(key), Namespace: "fugue-system"}, Data: map[string]string{"desired.json": string(desiredRaw)}},
			)
			store, err := NewKubeStore(client, []TargetConfig{{Key: key, Namespace: "fugue-system", MonitorComponent: key.Component, DependencyService: "fugue-fugue"}})
			if err != nil {
				t.Fatal(err)
			}
			store.now = func() time.Time { return now }
			got, waitErr := store.WaitForTerminal(context.Background(), key, candidate, time.Second)
			if test.wantErr {
				if waitErr == nil || !strings.Contains(waitErr.Error(), test.wantReason) || got.StatusDigest != sealed.StatusDigest {
					t.Fatalf("status=%+v err=%v", got, waitErr)
				}
			} else if waitErr != nil || got.StatusDigest != sealed.StatusDigest {
				t.Fatalf("status=%+v err=%v", got, waitErr)
			}
		})
	}
}

func guardianStableFixture(t *testing.T, key Key, now time.Time) (map[string]string, declarativerelease.MonitorRecord, declarativerelease.ArtifactReceipt, declarativerelease.TargetIdentity) {
	t.Helper()
	baseSHA, stableSHA := strings.Repeat("0", 40), strings.Repeat("1", 40)
	baseDigest, stableDigest := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	release := guardianPlanRelease(key, stableSHA, baseSHA, baseDigest, nil)
	plan := guardianPlan(t, baseSHA, stableSHA, release)
	artifact := guardianArtifact(t, plan, stableDigest)
	forward, lkg := guardianManifests(t, plan, artifact)
	stableTarget := declarativerelease.TargetIdentity{Present: true, ImageRef: artifact.ImmutableRef, ConfigSHA: stableSHA, ManifestSHA: stableSHA, OCIRevision: stableSHA, ManifestDigest: digest(forward)}
	lkgTarget := declarativerelease.TargetIdentity{Present: true, ImageRef: "ghcr.io/example/edge-control@" + baseDigest, ConfigSHA: baseSHA, ManifestSHA: baseSHA, OCIRevision: baseSHA, ManifestDigest: digest(lkg)}
	prepared := guardianPrepared(t, plan, release, artifact, stableTarget, lkgTarget, now)
	terminal := declarativerelease.ExecutionResult{APIVersion: declarativerelease.ExecutionPlanAPIVersion, Kind: declarativerelease.ExecutionResultKind, Component: key.Component, ConfigSHA: stableSHA, ExecutionPlanDigest: prepared.PlanDigest, Status: "verified", Reason: "forward-verified", ForwardApplyCount: 1, Final: guardianObservation(stableTarget)}
	terminalRaw, _ := declarativerelease.CanonicalJSON(terminal)
	terminal.ReceiptDigest = digest(terminalRaw)
	monitor, err := declarativerelease.NewMonitorRecord(plan, artifact, prepared, terminal, forward, lkg)
	if err != nil {
		t.Fatal(err)
	}
	data := map[string]string{}
	for name, value := range map[string]any{"release-plan.json": plan, "artifact-receipt.json": artifact, "execution-plan.json": prepared, "record.json": monitor, "terminal-result.json": terminal} {
		raw, _ := declarativerelease.CanonicalJSON(value)
		data[name] = string(raw)
	}
	data["forward.json"], data["lkg.json"] = string(forward), string(lkg)
	return data, monitor, artifact, stableTarget
}

func guardianCandidateFixture(t *testing.T, key Key, stable declarativerelease.TargetIdentity, stableDigest string, stableManifest []byte, now time.Time) map[string][]byte {
	t.Helper()
	targetSHA, targetDigest := strings.Repeat("2", 40), "sha256:"+strings.Repeat("c", 64)
	delivery := &declarativerelease.Delivery{Writer: "guardian", Group: key.Group, DependencyService: "fugue-fugue"}
	release := guardianPlanRelease(key, targetSHA, stable.ConfigSHA, stableDigest, delivery)
	plan := guardianPlan(t, stable.ConfigSHA, targetSHA, release)
	artifact := guardianArtifact(t, plan, targetDigest)
	forward, _ := guardianManifests(t, plan, artifact)
	lkg := append([]byte(nil), stableManifest...)
	if digest(lkg) != stable.ManifestDigest {
		t.Fatalf("stable monitor manifest digest mismatch: got=%s want=%s", digest(lkg), stable.ManifestDigest)
	}
	target := declarativerelease.TargetIdentity{Present: true, ImageRef: artifact.ImmutableRef, ConfigSHA: targetSHA, ManifestSHA: targetSHA, OCIRevision: targetSHA, ManifestDigest: digest(forward)}
	prepared := guardianPrepared(t, plan, release, artifact, target, stable, now)
	if err := prepared.Validate(plan, forward, lkg); err != nil {
		t.Fatalf("candidate execution fixture is invalid: %v (forward=%s/%s lkg=%s/%s)", err, prepared.Forward.ManifestDigest, digest(forward), prepared.LKG.ManifestDigest, digest(lkg))
	}
	files := map[string][]byte{"forward.json": forward, "lkg.json": lkg}
	for name, value := range map[string]any{"release-plan.json": plan, "artifact-receipt.json": artifact, "execution-plan.json": prepared} {
		raw, _ := declarativerelease.CanonicalJSON(value)
		files[name] = raw
	}
	return files
}

func guardianPlanRelease(key Key, headSHA, previousSHA, previousDigest string, delivery *declarativerelease.Delivery) declarativerelease.PlanRelease {
	return declarativerelease.PlanRelease{
		ComponentID: key.Component, ChangedPaths: []string{"cmd/fugue-edge-control/main.go", "deploy/releases/edge-control-de/intent.json"},
		IntentPath: "deploy/releases/edge-control-de/intent.json", IntentDigest: "sha256:" + strings.Repeat("3", 64), IntentGeneration: 2,
		ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: previousSHA, ExpectedPreviousManifestSHA: previousSHA,
		ExpectedPreviousOCIRevision: previousSHA, ExpectedPreviousImageDigest: previousDigest,
		ManifestPath: "internal/edgecontrol/component/resources.authority.de.json", Delivery: delivery,
		Artifact: declarativerelease.Artifact{Repository: "ghcr.io/example/edge-control", Dockerfile: "Dockerfile.edge-control", Context: ".", BuildPackage: "./cmd/fugue-edge-control"},
		Workload: declarativerelease.Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de", Container: "edge-control", FieldManager: "edge-control-de-declarative", Replicas: 1, RolloutMode: "recreate"},
		Health:   []declarativerelease.HealthProbe{{Type: "deployment", Name: "edge-control-de"}}, Concurrency: "fugue-production-edge-control-de",
	}
}

func guardianPlan(t *testing.T, baseSHA, headSHA string, release declarativerelease.PlanRelease) declarativerelease.Plan {
	t.Helper()
	plan := declarativerelease.Plan{APIVersion: declarativerelease.IntentAPIVersion, Kind: "ProductionReleasePlan", BaseSHA: baseSHA, HeadSHA: headSHA, Releases: []declarativerelease.PlanRelease{release}}
	raw, _ := declarativerelease.CanonicalJSON(plan)
	plan.PlanDigest = digest(raw)
	return plan
}

func guardianArtifact(t *testing.T, plan declarativerelease.Plan, topDigest string) declarativerelease.ArtifactReceipt {
	t.Helper()
	receipt, err := declarativerelease.MaterializeArtifactReceipt(plan, plan.Releases[0].ComponentID, declarativerelease.RegistryVerification{
		Image: "ghcr.io/example/edge-control@" + topDigest, IndexDigest: topDigest,
		ManifestDigest: "sha256:" + strings.Repeat("4", 64), ConfigDigest: "sha256:" + strings.Repeat("5", 64),
		OCIRevision: plan.HeadSHA, Platform: "linux/amd64", Verification: "registry_manifest_config_and_layer_get",
		BlobCount: 2, LayerProbeCount: 1, RequestCount: 4, TotalLayerBytes: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func guardianManifests(t *testing.T, plan declarativerelease.Plan, artifact declarativerelease.ArtifactReceipt) ([]byte, []byte) {
	t.Helper()
	manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"edge-control-de","namespace":"fugue-system"},"spec":{"replicas":1,"strategy":{"type":"Recreate"},"template":{"metadata":{},"spec":{"containers":[{"image":"ghcr.io/example/edge-control:old","name":"edge-control"}]}}}}],"kind":"ComponentResourceSet"}`)
	rendered, err := declarativerelease.RenderManifests(plan, plan.Releases[0].ComponentID, artifact, bytes.NewReader(manifest), bytes.NewReader(manifest))
	if err != nil {
		t.Fatal(err)
	}
	return rendered.Forward, rendered.LKG
}

func guardianPrepared(t *testing.T, plan declarativerelease.Plan, release declarativerelease.PlanRelease, artifact declarativerelease.ArtifactReceipt, forward, lkg declarativerelease.TargetIdentity, now time.Time) declarativerelease.ExecutionPlan {
	t.Helper()
	prepared := declarativerelease.ExecutionPlan{
		APIVersion: declarativerelease.ExecutionPlanAPIVersion, Kind: declarativerelease.ExecutionPlanKind,
		Component: release.ComponentID, ConfigSHA: plan.HeadSHA, ReleasePlanDigest: plan.PlanDigest, IntentDigest: release.IntentDigest,
		ArtifactDigest: artifact.ReceiptDigest, Forward: forward, LKG: lkg, Prewrite: guardianObservation(lkg), PreparedAt: now.Format(time.RFC3339Nano),
	}
	raw, _ := declarativerelease.CanonicalJSON(prepared)
	prepared.PlanDigest = digest(raw)
	return prepared
}

func guardianObservation(target declarativerelease.TargetIdentity) declarativerelease.Observation {
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de"}
	imageID := target.ImageRef[strings.LastIndex(target.ImageRef, "@")+1:]
	return declarativerelease.Observation{
		Present: true, Primary: identity, UID: "uid", ResourceVersion: "10", Generation: 4, ObservedGeneration: 4,
		Desired: 1, Updated: 1, Ready: 1, Available: 1, ImageRef: target.ImageRef, ImageID: imageID,
		ConfigSHA: target.ConfigSHA, ManifestSHA: target.ManifestSHA, OCIRevision: target.OCIRevision,
		TemplateDigest: testDigest, HealthDigest: otherDigest, FieldManagers: []string{"edge-control-de-declarative"},
		Resources: []declarativerelease.ResourceObservation{{Identity: identity, Present: true, UID: "uid", ResourceVersion: "10", Generation: 4, ObjectDigest: testDigest, FieldManagers: []string{"edge-control-de-declarative"}}},
	}
}

func stableDataHealth(t *testing.T, data map[string]string) []declarativerelease.HealthProbe {
	t.Helper()
	plan, err := declarativerelease.DecodePlan(strings.NewReader(data["release-plan.json"]))
	if err != nil {
		t.Fatal(err)
	}
	return plan.Releases[0].Health
}
