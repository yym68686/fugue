package releaseguardian

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestLoadStableLKGIgnoresBrokenCandidateStateAndRejectsIdentityDrift(t *testing.T) {
	key := Key{Component: "edge-control-test", Group: "test"}
	now := time.Unix(100, 0).UTC()
	data, monitor, artifact, target := guardianStableFixture(t, key, now)
	recordName := monitorRecordNameFromDigest(key.Component, monitor.RecordDigest)
	immutable := true
	state := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "fugue-release-monitor-" + key.Component, Namespace: "fugue-system"},
		Data:       map[string]string{"recordName": recordName},
	}
	recordMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: recordName, Namespace: "fugue-system"},
		Immutable:  &immutable, Data: data,
	}
	client := fake.NewSimpleClientset(state, recordMap,
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: desiredName(key), Namespace: "fugue-system"}, Data: map[string]string{"desired.json": "broken candidate"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: statusName(key), Namespace: "fugue-system"}, Data: map[string]string{"status.json": "broken status"}},
	)
	store, err := NewKubeStore(client, []TargetConfig{{Key: key, Namespace: "fugue-system", MonitorComponent: key.Component, DependencyService: "service"}})
	if err != nil {
		t.Fatal(err)
	}
	candidate := guardianCandidateFixture(t, key, target, artifact.TopDigest, []byte(data["forward.json"]), now)
	plan, err := declarativerelease.DecodePlan(bytes.NewReader(candidate["release-plan.json"]))
	if err != nil {
		t.Fatal(err)
	}
	release := plan.Releases[0]
	lkg, err := store.LoadStableLKG(context.Background(), key, release)
	if err != nil || !bytes.Equal(lkg, bytes.TrimSpace([]byte(data["forward.json"]))) {
		t.Fatalf("positive LKG was blocked by candidate state: %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" {
			t.Fatalf("read-only LKG validation mutated the cluster: %v", action)
		}
	}
	for _, test := range []struct {
		name string
		edit func(*declarativerelease.PlanRelease)
	}{
		{"config SHA", func(r *declarativerelease.PlanRelease) { r.ExpectedPreviousConfigSHA = strings.Repeat("9", 40) }},
		{"manifest SHA", func(r *declarativerelease.PlanRelease) { r.ExpectedPreviousManifestSHA = strings.Repeat("9", 40) }},
		{"OCI revision", func(r *declarativerelease.PlanRelease) { r.ExpectedPreviousOCIRevision = strings.Repeat("9", 40) }},
		{"image digest", func(r *declarativerelease.PlanRelease) { r.ExpectedPreviousImageDigest = testDigest }},
		{"image reference", func(r *declarativerelease.PlanRelease) { r.Artifact.Repository = "ghcr.io/example/other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			drifted := release
			test.edit(&drifted)
			_, err := store.LoadStableLKG(context.Background(), key, drifted)
			if err == nil || !strings.Contains(err.Error(), test.name) || !strings.Contains(err.Error(), monitor.RecordDigest) {
				t.Fatalf("identity drift lacks a record-bound error: %v", err)
			}
		})
	}
	wrongName := monitorRecordNameFromDigest(key.Component, otherDigest)
	recordMap = recordMap.DeepCopy()
	recordMap.Name = wrongName
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Create(context.Background(), recordMap, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	state.Data["recordName"] = wrongName
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Update(context.Background(), state, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadStableLKG(context.Background(), key, release); err == nil || !strings.Contains(err.Error(), "pointer does not match") {
		t.Fatalf("mismatched stable record pointer was accepted: %v", err)
	}
}

func TestLoadStableLKGRecoversRecordedLKGFromSupersededMonitorAtom(t *testing.T) {
	key := Key{Component: "edge-control-test", Group: "test"}
	now := time.Unix(100, 0).UTC()
	stableData, _, stableArtifact, stableTarget := guardianStableFixture(t, key, now)
	stableForward := []byte(stableData["forward.json"])
	targetSHA := strings.Repeat("2", 40)
	release := guardianPlanRelease(key, targetSHA, stableTarget.ConfigSHA, stableArtifact.TopDigest, &declarativerelease.Delivery{Writer: "guardian", Group: key.Group, DependencyService: "service"})
	release.SupersedesFailedConfigSHA = targetSHA
	plan := guardianPlan(t, stableTarget.ConfigSHA, targetSHA, release)
	artifact := guardianArtifact(t, plan, "sha256:"+strings.Repeat("c", 64))
	forward, _ := guardianManifests(t, plan, artifact)
	prepared := guardianPrepared(t, plan, release, artifact,
		declarativerelease.TargetIdentity{Present: true, ImageRef: artifact.ImmutableRef, ConfigSHA: targetSHA, ManifestSHA: targetSHA, OCIRevision: targetSHA, ManifestDigest: digest(forward)},
		stableTarget, now)
	terminal := declarativerelease.ExecutionResult{APIVersion: declarativerelease.ExecutionPlanAPIVersion, Kind: declarativerelease.ExecutionResultKind, Component: key.Component, ConfigSHA: targetSHA, ExecutionPlanDigest: prepared.PlanDigest, Status: "verified", Reason: "forward-verified", ForwardApplyCount: 1, Final: guardianObservation(prepared.Forward)}
	terminalRaw, _ := declarativerelease.CanonicalJSON(terminal)
	terminal.ReceiptDigest = digest(terminalRaw)
	monitor, err := declarativerelease.NewMonitorRecord(plan, artifact, prepared, terminal, forward, stableForward)
	if err != nil {
		t.Fatalf("new failed monitor record: %v", err)
	}
	data := map[string]string{"forward.json": string(forward), "lkg.json": string(stableForward)}
	for name, value := range map[string]any{"release-plan.json": plan, "artifact-receipt.json": artifact, "execution-plan.json": prepared, "record.json": monitor, "terminal-result.json": terminal} {
		raw, _ := declarativerelease.CanonicalJSON(value)
		data[name] = string(raw)
	}
	immutable := true
	recordName := monitorRecordNameFromDigest(key.Component, monitor.RecordDigest)
	client := fake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "fugue-release-monitor-" + key.Component, Namespace: "fugue-system"}, Data: map[string]string{"recordName": recordName}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: recordName, Namespace: "fugue-system"}, Immutable: &immutable, Data: data},
	)
	store, err := NewKubeStore(client, []TargetConfig{{Key: key, Namespace: "fugue-system", MonitorComponent: key.Component, DependencyService: "service"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.LoadStableLKG(context.Background(), key, release)
	if err != nil || !bytes.Equal(got, bytes.TrimSpace(stableForward)) {
		t.Fatalf("recorded LKG recovery failed: %v", err)
	}
}

func TestCanaryResultIsImmutableRecordBoundAndFresh(t *testing.T) {
	key := Key{Component: "edge-control-de", Group: "de"}
	record, err := NewReleaseRecord(key, testSHA, testDigest, testDigest, otherDigest, testDigest)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	result, err := NewCanaryResult(record, HealthHealthy, testDigest, now, now.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Validate(now.Add(29 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := result.Validate(now.Add(31 * time.Second)); err == nil {
		t.Fatal("expired canary result was accepted")
	}
	result.RecordDigest = otherDigest
	if err := result.Validate(now); err == nil {
		t.Fatal("record drift did not invalidate canary result digest")
	}
}

func TestObjectNamesRemainComponentAndGroupScoped(t *testing.T) {
	de := Key{Component: "edge-control", Group: "edge-pool-a"}
	us := Key{Component: "edge-control", Group: "edge-pool-b"}
	if statusName(de) == statusName(us) || canaryName(de) == canaryName(us) {
		t.Fatal("group-scoped objects collided")
	}
}

func TestGuardianWriterResourcesKeepIndependentProberAndComponentScopedRBAC(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/releases/guardian/resources.json")
	if err != nil {
		t.Fatal(err)
	}
	set, err := declarativerelease.DecodeResourceSet(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Items) != 10 {
		t.Fatalf("resource count=%d", len(set.Items))
	}
	encoded, _ := json.Marshal(set)
	source := string(encoded)
	if strings.Contains(source, `"name":"FUGUE_RELEASE_GUARDIAN_AUTHORITY_BASELINES"`) {
		t.Fatal("one-time authority baseline importer configuration is still present")
	}
	for _, required := range []string{
		`"name":"fugue-release-guardian"`, `"name":"fugue-release-canary-prober"`,
		`"value":"write"`, `"value":"guardian"`, `"value":"canary-prober"`,
		`"value":"edge-control-de,de,fugue-system,edge-control-de,fugue-fugue;edge-worker-de,de,fugue-system,edge-worker-de,edge-control-de;edge-control-us,us,fugue-system,edge-control-us,fugue-fugue;edge-worker-us,us,fugue-system,edge-worker-us,edge-control-us;api,global,fugue-system,api,fugue-fugue"`,
		`"value":"edge-control-de,de,51.38.126.103:443,fugue.pro,/healthz,ok,10;edge-worker-de,de,51.38.126.103:443,fugue.pro,/healthz,ok,10;edge-control-us,us,15.204.94.71:443,fugue.pro,/healthz,ok,10;edge-worker-us,us,15.204.94.71:443,fugue.pro,/healthz,ok,10;api,global,15.204.94.71:443,api.fugue.pro,/healthz,ok,10"`,
		`"name":"FUGUE_RELEASE_GUARDIAN_CANDIDATE_CANARY"`,
		`"value":"edge-group-country-de,51.38.126.103:18443,51.38.126.103:28443,edge-observe-canary-0731-1333.fugue.pro,/,sha256:fb47468a2cd3953c7131431991afcc6a2703f14640520102eea0a685a7e8d6de,10,candidate-canary-de-v1,/var/run/secrets/fugue-candidate-canary-de/token;edge-group-country-us,15.204.94.71:18443,15.204.94.71:28443,edge-observe-canary-0731-1333.fugue.pro,/,sha256:fb47468a2cd3953c7131431991afcc6a2703f14640520102eea0a685a7e8d6de,10,candidate-canary-us-v1,/var/run/secrets/fugue-candidate-canary-us/token"`,
		`"mountPath":"/var/run/secrets/fugue-candidate-canary-de"`,
		`"mountPath":"/var/run/secrets/fugue-candidate-canary-us"`,
		`"secretName":"fugue-edge-worker-reader-de"`,
		`"secretName":"fugue-edge-worker-reader-us"`,
		`"fieldPath":"metadata.uid"`, `"mountPath":"/tmp"`,
		`"fugue.io/edge-authority-importer":"true"`,
		`"name":"FUGUE_RELEASE_GUARDIAN_CANDIDATE_IMPORTS","value":"edge-group-country-de,http://edge-control-de.fugue-system.svc:8092/v1/edge/candidate-envelope,/var/run/secrets/fugue-candidate-import-de/token;edge-group-country-us,http://edge-control-us.fugue-system.svc:8092/v1/edge/candidate-envelope,/var/run/secrets/fugue-candidate-import-us/token"`,
		`"name":"FUGUE_RELEASE_GUARDIAN_AUTHORITY_PUBLIC_KEY_SOURCE","value":"/var/run/secrets/fugue-candidate-import-de/token,/tmp/candidate-canary-de.pub;/var/run/secrets/fugue-candidate-import-us/token,/tmp/candidate-canary-us.pub"`,
		`"name":"FUGUE_RELEASE_GUARDIAN_AUTHORITY_GROUPS","value":"edge-group-country-de,candidate-canary-de-v1,/tmp/candidate-canary-de.pub;edge-group-country-us,candidate-canary-us-v1,/tmp/candidate-canary-us.pub"`,
		`"name":"FUGUE_RELEASE_GUARDIAN_AUTHORITY_ACTIVATORS","value":"edge-group-country-de,http://edge-control-de.fugue-system.svc:8092,/var/run/secrets/fugue-authority-recovery-de/keyring.json,1,51.38.126.103:443,edge-observe-canary-0731-1333.fugue.pro,/,sha256:fb47468a2cd3953c7131431991afcc6a2703f14640520102eea0a685a7e8d6de,51.38.126.103:18443,51.38.126.103:28443;edge-group-country-us,http://edge-control-us.fugue-system.svc:8092,/var/run/secrets/fugue-authority-recovery-us/keyring.json,2,15.204.94.71:443,edge-observe-canary-0731-1333.fugue.pro,/,sha256:fb47468a2cd3953c7131431991afcc6a2703f14640520102eea0a685a7e8d6de,15.204.94.71:18443,15.204.94.71:28443"`,
		`"mountPath":"/var/run/secrets/fugue-authority-recovery-de"`,
		`"mountPath":"/var/run/secrets/fugue-authority-recovery-us"`,
		`"mountPath":"/var/run/secrets/fugue-candidate-import-us"`,
		`"secretName":"fugue-edge-control-recovery-de"`,
		`"secretName":"fugue-edge-control-recovery-us"`,
		`"name":"fugue-release-guardian-edge-control-de-import"`,
		`"name":"fugue-release-guardian-edge-control-us-import"`,
		`"app.kubernetes.io/instance":"edge-control-de"`,
		`"app.kubernetes.io/instance":"edge-control-us"`,
		`"fugue.io/edge-group-id":"edge-group-country-de"`,
		`"fugue.io/edge-group-id":"edge-group-country-us"`,
		`"port":8092,"protocol":"TCP"`,
		`"resources":["configmaps"],"verbs":["create","delete","get","list","update"]`,
		`"resources":["configmaps"],"verbs":["create","delete","get","list","update","watch"]`,
		`"resourceNames":["fugue-authority-transition-activated-edge-group-country-de","fugue-authority-transition-prepared-edge-group-country-de","fugue-authority-transition-activated-edge-group-country-us","fugue-authority-transition-prepared-edge-group-country-us"],"resources":["configmaps"],"verbs":["delete"]`,
		`"name":"FUGUE_RELEASE_GUARDIAN_ARTIFACT_MINIMUM_AGE","value":"24h"`,
		`"name":"FUGUE_RELEASE_GUARDIAN_ARTIFACT_MINIMUM_HISTORY","value":"32"`,
		`"name":"FUGUE_RELEASE_GUARDIAN_ARTIFACT_MAXIMUM_DELETES","value":"512"`,
		`"name":"FUGUE_RELEASE_GUARDIAN_ARTIFACT_PRUNE_INTERVAL","value":"1m"`,
		`"resources":["pods"],"verbs":["get","list"]`,
		`"resourceNames":["fugue-api-route-intent-ca-de","fugue-api-route-intent-ca-us","fugue-api-tls","fugue-app-wildcard-tls","fugue-edge-control-inventory-writer-de","fugue-edge-control-inventory-writer-us","fugue-edge-control-reader-de","fugue-edge-control-reader-us","fugue-edge-control-recovery-de","fugue-edge-control-recovery-us","fugue-edge-control-route-intent-identity-de","fugue-edge-control-route-intent-identity-us","fugue-edge-control-signing-de","fugue-edge-control-signing-us","fugue-edge-route-intent-identity","fugue-edge-token-vps-591f4447","fugue-edge-token-vps-84c8f0a9","fugue-edge-worker-reader-de","fugue-edge-worker-reader-us","fugue-fugue-config","fugue-fugue-edge-activation-signing-v1","fugue-fugue-platform-component-identity"],"resources":["secrets"],"verbs":["get"]`,
		`"resources":["events"],"verbs":["get","list","watch"]`,
		`"resources":["pods"],"verbs":["delete","get","list","watch"]`,
		`"resources":["pods/exec"],"verbs":["create"]`,
		`"resources":["deployments"],"verbs":["create","delete","get","list","patch","update","watch"]`,
		`"resources":["daemonsets"],"verbs":["create","delete","get","list","patch","update","watch"]`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("Guardian resources lack %s", required)
		}
	}
	if strings.Count(source, `"resources":["secrets"]`) != 1 {
		t.Fatal("Guardian Secret metadata access is not one exact resourceNames-scoped rule")
	}
	guardianConfigMapRules := 0
	guardianGeneralDelete := false
	guardianTransitionDelete := false
	for _, item := range set.Items {
		metadata, _ := item["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		if item["kind"] != "Role" || name != "fugue-release-guardian" {
			continue
		}
		var role struct {
			Rules []struct {
				Resources     []string `json:"resources"`
				ResourceNames []string `json:"resourceNames"`
				Verbs         []string `json:"verbs"`
			} `json:"rules"`
		}
		itemRaw, _ := json.Marshal(item)
		_ = json.Unmarshal(itemRaw, &role)
		for _, rule := range role.Rules {
			if len(rule.Resources) == 1 && rule.Resources[0] == "configmaps" {
				guardianConfigMapRules++
				switch {
				case len(rule.ResourceNames) == 0 && strings.Join(rule.Verbs, ",") == "create,delete,get,list,update,watch":
					guardianGeneralDelete = true
				case strings.Join(rule.ResourceNames, ",") == "fugue-authority-transition-activated-edge-group-country-de,fugue-authority-transition-prepared-edge-group-country-de,fugue-authority-transition-activated-edge-group-country-us,fugue-authority-transition-prepared-edge-group-country-us" && strings.Join(rule.Verbs, ",") == "delete":
					guardianTransitionDelete = true
				default:
					t.Fatal("Guardian ConfigMap retention permission is not the exact bounded writer rule")
				}
			}
		}
	}
	if guardianConfigMapRules != 2 || !guardianGeneralDelete || !guardianTransitionDelete {
		t.Fatal("Guardian ConfigMap permissions do not preserve the transition bridge and retention writer")
	}
	for _, forbidden := range []string{`"resources":["events","pods"]`, `"resources":["pods"],"verbs":["*"]`, `"resources":["pods/exec"],"verbs":["*"]`, `"daemonsets/status"`, `"deployments/status"`, `"clusterroles"`, `"clusterrolebindings"`} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("shadow Guardian resources grant forbidden capability %s", forbidden)
		}
	}
}

func TestLocalHealthCoversEveryDeclaredDaemonSetArtifact(t *testing.T) {
	now := time.Unix(500, 0).UTC()
	image := "ghcr.io/example/fugue-edge@" + testDigest
	target := declarativerelease.TargetIdentity{ConfigSHA: testSHA, ManifestSHA: testSHA, OCIRevision: testSHA, ImageRef: image}
	release := declarativerelease.PlanRelease{
		ComponentID: "edge-worker-de",
		Workload:    declarativerelease.Workload{Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-front", Container: "edge-front"},
		ArtifactTargets: []declarativerelease.ArtifactTarget{
			{Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-front", Container: "edge-front", ContainerType: "container"},
			{Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-a", Container: "edge", ContainerType: "container"},
			{Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-a", Container: "identity", ContainerType: "init-container"},
			{Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-b", Container: "edge", ContainerType: "container"},
			{Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-b", Container: "identity", ContainerType: "init-container"},
		},
		Health: []declarativerelease.HealthProbe{{Type: "daemonset", Name: "edge-front"}, {Type: "daemonset", Name: "worker-a"}, {Type: "daemonset", Name: "worker-b"}},
	}
	objects := []runtime.Object{}
	for _, fixture := range []struct {
		name       string
		containers []string
		init       []string
	}{{"edge-front", []string{"edge-front"}, nil}, {"worker-a", []string{"edge"}, []string{"identity"}}, {"worker-b", []string{"edge"}, []string{"identity"}}} {
		labels := map[string]string{"fixture": fixture.name}
		containers := make([]corev1.Container, 0, len(fixture.containers))
		statuses := make([]corev1.ContainerStatus, 0, len(fixture.containers))
		for _, name := range fixture.containers {
			containers = append(containers, corev1.Container{Name: name, Image: image})
			statuses = append(statuses, corev1.ContainerStatus{Name: name, ImageID: image, Ready: true})
		}
		initContainers := make([]corev1.Container, 0, len(fixture.init))
		initStatuses := make([]corev1.ContainerStatus, 0, len(fixture.init))
		for _, name := range fixture.init {
			initContainers = append(initContainers, corev1.Container{Name: name, Image: image})
			initStatuses = append(initStatuses, corev1.ContainerStatus{Name: name, ImageID: image, Ready: true})
		}
		objects = append(objects,
			&appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: fixture.name, Namespace: "fugue-system", Generation: 1, Annotations: map[string]string{"fugue.pro/production-config-sha": testSHA}},
				Spec:       appsv1.DaemonSetSpec{Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: map[string]string{"fugue.pro/oci-revision": testSHA}}, Spec: corev1.PodSpec{Containers: containers, InitContainers: initContainers}}},
				Status:     appsv1.DaemonSetStatus{ObservedGeneration: 1, DesiredNumberScheduled: 1, CurrentNumberScheduled: 1, UpdatedNumberScheduled: 1, NumberReady: 1, NumberAvailable: 1},
			},
			&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: fixture.name + "-pod", Namespace: "fugue-system", Labels: labels},
				Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: statuses, InitContainerStatuses: initStatuses},
			},
		)
	}
	workerBPod := objects[len(objects)-1].(*corev1.Pod)
	workerBPod.Status.ContainerStatuses[0].RestartCount = 3
	client := fake.NewSimpleClientset(objects...)
	store := &KubeStore{client: client}
	if health := store.localHealth(context.Background(), release, target, nil, now); health.State != HealthHealthy {
		t.Fatalf("healthy component classified as %+v", health)
	}
	worker, err := client.AppsV1().DaemonSets("fugue-system").Get(context.Background(), "worker-b", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	worker.Status.NumberReady = 0
	if _, err := client.AppsV1().DaemonSets("fugue-system").UpdateStatus(context.Background(), worker, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	health := store.localHealth(context.Background(), release, target, nil, now)
	if health.State != HealthDegraded || !strings.Contains(health.Reason, "worker-b") {
		t.Fatalf("unhealthy auxiliary worker was not attributed: %+v", health)
	}
}

func TestLocalHealthUsesPodTemplateLabelsForBroadDeploymentSelector(t *testing.T) {
	now := time.Unix(600, 0).UTC()
	image := "ghcr.io/example/fugue-api@" + testDigest
	target := declarativerelease.TargetIdentity{ConfigSHA: testSHA, ManifestSHA: testSHA, OCIRevision: testSHA, ImageRef: image}
	release := declarativerelease.PlanRelease{
		ComponentID: "api",
		Workload: declarativerelease.Workload{
			Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api",
		},
		Health: []declarativerelease.HealthProbe{{Type: "deployment", Name: "fugue-fugue-api"}},
	}
	broad := map[string]string{"app.kubernetes.io/instance": "fugue", "app.kubernetes.io/name": "fugue"}
	apiLabels := map[string]string{"app.kubernetes.io/component": "api", "app.kubernetes.io/instance": "fugue", "app.kubernetes.io/name": "fugue"}
	controllerLabels := map[string]string{"app.kubernetes.io/component": "controller", "app.kubernetes.io/instance": "fugue", "app.kubernetes.io/name": "fugue"}
	readyPod := func(name string, podLabels map[string]string, container string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "fugue-system", Labels: podLabels},
			Status: corev1.PodStatus{
				Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
				ContainerStatuses: []corev1.ContainerStatus{{Name: container, ImageID: image, Ready: true}},
			},
		}
	}
	replicas := int32(2)
	apiOne := readyPod("api-1", apiLabels, "api")
	apiOne.Status.ContainerStatuses[0].RestartCount = 3
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "fugue-fugue-api", Namespace: "fugue-system", Generation: 1, Annotations: map[string]string{"fugue.pro/production-config-sha": testSHA}},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: broad},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: apiLabels, Annotations: map[string]string{"fugue.pro/oci-revision": testSHA}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: image}}}},
			},
			Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2},
		},
		apiOne,
		readyPod("api-2", apiLabels, "api"),
		readyPod("controller-1", controllerLabels, "controller"),
	)
	store := &KubeStore{client: client}
	if health := store.localHealth(context.Background(), release, target, nil, now); health.State != HealthHealthy {
		t.Fatalf("broad workload selector or historical restart changed health: %+v", health)
	}
	for _, phase := range []corev1.PodPhase{corev1.PodFailed, corev1.PodSucceeded} {
		terminal := readyPod("old-"+strings.ToLower(string(phase)), apiLabels, "api")
		terminal.Status.Phase = phase
		terminal.Status.Conditions = nil
		terminal.Status.ContainerStatuses = nil
		if _, err := client.CoreV1().Pods("fugue-system").Create(context.Background(), terminal, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if health := store.localHealth(context.Background(), release, target, nil, now); health.State != HealthHealthy {
		t.Fatalf("terminal historical pods blocked healthy replacement: %+v", health)
	}
	for _, phase := range []corev1.PodPhase{corev1.PodPending, corev1.PodUnknown} {
		active := readyPod("extra", apiLabels, "api")
		active.Status.Phase = phase
		if _, err := client.CoreV1().Pods("fugue-system").Create(context.Background(), active, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		if health := store.localHealth(context.Background(), release, target, nil, now); health.State != HealthDegraded {
			t.Fatalf("non-terminal %s pod was incorrectly ignored", phase)
		}
		if _, err := store.oneWorkloadHealth(context.Background(), release, target, "deployment", release.Workload.Name); err == nil {
			t.Fatal("component health ignored non-terminal pod")
		}
		if err := client.CoreV1().Pods("fugue-system").Delete(context.Background(), "extra", metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	pod, err := client.CoreV1().Pods("fugue-system").Get(context.Background(), "api-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses[0].ImageID = ""
	if _, err := client.CoreV1().Pods("fugue-system").UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	health := store.localHealth(context.Background(), release, target, nil, now)
	if health.State != HealthDegraded || !strings.Contains(health.Reason, "lacks immutable image identity") {
		t.Fatalf("missing runtime image identity was accepted: %+v", health)
	}
	pod.Status.ContainerStatuses[0].ImageID = image
	if _, err := client.CoreV1().Pods("fugue-system").UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods("fugue-system").Create(context.Background(), readyPod("api-collision", apiLabels, "api"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	health = store.localHealth(context.Background(), release, target, nil, now)
	if health.State != HealthDegraded || !strings.Contains(health.Reason, "inventory") {
		t.Fatalf("same-template label collision was not rejected: %+v", health)
	}
}
