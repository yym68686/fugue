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
	if len(set.Items) != 8 {
		t.Fatalf("resource count=%d", len(set.Items))
	}
	encoded, _ := json.Marshal(set)
	source := string(encoded)
	for _, required := range []string{
		`"name":"fugue-release-guardian"`, `"name":"fugue-release-canary-prober"`,
		`"value":"write"`, `"value":"guardian"`, `"value":"canary-prober"`,
		`"value":"edge-control-de,de,fugue-system,edge-control-de,fugue-fugue;edge-worker-de,de,fugue-system,edge-worker-de,edge-control-de;edge-control-us,us,fugue-system,edge-control-us,fugue-fugue"`,
		`"value":"edge-control-de,de,51.38.126.103:443,fugue.pro,/healthz,ok,10;edge-worker-de,de,51.38.126.103:443,fugue.pro,/healthz,ok,10;edge-control-us,us,15.204.94.71:443,fugue.pro,/healthz,ok,10"`,
		`"fieldPath":"metadata.uid"`, `"mountPath":"/tmp"`,
		`"resources":["configmaps"],"verbs":["create","get","update"]`,
		`"resourceNames":["fugue-api-route-intent-ca-de","fugue-api-route-intent-ca-us","fugue-edge-control-inventory-writer-de","fugue-edge-control-inventory-writer-us","fugue-edge-control-reader-de","fugue-edge-control-reader-us","fugue-edge-control-recovery-de","fugue-edge-control-recovery-us","fugue-edge-control-route-intent-identity-de","fugue-edge-control-route-intent-identity-us","fugue-edge-control-signing-de","fugue-edge-control-signing-us","fugue-edge-token-vps-84c8f0a9","fugue-edge-worker-reader-de","fugue-fugue-config"],"resources":["secrets"],"verbs":["get"]`,
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
	client := fake.NewSimpleClientset(objects...)
	store := &KubeStore{client: client}
	if health := store.localHealth(context.Background(), release, target, now); health.State != HealthHealthy {
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
	health := store.localHealth(context.Background(), release, target, now)
	if health.State != HealthDegraded || !strings.Contains(health.Reason, "worker-b") {
		t.Fatalf("unhealthy auxiliary worker was not attributed: %+v", health)
	}
}
