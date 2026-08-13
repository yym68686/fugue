package main

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestAuthorityBaselineAdoptsExactServingFrontWithoutChangingWorkloads(t *testing.T) {
	now := time.Date(2026, 8, 13, 2, 0, 0, 0, time.UTC)
	group := "edge-pool-a"
	beforeDigest := "sha256:" + strings.Repeat("a", 64)
	source := strings.Repeat("b", 40)
	image := "sha256:" + strings.Repeat("c", 64)
	record, err := (releaseguardian.RouteBundleRecord{GroupID: group, Epoch: 41, BundleDigest: "sha256:" + strings.Repeat("d", 64),
		SourceSHA: source, ControlImageDigest: "sha256:" + strings.Repeat("e", 64), InventoryDigest: "sha256:" + strings.Repeat("f", 64),
		ManifestDigest: "sha256:" + strings.Repeat("1", 64), HealthContractDigest: "sha256:" + strings.Repeat("2", 64),
		IssuedAt: now.Add(-time.Minute).Format(time.RFC3339Nano), KeyID: "key-a", Signature: strings.Repeat("A", 43)}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	front := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "front-a", Namespace: "fugue-system", UID: types.UID("front-pod-uid"), ResourceVersion: "31",
		Labels: map[string]string{"fugue.io/edge-group-id": group, "app.kubernetes.io/component": "edge-front-a"}, Annotations: map[string]string{"fugue.pro/source-commit": source}},
		Spec: corev1.PodSpec{NodeName: "edge-node-a"}, Status: readyPodStatus("10.0.0.1", "edge-front", image)}
	worker := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-b", Namespace: "fugue-system", UID: types.UID("worker-pod-uid"), ResourceVersion: "32",
		Labels: map[string]string{"fugue.io/edge-group-id": group, "fugue.io/edge-slot": "b"}, Annotations: map[string]string{"fugue.pro/source-commit": source}},
		Spec: corev1.PodSpec{NodeName: "edge-node-a"}, Status: readyPodStatus("10.0.0.2", "edge", image)}
	client := fake.NewSimpleClientset(front, worker)
	store, _ := releaseguardian.NewAuthorityStore(client, "fugue-system")
	if err := store.CreateRouteBundleRecord(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	before := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: group, CurrentRecordDigest: beforeDigest, CurrentWorkerSlot: releaseguardian.AuthoritySlotA, AuthorityEpoch: 7}
	if _, _, err := store.SwitchCurrent(context.Background(), before, "", ""); err != nil {
		t.Fatal(err)
	}
	setMutableAuthorityFixture(t, client, "fugue-current-authority-"+group, "current-baseline", "40")

	oldRead, oldRoute := readAuthorityBaselineJSON, requestAuthorityBaselineRoute
	defer func() { readAuthorityBaselineJSON, requestAuthorityBaselineRoute = oldRead, oldRoute }()
	readAuthorityBaselineJSON = func(_ context.Context, endpoint string, destination any) error {
		switch value := destination.(type) {
		case *baselineFrontHealth:
			*value = baselineFrontHealth{Status: "ok", ActiveSlot: "b", Generation: 34,
				BundleGeneration: "routes-serving.p41.r0", WorkerSourceCommit: source, WorkerImageDigest: image, RouteAuthority: "edge-control"}
		case *baselineWorkerHealth:
			*value = baselineWorkerHealth{Healthy: true, EdgeGroupID: group, BundleVersion: "routes-serving.p41.r0", PublicationSequence: 41,
				ServingGeneration: "routes-serving", CandidateBundleLoaded: true, CandidateRecordDigest: record.RecordDigest, CandidateWorkerSlot: "b"}
		default:
			t.Fatalf("unexpected baseline endpoint %s", endpoint)
		}
		return nil
	}
	requestAuthorityBaselineRoute = func(context.Context, string, string, string) (int, []byte, http.Header, error) {
		headers := http.Header{}
		headers.Set("X-Fugue-Candidate-Record-Digest", record.RecordDigest)
		headers.Set("X-Fugue-Candidate-Worker-Slot", "b")
		return http.StatusOK, []byte("ok"), headers, nil
	}
	config := authorityBaselineConfig{GroupID: group, ExpectedRecordDigest: beforeDigest, ExpectedWorkerSlot: releaseguardian.AuthoritySlotA,
		ExpectedEpoch: 7, FrontComponent: "edge-front-a", ExpectedNodes: 1,
		SlotAddresses: map[releaseguardian.AuthoritySlot]string{releaseguardian.AuthoritySlotA: "127.0.0.1:18443", releaseguardian.AuthoritySlotB: "127.0.0.1:28443"},
		Host:          "route.example.test", Path: "/", ExpectedBodyDigest: shaDigest([]byte("ok"))}
	changed, err := adoptAuthorityBaselineOnce(context.Background(), store, client, "fugue-system", config, now)
	if err != nil || !changed {
		t.Fatalf("baseline adoption changed=%v err=%v", changed, err)
	}
	current, _, _, err := store.LoadCurrent(context.Background(), group)
	if err != nil || current.CurrentRecordDigest != record.RecordDigest || current.CurrentWorkerSlot != releaseguardian.AuthoritySlotB ||
		current.AuthorityEpoch != record.Epoch || current.BaselineReceiptDigest == "" {
		t.Fatalf("baseline current=%+v err=%v", current, err)
	}
	frontAfter, _ := client.CoreV1().Pods("fugue-system").Get(context.Background(), front.Name, metav1.GetOptions{})
	workerAfter, _ := client.CoreV1().Pods("fugue-system").Get(context.Background(), worker.Name, metav1.GetOptions{})
	if frontAfter.ResourceVersion != front.ResourceVersion || workerAfter.ResourceVersion != worker.ResourceVersion {
		t.Fatal("baseline adoption changed a workload")
	}
}

func TestAuthorityBaselineRejectsRouteAttestationDriftWithoutCAS(t *testing.T) {
	configs, err := parseAuthorityBaselines("edge-pool-a,sha256:" + strings.Repeat("a", 64) + ",a,7,edge-front-a,1,127.0.0.1:18443,127.0.0.1:28443,route.example.test,/,sha256:" + strings.Repeat("b", 64))
	if err != nil || len(configs) != 1 {
		t.Fatalf("valid baseline config rejected: %+v %v", configs, err)
	}
	for _, invalid := range []string{"", "edge-pool-a,bad,a,7,edge-front-a,1,a,b,h,/,bad", strings.Repeat("x", 10)} {
		if invalid == "" {
			continue
		}
		if _, err := parseAuthorityBaselines(invalid); err == nil {
			t.Fatalf("invalid baseline config accepted: %s", invalid)
		}
	}
}

func readyPodStatus(ip, container, digest string) corev1.PodStatus {
	return corev1.PodStatus{PodIP: ip, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		ContainerStatuses: []corev1.ContainerStatus{{Name: container, Ready: true, RestartCount: 0, ImageID: "ghcr.io/example/fugue-edge@" + digest}}}
}
