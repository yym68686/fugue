package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

type baselineActivationExecutor struct {
	raw []byte
}

func (executor baselineActivationExecutor) Exec(context.Context, string, string, string, ...string) ([]byte, error) {
	return append([]byte(nil), executor.raw...), nil
}

func TestAuthorityBaselineReadsActivationFromWorkerWhenFrontIsUnready(t *testing.T) {
	group := "edge-pool-a"
	source := strings.Repeat("a", 40)
	image := "sha256:" + strings.Repeat("b", 64)
	front := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "front-a", Namespace: "fugue-system", UID: types.UID("front-uid"), ResourceVersion: "11",
		Labels: map[string]string{"fugue.io/edge-group-id": group, "app.kubernetes.io/component": "edge-front-a"}},
		Spec:   corev1.PodSpec{NodeName: "edge-node-a", Containers: []corev1.Container{{Name: "edge-front"}}},
		Status: corev1.PodStatus{PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}}}
	workerA := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Namespace: "fugue-system", UID: types.UID("worker-a-uid"), ResourceVersion: "12",
		Labels: map[string]string{"fugue.io/edge-group-id": group, "fugue.io/edge-slot": "a"}},
		Spec: corev1.PodSpec{NodeName: "edge-node-a", Containers: []corev1.Container{{Name: "edge"}}}}
	workerB := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-b", Namespace: "fugue-system", UID: types.UID("worker-b-uid"), ResourceVersion: "13",
		Labels: map[string]string{"fugue.io/edge-group-id": group, "fugue.io/edge-slot": "b"}},
		Spec: corev1.PodSpec{NodeName: "edge-node-a", Containers: []corev1.Container{{Name: "edge"}}}}
	raw, err := json.Marshal(edgegroupfront.ActivationState{Schema: edgegroupfront.ActivationStateSchemaV1, GroupID: group, Generation: 135,
		ActiveSlot: "a", BundleGeneration: "routes-serving.p15778.r151", WorkerSourceCommit: source, WorkerImageDigest: image,
		Authority: edgegroupfront.ActivationAuthority, Operation: edgegroupfront.ActivationOperationPromote,
		Reason: "recover activation witness", UpdatedAt: time.Unix(1, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(front, workerA, workerB)
	before := releaseguardian.CurrentAuthority{BaselineReceiptDigest: "sha256:" + strings.Repeat("c", 64)}
	fronts, err := observeBaselineFronts(context.Background(), client, "fugue-system", authorityBaselineConfig{GroupID: group, FrontComponent: "edge-front-a", ExpectedNodes: 1}, before, baselineActivationExecutor{raw: raw})
	if err != nil {
		t.Fatalf("activation fallback failed: %v", err)
	}
	health, ok := fronts["edge-node-a"]
	if !ok || health.pod.Name != front.Name || health.health.Status != "recovery-witness" || health.health.ActiveSlot != "a" ||
		health.health.Generation != 135 || health.health.BundleGeneration != "routes-serving.p15778.r151" || health.health.WorkerSourceCommit != source || health.health.WorkerImageDigest != image {
		t.Fatalf("unexpected activation fallback witness: %+v", fronts)
	}
}

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

func TestAuthorityBaselineNormalizesOneLegacySwitchWithoutChangingWorkloads(t *testing.T) {
	now := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	group := "edge-pool-a"
	oldRecord := "sha256:" + strings.Repeat("a", 64)
	olderRecord := "sha256:" + strings.Repeat("b", 64)
	newSource := strings.Repeat("c", 40)
	newImage := "sha256:" + strings.Repeat("d", 64)
	newRecord, err := (releaseguardian.RouteBundleRecord{GroupID: group, Epoch: 11, BundleDigest: "sha256:" + strings.Repeat("e", 64),
		SourceSHA: newSource, ControlImageDigest: "sha256:" + strings.Repeat("f", 64), InventoryDigest: "sha256:" + strings.Repeat("1", 64),
		ManifestDigest: "sha256:" + strings.Repeat("2", 64), HealthContractDigest: "sha256:" + strings.Repeat("3", 64),
		IssuedAt: now.Add(-time.Minute).Format(time.RFC3339Nano), KeyID: "key-a", Signature: strings.Repeat("A", 43)}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	front := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "front-b", Namespace: "fugue-system", UID: types.UID("front-pod-uid-b"), ResourceVersion: "51",
		Labels: map[string]string{"fugue.io/edge-group-id": group, "app.kubernetes.io/component": "edge-front-a"}, Annotations: map[string]string{"fugue.pro/source-commit": newSource}},
		Spec: corev1.PodSpec{NodeName: "edge-node-a"}, Status: readyPodStatus("10.0.0.11", "edge-front", newImage)}
	worker := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-b", Namespace: "fugue-system", UID: types.UID("worker-pod-uid-b"), ResourceVersion: "52",
		Labels: map[string]string{"fugue.io/edge-group-id": group, "fugue.io/edge-slot": "b"}, Annotations: map[string]string{"fugue.pro/source-commit": newSource}},
		Spec: corev1.PodSpec{NodeName: "edge-node-a"}, Status: readyPodStatus("10.0.0.12", "edge", newImage)}
	client := fake.NewSimpleClientset(front, worker)
	store, _ := releaseguardian.NewAuthorityStore(client, "fugue-system")
	if err := store.CreateRouteBundleRecord(context.Background(), newRecord); err != nil {
		t.Fatal(err)
	}
	baseline, err := (releaseguardian.AuthorityBaselineReceipt{GroupID: group, BeforeRecordDigest: olderRecord, BeforeWorkerSlot: releaseguardian.AuthoritySlotB,
		BeforeAuthorityEpoch: 5, RecordDigest: oldRecord, WorkerSlot: releaseguardian.AuthoritySlotA, AuthorityEpoch: 6,
		Nodes: []releaseguardian.AuthorityBaselineNodeWitness{{NodeName: "edge-node-a", FrontPodUID: "front-old-uid", FrontResourceVersion: "31",
			WorkerPodUID: "worker-old-uid", WorkerResourceVersion: "32", ActivationGeneration: 7, BundleGeneration: "old.p6.r0",
			ServingGeneration: "old", WorkerSourceSHA: strings.Repeat("4", 40), WorkerImageDigest: "sha256:" + strings.Repeat("5", 64)}},
		ObservedAt: now.Add(-time.Hour).Format(time.RFC3339Nano)}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	before := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: group, CurrentRecordDigest: oldRecord, CurrentWorkerSlot: releaseguardian.AuthoritySlotA,
		CurrentFrontGeneration: 8, CurrentBundleGeneration: "old.p7.r1", CurrentWorkerSourceSHA: strings.Repeat("4", 40), CurrentWorkerImageDigest: "sha256:" + strings.Repeat("5", 64),
		PreviousRecordDigest: olderRecord, PreviousWorkerSlot: releaseguardian.AuthoritySlotB, PreviousFrontGeneration: 7,
		PreviousBundleGeneration: "older.p5.r0", PreviousWorkerSourceSHA: strings.Repeat("6", 40), PreviousWorkerImageDigest: "sha256:" + strings.Repeat("7", 64),
		AuthorityEpoch: 7, BaselineReceiptDigest: baseline.ReceiptDigest}
	authorityRaw, _ := declarativerelease.CanonicalJSON(before)
	baselineRaw, _ := declarativerelease.CanonicalJSON(baseline)
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "fugue-current-authority-" + group, Namespace: "fugue-system",
		UID: types.UID("current-authority-uid"), ResourceVersion: "60", Labels: map[string]string{"fugue.pro/group": group, "fugue.pro/authority-store": "true"}},
		Data: map[string]string{"authority.json": string(authorityRaw), "baseline-receipt.json": string(baselineRaw)}}
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Create(context.Background(), object, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	oldRead, oldRoute := readAuthorityBaselineJSON, requestAuthorityBaselineRoute
	defer func() { readAuthorityBaselineJSON, requestAuthorityBaselineRoute = oldRead, oldRoute }()
	candidateBundleLoaded := true
	readAuthorityBaselineJSON = func(_ context.Context, _ string, destination any) error {
		switch value := destination.(type) {
		case *baselineFrontHealth:
			*value = baselineFrontHealth{Status: "ok", ActiveSlot: "b", Generation: 9, BundleGeneration: "new.p11.r0",
				WorkerSourceCommit: newSource, WorkerImageDigest: newImage, RouteAuthority: "edge-control"}
		case *baselineWorkerHealth:
			*value = baselineWorkerHealth{Healthy: true, EdgeGroupID: group, BundleVersion: "new.p11.r0", PublicationSequence: 11,
				ServingGeneration: "new", CandidateBundleLoaded: candidateBundleLoaded, CandidateRecordDigest: newRecord.RecordDigest, CandidateWorkerSlot: "b"}
		}
		return nil
	}
	requestAuthorityBaselineRoute = func(context.Context, string, string, string) (int, []byte, http.Header, error) {
		headers := http.Header{}
		headers.Set("X-Fugue-Candidate-Record-Digest", newRecord.RecordDigest)
		headers.Set("X-Fugue-Candidate-Worker-Slot", "b")
		return http.StatusOK, []byte("ok"), headers, nil
	}
	config := authorityBaselineConfig{GroupID: group, ExpectedRecordDigest: baseline.BeforeRecordDigest, ExpectedWorkerSlot: baseline.BeforeWorkerSlot,
		ExpectedEpoch: baseline.BeforeAuthorityEpoch, FrontComponent: "edge-front-a", ExpectedNodes: 1,
		SlotAddresses: map[releaseguardian.AuthoritySlot]string{releaseguardian.AuthoritySlotA: "127.0.0.1:18443", releaseguardian.AuthoritySlotB: "127.0.0.1:28443"},
		Host:          "route.example.test", Path: "/", ExpectedBodyDigest: shaDigest([]byte("ok"))}
	done, err := adoptAuthorityBaselineOnce(context.Background(), store, client, "fugue-system", config, now)
	if err != nil || !done {
		t.Fatalf("normalization done=%v err=%v", done, err)
	}
	after, _, _, err := store.LoadCurrent(context.Background(), group)
	if err != nil || after.CurrentRecordDigest != newRecord.RecordDigest || after.CurrentWorkerSlot != releaseguardian.AuthoritySlotB ||
		after.PreviousRecordDigest != "" || after.PreviousWorkerSlot != "" || after.BaselineReceiptDigest != baseline.ReceiptDigest {
		t.Fatalf("normalized authority=%+v err=%v", after, err)
	}
	if next, err := client.CoreV1().ConfigMaps("fugue-system").Get(context.Background(), object.Name, metav1.GetOptions{}); err != nil || next.Data["normalization-receipt.json"] == "" {
		t.Fatalf("normalization receipt missing: %v", err)
	}
	candidateBundleLoaded = false
	if done, err := adoptAuthorityBaselineOnce(context.Background(), store, client, "fugue-system", config, now.Add(time.Minute)); err != nil || !done {
		t.Fatalf("established current without candidate flag done=%v err=%v", done, err)
	}
}

func TestBaselineWorkerHealthMatchesOnlyExactEstablishedCurrent(t *testing.T) {
	record := "sha256:" + strings.Repeat("a", 64)
	source := strings.Repeat("b", 40)
	image := "sha256:" + strings.Repeat("c", 64)
	health := baselineWorkerHealth{Healthy: true, EdgeGroupID: "edge-pool-a", BundleVersion: "routes.p12.r3", PublicationSequence: 12,
		ServingGeneration: "routes", CandidateRecordDigest: record, CandidateWorkerSlot: "b"}
	front := baselineFrontHealth{Status: "ok", ActiveSlot: "b", Generation: 9, BundleGeneration: health.BundleVersion,
		WorkerSourceCommit: source, WorkerImageDigest: image, RouteAuthority: "edge-control"}
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: "edge-pool-a", CurrentRecordDigest: record, CurrentWorkerSlot: releaseguardian.AuthoritySlotB,
		CurrentFrontGeneration: front.Generation, CurrentBundleGeneration: health.BundleVersion,
		CurrentWorkerSourceSHA: source, CurrentWorkerImageDigest: image, AuthorityEpoch: 8,
		BaselineReceiptDigest: "sha256:" + strings.Repeat("d", 64)}
	if !baselineWorkerHealthMatchesCurrent(health, front, releaseguardian.AuthoritySlotB, current) {
		t.Fatal("exact established current was rejected")
	}
	for name, mutate := range map[string]func(*releaseguardian.CurrentAuthority){
		"no baseline": func(value *releaseguardian.CurrentAuthority) { value.BaselineReceiptDigest = "" },
		"wrong slot": func(value *releaseguardian.CurrentAuthority) {
			value.CurrentWorkerSlot = releaseguardian.AuthoritySlotA
		},
		"wrong record": func(value *releaseguardian.CurrentAuthority) {
			value.CurrentRecordDigest = "sha256:" + strings.Repeat("e", 64)
		},
		"wrong generation": func(value *releaseguardian.CurrentAuthority) { value.CurrentFrontGeneration++ },
		"wrong bundle":     func(value *releaseguardian.CurrentAuthority) { value.CurrentBundleGeneration = "other.p12.r3" },
		"wrong source":     func(value *releaseguardian.CurrentAuthority) { value.CurrentWorkerSourceSHA = strings.Repeat("f", 40) },
		"wrong image": func(value *releaseguardian.CurrentAuthority) {
			value.CurrentWorkerImageDigest = "sha256:" + strings.Repeat("1", 64)
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := current
			mutate(&changed)
			if baselineWorkerHealthMatchesCurrent(health, front, releaseguardian.AuthoritySlotB, changed) {
				t.Fatal("drifted established current was accepted")
			}
		})
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
