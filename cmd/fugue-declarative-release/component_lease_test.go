package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestComponentLeaseLoadsDefaultKubeconfigAndFailsClosed(t *testing.T) {
	directory := t.TempDir()
	valid := filepath.Join(directory, "config")
	content := []byte("apiVersion: v1\nkind: Config\nclusters:\n- name: production\n  cluster:\n    server: https://127.0.0.1:6443\n    insecure-skip-tls-verify: true\ncontexts:\n- name: production\n  context:\n    cluster: production\n    user: release\ncurrent-context: production\nusers:\n- name: release\n  user:\n    token: test-only-token\n")
	if err := os.WriteFile(valid, content, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", valid)
	config, err := loadComponentLeaseClientConfig()
	if err != nil || config.Host != "https://127.0.0.1:6443" {
		t.Fatalf("default kubeconfig loading rules did not select the explicit runner config: host=%q err=%v", config.Host, err)
	}
	t.Setenv("KUBECONFIG", filepath.Join(directory, "missing"))
	if _, err := loadComponentLeaseClientConfig(); err == nil {
		t.Fatal("missing explicit kubeconfig was accepted")
	}
	invalid := filepath.Join(directory, "invalid")
	if err := os.WriteFile(invalid, []byte("not: [valid"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", invalid)
	if _, err := loadComponentLeaseClientConfig(); err == nil {
		t.Fatal("invalid explicit kubeconfig was accepted")
	}
}

func TestComponentLeaseNamesRemainComponentScoped(t *testing.T) {
	want := map[string]string{
		"api": "fugue-production-api", "controller": "fugue-production-controller",
		"edge-control-de": "fugue-production-edge-control-de", "telemetry": "fugue-production-telemetry",
	}
	seen := map[string]bool{}
	for component, leaseName := range want {
		release := declarativerelease.PlanRelease{ComponentID: component, Concurrency: leaseName, Workload: declarativerelease.Workload{Namespace: "fugue-system"}}
		lease := newComponentLease(release, "holder", time.Unix(1, 0))
		if lease.GetName() != leaseName || lease.GetLabels()["fugue.pro/component"] != component || seen[lease.GetName()] {
			t.Fatalf("component lease identity collided or drifted: component=%s lease=%s labels=%v", component, lease.GetName(), lease.GetLabels())
		}
		seen[lease.GetName()] = true
	}
}

func TestComponentLeaseMicroTimeRoundTripsThroughKubernetesCodec(t *testing.T) {
	release := testLeaseRelease()
	now := time.Date(2026, 8, 5, 9, 38, 12, 422221054, time.UTC)
	lease := newComponentLease(release, "holder", now)
	scheme := runtime.NewScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	codecs := serializer.NewCodecFactory(scheme)
	encoded, err := runtime.Encode(codecs.LegacyCodec(coordinationv1.SchemeGroupVersion), lease)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"acquireTime":"2026-08-05T09:38:12.422221Z"`, `"renewTime":"2026-08-05T09:38:12.422221Z"`} {
		if !bytes.Contains(encoded, []byte(field)) {
			t.Fatalf("Kubernetes Lease codec did not emit canonical MicroTime %s: %s", field, encoded)
		}
	}
	decodedObject, _, err := codecs.UniversalDeserializer().Decode(encoded, nil, nil)
	if err != nil {
		t.Fatalf("Kubernetes Lease codec rejected its Create payload: %v", err)
	}
	decoded, ok := decodedObject.(*coordinationv1.Lease)
	if !ok || decoded.Spec.AcquireTime == nil || decoded.Spec.RenewTime == nil {
		t.Fatalf("decoded Lease times are missing: %T %+v", decodedObject, decodedObject)
	}
	want := now.Truncate(time.Microsecond)
	if !decoded.Spec.AcquireTime.Time.Equal(want) || !decoded.Spec.RenewTime.Time.Equal(want) {
		t.Fatalf("decoded Lease MicroTime drifted: acquire=%v renew=%v want=%v", decoded.Spec.AcquireTime, decoded.Spec.RenewTime, want)
	}
}

func TestComponentLeaseCASRejectsActiveForeignHolderAndReclaimsExpiredLease(t *testing.T) {
	now := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)
	release := testLeaseRelease()
	active := testLeaseObject(release, "github:other/repo:1:1:"+strings.Repeat("a", 40)+":controller", now.Add(-time.Minute))
	client := kubernetesfake.NewSimpleClientset(active)
	coordinator := &componentLeaseCoordinator{client: client.CoordinationV1(), now: func() time.Time { return now }}
	t.Setenv("GITHUB_REPOSITORY", "example/fugue")
	t.Setenv("GITHUB_RUN_ID", "42")
	t.Setenv("GITHUB_RUN_ATTEMPT", "1")
	if _, err := coordinator.acquire(context.Background(), release, strings.Repeat("b", 40)); err == nil || !strings.Contains(err.Error(), "held by another release") {
		t.Fatalf("active foreign holder was not rejected: %v", err)
	}

	expired := testLeaseObject(release, "github:other/repo:1:1:"+strings.Repeat("a", 40)+":controller", now.Add(-20*time.Minute))
	client = kubernetesfake.NewSimpleClientset(expired)
	coordinator.client = client.CoordinationV1()
	held, err := coordinator.acquire(context.Background(), release, strings.Repeat("b", 40))
	if err != nil {
		t.Fatalf("reclaim expired lease: %v", err)
	}
	if held.Holder != "github:example/fugue:42:1:"+strings.Repeat("b", 40)+":controller" {
		t.Fatalf("unexpected held identity: %+v", held)
	}
	if err := coordinator.release(context.Background(), held); err != nil {
		t.Fatalf("release exact lease: %v", err)
	}
	readback, err := client.CoordinationV1().Leases(release.Workload.Namespace).Get(context.Background(), release.Concurrency, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	holder, _, _, err := parseComponentLease(readback)
	if err != nil || holder != "" {
		t.Fatalf("released lease readback is invalid: holder=%q err=%v", holder, err)
	}
}

func TestComponentLeaseAcquiresReleasedLeaseWithoutTimestamps(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 10, 0, 0, time.UTC)
	release := testLeaseRelease()
	released := testLeaseObject(release, "", now.Add(-time.Hour))
	released.Spec.AcquireTime = nil
	released.Spec.RenewTime = nil
	client := kubernetesfake.NewSimpleClientset(released)
	coordinator := &componentLeaseCoordinator{client: client.CoordinationV1(), now: func() time.Time { return now }}
	t.Setenv("GITHUB_REPOSITORY", "example/fugue")
	t.Setenv("GITHUB_RUN_ID", "31176668421")
	t.Setenv("GITHUB_RUN_ATTEMPT", "1")
	held, err := coordinator.acquire(context.Background(), release, strings.Repeat("c", 40))
	if err != nil {
		t.Fatalf("acquire released lease without timestamps: %v", err)
	}
	if held.Holder != "github:example/fugue:31176668421:1:"+strings.Repeat("c", 40)+":controller" {
		t.Fatalf("unexpected holder: %+v", held)
	}
}

func TestComponentLeaseRejectsHeldLeaseWithoutRenewTime(t *testing.T) {
	release := testLeaseRelease()
	held := testLeaseObject(release, "github:other/repo:1:1:"+strings.Repeat("a", 40)+":controller", time.Now().UTC())
	held.Spec.RenewTime = nil
	if _, _, _, err := parseComponentLease(held); err == nil {
		t.Fatal("held lease without renewTime was accepted")
	}
}

func TestComponentLeaseIdentityIsStrict(t *testing.T) {
	release := testLeaseRelease()
	t.Setenv("GITHUB_REPOSITORY", "example/fugue")
	t.Setenv("GITHUB_RUN_ID", "0042")
	t.Setenv("GITHUB_RUN_ATTEMPT", "1")
	if _, err := componentLeaseHolder(release, strings.Repeat("a", 40)); err == nil {
		t.Fatal("non-canonical run id was accepted")
	}
	t.Setenv("GITHUB_RUN_ID", "42")
	if holder, err := componentLeaseHolder(release, strings.Repeat("a", 40)); err != nil || holder != "github:example/fugue:42:1:"+strings.Repeat("a", 40)+":controller" {
		t.Fatalf("canonical holder was rejected: holder=%q err=%v", holder, err)
	}
}

func TestComponentLeaseCoversLongestBoundedExecution(t *testing.T) {
	if componentLeaseDurationSeconds <= int64((12*time.Minute)/time.Second) {
		t.Fatalf("component lease duration %ds does not cover the 12-minute Edge executor", componentLeaseDurationSeconds)
	}
}

func testLeaseRelease() declarativerelease.PlanRelease {
	return declarativerelease.PlanRelease{ComponentID: "controller", Concurrency: "fugue-production-controller", Workload: declarativerelease.Workload{Namespace: "fugue-system"}}
}

func testLeaseObject(release declarativerelease.PlanRelease, holder string, renew time.Time) *coordinationv1.Lease {
	value := newComponentLease(release, holder, renew)
	value.SetUID(types.UID("lease-uid"))
	value.SetResourceVersion("50")
	return value
}
