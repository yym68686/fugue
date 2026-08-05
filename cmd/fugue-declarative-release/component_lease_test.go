package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestComponentLeaseCASRejectsActiveForeignHolderAndReclaimsExpiredLease(t *testing.T) {
	now := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)
	release := testLeaseRelease()
	active := testLeaseObject(release, "github:other/repo:1:1:"+strings.Repeat("a", 40)+":controller", now.Add(-time.Minute))
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), active)
	coordinator := &componentLeaseCoordinator{client: client, now: func() time.Time { return now }}
	t.Setenv("GITHUB_REPOSITORY", "example/fugue")
	t.Setenv("GITHUB_RUN_ID", "42")
	t.Setenv("GITHUB_RUN_ATTEMPT", "1")
	if _, err := coordinator.acquire(context.Background(), release, strings.Repeat("b", 40)); err == nil || !strings.Contains(err.Error(), "held by another release") {
		t.Fatalf("active foreign holder was not rejected: %v", err)
	}

	expired := testLeaseObject(release, "github:other/repo:1:1:"+strings.Repeat("a", 40)+":controller", now.Add(-20*time.Minute))
	client = dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), expired)
	coordinator.client = client
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
	readback, err := client.Resource(componentLeaseGVR).Namespace(release.Workload.Namespace).Get(context.Background(), release.Concurrency, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	holder, _, _, err := parseComponentLease(readback)
	if err != nil || holder != "" {
		t.Fatalf("released lease readback is invalid: holder=%q err=%v", holder, err)
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

func testLeaseObject(release declarativerelease.PlanRelease, holder string, renew time.Time) *unstructured.Unstructured {
	value := newComponentLease(release, holder, renew)
	value.SetUID(types.UID("lease-uid"))
	value.SetResourceVersion("50")
	return value
}
