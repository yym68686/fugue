package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

// The workload is already back at its exact LKG. This makes the metadata
// transaction observable without permitting any workload mutation in the test.
type restoredMonitorCluster struct {
	declarativerelease.Cluster
	lkg          declarativerelease.Observation
	observations int
}

func (cluster *restoredMonitorCluster) Observe(context.Context, declarativerelease.PlanRelease, declarativerelease.TargetIdentity, []byte) (declarativerelease.Observation, error) {
	cluster.observations++
	return declarativerelease.Observation{}, errors.New("forward is no longer running")
}

func (cluster *restoredMonitorCluster) ObserveCAS(context.Context, declarativerelease.PlanRelease, []byte) (declarativerelease.Observation, error) {
	return declarativerelease.Observation{}, errors.New("no forward CAS")
}

func (cluster *restoredMonitorCluster) WaitHealthy(context.Context, declarativerelease.PlanRelease, declarativerelease.TargetIdentity, []byte) (declarativerelease.Observation, error) {
	return cluster.lkg, nil
}

func (cluster *restoredMonitorCluster) MonitorConverged(context.Context, declarativerelease.PlanRelease, []byte) error {
	return nil
}
func (cluster *restoredMonitorCluster) VerifyOwnershipConverged(context.Context, declarativerelease.PlanRelease, []byte) error {
	return nil
}

func TestRestoreMonitorLeaseFencesCurrentPointerNotRollbackDestination(t *testing.T) {
	for _, mode := range []string{"monitor", "committed-candidate", "uncommitted-candidate", "changed-pointer", "missing-pointer", "recreated-pointer", "state-race", "tampered-source"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 8, 10, 19, 0, 0, 0, time.UTC)
			parentFiles, parentTerminal, _ := monitorBundleFixtureBetween(t, strings.Repeat("c", 40), strings.Repeat("a", 40), "sha256:"+strings.Repeat("9", 64), "sha256:"+strings.Repeat("1", 64))
			files, terminal, release := monitorBundleFixtureBetween(t, strings.Repeat("a", 40), strings.Repeat("b", 40), "sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("2", 64), parentFiles["forward.json"])
			holder := "github:example/release:42:1:" + strings.Repeat("b", 40) + ":api"
			client := kubernetesfake.NewSimpleClientset(testLeaseObject(release, holder, now.Add(-20*time.Minute)))
			store := &monitorStore{client: client.CoreV1(), now: func() time.Time { return now }}
			parent, err := store.persistVerified(ctx, release, parentFiles, parentTerminal)
			if err != nil {
				t.Fatal(err)
			}
			current, err := store.persistVerified(ctx, release, files, terminal)
			if err != nil {
				t.Fatal(err)
			}
			if current.Bundle.Prepared.LKG != parent.Bundle.Prepared.Forward {
				t.Fatalf("fixture predecessor is not exact: lkg=%+v parent=%+v", current.Bundle.Prepared.LKG, parent.Bundle.Prepared.Forward)
			}
			if mode == "uncommitted-candidate" {
				current, err = store.activateExistingRecord(ctx, current, parent.RecordName, parent.Bundle)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := current
			if mode == "recreated-pointer" {
				before.StateUID = "previous-pointer-uid"
			}
			if mode == "state-race" {
				before.StateRV = "previous-resource-version"
			}
			if mode == "changed-pointer" {
				if _, err := store.activateExistingRecord(ctx, current, parent.RecordName, parent.Bundle); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "missing-pointer" {
				if err := client.CoreV1().ConfigMaps(release.Workload.Namespace).Delete(ctx, current.StateName, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("GITHUB_REPOSITORY", "example/release")
			t.Setenv("GITHUB_RUN_ID", "42")
			t.Setenv("GITHUB_RUN_ATTEMPT", "1")
			t.Setenv("FUGUE_COMPONENT_LEASE_OWNER", "")
			lease := &componentLeaseCoordinator{client: client.CoordinationV1(), pods: client.CoreV1(), now: func() time.Time { return now }}
			cluster := &restoredMonitorCluster{lkg: parent.Bundle.Terminal.Final}
			var output bytes.Buffer
			bundle := currentPlan(t, files, terminal)
			if mode != "monitor" {
				// Candidate directories lack a terminal receipt; only the live
				// verified monitor can supply that independent source binding.
				bundle.Terminal = declarativerelease.ExecutionResult{}
				bundle.Record = declarativerelease.MonitorRecord{}
			}
			if mode == "tampered-source" {
				bundle.Artifact = parent.Bundle.Artifact
			}
			err = restoreMonitorWithLease(ctx, &output, store, cluster, lease, release, monitorRestoreBinding{before, bundle, parent.RecordName, parent.Bundle})
			if mode == "tampered-source" {
				if err == nil || !strings.Contains(err.Error(), "not bound") || cluster.observations != 0 || output.Len() != 0 {
					t.Fatalf("unbound rollback source reached workload recovery: %v", err)
				}
				return // No Lease was acquired for an unauthorized source.
			} else if mode == "changed-pointer" || mode == "missing-pointer" || mode == "recreated-pointer" || mode == "state-race" {
				if err == nil || !strings.Contains(err.Error(), "monitor pointer changed") || cluster.observations != 0 || output.Len() != 0 {
					t.Fatalf("pointer race reached workload recovery: err=%v observations=%d output=%s", err, cluster.observations, output.String())
				}
			} else {
				if err != nil {
					t.Fatalf("unchanged pointer was mistaken for rollback destination: %v", err)
				}
				result, err := declarativerelease.DecodeExecutionResult(bytes.NewReader(output.Bytes()))
				if err != nil || result.Status != "compensated" || result.LKGApplyCount != 0 {
					t.Fatalf("LKG receipt=%+v err=%v", result, err)
				}
				after, err := store.load(ctx, release.Workload.Namespace, release.ComponentID)
				if err != nil || after.Bundle.Record != parent.Bundle.Record {
					t.Fatalf("compensated workload retained forward monitor: err=%v", err)
				}
			}
			object, err := client.CoordinationV1().Leases(release.Workload.Namespace).Get(ctx, release.Concurrency, metav1.GetOptions{})
			if err != nil || object.Spec.HolderIdentity == nil || *object.Spec.HolderIdentity != "" {
				t.Fatalf("terminal path leaked component Lease: %v", err)
			}
		})
	}
}

func currentPlan(t *testing.T, files map[string][]byte, terminal declarativerelease.ExecutionResult) monitorBundle {
	t.Helper()
	raw, err := declarativerelease.CanonicalJSON(terminal)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := decodeMonitorBundle(files, raw)
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}
