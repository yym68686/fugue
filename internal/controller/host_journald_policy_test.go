package controller

import (
	"context"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestScheduleHostJournaldPolicyIsSequentialAndIdempotent(t *testing.T) {
	t.Parallel()

	stateStore, nodeSecret := newImageCacheControllerTestStore(t)
	for _, node := range []string{"worker-a", "worker-b"} {
		if _, _, err := stateStore.EnrollNodeUpdater(
			nodeSecret,
			node,
			"https://"+node+".example.com",
			nil,
			node,
			"fingerprint-"+node,
			model.NodeUpdaterCurrentVersion,
			"join-v36",
			[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypeReconcileHostJournaldPolicy},
		); err != nil {
			t.Fatalf("enroll updater %s: %v", node, err)
		}
	}

	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			HostJournaldPolicyEnabled:           true,
			HostJournaldMaxRetentionSec:         "30day",
			HostJournaldSystemMaxUse:            "1G",
			HostJournaldPolicyReconcileInterval: 24 * time.Hour,
		},
	}
	if err := svc.scheduleHostJournaldPolicyReconciliation(context.Background()); err != nil {
		t.Fatalf("schedule first journald policy: %v", err)
	}
	if err := svc.scheduleHostJournaldPolicyReconciliation(context.Background()); err != nil {
		t.Fatalf("schedule duplicate journald policy: %v", err)
	}
	tasks, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list pending tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ClusterNodeName != "worker-a" {
		t.Fatalf("expected one sequential task for worker-a, got %+v", tasks)
	}
	if tasks[0].Payload["dry_run"] != "false" || tasks[0].Payload["allow_delete"] != "true" || tasks[0].Payload["allow_restart"] != "true" || tasks[0].Payload["max_retention_sec"] != "30day" || tasks[0].Payload["system_max_use"] != "1G" {
		t.Fatalf("unexpected task payload: %+v", tasks[0].Payload)
	}

	if _, err := stateStore.ClaimNodeUpdateTask(tasks[0].ID, tasks[0].NodeUpdaterID); err != nil {
		t.Fatalf("claim first task: %v", err)
	}
	if _, err := stateStore.CompleteNodeUpdateTask(tasks[0].ID, tasks[0].NodeUpdaterID, model.NodeUpdateTaskStatusCompleted, "journald policy applied", ""); err != nil {
		t.Fatalf("complete first task: %v", err)
	}
	if err := svc.scheduleHostJournaldPolicyReconciliation(context.Background()); err != nil {
		t.Fatalf("schedule second journald policy: %v", err)
	}
	tasks, err = stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list second pending task: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ClusterNodeName != "worker-b" {
		t.Fatalf("expected one sequential task for worker-b, got %+v", tasks)
	}
}

func TestScheduleHostJournaldPolicySkipsUnsupportedAndInvalidConfig(t *testing.T) {
	t.Parallel()

	stateStore, nodeSecret := newImageCacheControllerTestStore(t)
	if _, _, err := stateStore.EnrollNodeUpdater(
		nodeSecret,
		"worker-old",
		"https://worker-old.example.com",
		nil,
		"worker-old",
		"fingerprint-worker-old",
		"v35",
		"join-v35",
		[]string{"heartbeat", "tasks"},
	); err != nil {
		t.Fatalf("enroll updater: %v", err)
	}
	svc := &Service{Store: stateStore, Config: config.ControllerConfig{
		HostJournaldPolicyEnabled:   true,
		HostJournaldMaxRetentionSec: "30day;rm",
		HostJournaldSystemMaxUse:    "1G",
	}}
	if err := svc.scheduleHostJournaldPolicyReconciliation(context.Background()); err == nil {
		t.Fatal("expected invalid journald policy to fail closed")
	}
	svc.Config.HostJournaldMaxRetentionSec = "30day"
	if err := svc.scheduleHostJournaldPolicyReconciliation(context.Background()); err != nil {
		t.Fatalf("unsupported updater should be skipped: %v", err)
	}
	tasks, err := stateStore.ListNodeUpdateTasks("", true, "", "")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("unsupported updater received a task: %+v", tasks)
	}
}

func TestScheduleHostJournaldPolicyRetriesFailedNodeAfterCooldown(t *testing.T) {
	t.Parallel()

	stateStore, nodeSecret := newImageCacheControllerTestStore(t)
	updater, _, err := stateStore.EnrollNodeUpdater(
		nodeSecret,
		"worker-failed",
		"https://worker-failed.example.com",
		nil,
		"worker-failed",
		"fingerprint-worker-failed",
		model.NodeUpdaterCurrentVersion,
		"join-v36",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypeReconcileHostJournaldPolicy},
	)
	if err != nil {
		t.Fatalf("enroll updater: %v", err)
	}
	svc := &Service{Store: stateStore, Config: config.ControllerConfig{
		HostJournaldPolicyEnabled:           true,
		HostJournaldMaxRetentionSec:         "30day",
		HostJournaldSystemMaxUse:            "1G",
		HostJournaldPolicyReconcileInterval: 24 * time.Hour,
	}}
	if err := svc.scheduleHostJournaldPolicyReconciliation(context.Background()); err != nil {
		t.Fatalf("schedule task: %v", err)
	}
	tasks, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("unexpected pending tasks: %v %+v", err, tasks)
	}
	if _, err := stateStore.ClaimNodeUpdateTask(tasks[0].ID, updater.ID); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := stateStore.CompleteNodeUpdateTask(tasks[0].ID, updater.ID, model.NodeUpdateTaskStatusFailed, "", "journald unavailable"); err != nil {
		t.Fatalf("fail task: %v", err)
	}
	if err := svc.scheduleHostJournaldPolicyReconciliation(context.Background()); err != nil {
		t.Fatalf("schedule during failure cooldown: %v", err)
	}
	failedTasks, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusFailed)
	if err != nil || len(failedTasks) != 1 {
		t.Fatalf("unexpected failed task history: %v %+v", err, failedTasks)
	}
}

func TestControllerHostJournaldPolicyPayloadIsStable(t *testing.T) {
	t.Parallel()

	first, err := controllerHostJournaldPolicyPayload("30day", "1G")
	if err != nil {
		t.Fatal(err)
	}
	second, err := controllerHostJournaldPolicyPayload("30day", "1G")
	if err != nil {
		t.Fatal(err)
	}
	if first["policy_hash"] == "" || first["policy_hash"] != second["policy_hash"] {
		t.Fatalf("policy hash is not stable: %v %v", first, second)
	}
	if _, err := controllerHostJournaldPolicyPayload("0day", "1G"); err == nil {
		t.Fatal("expected zero retention to be rejected")
	}
	if _, err := controllerHostJournaldPolicyPayload("30day", "1Gi"); err == nil {
		t.Fatal("expected unsupported SystemMaxUse suffix to be rejected")
	}
}
