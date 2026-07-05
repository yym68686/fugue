package controller

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPlanDistributedImageRetentionKeepsLatestLimitAndCurrentWorkload(t *testing.T) {
	now := time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC)
	digestCurrent := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	images := []model.Image{
		{ID: "img-old", AppID: "app_1", ImageRef: "registry.example/fugue-apps/demo:old", LifecycleState: model.ImageLifecycleAvailable, CreatedAt: now.Add(-3 * time.Hour), UpdatedAt: now.Add(-3 * time.Hour)},
		{ID: "img-new", AppID: "app_1", ImageRef: "registry.example/fugue-apps/demo:new", CanonicalDigest: digestCurrent, LifecycleState: model.ImageLifecycleAvailable, CreatedAt: now.Add(-1 * time.Hour), UpdatedAt: now.Add(-1 * time.Hour)},
		{ID: "img-mid", AppID: "app_1", ImageRef: "registry.example/fugue-apps/demo:mid", LifecycleState: model.ImageLifecycleAvailable, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour)},
	}
	plan := planDistributedImageRetention(model.App{ID: "app_1", Spec: model.AppSpec{ImageMirrorLimit: 1}}, images, nil, nil, map[string]struct{}{
		"registry.example/fugue-apps/demo@" + digestCurrent: {},
	}, now)
	if plan.EffectiveLimit != 1 {
		t.Fatalf("effective limit = %d", plan.EffectiveLimit)
	}
	if got := stringSet(plan.KeepImageIDs); len(got) != 1 {
		t.Fatalf("expected only current image kept, got %+v", plan)
	} else if _, ok := got["img-new"]; !ok {
		t.Fatalf("expected current image kept, got %+v", plan)
	}
	for _, decision := range plan.ImageDecisions {
		if decision.ImageID == "img-old" && (decision.Keep || decision.Reason != "retention_excess") {
			t.Fatalf("expected old image to be retention_excess, got %+v", decision)
		}
	}
}

func TestPlanDistributedImageRetentionCountsCurrentWorkloadAgainstLimit(t *testing.T) {
	now := time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC)
	digestCurrent := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	images := []model.Image{
		{ID: "img-current-old", AppID: "app_1", ImageRef: "registry.example/fugue-apps/demo:old", CanonicalDigest: digestCurrent, LifecycleState: model.ImageLifecycleAvailable, CreatedAt: now.Add(-3 * time.Hour), UpdatedAt: now.Add(-3 * time.Hour)},
		{ID: "img-newer", AppID: "app_1", ImageRef: "registry.example/fugue-apps/demo:newer", LifecycleState: model.ImageLifecycleAvailable, CreatedAt: now.Add(-1 * time.Hour), UpdatedAt: now.Add(-1 * time.Hour)},
	}
	plan := planDistributedImageRetention(model.App{ID: "app_1", Spec: model.AppSpec{ImageMirrorLimit: 1}}, images, nil, nil, map[string]struct{}{
		"registry.example/fugue-apps/demo@" + digestCurrent: {},
	}, now)
	kept := stringSet(plan.KeepImageIDs)
	if len(kept) != 1 {
		t.Fatalf("expected current workload to consume limit=1, got %+v", plan)
	}
	if _, ok := kept["img-current-old"]; !ok {
		t.Fatalf("expected old current workload kept, got %+v", plan)
	}
	for _, decision := range plan.ImageDecisions {
		if decision.ImageID == "img-newer" && (decision.Keep || decision.Reason != "retention_excess") {
			t.Fatalf("expected newer non-current image to be retention_excess when limit is consumed, got %+v", decision)
		}
	}
}

func TestPlanDistributedImageRetentionHonorsUserPinAndActiveOperation(t *testing.T) {
	now := time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC)
	images := []model.Image{
		{ID: "img-pinned", AppID: "app_1", ImageRef: "registry.example/fugue-apps/demo:pinned", SourceOperationID: "op-old", LifecycleState: model.ImageLifecycleAvailable, CreatedAt: now.Add(-4 * time.Hour), UpdatedAt: now.Add(-4 * time.Hour)},
		{ID: "img-active", AppID: "app_1", ImageRef: "registry.example/fugue-apps/demo:active", SourceOperationID: "op-active", LifecycleState: model.ImageLifecycleImporting, CreatedAt: now.Add(-3 * time.Hour), UpdatedAt: now.Add(-3 * time.Hour)},
		{ID: "img-latest", AppID: "app_1", ImageRef: "registry.example/fugue-apps/demo:latest", LifecycleState: model.ImageLifecycleAvailable, CreatedAt: now.Add(-1 * time.Hour), UpdatedAt: now.Add(-1 * time.Hour)},
	}
	ops := []model.Operation{{ID: "op-active", AppID: "app_1", Status: model.OperationStatusRunning}}
	pins := []model.ImagePin{{ImageID: "img-pinned", Reason: model.ImagePinReasonUserPin}}
	plan := planDistributedImageRetention(model.App{ID: "app_1", Spec: model.AppSpec{ImageMirrorLimit: 1}}, images, ops, pins, nil, now)
	kept := stringSet(plan.KeepImageIDs)
	for _, id := range []string{"img-pinned", "img-active", "img-latest"} {
		if _, ok := kept[id]; !ok {
			t.Fatalf("expected %s kept, got %+v", id, plan)
		}
	}
	reasons := map[string]string{}
	for _, decision := range plan.ImageDecisions {
		reasons[decision.ImageID] = decision.Reason
	}
	if reasons["img-pinned"] != "user_pin" || reasons["img-active"] != "active_operation" || reasons["img-latest"] != "retention_keep_latest_n" {
		t.Fatalf("unexpected reasons: %+v", reasons)
	}
}
