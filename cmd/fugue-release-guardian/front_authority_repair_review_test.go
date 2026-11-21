package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
)

func TestFrontRepairPreviewApprovalRejectsScopeAndExecutorDrift(t *testing.T) {
	before, nodes, witnesses := partialFrontRollbackFixture()
	plan, err := planPartialFrontRollbackRepair(before, "authority-uid", "23", nodes, witnesses)
	if err != nil {
		t.Fatal(err)
	}
	digest := func(s string) string { return "sha256:" + strings.Repeat(s, 64) }
	preview := frontRepairPreview{Schema: frontRepairPreviewSchema, Plan: plan, Release: frontRepairReleaseBinding{
		Component: "edge-worker-fixture", Namespace: "fixture-system", GroupID: before.GroupID,
		PlanDigest: digest("1"), MonitorDigest: digest("2"), ArtifactDigest: digest("3"),
		WorkerSourceSHA: before.CurrentWorkerSourceSHA, WorkerImageDigest: before.CurrentWorkerImageDigest}}
	preview.Digest, err = preview.validatedDigest()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	executor := strings.Repeat("a", 40)
	approval := frontRepairApproval{Schema: frontRepairApprovalSchema, PreviewDigest: preview.Digest, ExecutorCommit: executor,
		ApprovedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour)}
	intent, err := approval.reviewedIntent(preview, executor, now)
	if err != nil || intent.PlanDigest != plan.Digest || len(intent.Nodes) != len(nodes) || !exactSHA256Digest(intent.AuthorizationDigest) {
		t.Fatalf("exact preview did not bind review intent: %+v err=%v", intent, err)
	}
	repeated, err := approval.reviewedIntent(preview, executor, now.Add(time.Minute))
	if err != nil || repeated.AuthorizationDigest != intent.AuthorizationDigest {
		t.Fatalf("retry changed the approval digest: %+v err=%v", repeated, err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*frontRepairPreview, *frontRepairApproval, *string, *time.Time)
	}{
		{"executor revision", func(_ *frontRepairPreview, _ *frontRepairApproval, commit *string, _ *time.Time) {
			*commit = strings.Repeat("b", 40)
		}},
		{"approval expired", func(_ *frontRepairPreview, a *frontRepairApproval, _ *string, clock *time.Time) { *clock = a.ExpiresAt }},
		{"approval premature", func(_ *frontRepairPreview, a *frontRepairApproval, _ *string, clock *time.Time) {
			*clock = a.ApprovedAt.Add(-time.Second)
		}},
		{"approval overlong", func(_ *frontRepairPreview, a *frontRepairApproval, _ *string, _ *time.Time) {
			a.ExpiresAt = a.ApprovedAt.Add(frontRepairApprovalMaxAge + time.Second)
		}},
		{"monitor record changed", func(p *frontRepairPreview, _ *frontRepairApproval, _ *string, _ *time.Time) {
			p.Release.MonitorDigest = digest("4")
		}},
		{"worker image changed", func(p *frontRepairPreview, _ *frontRepairApproval, _ *string, _ *time.Time) {
			p.Release.WorkerImageDigest = digest("5")
		}},
		{"incomplete cohort", func(p *frontRepairPreview, _ *frontRepairApproval, _ *string, _ *time.Time) {
			p.Plan.Members = p.Plan.Members[1:]
		}},
		{"replaced authority UID", func(p *frontRepairPreview, _ *frontRepairApproval, _ *string, _ *time.Time) {
			p.Plan.CurrentUID = "different-uid"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, a, commit, clock := preview, approval, executor, now
			tc.mutate(&p, &a, &commit, &clock)
			if _, err := a.reviewedIntent(p, commit, clock); err == nil {
				t.Fatal("drifted preview or approval was accepted")
			}
		})
	}
	changed := preview
	changed.Release.MonitorDigest = digest("6")
	changed.Digest, err = changed.validatedDigest()
	if err != nil || changed.Digest == preview.Digest {
		t.Fatalf("alternate immutable monitor did not yield a different preview: err=%v", err)
	}
	if _, err := approval.reviewedIntent(changed, executor, now); err == nil {
		t.Fatal("approval was reused even though preview digest changed")
	}
}

func TestFrontRepairPreviewOnlyAcceptsRegistryWorkerAndHasNoApplyMode(t *testing.T) {
	raw, err := os.ReadFile("../../" + defaultFrontRepairRegistry)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := declarativerelease.DecodeEdgeGroupRegistry(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range registry.Groups {
		selected, err := selectFrontRepairWorker(registry, group.Worker.ID)
		if err != nil || selected.target.Key.Component != group.Worker.ID || selected.runtime.GroupID != group.GroupID ||
			selected.runtime.ExpectedNodes != group.Worker.Transition.EdgeGroupAB.ExpectedNodes {
			t.Fatalf("invalid registry binding for %s: %+v err=%v", group.ID, selected, err)
		}
		if _, err := selectFrontRepairWorker(registry, group.Control.ID); err == nil {
			t.Fatalf("selected non-Worker component %s", group.Control.ID)
		}
	}
	var out bytes.Buffer
	if err := runFrontRepairPreview([]string{"--component", registry.Groups[0].Worker.ID, "--apply"}, &out); err == nil || out.Len() != 0 {
		t.Fatal("preview accepts a mutation request")
	}
	if err := runFrontRepairPreview(nil, io.Discard); err == nil {
		t.Fatal("preview guesses a default Worker component")
	}
}
