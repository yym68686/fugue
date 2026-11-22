package releaseguardian

import (
	"testing"

	"fugue/internal/declarativerelease"
)

func TestStableFrontRepairWorkerProofBindsGroupPlanAndCommittedCode(t *testing.T) {
	target := TargetConfig{Key: Key{Component: "edge-worker-fixture", Group: "fixture"}, Namespace: "fixture-system",
		MonitorComponent: "edge-worker-fixture", DependencyService: "fixture-control"}
	current := CurrentAuthority{GroupID: "edge-group-fixture", CurrentRecordDigest: otherDigest,
		CurrentWorkerSourceSHA: testSHA, CurrentWorkerImageDigest: testDigest}
	release := declarativerelease.PlanRelease{
		ComponentID: target.Key.Component, Concurrency: "fugue-production-" + target.Key.Component,
		Workload: declarativerelease.Workload{Namespace: target.Namespace, Name: "fixture-front"},
		Artifact: declarativerelease.Artifact{Repository: "ghcr.io/example/worker"},
		Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{
			GroupID: current.GroupID, FrontName: "fixture-front"}},
		Delivery: &declarativerelease.Delivery{Writer: "guardian", Group: target.Key.Group, DependencyService: target.DependencyService},
	}
	plan := declarativerelease.Plan{Releases: []declarativerelease.PlanRelease{release}, PlanDigest: testDigest}
	artifact := declarativerelease.ArtifactReceipt{Component: release.ComponentID, OCIRevision: testSHA, TopDigest: testDigest,
		ImmutableRef: release.Artifact.Repository + "@" + testDigest, ReceiptDigest: otherDigest}
	prepared := declarativerelease.ExecutionPlan{Component: release.ComponentID, Forward: declarativerelease.TargetIdentity{
		OCIRevision: testSHA, ImageRef: artifact.ImmutableRef}}
	monitor := declarativerelease.MonitorRecord{Component: release.ComponentID, RecordDigest: testDigest}
	proof, err := bindFrontRepairWorkerRelease(target, current, plan, artifact, prepared, monitor)
	if err != nil || proof.Release.ComponentID != release.ComponentID || proof.PlanDigest != plan.PlanDigest ||
		proof.ArtifactDigest != artifact.ReceiptDigest || proof.WorkerSourceSHA != testSHA || proof.WorkerImageDigest != testDigest {
		t.Fatalf("did not bind the immutable current Worker release: %+v, err=%v", proof, err)
	}
	for _, tc := range []struct {
		name   string
		change func(*CurrentAuthority, *declarativerelease.PlanRelease, *declarativerelease.ArtifactReceipt, *declarativerelease.MonitorRecord)
	}{
		{"different physical group", func(c *CurrentAuthority, _ *declarativerelease.PlanRelease, _ *declarativerelease.ArtifactReceipt, _ *declarativerelease.MonitorRecord) {
			c.GroupID = "another-group"
		}},
		{"wrong correlation lock", func(_ *CurrentAuthority, r *declarativerelease.PlanRelease, _ *declarativerelease.ArtifactReceipt, _ *declarativerelease.MonitorRecord) {
			r.Concurrency = "another-lock"
		}},
		{"monitor for another release", func(_ *CurrentAuthority, _ *declarativerelease.PlanRelease, _ *declarativerelease.ArtifactReceipt, m *declarativerelease.MonitorRecord) {
			m.Component = "another-worker"
		}},
		{"different image", func(c *CurrentAuthority, _ *declarativerelease.PlanRelease, _ *declarativerelease.ArtifactReceipt, _ *declarativerelease.MonitorRecord) {
			c.CurrentWorkerImageDigest = otherDigest
		}},
		{"wrong registry", func(_ *CurrentAuthority, _ *declarativerelease.PlanRelease, a *declarativerelease.ArtifactReceipt, _ *declarativerelease.MonitorRecord) {
			a.ImmutableRef = "ghcr.io/example/different@" + testDigest
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, r, a, m := current, release, artifact, monitor
			tc.change(&c, &r, &a, &m)
			p := plan
			p.Releases = []declarativerelease.PlanRelease{r}
			if _, err := bindFrontRepairWorkerRelease(target, c, p, a, prepared, m); err == nil {
				t.Fatal("unbound production Worker plan authorized a repair")
			}
		})
	}
}
