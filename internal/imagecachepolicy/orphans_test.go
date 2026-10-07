package imagecachepolicy

import (
	"fugue/internal/model"
	"strings"
	"testing"
	"time"
)

func orphanFixture() (model.ImageOrphanPolicy, model.ImageCacheManifest, time.Time) {
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)
	return model.ImageOrphanPolicy{Generation: 2, Mode: "retire", RepositoryPrefixes: []string{"apps"}, QuarantineSeconds: 600, MinimumObservations: 2, InventoryMaxAgeSeconds: 7200, UpdatedAt: now.Add(-time.Hour)}, model.ImageCacheManifest{ClusterNodeName: "worker", Repo: "apps/sample", Target: "historical", Digest: "sha256:" + strings.Repeat("a", 64), GraphStatus: "complete", Present: true, CreatedAtObserved: &old, LastSeenAt: now}, now
}
func TestOrphanRetirementRequiresExplicitPolicyIndependentObservationsAndFreshGuards(t *testing.T) {
	p, m, now := orphanFixture()
	c := Evaluate(m, Facts{}, now, 24*time.Hour)
	d := ObserveOrphan(p, model.ImageOrphanDecision{}, m, c, true, now)
	for i := 0; i < 5; i++ {
		d = ObserveOrphan(p, d, m, c, true, now.Add(time.Hour))
	}
	if d.Observations != 1 || d.State != "quarantined" {
		t.Fatalf("same inventory advanced authority: %+v", d)
	}
	m.LastSeenAt = now.Add(10 * time.Minute)
	d = ObserveOrphan(p, d, m, c, true, m.LastSeenAt)
	if d.State != "retirement_authorized" || d.Ownership != "unknown" {
		t.Fatalf("missing independent authority: %+v", d)
	}
	approved := ApplyOrphanDecision(c, m, p, d, true, m.LastSeenAt)
	if approved.Reason != "orphan_retirement" || len(approved.RetirementEvidence) != 2 {
		t.Fatalf("missing receipt %+v", approved)
	}
	for _, tc := range []string{"coverage", "policy", "graph", "pin", "known", "stale"} {
		t.Run(tc, func(t *testing.T) {
			q, n, b, complete, at := p, m, c, true, m.LastSeenAt
			switch tc {
			case "coverage":
				complete = false
			case "policy":
				q.Generation++
			case "graph":
				n.ReferencedBlobs = []string{"changed"}
			case "pin":
				b = Evaluate(n, Facts{Pin: true}, at, 24*time.Hour)
			case "known":
				b.MatchedImageIDs = []string{"image"}
			case "stale":
				at = at.Add(3 * time.Hour)
			}
			if got := ApplyOrphanDecision(b, n, q, d, complete, at); got.Reason == "orphan_retirement" {
				t.Fatalf("guard bypass: %+v", got)
			}
		})
	}
	reset := ObserveOrphan(p, d, m, c, false, m.LastSeenAt)
	if reset.State != "protected" || reset.Observations != 0 {
		t.Fatalf("incomplete scan retained quarantine credit: %+v", reset)
	}
}
func TestOrphanCoverageRequiresAllParticipantsUnlessExplicitlyExcluded(t *testing.T) {
	p, m, now := orphanFixture()
	updaters := []model.NodeUpdater{{ID: "reporter", MachineID: "machine", ClusterNodeName: m.ClusterNodeName, Status: model.NodeUpdaterStatusActive, LastHeartbeatAt: &now, Capabilities: []string{model.NodeUpdateTaskTypeReportImageCache}}, {ID: "offline", ClusterNodeName: "offline", Status: model.NodeUpdaterStatusActive, Capabilities: []string{model.NodeUpdateTaskTypeReportImageCache}}}
	nodes := []model.ImageCacheNodeInventory{{NodeID: "machine", ClusterNodeName: m.ClusterNodeName, ReportedByNodeUpdaterID: "reporter", SnapshotComplete: true, ObservedAt: now}}
	if ok, _ := OrphanCoverage(p, nodes, updaters, now); ok {
		t.Fatal("offline participant silently dropped")
	}
	p.ExcludedNodes = []string{"offline"}
	if ok, r := OrphanCoverage(p, nodes, updaters, now); !ok {
		t.Fatal(r)
	}
	nodes[0].SnapshotComplete = false
	if ok, _ := OrphanCoverage(p, nodes, updaters, now); ok {
		t.Fatal("partial snapshot accepted")
	}
	m.ClusterNodeName = "offline"
	if OrphanScope(p, m) {
		t.Fatal("excluded node acquired delete authority")
	}
}
func TestOrphanRetirementStillProtectsSharedAliasAndGraph(t *testing.T) {
	p, m, now := orphanFixture()
	child := m
	child.Target = "child"
	child.Digest = "sha256:" + strings.Repeat("b", 64)
	m.ReferencedManifests = []string{child.Digest}
	c := Evaluate(child, Facts{}, now, 24*time.Hour)
	d := ObserveOrphan(p, model.ImageOrphanDecision{}, child, c, true, now)
	child.LastSeenAt = now.Add(10 * time.Minute)
	d = ObserveOrphan(p, d, child, c, true, child.LastSeenAt)
	c = ApplyOrphanDecision(c, child, p, d, true, child.LastSeenAt)
	parent := Evaluate(m, Facts{Pin: true}, now, 24*time.Hour)
	out := Finalize([]model.ImageCacheManifest{m, child}, []model.ImageCachePruneCandidate{parent, c}, model.ImageCachePruneModeDelete)
	if !out[1].Protected {
		t.Fatalf("pinned parent lost child: %+v", out)
	}
}
