package imagecachepolicy

import (
	"fugue/internal/model"
	"strings"
	"testing"
	"time"
)

func TestProtectedGenerationAlwaysWinsRetirement(t *testing.T) {
	old := time.Now().Add(-72 * time.Hour)
	m := model.ImageCacheManifest{Repo: "demo", Target: "old", Digest: "sha256:" + strings.Repeat("a", 64), CreatedAtObserved: &old}
	for _, facts := range []Facts{{ActiveDigest: true, Deleted: true}, {Migration: true, Deleted: true}, {Live: true, Deleted: true}, {Pin: true, Deleted: true}, {Task: true, Deleted: true}, {KeeperIDs: []string{"replica"}, Deleted: true}} {
		got := Evaluate(m, facts, time.Now(), DefaultGracePeriod)
		if !got.Protected || got.LifecycleState != "protected" {
			t.Fatalf("retirement escaped protection: %+v", got)
		}
	}
	got := Evaluate(m, Facts{}, time.Now(), DefaultGracePeriod)
	if got.LifecycleState != "quarantined" || len(got.RetirementEvidence) > 0 {
		t.Fatalf("unknown became authorized: %+v", got)
	}
	got = Evaluate(m, Facts{Deleted: true, ImageIDs: []string{"retired-generation"}}, time.Now(), DefaultGracePeriod)
	if got.LifecycleState != "delete_eligible" || len(got.RetirementEvidence) != 1 {
		t.Fatalf("positive authority lost: %+v", got)
	}
}
func TestPlanDigestIgnoresTimestampButBindsContent(t *testing.T) {
	p := model.ImageCachePrunePlan{NodeID: "node", Mode: "delete", Candidates: []model.ImageCachePruneCandidate{{Repo: "demo", Target: "tag", Digest: "sha256:first", Reason: "deleted_image_generation"}}}
	Seal(&p)
	first := p.PlanHash
	p.CreatedAt = time.Now()
	p.ID = "another"
	Seal(&p)
	if p.PlanHash != first {
		t.Fatal("timestamp changed authority hash")
	}
	p.Candidates[0].Digest = "sha256:replacement"
	Seal(&p)
	if p.PlanHash == first {
		t.Fatal("different content reused receipt")
	}
}
