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

func TestRetirementAgeRequiresPositiveAuthorityAndCompleteGraph(t *testing.T) {
	now := time.Now().UTC()
	created := now.Add(-30 * time.Minute)
	manifest := model.ImageCacheManifest{Digest: "sha256:" + strings.Repeat("a", 64), CreatedAtObserved: &created, GraphStatus: "complete"}
	for _, scenario := range []string{"retired", "unknown", "no-image", "incomplete", "pin", "live", "task", "migration", "local-pin", "keeper", "young"} {
		t.Run(scenario, func(t *testing.T) {
			m := manifest
			facts := Facts{Deleted: true, ImageIDs: []string{"retired"}}
			switch scenario {
			case "unknown":
				facts.Deleted = false
			case "no-image":
				facts.ImageIDs = nil
			case "incomplete":
				m.GraphStatus = "incomplete"
			case "pin":
				facts.Pin = true
			case "live":
				facts.Live = true
			case "task":
				facts.Task = true
			case "migration":
				facts.Migration = true
			case "local-pin":
				m.PinnedLocally = true
			case "keeper":
				facts.KeeperIDs = []string{"keeper"}
			case "young":
				fresh := now.Add(-5 * time.Minute)
				m.CreatedAtObserved = &fresh
			}
			got := Evaluate(m, facts, now, DefaultGracePeriod)
			if scenario == "retired" {
				if got.Protected || got.LifecycleState != "delete_eligible" {
					t.Fatal(got)
				}
				return
			}
			if !got.Protected {
				t.Fatal("age bypass escaped guard", scenario, got)
			}
		})
	}
}

func TestProtectionReasonAndExecutionAgeArePrecise(t *testing.T) {
	m := model.ImageCacheManifest{Digest: "sha256:" + strings.Repeat("a", 64)}
	got := Evaluate(m, Facts{ActiveDigest: true, ActiveDigestReason: "active_app_image", ImageIDs: []string{"image"}}, time.Now(), DefaultGracePeriod)
	if got.SkipReason != "active_app_image" || len(got.MatchedImageIDs) != 1 || len(got.MatchedWorkloadRefs) != 0 {
		t.Fatal(got)
	}
	p := model.ImageCachePrunePlan{MinManifestAge: DefaultGracePeriod.String()}
	if ExecutionMinAge(p) != DefaultGracePeriod.String() {
		t.Fatal("unknown age relaxed")
	}
	p.Candidates = []model.ImageCachePruneCandidate{{Reason: "deleted_image_generation", RetirementEvidence: []string{"retired"}}}
	if ExecutionMinAge(p) != ConfirmedRetirementGracePeriod.String() {
		t.Fatal("node age disagrees with policy")
	}
}
