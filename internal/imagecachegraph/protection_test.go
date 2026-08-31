package imagecachegraph

import (
	"testing"

	"fugue/internal/model"
)

func TestProtectManifestGraphPropagatesParentDisposition(t *testing.T) {
	t.Parallel()

	const (
		protectedChild = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		deletedChild   = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		unknownChild   = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	)
	manifests := []model.ImageCacheManifest{
		{Repo: "fugue-apps/demo", Target: "protected-parent", Digest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", ReferencedManifests: []string{protectedChild}},
		{Repo: "fugue-apps/demo", Target: protectedChild, Digest: protectedChild},
		{Repo: "fugue-apps/demo", Target: "deleted-parent", Digest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", ReferencedManifests: []string{deletedChild}},
		{Repo: "fugue-apps/demo", Target: deletedChild, Digest: deletedChild},
		{Repo: "fugue-apps/demo", Target: "unknown-parent", Digest: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", ReferencedManifests: []string{unknownChild}},
		{Repo: "fugue-apps/demo", Target: unknownChild, Digest: unknownChild},
	}
	candidates := []model.ImageCachePruneCandidate{
		{Repo: manifests[0].Repo, Target: manifests[0].Target, Digest: manifests[0].Digest, Protected: true, SkipReason: "current_workload"},
		{Repo: manifests[1].Repo, Target: manifests[1].Target, Digest: manifests[1].Digest, Reason: "missing_control_plane_image"},
		{Repo: manifests[2].Repo, Target: manifests[2].Target, Digest: manifests[2].Digest, Reason: "deleted_image_generation", MatchedImageIDs: []string{"img_deleted"}},
		{Repo: manifests[3].Repo, Target: manifests[3].Target, Digest: manifests[3].Digest, Reason: "missing_control_plane_image"},
		{Repo: manifests[4].Repo, Target: manifests[4].Target, Digest: manifests[4].Digest, Reason: "missing_control_plane_image"},
		{Repo: manifests[5].Repo, Target: manifests[5].Target, Digest: manifests[5].Digest, Reason: "missing_control_plane_image"},
	}

	got := ProtectManifestGraph(manifests, candidates)
	if !got[1].Protected || got[1].SkipReason != "referenced_by_protected_manifest" {
		t.Fatalf("protected parent did not protect child: %+v", got[1])
	}
	if got[3].Protected || got[3].Reason != "deleted_image_generation" || len(got[3].MatchedImageIDs) != 1 {
		t.Fatalf("authorized parent disposition did not reach child: %+v", got[3])
	}
	if !got[5].Protected || got[5].SkipReason != "referenced_by_quarantined_manifest" {
		t.Fatalf("untracked parent did not quarantine child: %+v", got[5])
	}
}

func TestProtectManifestGraphQuarantinesIndexWithoutChildEvidence(t *testing.T) {
	t.Parallel()

	manifests := []model.ImageCacheManifest{
		{
			Repo:      "fugue-apps/demo",
			Target:    "legacy-index",
			Digest:    "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			MediaType: "application/vnd.oci.image.index.v1+json; charset=utf-8",
		},
		{
			Repo:      "fugue-apps/demo",
			Target:    "ordinary-manifest",
			Digest:    "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			MediaType: "application/vnd.oci.image.manifest.v1+json",
		},
		{
			Repo:   "fugue-apps/demo",
			Target: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			Digest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		},
	}
	candidates := []model.ImageCachePruneCandidate{
		{Repo: manifests[0].Repo, Target: manifests[0].Target, Digest: manifests[0].Digest, Reason: "missing_control_plane_image"},
		{Repo: manifests[1].Repo, Target: manifests[1].Target, Digest: manifests[1].Digest, Reason: "deleted_image_generation"},
		{Repo: manifests[2].Repo, Target: manifests[2].Target, Digest: manifests[2].Digest, Reason: "missing_control_plane_image"},
	}

	got := ProtectManifestGraph(manifests, candidates)
	if !got[0].Protected || got[0].Reason != "" || got[0].SkipReason != "manifest_graph_evidence_missing" {
		t.Fatalf("index without child evidence was not quarantined: %+v", got[0])
	}
	if got[1].Protected || got[1].Reason != "deleted_image_generation" {
		t.Fatalf("ordinary manifest was changed: %+v", got[1])
	}
	if !got[2].Protected || got[2].SkipReason != "manifest_graph_evidence_missing" {
		t.Fatalf("same-digest alias was not quarantined: %+v", got[2])
	}
}

func TestOrderCandidatesParentsFirstBeforeApplyingTargetLimit(t *testing.T) {
	t.Parallel()

	const childDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	candidates := []model.ImageCachePruneCandidate{
		{
			Repo:               "fugue-apps/demo",
			Target:             childDigest,
			Digest:             childDigest,
			Reason:             "deleted_image_generation",
			PlannedDeleteBytes: 10 << 30,
		},
		{
			Repo:                "fugue-apps/demo",
			Target:              "deleted-parent",
			Digest:              "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Reason:              "deleted_image_generation",
			PlannedDeleteBytes:  1,
			ReferencedManifests: []string{childDigest},
		},
	}

	got := OrderCandidatesParentsFirst(candidates)
	if len(got) != 2 || got[0].Target != "deleted-parent" || got[1].Target != childDigest {
		t.Fatalf("candidate order = %+v, want parent before larger child", got)
	}
}

func TestSelectCandidatesWithGraphClosureUsesSoftLimit(t *testing.T) {
	t.Parallel()

	const childDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	candidates := []model.ImageCachePruneCandidate{
		{
			Repo:                "fugue-apps/demo",
			Target:              "deleted-parent",
			Digest:              "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Reason:              "deleted_image_generation",
			ReferencedManifests: []string{childDigest},
		},
		{
			Repo:   "fugue-apps/demo",
			Target: childDigest,
			Digest: childDigest,
			Reason: "deleted_image_generation",
		},
		{
			Repo:   "fugue-apps/demo",
			Target: "unrelated",
			Digest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			Reason: "deleted_image_generation",
		},
	}

	got := SelectCandidatesWithGraphClosure(candidates, 1)
	if len(got) != 2 || got[0].Target != "deleted-parent" || got[1].Target != childDigest {
		t.Fatalf("selected candidates = %+v, want complete parent graph despite limit", got)
	}
}
