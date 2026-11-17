package api

import (
	"fugue/internal/model"
	"testing"
)

func TestImageCacheOrphanDigestClaimRevalidatesEveryDigest(t *testing.T) {
	plan := model.ImageCachePrunePlan{UnreferencedBlobs: []model.ImageCachePruneBlobCandidate{{Digest: "sha256:admitted"}}}
	for _, tc := range []struct {
		name, raw string
		allowed   bool
	}{
		{"admitted", `["sha256:admitted"]`, true},
		{"empty", `[]`, true},
		{"became_protected", `["sha256:admitted","sha256:protected"]`, false},
		{"malformed", `{}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason := imageCacheOrphanDigestRefusal(map[string]string{"unreferenced_blob_digests": tc.raw}, plan)
			if (reason == "") != tc.allowed {
				t.Fatalf("claim allowed=%v, reason=%q", tc.allowed, reason)
			}
		})
	}
}
