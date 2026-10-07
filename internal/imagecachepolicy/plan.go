package imagecachepolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fugue/internal/model"
	"sort"
)

// Seal binds the deletion explanation to identities and policy without making
// inventory refresh timestamps a new authorization. A receipt is never a
// substitute for the fresh guard at claim time or node-local graph checks.
func Seal(plan *model.ImageCachePrunePlan) {
	plan.PolicyVersion = Version
	type row struct {
		Repo, Target, Digest, Reason, Skip string
		Protected                          bool
		Evidence                           []string
	}
	rows := []row{}
	for _, set := range [][]model.ImageCachePruneCandidate{plan.Candidates, plan.ProtectedManifests} {
		for _, c := range set {
			ev := append(append([]string{}, c.MatchedImageIDs...), c.MatchedReplicaIDs...)
			ev = append(ev, c.RetirementEvidence...)
			sort.Strings(ev)
			rows = append(rows, row{c.Repo, c.Target, c.Digest, c.Reason, c.SkipReason, c.Protected, ev})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, _ := json.Marshal(rows[i])
		b, _ := json.Marshal(rows[j])
		return string(a) < string(b)
	})
	blobs := append([]model.ImageCachePruneBlobCandidate{}, plan.UnreferencedBlobs...)
	sort.Slice(blobs, func(i, j int) bool { return blobs[i].Digest < blobs[j].Digest })
	raw, _ := json.Marshal(struct {
		Version, Node, Cluster, Runtime, Mode, Grace string
		Budget                                       int64
		Rows                                         []row
		Blobs                                        []model.ImageCachePruneBlobCandidate
	}{Version, plan.NodeID, plan.ClusterNodeName, plan.RuntimeID, plan.Mode, plan.MinManifestAge, plan.MaxDeleteBytes, rows, blobs})
	sum := sha256.Sum256(raw)
	plan.PlanHash = "sha256:" + hex.EncodeToString(sum[:])
}
