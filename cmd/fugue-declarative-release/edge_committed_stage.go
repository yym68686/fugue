package main

import (
	"context"

	"fugue/internal/releaseguardian"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func (runtime *kubectlEdgeGroupRuntime) committedStageForRelease(ctx context.Context, current releaseguardian.CurrentAuthority, staged edgeCandidateStageReceipt) (edgeCandidateStageReceipt, bool) {
	if edgeCurrentAuthorityMatchesCandidate(current, staged) {
		return staged, true
	}
	if current.Validate() != nil || runtime.client == nil || current.GroupID != staged.GroupID || string(current.CurrentWorkerSlot) != staged.WorkerSlot ||
		current.CurrentWorkerSourceSHA != staged.WorkerSourceSHA || current.CurrentWorkerImageDigest != staged.WorkerImageDigest || !edgePromotionDigestPattern.MatchString(staged.ReleaseRecordDigest) {
		return staged, false
	}
	object, err := runtime.client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace(runtime.release.Workload.Namespace).
		Get(ctx, "fugue-candidate-authority-"+staged.GroupID, metav1.GetOptions{})
	if err != nil || object.GetUID() == "" || object.GetResourceVersion() == "" || object.GetLabels()["fugue.pro/group"] != staged.GroupID || object.GetLabels()["fugue.pro/authority-store"] != "true" {
		return staged, false
	}
	raw, found, err := unstructured.NestedString(object.Object, "data", "candidate.json")
	var candidate releaseguardian.CandidateAuthority
	if err != nil || !found || decodeStrictJSON([]byte(raw), &candidate) != nil {
		return staged, false
	}
	return resolveCommittedStage(current, staged, candidate)
}

func resolveCommittedStage(current releaseguardian.CurrentAuthority, staged edgeCandidateStageReceipt, candidate releaseguardian.CandidateAuthority) (edgeCandidateStageReceipt, bool) {
	if candidate.Validate() != nil || candidate.State != releaseguardian.CandidateAuthorityVerified || candidate.RecordDigest != current.CurrentRecordDigest ||
		candidate.ReleaseRecordDigest != staged.ReleaseRecordDigest || !edgePromotionDigestPattern.MatchString(staged.ReleaseRecordDigest) ||
		candidate.GroupID != staged.GroupID || string(candidate.WorkerSlot) != staged.WorkerSlot || candidate.WorkerSourceSHA != staged.WorkerSourceSHA || candidate.WorkerImageDigest != staged.WorkerImageDigest ||
		candidate.CandidateEpoch < staged.CandidateEpoch || candidate.AuthoritySequence < staged.AuthoritySequence || candidate.AllowDegradedPrevious != staged.AllowDegradedPrevious ||
		string(current.PreviousWorkerSlot) != staged.CurrentWorkerSlot {
		return staged, false
	}
	resolved := staged
	resolved.CandidateRecordDigest, resolved.CandidateBundleGeneration, resolved.CandidateEpoch = candidate.RecordDigest, candidate.ServingGeneration, candidate.CandidateEpoch
	return resolved, edgeCurrentAuthorityMatchesCandidate(current, resolved)
}
