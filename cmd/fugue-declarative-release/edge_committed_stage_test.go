package main

import (
	"strings"
	"testing"

	"fugue/internal/releaseguardian"
)

func TestCommittedConfigurationRefreshMustBindSameImmutableCodeRelease(t *testing.T) {
	digest := func(value string) string { return "sha256:" + strings.Repeat(value, 64) }
	stage := edgeCandidateStageReceipt{GroupID: "edge-group-test", CandidateRecordDigest: digest("1"), CandidateBundleGeneration: "old-config",
		WorkerSlot: "b", CurrentWorkerSlot: "a", WorkerSourceSHA: strings.Repeat("2", 40), WorkerImageDigest: digest("3"), ReleaseRecordDigest: digest("4"), CandidateEpoch: 11, AuthoritySequence: 10}
	candidate := releaseguardian.CandidateAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CandidateAuthorityKind, GroupID: stage.GroupID,
		RecordDigest: digest("5"), BundleGeneration: "new-config.p13.r0", ServingGeneration: "new-config", AuthoritySequence: 12, CandidateSequence: 12,
		CurrentPublicationSequence: 12, CurrentBundleDigest: digest("6"), CurrentServingGeneration: "prior-config", CandidateEpoch: 13,
		WorkerSlot: releaseguardian.AuthoritySlotB, ReleaseRecordDigest: stage.ReleaseRecordDigest, WorkerSourceSHA: stage.WorkerSourceSHA, WorkerImageDigest: stage.WorkerImageDigest,
		State: releaseguardian.CandidateAuthorityVerified, Generation: 2, CanaryResultDigest: digest("7")}
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind, GroupID: stage.GroupID,
		CurrentRecordDigest: candidate.RecordDigest, CurrentWorkerSlot: releaseguardian.AuthoritySlotB, CurrentFrontGeneration: 12, CurrentBundleGeneration: "new-config.p14.r0",
		CurrentWorkerSourceSHA: stage.WorkerSourceSHA, CurrentWorkerImageDigest: stage.WorkerImageDigest, PreviousRecordDigest: digest("8"), PreviousWorkerSlot: releaseguardian.AuthoritySlotA,
		PreviousFrontGeneration: 11, PreviousBundleGeneration: "prior-config.p10.r0", PreviousWorkerSourceSHA: strings.Repeat("9", 40), PreviousWorkerImageDigest: digest("a"), AuthorityEpoch: 7}
	resolved, ok := resolveCommittedStage(current, stage, candidate)
	if !ok || resolved.CandidateRecordDigest != candidate.RecordDigest || !edgeCurrentAuthorityMatchesCandidate(current, resolved) {
		t.Fatal("committed configuration refresh was mistaken for a different code release")
	}
	for _, mutate := range []func(*releaseguardian.CandidateAuthority){
		func(value *releaseguardian.CandidateAuthority) { value.ReleaseRecordDigest = digest("b") },
		func(value *releaseguardian.CandidateAuthority) { value.WorkerSourceSHA = strings.Repeat("c", 40) },
		func(value *releaseguardian.CandidateAuthority) { value.RecordDigest = digest("c") },
		func(value *releaseguardian.CandidateAuthority) {
			value.State = releaseguardian.CandidateAuthorityLoaded
			value.CanaryResultDigest = ""
		},
		func(value *releaseguardian.CandidateAuthority) { value.CandidateEpoch = 10 },
	} {
		changed := candidate
		mutate(&changed)
		if _, ok := resolveCommittedStage(current, stage, changed); ok {
			t.Fatal("uncommitted or unrelated refresh accepted")
		}
	}
}
