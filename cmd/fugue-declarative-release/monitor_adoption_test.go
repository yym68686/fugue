package main

import (
	"strings"
	"testing"

	"fugue/internal/releaseguardian"
)

func TestCommittedGuardianCandidateFenceRequiresExactPublishedFailure(t *testing.T) {
	key := releaseguardian.Key{Component: "edge-worker-de", Group: "de"}
	stableDigest := "sha256:" + strings.Repeat("1", 64)
	candidate, err := releaseguardian.NewReleaseRecord(key, strings.Repeat("2", 40), "sha256:"+strings.Repeat("3", 64),
		"sha256:"+strings.Repeat("4", 64), stableDigest, "sha256:"+strings.Repeat("5", 64))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := releaseguardian.Snapshot{
		Key: key, Record: candidate, Managed: true, CurrentRecordDigest: stableDigest, LastSuccessfulLKG: stableDigest,
		Desired: releaseguardian.DesiredRelease{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.DesiredReleaseKind,
			Component: key.Component, Group: key.Group, RecordDigest: candidate.RecordDigest, Generation: 9},
		PreviousStatus: &releaseguardian.ReleaseStatus{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.ReleaseStatusKind,
			Component: key.Component, Group: key.Group, State: releaseguardian.StateRecoveryRequired,
			CurrentRecordDigest: stableDigest, TargetRecordDigest: candidate.RecordDigest, LastSuccessfulLKG: stableDigest,
			Reason: "lkg-unproven: committed authority receipt was lost", RolloutReceiptDigest: "sha256:" + strings.Repeat("6", 64)},
	}
	if !committedGuardianCandidateFenced(snapshot, key, candidate) {
		t.Fatal("exact committed Guardian failure was rejected")
	}

	for name, mutate := range map[string]func(*releaseguardian.Snapshot, *releaseguardian.ReleaseRecord){
		"unmanaged": func(value *releaseguardian.Snapshot, _ *releaseguardian.ReleaseRecord) { value.Managed = false },
		"desired drift": func(value *releaseguardian.Snapshot, _ *releaseguardian.ReleaseRecord) {
			value.Desired.RecordDigest = stableDigest
		},
		"current drift": func(value *releaseguardian.Snapshot, _ *releaseguardian.ReleaseRecord) {
			value.CurrentRecordDigest = "sha256:" + strings.Repeat("7", 64)
		},
		"candidate drift": func(_ *releaseguardian.Snapshot, value *releaseguardian.ReleaseRecord) {
			value.ConfigSHA = strings.Repeat("8", 40)
		},
		"target drift": func(value *releaseguardian.Snapshot, _ *releaseguardian.ReleaseRecord) {
			value.PreviousStatus.TargetRecordDigest = stableDigest
		},
		"missing receipt": func(value *releaseguardian.Snapshot, _ *releaseguardian.ReleaseRecord) {
			value.PreviousStatus.RolloutReceiptDigest = ""
		},
		"rollback receipt": func(value *releaseguardian.Snapshot, _ *releaseguardian.ReleaseRecord) {
			value.PreviousStatus.RollbackReceiptDigest = "sha256:" + strings.Repeat("9", 64)
		},
		"wrong reason": func(value *releaseguardian.Snapshot, _ *releaseguardian.ReleaseRecord) {
			value.PreviousStatus.Reason = "ordinary failure"
		},
		"wrong state": func(value *releaseguardian.Snapshot, _ *releaseguardian.ReleaseRecord) {
			value.PreviousStatus.State = releaseguardian.StateDegraded
		},
	} {
		t.Run(name, func(t *testing.T) {
			value, record := snapshot, candidate
			status := *snapshot.PreviousStatus
			value.PreviousStatus = &status
			mutate(&value, &record)
			if committedGuardianCandidateFenced(value, key, record) {
				t.Fatal("drifted committed Guardian failure was accepted")
			}
		})
	}
}
