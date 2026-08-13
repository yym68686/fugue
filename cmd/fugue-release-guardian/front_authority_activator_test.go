package main

import (
	"strings"
	"testing"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestFrontLKGGenerationAcceptsOnlyExactCompensationChain(t *testing.T) {
	base := uint64(34)
	if !frontLKGGenerationMatches(edgegroupfront.ActivationState{Generation: base}, base, edgegroupfront.ActivationOperationPromote) {
		t.Fatal("exact baseline generation was rejected")
	}
	compensated := edgegroupfront.ActivationState{Generation: base + 2, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 1}
	if !frontLKGGenerationMatches(compensated, base, edgegroupfront.ActivationOperationPromote) {
		t.Fatal("exact adjacent compensation was rejected")
	}
	secondCompensation := edgegroupfront.ActivationState{Generation: base + 4, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 3}
	if !frontLKGGenerationMatches(secondCompensation, base, edgegroupfront.ActivationOperationPromote) {
		t.Fatal("second exact compensation in one durable journal was rejected")
	}
	for name, state := range map[string]edgegroupfront.ActivationState{
		"generation gap":   {Generation: base + 3, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 2},
		"wrong link":       {Generation: base + 2, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base},
		"later wrong link": {Generation: base + 4, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 2},
		"not rollback":     {Generation: base + 2, Operation: edgegroupfront.ActivationOperationPromote},
	} {
		t.Run(name, func(t *testing.T) {
			if frontLKGGenerationMatches(state, base, edgegroupfront.ActivationOperationPromote) {
				t.Fatal("non-adjacent compensation was accepted")
			}
		})
	}
	if !frontLKGGenerationMatches(compensated, base, edgegroupfront.ActivationOperationRollback) {
		t.Fatal("restore retry rejected exact compensation chain")
	}
}

func TestFrontRestoreGenerationAcceptsOnlyExactRetryChain(t *testing.T) {
	base := uint64(95)
	if !frontTargetGenerationMatches(edgegroupfront.ActivationState{Generation: base + 1}, base, edgegroupfront.ActivationOperationRollback) {
		t.Fatal("first restore generation was rejected")
	}
	retry := edgegroupfront.ActivationState{Generation: base + 3, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 2}
	if !frontTargetGenerationMatches(retry, base, edgegroupfront.ActivationOperationRollback) {
		t.Fatal("exact compensated restore retry was rejected")
	}
	for name, state := range map[string]edgegroupfront.ActivationState{
		"even target":  {Generation: base + 2, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 1},
		"wrong link":   {Generation: base + 3, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 1},
		"not rollback": {Generation: base + 3, Operation: edgegroupfront.ActivationOperationPromote},
	} {
		t.Run(name, func(t *testing.T) {
			if frontTargetGenerationMatches(state, base, edgegroupfront.ActivationOperationRollback) {
				t.Fatal("invalid restore retry generation was accepted")
			}
		})
	}
}

func TestAuthorityRuntimeRequiresExactWorkerAndFrontIdentity(t *testing.T) {
	source := strings.Repeat("1", 40)
	digest := "sha256:" + strings.Repeat("2", 64)
	worker := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Annotations: map[string]string{"fugue.pro/source-commit": source}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "edge", Ready: true,
			ImageID: "ghcr.io/example/edge@" + digest}}}}
	workers := map[string]corev1.Pod{"node-a": worker}
	cohort := releaseguardian.CandidateWorkerCohort{WorkerSourceSHA: source, WorkerImageDigest: digest}
	fronts := map[string]observedFront{"node-a": {Generation: 12, ActiveSlot: "a", BundleGeneration: "bundle-a.p8.r1",
		WorkerSourceCommit: source, WorkerImageDigest: digest}}
	if !authorityRuntimeMatches(workers, cohort, fronts, releaseguardian.AuthoritySlotA, source, digest, 12, "bundle-a.p8.r1", true) {
		t.Fatal("exact current authority runtime was rejected")
	}
	if !authorityRuntimeMatches(workers, cohort, nil, releaseguardian.AuthoritySlotA, source, digest, 0, "bundle-a.p8.r1", false) {
		t.Fatal("exact inactive LKG Worker was rejected")
	}
	oldSource := strings.Repeat("3", 40)
	oldDigest := "sha256:" + strings.Repeat("4", 64)
	oldWorker := worker
	oldWorker.Annotations = map[string]string{"fugue.pro/source-commit": oldSource}
	oldWorker.Status.ContainerStatuses[0].ImageID = "ghcr.io/example/edge@" + oldDigest
	if authorityRuntimeMatches(map[string]corev1.Pod{"node-a": oldWorker}, cohort, fronts, releaseguardian.AuthoritySlotA, source, digest, 12, "bundle-a.p8.r1", true) {
		t.Fatal("candidate pointer accepted a compensated LKG Worker runtime")
	}
	if authorityRuntimeMatches(workers, cohort, map[string]observedFront{"node-a": {
		Generation: 12, ActiveSlot: "a", BundleGeneration: "bundle-a.p8.r1", WorkerSourceCommit: oldSource, WorkerImageDigest: oldDigest,
	}}, releaseguardian.AuthoritySlotA, source, digest, 12, "bundle-a.p8.r1", true) {
		t.Fatal("candidate pointer accepted a compensated LKG Front runtime")
	}
}
