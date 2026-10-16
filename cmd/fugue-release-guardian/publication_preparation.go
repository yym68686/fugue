package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
)

// Prepare every target while Front still selects the previous slot. The
// preparation file is not a serving pointer: Workers must independently load
// the signed current publication, with Caddy applied, before traffic moves.
func (activator *frontAuthorityActivator) prepareWorkerPublications(ctx context.Context, target releaseguardian.FrontAuthorityTarget, before frontAuthorityPreflight) error {
	if before.alreadyAtNew {
		return nil // recovery observes the already-committed authority below
	}
	prepared := make(map[string]edgegroupfront.PublicationPreparation, len(before.workers))
	for node, worker := range before.workers {
		state := before.states[node]
		args := []string{frontActivationCASBinary, "--state-file", frontActivationStatePath,
			"--group", target.GroupID, "--expected-generation", strconv.FormatUint(state.Generation, 10),
			"--expected-slot", state.ActiveSlot, "--target-slot", string(target.TargetSlot),
			"--bundle-generation", target.FrontBundleGeneration, "--worker-source-commit", target.WorkerSourceSHA,
			"--worker-image-digest", target.WorkerImageDigest, "--operation", edgegroupfront.ActivationOperationPromote,
			"--prepare-publication", "--reason", "prepare signed current publication before public traffic"}
		raw, err := activator.executor.Exec(ctx, activator.config.Namespace, worker.Name, "edge", args...)
		var receipt edgegroupfront.PublicationPreparation
		if err != nil || decodeStrictJSON(raw, &receipt) != nil ||
			!receipt.Matches(state, string(target.TargetSlot), target.WorkerSourceSHA, activator.now()) ||
			receipt.BundleGeneration != target.FrontBundleGeneration || receipt.WorkerImageDigest != target.WorkerImageDigest {
			return errors.New("target Worker cannot prepare the exact current publication; Front unchanged")
		}
		prepared[node] = receipt
	}
	waitCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	for {
		for _, receipt := range prepared {
			if !activator.now().Add(5 * time.Second).Before(receipt.ExpiresAt) {
				return errors.New("Worker publication preparation expired before traffic activation")
			}
		}
		if activator.preparedWorkersLoaded(waitCtx, target) {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("target Workers did not load signed current publication before activation: %w", waitCtx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (activator *frontAuthorityActivator) preparedWorkersLoaded(ctx context.Context, target releaseguardian.FrontAuthorityTarget) bool {
	workers, cohort, err := activator.observeWorkers(ctx, target)
	if err != nil || cohort.CohortDigest != target.WorkerCohortDigest {
		return false
	}
	for _, worker := range workers {
		if worker.Status.PodIP == "" {
			return false
		}
		var health authorityWorkerHealth
		if readAuthorityRuntimeJSON(ctx, "http://"+worker.Status.PodIP+":"+strconv.Itoa(workerHealthPort)+"/healthz", &health) != nil ||
			!preparedWorkerPublicationReady(health, target) {
			return false
		}
	}
	return true
}

func preparedWorkerPublicationReady(health authorityWorkerHealth, target releaseguardian.FrontAuthorityTarget) bool {
	return !health.CandidateBundleLoaded && health.CaddyAppliedVersion == health.BundleVersion &&
		authorityWorkerHealthAtOrAfter(health, target.GroupID, target.FrontBundleGeneration)
}
