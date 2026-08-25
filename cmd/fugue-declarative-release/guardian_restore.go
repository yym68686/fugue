package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func runRestoreMonitor(args []string, output io.Writer) error {
	return runRestoreMonitorContext(context.Background(), args, output)
}

func runRepairMonitorContext(parent context.Context, args []string, output io.Writer) error {
	if len(args) != 3 {
		return errors.New("usage: fugue-declarative-release repair-monitor MONITOR_DIR LKG_RECORD_DIGEST")
	}
	currentBundle, err := readMonitorDirectory(args[1])
	if err != nil {
		return err
	}
	release, err := selectedRelease(currentBundle.Plan, currentBundle.Record.Component)
	if err != nil {
		return err
	}
	store, err := newMonitorStore()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 8*time.Minute)
	defer cancel()
	current, err := store.load(ctx, release.Workload.Namespace, release.ComponentID)
	if err != nil || current.Bundle.Record != currentBundle.Record {
		return errors.New("live monitor pointer is not bound to the requested stable record")
	}
	lkgName, lkgBundle, err := loadBoundMonitorLKG(ctx, store, currentBundle, release, args[2])
	if err != nil {
		return err
	}
	cluster, err := newKubectlCluster()
	if err != nil {
		return err
	}
	lease, err := newComponentLeaseCoordinator()
	if err != nil {
		return err
	}
	held, err := lease.acquire(ctx, release, currentBundle.Record.ConfigSHA)
	if err != nil {
		return fmt.Errorf("acquire component mutation lease: %w", err)
	}
	fresh, freshErr := store.load(ctx, release.Workload.Namespace, release.ComponentID)
	if freshErr != nil || fresh.Bundle.Record != currentBundle.Record {
		releaseCtx, releaseCancel := componentLeaseFinalizationContext(ctx)
		_ = lease.release(releaseCtx, held)
		releaseCancel()
		return errors.New("monitor pointer changed after component Lease acquisition")
	}
	result := declarativerelease.RepairMonitoredForward(ctx, cluster, currentBundle.Plan, currentBundle.Prepared, currentBundle.Forward, currentBundle.LKG, release)
	finalizeCtx, finalizeCancel := componentLeaseFinalizationContext(ctx)
	defer finalizeCancel()
	var activateErr error
	lkgMetadataRestored := result.Status == "compensated"
	if result.Status == "recovery-required" && result.Reason == "continuous-repair-lkg-unproven" && result.LKGApplyCount == 1 &&
		result.Final.Matches(currentBundle.Prepared.LKG, release, true) &&
		errors.Join(cluster.MonitorConverged(finalizeCtx, release, currentBundle.LKG), cluster.VerifyOwnershipConverged(finalizeCtx, release, currentBundle.LKG)) == nil {
		// The predecessor can be byte-exact and ownership-converged while its
		// business health remains degraded.  Persist that truthful runtime
		// identity so a reviewed degraded-predecessor successor can repair it;
		// do not call it healthy or compensated.
		lkgMetadataRestored = true
	}
	if lkgMetadataRestored {
		_, activateErr = store.activateExistingRecord(finalizeCtx, fresh, lkgName, lkgBundle)
	}
	releaseErr := lease.release(finalizeCtx, held)
	raw, encodeErr := declarativerelease.CanonicalJSON(result)
	if encodeErr != nil {
		return encodeErr
	}
	if _, encodeErr = output.Write(append(raw, '\n')); encodeErr != nil {
		return encodeErr
	}
	if activateErr != nil {
		return fmt.Errorf("stable repair restored the LKG workload but monitor pointer activation failed: %w", activateErr)
	}
	if releaseErr != nil {
		return fmt.Errorf("stable repair terminal state is recorded but Lease release is unproven: %w", releaseErr)
	}
	if result.Status != "verified" && result.Status != "compensated" && !lkgMetadataRestored {
		return fmt.Errorf("Guardian stable repair ended with status=%s reason=%s", result.Status, result.Reason)
	}
	return nil
}

func runRestoreMonitorContext(parent context.Context, args []string, output io.Writer) error {
	if len(args) != 3 {
		return errors.New("usage: fugue-declarative-release restore-monitor MONITOR_DIR LKG_RECORD_DIGEST")
	}
	currentBundle, monitorErr := readMonitorDirectory(args[1])
	monitorRestore := monitorErr == nil
	var candidateBundle releaseguardian.ExecutionBundle
	var release declarativerelease.PlanRelease
	var err error
	if monitorRestore {
		release, err = selectedRelease(currentBundle.Plan, currentBundle.Record.Component)
	} else {
		files, readErr := readPlanDirectory(args[1])
		if readErr != nil {
			return monitorErr
		}
		plan, planErr := declarativerelease.DecodePlan(bytes.NewReader(files["release-plan.json"]))
		artifact, artifactErr := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(files["artifact-receipt.json"]))
		if planErr != nil || artifactErr != nil {
			return monitorErr
		}
		release, err = selectedRelease(plan, artifact.Component)
		if err != nil {
			return monitorErr
		}
		if release.Delivery == nil {
			return monitorErr
		}
		candidateBundle, err = releaseguardian.DecodeExecutionBundle(files, releaseguardian.Key{Component: release.ComponentID, Group: release.Delivery.Group})
	}
	if err != nil {
		return err
	}
	store, err := newMonitorStore()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 8*time.Minute)
	defer cancel()
	current, err := store.load(ctx, release.Workload.Namespace, release.ComponentID)
	if err != nil {
		return err
	}
	var lkgName string
	var lkgBundle monitorBundle
	if monitorRestore {
		if current.Bundle.Record != currentBundle.Record {
			return errors.New("live monitor pointer is not bound to the requested rollback record")
		}
		lkgName, lkgBundle, err = loadBoundMonitorLKG(ctx, store, currentBundle, release, args[2])
	} else {
		lkgName, lkgBundle, err = loadBoundExecutionLKG(ctx, store, candidateBundle, release, args[2])
	}
	if err != nil {
		return err
	}
	cluster, err := newKubectlCluster()
	if err != nil {
		return err
	}
	lease, err := newComponentLeaseCoordinator()
	if err != nil {
		return err
	}
	held, err := lease.acquire(ctx, release, current.Bundle.Record.ConfigSHA)
	if err != nil {
		return fmt.Errorf("acquire component mutation lease: %w", err)
	}
	fresh, freshErr := store.load(ctx, release.Workload.Namespace, release.ComponentID)
	if freshErr != nil || fresh.Bundle.Record != lkgBundle.Record {
		releaseCtx, releaseCancel := componentLeaseFinalizationContext(ctx)
		_ = lease.release(releaseCtx, held)
		releaseCancel()
		return errors.New("monitor pointer changed after component Lease acquisition")
	}
	plan := currentBundle.Plan
	prepared, forward, lkg := currentBundle.Prepared, currentBundle.Forward, currentBundle.LKG
	if !monitorRestore {
		plan, prepared, forward, lkg = candidateBundle.Plan, candidateBundle.Prepared, candidateBundle.Forward, candidateBundle.LKG
	}
	result := declarativerelease.RestoreMonitoredLKG(ctx, cluster, plan, prepared, forward, lkg, release)
	finalizeCtx, finalizeCancel := componentLeaseFinalizationContext(ctx)
	defer finalizeCancel()
	var activateErr error
	if monitorRestore && result.Status == "compensated" {
		_, activateErr = store.activateExistingRecord(finalizeCtx, fresh, lkgName, lkgBundle)
	}
	releaseErr := lease.release(finalizeCtx, held)
	raw, encodeErr := declarativerelease.CanonicalJSON(result)
	if encodeErr != nil {
		return encodeErr
	}
	if _, encodeErr = output.Write(append(raw, '\n')); encodeErr != nil {
		return encodeErr
	}
	if activateErr != nil {
		return fmt.Errorf("LKG workload is restored but monitor pointer activation failed: %w", activateErr)
	}
	if releaseErr != nil {
		return fmt.Errorf("LKG terminal state is recorded but Lease release is unproven: %w", releaseErr)
	}
	if result.Status != "compensated" {
		return fmt.Errorf("Guardian LKG rollback ended with status=%s reason=%s", result.Status, result.Reason)
	}
	return nil
}

func loadBoundExecutionLKG(ctx context.Context, store *monitorStore, candidate releaseguardian.ExecutionBundle, release declarativerelease.PlanRelease, recordDigest string) (string, monitorBundle, error) {
	if !strings.HasPrefix(recordDigest, "sha256:") || len(recordDigest) != 71 || strings.Trim(recordDigest[7:], "0123456789abcdef") != "" {
		return "", monitorBundle{}, errors.New("LKG monitor record digest is invalid")
	}
	lkgName := monitorRecordNameFromIdentity(release.ComponentID, recordDigest)
	lkgMap, err := store.client.ConfigMaps(release.Workload.Namespace).Get(ctx, lkgName, metav1.GetOptions{})
	if err != nil || lkgMap.Immutable == nil || !*lkgMap.Immutable {
		return "", monitorBundle{}, errors.New("immutable LKG monitor record is unavailable")
	}
	lkgBundle, err := decodeStoredMonitorBundle(lkgMap.Data)
	if err != nil || lkgBundle.Record.RecordDigest != recordDigest || !candidate.Prepared.LKG.Present ||
		candidate.Prepared.LKG.ManifestDigest != lkgBundle.Record.ForwardManifestDigest ||
		candidate.Prepared.LKG.ConfigSHA != lkgBundle.Prepared.Forward.ConfigSHA ||
		candidate.Prepared.LKG.OCIRevision != lkgBundle.Prepared.Forward.OCIRevision ||
		candidate.Prepared.LKG.ImageRef != lkgBundle.Prepared.Forward.ImageRef {
		return "", monitorBundle{}, errors.New("LKG monitor record is not the exact predecessor of the candidate release")
	}
	return lkgName, lkgBundle, nil
}

func loadBoundMonitorLKG(ctx context.Context, store *monitorStore, currentBundle monitorBundle, release declarativerelease.PlanRelease, recordDigest string) (string, monitorBundle, error) {
	if !strings.HasPrefix(recordDigest, "sha256:") || len(recordDigest) != 71 || strings.Trim(recordDigest[7:], "0123456789abcdef") != "" {
		return "", monitorBundle{}, errors.New("LKG monitor record digest is invalid")
	}
	lkgName := monitorRecordNameFromIdentity(release.ComponentID, recordDigest)
	lkgMap, err := store.client.ConfigMaps(release.Workload.Namespace).Get(ctx, lkgName, metav1.GetOptions{})
	if err != nil || lkgMap.Immutable == nil || !*lkgMap.Immutable {
		return "", monitorBundle{}, errors.New("immutable LKG monitor record is unavailable")
	}
	lkgBundle, err := decodeStoredMonitorBundle(lkgMap.Data)
	if err != nil || lkgBundle.Record.RecordDigest != recordDigest ||
		currentBundle.Record.LKGManifestDigest != lkgBundle.Record.ForwardManifestDigest ||
		currentBundle.Prepared.LKG.ConfigSHA != lkgBundle.Prepared.Forward.ConfigSHA ||
		currentBundle.Prepared.LKG.OCIRevision != lkgBundle.Prepared.Forward.OCIRevision ||
		currentBundle.Prepared.LKG.ImageRef != lkgBundle.Prepared.Forward.ImageRef {
		return "", monitorBundle{}, errors.New("LKG monitor record is not the exact predecessor of the current release")
	}
	return lkgName, lkgBundle, nil
}

func readMonitorDirectory(directory string) (monitorBundle, error) {
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return monitorBundle{}, errors.New("monitor directory type or mode is invalid")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return monitorBundle{}, err
	}
	want := []string{"artifact-receipt.json", "execution-plan.json", "forward.json", "lkg.json", "record.json", "release-plan.json", "terminal-result.json"}
	got := make([]string, 0, len(entries))
	raw := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
		fileInfo, err := entry.Info()
		if err != nil || !fileInfo.Mode().IsRegular() || fileInfo.Mode().Perm() != 0o600 || fileInfo.Size() < 1 || fileInfo.Size() > 4<<20 {
			return monitorBundle{}, fmt.Errorf("monitor file %q is invalid", entry.Name())
		}
		value, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return monitorBundle{}, err
		}
		raw[entry.Name()] = bytes.TrimSpace(value)
	}
	sort.Strings(got)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		return monitorBundle{}, errors.New("monitor directory file set is invalid")
	}
	files := make(map[string][]byte, 5)
	for _, name := range []string{"artifact-receipt.json", "execution-plan.json", "forward.json", "lkg.json", "release-plan.json"} {
		files[name] = raw[name]
	}
	bundle, err := decodeMonitorBundle(files, raw["terminal-result.json"])
	if err != nil {
		return monitorBundle{}, err
	}
	var stored declarativerelease.MonitorRecord
	if err := decodeStrictJSON(raw["record.json"], &stored); err != nil || stored != bundle.Record {
		return monitorBundle{}, errors.New("monitor directory record envelope is invalid")
	}
	return bundle, nil
}
