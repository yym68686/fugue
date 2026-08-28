package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func runInstallMonitorRecord(args []string, output io.Writer) error {
	if len(args) != 3 {
		return errors.New("usage: fugue-declarative-release install-monitor-record PLAN_DIR TERMINAL_RESULT")
	}
	files, err := readPlanDirectory(args[1])
	if err != nil {
		return err
	}
	terminalRaw, err := os.ReadFile(args[2])
	if err != nil {
		return err
	}
	bundle, err := decodeMonitorBundle(files, terminalRaw)
	if err != nil {
		return err
	}
	release, err := selectedRelease(bundle.Plan, bundle.Record.Component)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lease, err := newComponentLeaseCoordinator()
	if err != nil {
		return err
	}
	held, err := lease.acquire(ctx, release, bundle.Record.ConfigSHA)
	if err != nil {
		return err
	}
	store, err := newMonitorStore()
	if err == nil {
		_, err = store.persistVerified(ctx, release, files, bundle.Terminal)
	}
	releaseErr := lease.release(ctx, held)
	if err != nil {
		return err
	}
	if releaseErr != nil {
		return releaseErr
	}
	raw, err := declarativerelease.CanonicalJSON(bundle.Record)
	if err != nil {
		return err
	}
	_, err = output.Write(append(raw, '\n'))
	return err
}

func runAdoptCommittedMonitorContext(parent context.Context, args []string, output io.Writer) error {
	if len(args) != 2 {
		return errors.New("usage: fugue-declarative-release adopt-committed-monitor PLAN_DIR")
	}
	files, err := readPlanDirectory(args[1])
	if err != nil {
		return err
	}
	plan, err := declarativerelease.DecodePlan(bytes.NewReader(files["release-plan.json"]))
	if err != nil {
		return err
	}
	artifact, err := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(files["artifact-receipt.json"]))
	if err != nil {
		return err
	}
	prepared, err := declarativerelease.DecodeRecordedExecutionPlan(bytes.NewReader(files["execution-plan.json"]), plan, files["forward.json"], files["lkg.json"])
	if err != nil || prepared.ArtifactDigest != artifact.ReceiptDigest || prepared.Component != artifact.Component ||
		prepared.ConfigSHA != artifact.ConfigSHA || prepared.Forward.ImageRef != artifact.ImmutableRef {
		return errors.New("committed monitor adoption is not bound to its immutable execution bundle")
	}
	release, err := selectedRelease(plan, prepared.Component)
	if err != nil {
		return err
	}
	if release.Delivery == nil || release.Delivery.Writer != "guardian" || release.Transition == nil ||
		release.Transition.Type != "edge-group-ab" || release.Transition.EdgeGroupAB == nil || release.SupersedesFailedConfigSHA == "" {
		return errors.New("committed monitor adoption is allowed only for an explicit Guardian Edge A/B supersession")
	}
	key := releaseguardian.Key{Component: release.ComponentID, Group: release.Delivery.Group}
	candidate, err := releaseguardian.DecodeExecutionBundle(files, key)
	if err != nil {
		return err
	}
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	guardian, err := releaseguardian.NewKubeStore(client, []releaseguardian.TargetConfig{{Key: key, Namespace: release.Workload.Namespace,
		MonitorComponent: release.ComponentID, DependencyService: release.Delivery.DependencyService}})
	if err != nil {
		return err
	}
	monitor, err := newMonitorStore()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 8*time.Minute)
	defer cancel()
	before, err := guardian.Load(ctx, key)
	if err != nil {
		return err
	}
	stableMonitor, err := monitor.load(ctx, release.Workload.Namespace, release.ComponentID)
	if err != nil {
		return err
	}
	monitorAdopted := monitorBundleMatchesExecution(stableMonitor.Bundle, files, prepared)
	predecessorRecordDigest := before.CurrentRecordDigest
	if monitorAdopted {
		stable, stableErr := committedStableRecord(key, release, artifact, prepared, stableMonitor.Bundle.Record)
		if stableErr != nil || before.CurrentRecordDigest != stable.RecordDigest || before.LastSuccessfulLKG != stable.RecordDigest {
			return errors.New("adopted monitor does not derive the current Guardian record")
		}
		if before.Desired.RecordDigest == stable.RecordDigest {
			raw, encodeErr := declarativerelease.CanonicalJSON(stableMonitor.Bundle.Terminal)
			if encodeErr != nil {
				return encodeErr
			}
			_, encodeErr = output.Write(append(raw, '\n'))
			return encodeErr
		}
		predecessorRecordDigest = before.Desired.RecordDigest
	}
	candidateRecord, err := candidate.ReleaseRecord(key, predecessorRecordDigest)
	if err != nil {
		return err
	}
	if !monitorAdopted && !committedGuardianCandidateFenced(before, key, candidateRecord) {
		return errors.New("Guardian is not fenced on the exact failed committed transition")
	}
	lkgMonitorDigest, err := verifyPublishedGuardianCandidate(ctx, client, release.Workload.Namespace, key, candidateRecord, files)
	if err != nil {
		return err
	}
	if _, _, err := loadBoundExecutionLKG(ctx, monitor, candidate, release, lkgMonitorDigest); err != nil {
		return err
	}
	if !monitorAdopted && stableMonitor.Bundle.Record.RecordDigest != lkgMonitorDigest {
		return errors.New("active monitor is not the exact candidate predecessor")
	}
	lease, err := newComponentLeaseCoordinator()
	if err != nil {
		return err
	}
	held, err := lease.acquire(ctx, release, prepared.ConfigSHA)
	if err != nil {
		return fmt.Errorf("acquire committed adoption Lease: %w", err)
	}
	releaseHeld := func(operationErr error) error {
		finalizeCtx, finalizeCancel := componentLeaseFinalizationContext(ctx)
		defer finalizeCancel()
		if releaseErr := lease.release(finalizeCtx, held); releaseErr != nil {
			return errors.Join(operationErr, fmt.Errorf("release committed adoption Lease: %w", releaseErr))
		}
		return operationErr
	}
	fresh, err := guardian.Load(ctx, key)
	if err != nil || fresh.Desired != before.Desired || fresh.DesiredResourceVersion != before.DesiredResourceVersion ||
		fresh.CurrentRecordDigest != before.CurrentRecordDigest || fresh.LastSuccessfulLKG != before.LastSuccessfulLKG {
		return releaseHeld(errors.New("Guardian state changed after committed adoption Lease acquisition"))
	}
	freshMonitor, err := monitor.load(ctx, release.Workload.Namespace, release.ComponentID)
	if err != nil || freshMonitor.StateUID != stableMonitor.StateUID || freshMonitor.StateRV != stableMonitor.StateRV || freshMonitor.Bundle.Record != stableMonitor.Bundle.Record {
		return releaseHeld(errors.New("stable monitor changed after committed adoption Lease acquisition"))
	}
	cluster, err := newKubectlCluster()
	if err != nil {
		return releaseHeld(err)
	}
	result := declarativerelease.ReconcileExecution(ctx, cluster, plan, prepared, files["forward.json"], files["lkg.json"])
	if result.Status != "verified" {
		return releaseHeld(fmt.Errorf("committed authority reconciliation ended with status=%s reason=%s detail=%s", result.Status, result.Reason, result.FailureDetail))
	}
	adopted := freshMonitor
	if !monitorAdopted {
		adopted, err = monitor.persistVerified(ctx, release, files, result)
		if err != nil {
			return releaseHeld(err)
		}
	}
	stable, err := committedStableRecord(key, release, artifact, prepared, adopted.Bundle.Record)
	if err != nil {
		return releaseHeld(err)
	}
	if _, err := guardian.AdoptCurrentStable(ctx, key, fresh.Desired, fresh.DesiredResourceVersion, stable, adopted.Bundle.Record.RecordDigest); err != nil {
		return releaseHeld(err)
	}
	if err := releaseHeld(nil); err != nil {
		return err
	}
	raw, err := declarativerelease.CanonicalJSON(result)
	if err != nil {
		return err
	}
	_, err = output.Write(append(raw, '\n'))
	return err
}

func committedGuardianCandidateFenced(snapshot releaseguardian.Snapshot, key releaseguardian.Key, candidate releaseguardian.ReleaseRecord) bool {
	status := snapshot.PreviousStatus
	unproven := status != nil && (status.Reason == "lkg-unproven" || strings.HasPrefix(status.Reason, "lkg-unproven: ") ||
		strings.HasPrefix(status.Reason, "failed candidate is fenced while LKG health awaits complete evidence"))
	return snapshot.Managed && candidate.Validate() == nil && candidate.Key() == key && snapshot.Record == candidate &&
		snapshot.Desired.Key() == key && snapshot.Desired.RecordDigest == candidate.RecordDigest &&
		candidate.LKGRecordDigest == snapshot.CurrentRecordDigest && snapshot.LastSuccessfulLKG == snapshot.CurrentRecordDigest &&
		status != nil && status.Key() == key && status.State == releaseguardian.StateRecoveryRequired && unproven &&
		status.CurrentRecordDigest == snapshot.CurrentRecordDigest && status.LastSuccessfulLKG == snapshot.CurrentRecordDigest &&
		status.TargetRecordDigest == candidate.RecordDigest && status.RolloutReceiptDigest != "" && status.RollbackReceiptDigest == ""
}

func monitorBundleMatchesExecution(bundle monitorBundle, files map[string][]byte, prepared declarativerelease.ExecutionPlan) bool {
	release, err := selectedRelease(bundle.Plan, prepared.Component)
	if err != nil {
		return false
	}
	if bundle.Prepared.PlanDigest != prepared.PlanDigest || bundle.Terminal.Status != "verified" ||
		bundle.Terminal.Component != prepared.Component || bundle.Terminal.ConfigSHA != prepared.ConfigSHA ||
		bundle.Terminal.ExecutionPlanDigest != prepared.PlanDigest || !bundle.Terminal.Final.Matches(prepared.Forward, release, false) {
		return false
	}
	for name, value := range files {
		if !bytes.Equal(bytes.TrimSpace(bundle.Raw[name]), bytes.TrimSpace(value)) {
			return false
		}
	}
	return true
}

func committedStableRecord(key releaseguardian.Key, release declarativerelease.PlanRelease, artifact declarativerelease.ArtifactReceipt, prepared declarativerelease.ExecutionPlan, monitor declarativerelease.MonitorRecord) (releaseguardian.ReleaseRecord, error) {
	if monitor.Component != prepared.Component || monitor.ConfigSHA != prepared.ConfigSHA ||
		monitor.ArtifactDigest != artifact.ReceiptDigest || monitor.ExecutionPlanDigest != prepared.PlanDigest {
		return releaseguardian.ReleaseRecord{}, errors.New("adopted monitor is not bound to the committed execution")
	}
	healthRaw, err := declarativerelease.CanonicalJSON(release.Health)
	if err != nil {
		return releaseguardian.ReleaseRecord{}, err
	}
	return releaseguardian.NewReleaseRecord(key, prepared.ConfigSHA, artifact.TopDigest, monitor.ForwardManifestDigest,
		monitor.RecordDigest, digestBytesLocal(healthRaw))
}

func verifyPublishedGuardianCandidate(ctx context.Context, client kubernetes.Interface, namespace string, key releaseguardian.Key, record releaseguardian.ReleaseRecord, files map[string][]byte) (string, error) {
	suffix := strings.TrimPrefix(record.RecordDigest, "sha256:")
	if len(suffix) > 16 {
		suffix = suffix[:16]
	}
	name := "fugue-guardian-record-" + key.Component
	if key.Group != "" {
		name += "-" + key.Group
	}
	name += "-" + suffix
	object, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil || object.Immutable == nil || !*object.Immutable {
		return "", errors.New("immutable published Guardian candidate is unavailable")
	}
	var stored releaseguardian.ReleaseRecord
	if decodeStrictJSON([]byte(object.Data["guardian-record.json"]), &stored) != nil || stored != record {
		return "", errors.New("immutable published Guardian candidate envelope differs from the recovery bundle")
	}
	for name, value := range files {
		if strings.TrimSpace(object.Data[name]) != string(bytes.TrimSpace(value)) {
			return "", fmt.Errorf("immutable published Guardian candidate file %q differs from the recovery bundle", name)
		}
	}
	lkgMonitorDigest := strings.TrimSpace(object.Data["lkg-monitor-record-digest"])
	if !strings.HasPrefix(lkgMonitorDigest, "sha256:") || len(lkgMonitorDigest) != 71 || strings.Trim(lkgMonitorDigest[7:], "0123456789abcdef") != "" {
		return "", errors.New("immutable published Guardian candidate LKG binding is invalid")
	}
	return lkgMonitorDigest, nil
}
