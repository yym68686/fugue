package releaseguardian

import (
	"bytes"
	"context"
	"errors"
	"math"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
)

type NativeRolloutGate interface {
	ReadReleaseState(context.Context, Snapshot) (productioncontrol.ResourceState, error)
	ReserveRollout(context.Context, Snapshot) (bool, error)
}

type nativeLaunchLedger interface {
	ReadIntent(context.Context, string) (productioncontrol.Intent, []byte, error)
	ReadResource(context.Context, productioncontrol.Scope) (productioncontrol.ResourceState, error)
	ReserveReleaseLaunch(context.Context, string, productioncontrol.Intent, []byte, int64) (bool, error)
}

func (gate *nativeLaunchGate) ReadReleaseState(ctx context.Context, s Snapshot) (productioncontrol.ResourceState, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	target, ok := gate.store.targets[s.Key]
	if !ok || target.MonitorComponent != s.Key.Component || !s.Managed || !s.NativePublication || s.DesiredRecordMissing {
		return productioncontrol.ResourceState{}, errors.New("native release status has no enrolled snapshot")
	}
	intent, payload, err := readNativeReleaseIntent(ctx, gate.ledger.ReadIntent, gate.cluster, s.NativeIntent)
	if err != nil {
		return productioncontrol.ResourceState{}, err
	}
	workload := s.Bundle.Release.Workload
	identity := declarativerelease.ResourceIdentity{APIVersion: workload.APIVersion, Kind: workload.Kind, Namespace: workload.Namespace, Name: workload.Name}
	if payload.ProposedRecord.Key() != s.Key || payload.Namespace != target.Namespace || workload.Namespace != target.Namespace ||
		workload.Kind != "Deployment" || workload.APIVersion != "apps/v1" || nativeKubernetesResourceID(identity) != intent.ResourceID ||
		intent.Generation > math.MaxInt64-2 || !((s.Desired.Generation == intent.Generation+1 && s.Desired.RecordDigest == intent.TargetDigest) ||
		(s.Desired.Generation == intent.Generation+2 && s.Desired.RecordDigest == intent.RollbackDigest)) {
		return productioncontrol.ResourceState{}, errors.New("native release status does not match its production intent")
	}
	state, err := gate.ledger.ReadResource(ctx, productioncontrol.Scope{ClusterID: intent.ClusterID, ResourceID: intent.ResourceID})
	if err != nil {
		return productioncontrol.ResourceState{}, err
	}
	if state.LastFence != s.NativeFence || state.LastIntent != s.NativeIntent {
		return productioncontrol.ResourceState{}, errors.New("native release pointer differs from its durable production fence")
	}
	binding := NativeRuntimeBinding{Key: s.Key, DesiredResourceVersion: s.DesiredResourceVersion, DesiredRecordDigest: s.Desired.RecordDigest,
		LKGMonitorRecordDigest: payload.RollbackMonitor, IntentDigest: s.NativeIntent, Fence: s.NativeFence}
	pointer, err := binding.readPointer(ctx, gate.store.client.CoreV1(), target.Namespace)
	if err != nil || pointer != s.Desired {
		return productioncontrol.ResourceState{}, errors.New("native release status pointer changed while reading its durable fence")
	}
	return state, nil
}

type nativeLaunchGate struct {
	store   *KubeStore
	ledger  nativeLaunchLedger
	cluster string
}

func NewNativeLaunchGate(store *KubeStore, journal *productioncontrol.Journal, cluster string) (NativeRolloutGate, error) {
	if store == nil || store.client == nil || journal == nil || !componentPattern.MatchString(cluster) {
		return nil, errors.New("native rollout gate requires its configured cluster, provider and journal")
	}
	return &nativeLaunchGate{store, journal, cluster}, nil
}

func (gate *nativeLaunchGate) ReserveRollout(ctx context.Context, s Snapshot) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	target, ok := gate.store.targets[s.Key]
	if !ok || target.MonitorComponent != s.Key.Component || !s.Managed || !s.NativePublication || s.DesiredRecordMissing ||
		s.NativeFence < 1 || !digestPattern.MatchString(s.NativeIntent) || s.CurrentRecordDigest != s.LastSuccessfulLKG {
		return false, errors.New("native rollout has no exact enrolled snapshot")
	}
	intent, payload, err := readNativeReleaseIntent(ctx, gate.ledger.ReadIntent, gate.cluster, s.NativeIntent)
	if err != nil {
		return false, err
	}
	bundle, err := DecodeExecutionBundle(payload.Files, s.Key)
	if err != nil {
		return false, err
	}
	workload := bundle.Release.Workload
	identity := bundle.Prepared.Prewrite.Primary
	if payload.Namespace != target.Namespace || workload.Namespace != target.Namespace || workload.Name != identity.Name ||
		workload.Kind != "Deployment" || workload.APIVersion != "apps/v1" || identity.Namespace != target.Namespace ||
		identity.Kind != workload.Kind || identity.APIVersion != workload.APIVersion || nativeKubernetesResourceID(identity) != intent.ResourceID ||
		intent.Generation > math.MaxInt64-2 || s.Desired.Generation != intent.Generation+1 || s.Desired.RecordDigest != intent.TargetDigest ||
		s.Record != payload.ProposedRecord || s.CurrentRecordDigest != intent.CurrentDigest || s.LKGMonitorRecordDigest != payload.RollbackMonitor ||
		len(s.Bundle.Files) != len(payload.Files) {
		return false, errors.New("native rollout differs from the approved production intent")
	}
	for name, approved := range payload.Files {
		if !bytes.Equal(bytes.TrimSpace(approved), bytes.TrimSpace(s.Bundle.Files[name])) {
			return false, errors.New("native rollout files differ from the approved production intent")
		}
	}
	binding := NativeRuntimeBinding{Key: s.Key, DesiredResourceVersion: s.DesiredResourceVersion,
		DesiredRecordDigest: intent.TargetDigest, LKGMonitorRecordDigest: payload.RollbackMonitor, IntentDigest: s.NativeIntent, Fence: s.NativeFence}
	if err := binding.VerifyCandidate(ctx, gate.store.client.CoreV1(), target.Namespace, payload.Files); err != nil {
		return false, err
	}
	_, _, _, monitor, forward, _, err := decodeStableRecord(s.CurrentMonitorData)
	if err != nil || monitor.RecordDigest != payload.RollbackMonitor || digest(forward) != payload.RollbackManifest || !bytes.Equal(bundle.LKG, forward) {
		return false, errors.New("native rollout predecessor is not the approved rollback monitor")
	}
	raw, err := declarativerelease.CanonicalJSON(payload)
	if err != nil {
		return false, err
	}
	return gate.ledger.ReserveReleaseLaunch(ctx, "guardian", intent, raw, s.NativeFence)
}

func (controller *Controller) reconcileNative(ctx context.Context, snapshot Snapshot, now time.Time) error {
	if !snapshot.Managed || snapshot.DesiredRecordMissing {
		return nil
	}
	if controller.nativeGate == nil {
		return errors.New("native Guardian rollout gate is unavailable")
	}
	journalState, err := controller.nativeGate.ReadReleaseState(ctx, snapshot)
	if err != nil || !digestPattern.MatchString(journalState.LastVerified.Digest) {
		return errors.New("native release journal head is unavailable or disagrees with the pointer; prior status retained")
	}
	decision := Classify(snapshot.CurrentRecordDigest, snapshot.Desired.RecordDigest, snapshot.Health)
	status := ReleaseStatus{
		Component: snapshot.Key.Component, Group: snapshot.Key.Group, State: decision.State,
		CurrentRecordDigest: snapshot.CurrentRecordDigest, TargetRecordDigest: snapshot.Desired.RecordDigest,
		LastSuccessfulLKG: journalState.LastVerified.Digest, Health: snapshot.Health, Reason: decision.Reason,
		ObservedAt: now.Format(time.RFC3339Nano),
	}
	switch {
	case journalState.Pending == "":
		if journalState.LastVerified != (productioncontrol.Version{Generation: snapshot.Desired.Generation, Digest: snapshot.Desired.RecordDigest}) ||
			snapshot.CurrentRecordDigest != journalState.LastVerified.Digest {
			status.State, status.Reason = StateRecoveryRequired, "native serving pointer differs from the last independently verified version"
		} else if decision.RollbackEligible {
			status.State, status.Reason = StateRecoveryRequired, "native recovery requires separate fenced recovery authority"
		}
	case decision.State == StateStable:
		status.State, status.Reason = StateVerifying, "native serving observation is healthy; durable execution is still open"
	case journalState.LastVerified.Digest != snapshot.CurrentRecordDigest:
		status.State, status.Reason = StateRecoveryRequired, "native serving version is not yet durably verified"
	case decision.RolloutEligible:
		permitted, err := controller.nativeGate.ReserveRollout(ctx, snapshot)
		switch {
		case err != nil:
			status.State, status.Reason = StateRecoveryRequired, "native launch authorization is unavailable or denied; workload unchanged by this reconciliation"
		case !permitted:
			status.State, status.Reason = StateRecoveryRequired, "native launch permit was already consumed; awaiting independent observation"
		default:
			receipt, runErr := controller.executor.Rollout(ctx, snapshot)
			switch {
			case runErr != nil || !digestPattern.MatchString(receipt.ReceiptDigest):
				status.State, status.Reason = StateRecoveryRequired, "native rollout outcome is unknown; no second launch is authorized"
			case receipt.Status == "verified" && receipt.RecordDigest == snapshot.Record.RecordDigest:
				status.State, status.Reason = StateVerifying, "native rollout verified locally; awaiting independent serving and writer fencing evidence"
				status.RolloutReceiptDigest = receipt.ReceiptDigest
			case receipt.RecordDigest == snapshot.Record.RecordDigest && (receipt.Status == "failed-no-write" || receipt.Status == "compensated"):
				status.RolloutReceiptDigest = receipt.ReceiptDigest
				// The child has exited. Move only the DesiredRelease
				// pointer back to its bound predecessor, not the SQL head.
				if err := controller.store.SetDesiredToLKG(ctx, snapshot); err != nil {
					status.State, status.Reason = StateRecoveryRequired, "native rollback pointer CAS is unproven; awaiting independent observation"
				} else {
					status.State, status.Reason = StateVerifying, "native predecessor restored or untouched; awaiting independent serving and writer fencing evidence"
					status.TargetRecordDigest = snapshot.Record.LKGRecordDigest
				}
			default:
				status.State, status.Reason = StateRecoveryRequired, "native rollout has no confirmed terminal observation"
				status.RolloutReceiptDigest = receipt.ReceiptDigest
			}
		}
	case decision.RollbackEligible:
		// The initial launch includes the approved, scoped compensation.
		// A subsequent repair is another mutation and needs a separately
		// fenced recovery path. Never route this through the legacy retry.
		status.State, status.Reason = StateRecoveryRequired, "native recovery requires separate fenced recovery authority"
	}
	sealed, err := status.Seal()
	if err != nil {
		return err
	}
	return controller.store.UpdateStatus(ctx, snapshot, sealed)
}
