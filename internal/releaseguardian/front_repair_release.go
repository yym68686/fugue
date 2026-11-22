package releaseguardian

import (
	"context"
	"errors"
	"fmt"

	"fugue/internal/declarativerelease"
)

// FrontRepairWorkerRelease links the authority's concrete serving Worker to
// the checked immutable monitor record. It contains no cluster write authority.
type FrontRepairWorkerRelease struct {
	Release           declarativerelease.PlanRelease
	PlanDigest        string
	MonitorDigest     string
	ArtifactDigest    string
	WorkerSourceSHA   string
	WorkerImageDigest string
}

// LoadFrontRepairWorkerRelease reads the verified current production release,
// not DesiredRelease or an artifact nominated by the repair declaration.
// CurrentAuthority.CurrentRecordDigest identifies the ROUTE record, not this
// Worker's release; bind the two through the exact committed Worker source
// and image and the release's group and delivery metadata.
func (store *KubeStore) LoadFrontRepairWorkerRelease(ctx context.Context, key Key, before CurrentAuthority) (FrontRepairWorkerRelease, error) {
	target, ok := store.targets[key]
	if !ok || key.Validate() != nil || target.MonitorComponent != key.Component || before.Validate() != nil ||
		before.CurrentFrontGeneration == 0 || before.PreviousFrontGeneration == 0 {
		return FrontRepairWorkerRelease{}, errors.New("Front repair Worker release lookup lacks exact group or target authority")
	}
	record, err := store.loadStableMonitorMap(ctx, target)
	if err != nil {
		return FrontRepairWorkerRelease{}, err
	}
	if record.UID == "" || record.ResourceVersion == "" || record.DeletionTimestamp != nil {
		return FrontRepairWorkerRelease{}, errors.New("immutable stable Worker release record identity is incomplete")
	}
	plan, artifact, prepared, monitor, _, _, err := decodeStableRecord(record.Data)
	if err != nil {
		return FrontRepairWorkerRelease{}, err
	}
	proof, err := bindFrontRepairWorkerRelease(target, before, plan, artifact, prepared, monitor)
	if err != nil {
		return FrontRepairWorkerRelease{}, err
	}
	// Re-read the authoritative pointer after decoding. An advanced monitor
	// is not a valid release selection for this attempt, even if its image
	// happens to match. The launcher repeats this read under both Leases.
	fresh, err := store.loadStableMonitorMap(ctx, target)
	if err != nil || fresh.Name != record.Name || fresh.UID != record.UID || fresh.ResourceVersion != record.ResourceVersion || fresh.DeletionTimestamp != nil {
		return FrontRepairWorkerRelease{}, errors.Join(errors.New("stable Worker monitor changed while deriving repair authority"), err)
	}
	return proof, nil
}

// Inputs to this binding are already validated by decodeStableRecord.
func bindFrontRepairWorkerRelease(target TargetConfig, before CurrentAuthority, plan declarativerelease.Plan,
	artifact declarativerelease.ArtifactReceipt, prepared declarativerelease.ExecutionPlan,
	monitor declarativerelease.MonitorRecord) (FrontRepairWorkerRelease, error) {
	component := target.Key.Component
	release, found := releaseForComponent(plan, component)
	transition, delivery := release.Transition, release.Delivery
	if !found || monitor.Component != component || prepared.Component != component || artifact.Component != component ||
		release.Workload.Namespace != target.Namespace || delivery == nil || delivery.Writer != "guardian" ||
		delivery.Group != target.Key.Group || delivery.DependencyService != target.DependencyService ||
		release.Concurrency != "fugue-production-"+component || transition == nil || transition.Type != "edge-group-ab" ||
		transition.EdgeGroupAB == nil || transition.EdgeGroupAB.GroupID != before.GroupID ||
		transition.EdgeGroupAB.FrontName != release.Workload.Name ||
		artifact.OCIRevision != before.CurrentWorkerSourceSHA || artifact.TopDigest != before.CurrentWorkerImageDigest ||
		prepared.Forward.OCIRevision != artifact.OCIRevision || prepared.Forward.ImageRef != artifact.ImmutableRef ||
		artifact.ImmutableRef != release.Artifact.Repository+"@"+before.CurrentWorkerImageDigest {
		return FrontRepairWorkerRelease{}, fmt.Errorf("verified stable component %s does not identify the currently committed Front Worker", component)
	}
	return FrontRepairWorkerRelease{Release: release, PlanDigest: plan.PlanDigest, MonitorDigest: monitor.RecordDigest,
		ArtifactDigest: artifact.ReceiptDigest, WorkerSourceSHA: artifact.OCIRevision, WorkerImageDigest: artifact.TopDigest}, nil
}
