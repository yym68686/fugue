package releaseguardian

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/client-go/util/workqueue"
)

type Mode string

const (
	ModeShadow Mode = "shadow"
	ModeWrite  Mode = "write"
)

type Snapshot struct {
	Key                    Key
	Record                 ReleaseRecord
	Desired                DesiredRelease
	CurrentRecordDigest    string
	LastSuccessfulLKG      string
	Health                 HealthSnapshot
	StatusResourceVersion  string
	DesiredResourceVersion string
	PreviousStatus         *ReleaseStatus
	Bundle                 ExecutionBundle
	CurrentMonitorData     map[string]string
	LKGMonitorRecordDigest string
	// DesiredRecordMissing means the mutable DesiredRelease points at an
	// immutable candidate that was pruned. The stable monitor remains the
	// runtime authority until an explicit predecessor-bound successor replaces
	// that orphan pointer.
	DesiredRecordMissing bool
	Managed              bool
}

func (snapshot Snapshot) Validate(now time.Time) error {
	if snapshot.Key.Validate() != nil || snapshot.Record.Validate() != nil || snapshot.Desired.Validate() != nil ||
		snapshot.Record.Key() != snapshot.Key || snapshot.Desired.Key() != snapshot.Key ||
		(snapshot.Desired.RecordDigest != snapshot.Record.RecordDigest && !snapshot.DesiredRecordMissing) || snapshot.Health.Validate(now) != nil ||
		(snapshot.CurrentRecordDigest != "" && !digestPattern.MatchString(snapshot.CurrentRecordDigest)) ||
		!digestPattern.MatchString(snapshot.LastSuccessfulLKG) || snapshot.Bundle.Prepared.Component != snapshot.Key.Component ||
		(snapshot.Managed && !digestPattern.MatchString(snapshot.LKGMonitorRecordDigest)) {
		return errors.New("release guardian snapshot is invalid")
	}
	return nil
}

type Store interface {
	Load(context.Context, Key) (Snapshot, error)
	UpdateStatus(context.Context, Snapshot, ReleaseStatus) error
	SetDesiredToLKG(context.Context, Snapshot) error
}

type ExecutionReceipt struct {
	Status        string
	Reason        string
	RecordDigest  string
	ReceiptDigest string
}

type Executor interface {
	Rollout(context.Context, Snapshot) (ExecutionReceipt, error)
	Repair(context.Context, Snapshot) (ExecutionReceipt, error)
	Rollback(context.Context, Snapshot) (ExecutionReceipt, error)
}

type Controller struct {
	mode     Mode
	store    Store
	executor Executor
	queue    workqueue.TypedRateLimitingInterface[Key]
	now      func() time.Time
}

func NewController(mode Mode, store Store, executor Executor) (*Controller, error) {
	if mode != ModeShadow && mode != ModeWrite {
		return nil, errors.New("release guardian mode is invalid")
	}
	if store == nil || (mode == ModeWrite && executor == nil) {
		return nil, errors.New("release guardian dependencies are invalid")
	}
	return &Controller{
		mode: mode, store: store, executor: executor, now: time.Now,
		queue: workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[Key]()),
	}, nil
}

func (controller *Controller) Enqueue(key Key) error {
	if controller == nil || controller.queue == nil || key.Validate() != nil {
		return errors.New("release guardian queue key is invalid")
	}
	controller.queue.Add(key)
	return nil
}

func (controller *Controller) Run(ctx context.Context, workers int) error {
	if controller == nil || controller.queue == nil || workers < 1 || workers > 32 {
		return errors.New("release guardian worker configuration is invalid")
	}
	for index := 0; index < workers; index++ {
		go controller.worker(ctx)
	}
	<-ctx.Done()
	controller.queue.ShutDown()
	return nil
}

func (controller *Controller) worker(ctx context.Context) {
	for controller.processNext(ctx) {
	}
}

func (controller *Controller) processNext(ctx context.Context) bool {
	key, shutdown := controller.queue.Get()
	if shutdown {
		return false
	}
	defer controller.queue.Done(key)
	if err := controller.Reconcile(ctx, key); err != nil {
		controller.queue.AddRateLimited(key)
		return true
	}
	controller.queue.Forget(key)
	return true
}

func (controller *Controller) Reconcile(ctx context.Context, key Key) error {
	snapshot, err := controller.store.Load(ctx, key)
	if err != nil {
		return fmt.Errorf("load %s: %w", key.String(), err)
	}
	now := controller.now().UTC()
	if err := snapshot.Validate(now); err != nil {
		return err
	}
	decision := Classify(snapshot.CurrentRecordDigest, snapshot.Desired.RecordDigest, snapshot.Health)
	degradedPredecessorRollout := degradedPredecessorRolloutEligible(snapshot)
	degradedEdgeRouteRecovery := degradedEdgeRouteRecoveryEligible(snapshot, snapshot.Bundle)
	pendingUnprovenLKG := pendingUnprovenLKGRecovery(snapshot)
	recoveredPredecessorRetry := pendingUnprovenLKG && recoveredPredecessorRetryEligible(snapshot)
	status := ReleaseStatus{
		Component: key.Component, Group: key.Group, State: decision.State,
		CurrentRecordDigest: snapshot.CurrentRecordDigest, TargetRecordDigest: snapshot.Desired.RecordDigest,
		LastSuccessfulLKG: snapshot.LastSuccessfulLKG, Health: snapshot.Health,
		Reason: decision.Reason, ObservedAt: now.Format(time.RFC3339Nano),
	}
	if pendingUnprovenLKG {
		status.RecoveryRetryCount = snapshot.PreviousStatus.RecoveryRetryCount
	}
	if recoveredPredecessorRetry {
		status.RecoveryRetryCount++
	}
	if pendingTargetCanaryVerification(snapshot) {
		status.State = StateVerifying
		status.RolloutReceiptDigest = snapshot.PreviousStatus.RolloutReceiptDigest
		status.Reason = joinedReason("rollout is verified locally and is waiting for target-bound route evidence", snapshot.Health)
	}
	if degradedPredecessorRollout || degradedEdgeRouteRecovery {
		status.State = StateRolloutPending
		if degradedEdgeRouteRecovery {
			status.Reason = joinedReason("controlled edge route recovery is authorized by immutable predecessor and degraded-route evidence", snapshot.Health)
		} else {
			status.Reason = joinedReason("exact degraded predecessor repair is authorized by immutable prewrite evidence", snapshot.Health)
		}
	}
	if controller.mode == ModeWrite && snapshot.Managed && pendingUnprovenLKG && !recoveredPredecessorRetry {
		status.State = StateRecoveryRequired
		status.RolloutReceiptDigest = snapshot.PreviousStatus.RolloutReceiptDigest
		status.Reason = joinedReason("lkg-unproven: failed candidate is fenced while LKG health awaits complete evidence", snapshot.Health)
		if lkgRecoveryExecutionEligible(snapshot) {
			receipt, executeErr := controller.executor.Rollback(ctx, snapshot)
			if executeErr != nil {
				status.Reason = "LKG restore result is unknown: " + executeErr.Error()
				if strings.Contains(executeErr.Error(), "LKG monitor record is not the exact predecessor") {
					if err := controller.store.SetDesiredToLKG(ctx, snapshot); err != nil {
						status.Reason = "LKG monitor ledger is inconsistent and DesiredRelease rollback CAS failed: " + err.Error()
					} else {
						status.TargetRecordDigest = snapshot.CurrentRecordDigest
						status.Reason = "LKG monitor ledger is inconsistent; failed-candidate DesiredRelease was fenced back to the recorded LKG"
					}
				}
			} else {
				status.RollbackReceiptDigest = receipt.ReceiptDigest
				if receipt.Status == "compensated" && receipt.RecordDigest == snapshot.Record.LKGRecordDigest {
					if err := controller.store.SetDesiredToLKG(ctx, snapshot); err != nil {
						status.Reason = "LKG is restored but failed-candidate DesiredRelease rollback CAS failed: " + err.Error()
					} else {
						status.State = StateLKGStable
						status.CurrentRecordDigest = snapshot.CurrentRecordDigest
						status.TargetRecordDigest = snapshot.CurrentRecordDigest
						status.Reason = receipt.Reason
					}
				} else {
					status.Reason = receipt.Reason
				}
			}
		} else if allLayersHealthy(snapshot.Health) {
			if err := controller.store.SetDesiredToLKG(ctx, snapshot); err != nil {
				status.Reason = "LKG is healthy but failed-candidate DesiredRelease rollback CAS failed: " + err.Error()
			} else {
				status.State = StateLKGStable
				status.CurrentRecordDigest = snapshot.CurrentRecordDigest
				status.TargetRecordDigest = snapshot.CurrentRecordDigest
				status.Reason = "LKG health is verified and the failed candidate DesiredRelease was rolled back by CAS"
			}
		}
		sealed, err := status.Seal()
		if err != nil {
			return err
		}
		return controller.store.UpdateStatus(ctx, snapshot, sealed)
	}
	if controller.mode == ModeShadow || !snapshot.Managed {
		prefix := "shadow: "
		if controller.mode == ModeWrite && !snapshot.Managed {
			prefix = "unmanaged: "
		}
		status.Reason = prefix + status.Reason
		sealed, sealErr := status.Seal()
		if sealErr != nil {
			return sealErr
		}
		return controller.store.UpdateStatus(ctx, snapshot, sealed)
	}
	if decision.RolloutEligible || degradedPredecessorRollout || degradedEdgeRouteRecovery {
		status.State = StateRolling
		receipt, executeErr := controller.executor.Rollout(ctx, snapshot)
		if executeErr != nil {
			status.State = StateRecoveryRequired
			status.Reason = "rollout result is unknown: " + executeErr.Error()
		} else {
			status.RolloutReceiptDigest = receipt.ReceiptDigest
			status.Reason = receipt.Reason
			switch receipt.Status {
			case "verified":
				status.State = StateVerifying
				status.CurrentRecordDigest = snapshot.Record.RecordDigest
			case "compensated", "failed-no-write":
				if err := controller.store.SetDesiredToLKG(ctx, snapshot); err != nil {
					status.State = StateRecoveryRequired
					status.Reason = "rollout reached a known safe terminal state but DesiredRelease rollback CAS failed: " + err.Error()
				} else {
					status.State = StateLKGStable
					status.CurrentRecordDigest = snapshot.Record.LKGRecordDigest
					status.TargetRecordDigest = snapshot.Record.LKGRecordDigest
				}
			case "recovery-required":
				status.State = StateRecoveryRequired
				if recoveredPredecessorRetry {
					status.Reason = "lkg-unproven: bounded predecessor retry: " + receipt.Reason
				}
			default:
				status.State = StateRecoveryRequired
				status.Reason = "rollout returned an invalid terminal status"
			}
		}
		// Re-evaluate health and DesiredRelease after every rollout terminal
		// result. Continuing with the pre-rollout RollbackEligible decision can
		// incorrectly walk the stable monitor record's older LKG after a known
		// failed-no-write/compensated candidate has already been fenced back.
		sealed, err := status.Seal()
		if err != nil {
			return err
		}
		return controller.store.UpdateStatus(ctx, snapshot, sealed)
	}
	if decision.RollbackEligible {
		status.State = StateRollingBack
		stableDrift := decision.RepairEligible && snapshot.CurrentRecordDigest == snapshot.Desired.RecordDigest &&
			snapshot.LastSuccessfulLKG == snapshot.CurrentRecordDigest
		var receipt ExecutionReceipt
		var executeErr error
		if stableDrift {
			receipt, executeErr = controller.executor.Repair(ctx, snapshot)
		} else {
			receipt, executeErr = controller.executor.Rollback(ctx, snapshot)
		}
		if executeErr != nil {
			status.State = StateRecoveryRequired
			status.Reason = "component recovery result is unknown: " + executeErr.Error()
		} else {
			status.RollbackReceiptDigest = receipt.ReceiptDigest
			degradedLKGRestored := receipt.Status == "recovery-required" && receipt.RecordDigest == snapshot.Record.LKGRecordDigest
			if stableDrift && receipt.Status == "verified" {
				status.State = StateStable
				status.CurrentRecordDigest = receipt.RecordDigest
				status.TargetRecordDigest = receipt.RecordDigest
				status.Reason = receipt.Reason
			} else if receipt.Status != "compensated" && !degradedLKGRestored {
				status.State = StateRecoveryRequired
				status.Reason = receipt.Reason
			} else if err := controller.store.SetDesiredToLKG(ctx, snapshot); err != nil {
				status.State = StateRecoveryRequired
				status.Reason = "LKG is restored but DesiredRelease rollback CAS failed: " + err.Error()
			} else {
				status.CurrentRecordDigest = receipt.RecordDigest
				status.TargetRecordDigest = receipt.RecordDigest
				if degradedLKGRestored {
					status.State = StateRecoveryRequired
				} else {
					status.State = StateLKGStable
				}
				status.Reason = receipt.Reason
			}
		}
	}
	sealed, err := status.Seal()
	if err != nil {
		return err
	}
	return controller.store.UpdateStatus(ctx, snapshot, sealed)
}

// degradedEdgeRouteRecovery admits only the edge-group transition's explicit
// successor when the exact LKG resource identity is still present but its
// public route is degraded. The regular rollout gate remains closed for every
// other component and for unknown/dependency health; this is the controlled
// path that lets the worker transition repair its own route authority.
func degradedEdgeRouteRecoveryEligible(snapshot Snapshot, bundle ExecutionBundle) bool {
	prepared, release := bundle.Prepared, bundle.Release
	if !snapshot.Managed || !prepared.DegradedPredecessor || !prepared.DegradedRoute ||
		release.SupersedesFailedConfigSHA == "" || snapshot.CurrentRecordDigest != snapshot.Record.LKGRecordDigest ||
		snapshot.LastSuccessfulLKG != snapshot.CurrentRecordDigest || snapshot.CurrentRecordDigest == snapshot.Record.RecordDigest ||
		snapshot.Desired.RecordDigest != snapshot.Record.RecordDigest || snapshot.Health.Dependency.State != HealthHealthy ||
		snapshot.Health.Route.State == HealthHealthy {
		return false
	}
	edgeWorkerRecovery := snapshot.Health.Local.State == HealthDegraded && release.Transition != nil &&
		release.Transition.Type == "edge-group-ab" && release.Transition.EdgeGroupAB != nil
	edgeControlRecovery := snapshot.Health.Local.State != HealthUnknown && release.Transition == nil &&
		strings.HasPrefix(snapshot.Key.Component, "edge-control-")
	return (edgeWorkerRecovery || edgeControlRecovery) &&
		release.SupersedesFailedConfigSHA != "" && snapshot.CurrentRecordDigest == snapshot.Record.LKGRecordDigest &&
		snapshot.LastSuccessfulLKG == snapshot.CurrentRecordDigest
}

func degradedPredecessorRolloutEligible(snapshot Snapshot) bool {
	prepared := snapshot.Bundle.Prepared
	return prepared.DegradedPredecessor && prepared.Component == snapshot.Key.Component &&
		prepared.ConfigSHA == snapshot.Record.ConfigSHA && prepared.Forward.ConfigSHA == snapshot.Record.ConfigSHA &&
		snapshot.Desired.RecordDigest == snapshot.Record.RecordDigest &&
		snapshot.CurrentRecordDigest == snapshot.Record.LKGRecordDigest &&
		snapshot.LastSuccessfulLKG == snapshot.Record.LKGRecordDigest
}

func pendingUnprovenLKGRecovery(snapshot Snapshot) bool {
	previous := snapshot.PreviousStatus
	if previous == nil || previous.State != StateRecoveryRequired || previous.Key() != snapshot.Key ||
		(previous.RolloutReceiptDigest == "" && !strings.HasPrefix(previous.Reason, "rollout result is unknown:")) ||
		snapshot.CurrentRecordDigest == snapshot.Desired.RecordDigest ||
		snapshot.Desired.RecordDigest != snapshot.Record.RecordDigest || snapshot.CurrentRecordDigest != snapshot.Record.LKGRecordDigest ||
		previous.CurrentRecordDigest != snapshot.CurrentRecordDigest || previous.TargetRecordDigest != snapshot.Desired.RecordDigest ||
		previous.LastSuccessfulLKG != snapshot.CurrentRecordDigest {
		return false
	}
	return unprovenLKGReason(previous.Reason)
}

// recoveredPredecessorRetryEligible permits one retry of the exact immutable
// superseder after its independent serving LKG has recovered. The retry is
// deliberately limited to inactive-worker identity drift: arbitrary local
// degradation, unhealthy dependencies, and degraded public routes remain
// fenced. Persisting the retry count prevents a failed retry from becoming a
// reconcile loop while a new desired record receives an independent budget.
func recoveredPredecessorRetryEligible(snapshot Snapshot) bool {
	previous := snapshot.PreviousStatus
	if previous == nil || previous.RecoveryRetryCount != 0 || !degradedPredecessorRolloutEligible(snapshot) ||
		snapshot.Health.Dependency.State != HealthHealthy || snapshot.Health.Route.State != HealthHealthy ||
		snapshot.Health.Local.State != HealthDegraded {
		return false
	}
	reason := strings.ToLower(strings.TrimSpace(snapshot.Health.Local.Reason))
	return strings.HasPrefix(reason, "health daemonset/") && strings.Contains(reason, " release identity differs from the stable record")
}

func pendingTargetCanaryVerification(snapshot Snapshot) bool {
	previous := snapshot.PreviousStatus
	if previous == nil || previous.State != StateVerifying || previous.Key() != snapshot.Key ||
		previous.RolloutReceiptDigest == "" || snapshot.CurrentRecordDigest != snapshot.Desired.RecordDigest ||
		previous.CurrentRecordDigest != snapshot.CurrentRecordDigest || previous.TargetRecordDigest != snapshot.Desired.RecordDigest ||
		snapshot.Health.Local.State != HealthHealthy || snapshot.Health.Dependency.State != HealthHealthy ||
		snapshot.Health.Route.State != HealthUnknown {
		return false
	}
	return true
}

func allLayersHealthy(health HealthSnapshot) bool {
	return health.Local.State == HealthHealthy && health.Dependency.State == HealthHealthy && health.Route.State == HealthHealthy
}

// A failed candidate can leave its replacement DaemonSet unavailable while
// the independent LKG route is already healthy. In that narrow state, the
// ordinary restore-monitor transaction is the authority for bringing the
// workload back to the signed LKG; arbitrary local degradation remains
// fenced and cannot trigger a rollback from this recovery path.
func lkgRecoveryExecutionEligible(snapshot Snapshot) bool {
	if snapshot.Health.Local.State != HealthDegraded || snapshot.Health.Dependency.State != HealthHealthy || snapshot.Health.Route.State != HealthHealthy {
		return false
	}
	reason := strings.TrimSpace(snapshot.Health.Local.Reason)
	return strings.HasPrefix(reason, "health daemonset/") && strings.Contains(reason, " rollout is incomplete ")
}
