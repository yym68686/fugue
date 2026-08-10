package releaseguardian

import (
	"context"
	"errors"
	"fmt"
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
	Bundle                 ExecutionBundle
	CurrentMonitorData     map[string]string
	LKGMonitorRecordDigest string
	Managed                bool
}

func (snapshot Snapshot) Validate(now time.Time) error {
	if snapshot.Key.Validate() != nil || snapshot.Record.Validate() != nil || snapshot.Desired.Validate() != nil ||
		snapshot.Record.Key() != snapshot.Key || snapshot.Desired.Key() != snapshot.Key ||
		snapshot.Desired.RecordDigest != snapshot.Record.RecordDigest || snapshot.Health.Validate(now) != nil ||
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
	status := ReleaseStatus{
		Component: key.Component, Group: key.Group, State: decision.State,
		CurrentRecordDigest: snapshot.CurrentRecordDigest, TargetRecordDigest: snapshot.Desired.RecordDigest,
		LastSuccessfulLKG: snapshot.LastSuccessfulLKG, Health: snapshot.Health,
		Reason: decision.Reason, ObservedAt: now.Format(time.RFC3339Nano),
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
	if decision.RolloutEligible {
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
			default:
				status.State = StateRecoveryRequired
				status.Reason = "rollout returned an invalid terminal status"
			}
		}
	}
	if decision.RollbackEligible {
		status.State = StateRollingBack
		receipt, executeErr := controller.executor.Rollback(ctx, snapshot)
		if executeErr != nil {
			status.State = StateRecoveryRequired
			status.Reason = "rollback result is unknown: " + executeErr.Error()
		} else {
			status.RollbackReceiptDigest = receipt.ReceiptDigest
			if receipt.Status != "compensated" {
				status.State = StateRecoveryRequired
				status.Reason = receipt.Reason
			} else if err := controller.store.SetDesiredToLKG(ctx, snapshot); err != nil {
				status.State = StateRecoveryRequired
				status.Reason = "LKG is restored but DesiredRelease rollback CAS failed: " + err.Error()
			} else {
				status.CurrentRecordDigest = receipt.RecordDigest
				status.TargetRecordDigest = receipt.RecordDigest
				status.State = StateLKGStable
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
